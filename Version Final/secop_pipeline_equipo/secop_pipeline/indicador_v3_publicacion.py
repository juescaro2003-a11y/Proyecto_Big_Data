"""Validación común y publicación AWS compacta v3, independiente de la tabla v2."""
from pathlib import Path
import io,json,tempfile
import duckdb
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text
from .indicador_v3 import SIGNALS,COMPACT,Config,sha,lit,ROOT


def inspect_result(path):
    path=Path(path).resolve();report=json.loads(path.with_name('ejecucion.json').read_text(encoding='utf-8'))
    if report.get('estado')!='COMPLETADA' or report.get('version')!='alertas-cd-3.0':raise ValueError('Se requiere lote v3 COMPLETADO.')
    cfg=Config(**report['parametros'])
    if any(p.exists() for p in (ROOT/'.pipeline.lock',ROOT/'salida/alertas/.pipeline.lock')):raise RuntimeError('Hay una ejecución activa o un lock pendiente; no se elimina.')
    if sha(path)!=report.get('huella_resultados'):raise ValueError('El resultado cambió después de completar el lote.')
    with tempfile.TemporaryDirectory(prefix='secop_v3_validate_') as tmp:
        with duckdb.connect(config={'memory_limit':'1GB','threads':1,'temp_directory':tmp}) as c:
            c.execute(f'CREATE VIEW r AS SELECT * FROM read_parquet({lit(path)})')
            n,ids=c.execute('SELECT count(*),count(DISTINCT id_contrato) FROM r').fetchone()
            if n!=ids or n!=report['resumen']['contratos']:raise ValueError('Universo o IDs inconsistentes.')
            classes=dict(c.execute('SELECT clasificacion,count(*) FROM r GROUP BY 1').fetchall())
            if classes!=report['resumen']['clasificaciones']:raise ValueError('Clasificaciones inconsistentes.')
            bad=' OR '.join(f'{s} NOT IN (0,1)' for s in SIGNALS)
            score='((c1+c2+c3)/3.0+(v1+v2)/2.0+(p1+p2)/2.0)/3.0'
            invalid=c.execute(f'''SELECT count(*) FROM r WHERE {bad} OR
                version_modelo IS DISTINCT FROM {lit(cfg.version)} OR fecha_corte IS DISTINCT FROM DATE {lit(cfg.cutoff)} OR
                (puntaje IS NULL) IS DISTINCT FROM (({score}) IS NULL) OR abs(puntaje-({score}))>1e-10 OR
                clasificacion IS DISTINCT FROM CASE WHEN puntaje IS NULL THEN 'NO_EVALUABLE'
                    WHEN rango_revision<={cfg.review_top_k_per_quarter} THEN 'ALERTA' ELSE 'NO_ALERTA' END OR
                alerta_binaria IS DISTINCT FROM CASE WHEN clasificacion='ALERTA' THEN 1 WHEN clasificacion='NO_ALERTA' THEN 0 ELSE NULL END''').fetchone()[0]
            if invalid:raise ValueError('Puntaje, señales o clasificación v3 inconsistentes.')
    return path,report,cfg


def ddl():
    flags=','.join(f'{s} smallint CHECK({s} IN(0,1))' for s in SIGNALS)
    return f'''CREATE TABLE IF NOT EXISTS analytics_v3.indicadores (id_contrato text PRIMARY KEY,{flags},
        puntaje double precision CHECK(puntaje BETWEEN 0 AND 1),clasificacion text NOT NULL
        CHECK(clasificacion IN('ALERTA','NO_ALERTA','NO_EVALUABLE')),alerta_binaria smallint CHECK(alerta_binaria IN(0,1)))'''


def aggregates(c):
    c.execute(text('''CREATE TABLE IF NOT EXISTS analytics_v3.clasificaciones (
        clasificacion text PRIMARY KEY,contratos bigint,porcentaje numeric)'''))
    c.execute(text('''CREATE TABLE IF NOT EXISTS analytics_v3.dimensiones (
        dimension text PRIMARY KEY,total bigint,evaluados bigint,alertas bigint,sin_senal bigint,
        no_evaluables bigint,porcentaje_alertas_evaluados numeric,porcentaje_cobertura numeric)'''))
    c.execute(text('DELETE FROM analytics_v3.clasificaciones'));c.execute(text('DELETE FROM analytics_v3.dimensiones'))
    c.execute(text('''INSERT INTO analytics_v3.clasificaciones SELECT clasificacion,count(*),
        100.0*count(*)/nullif(sum(count(*)) OVER(),0) FROM analytics_v3.indicadores GROUP BY 1'''))
    measures=','.join(f'count({s}) e_{s},count(*) FILTER(WHERE {s}=1) a_{s}' for s in SIGNALS)
    rows=','.join(f"('{s.upper()}',e_{s},a_{s})" for s in SIGNALS)
    c.execute(text(f'''INSERT INTO analytics_v3.dimensiones WITH g AS (SELECT count(*) n,{measures} FROM analytics_v3.indicadores)
        SELECT d,n,e,a,e-a,n-e,100.0*a/nullif(e,0),100.0*e/nullif(n,0) FROM g CROSS JOIN LATERAL(VALUES {rows}) x(d,e,a)'''))


def verify_connection(c,report):
    n=c.execute(text('SELECT count(*) FROM analytics_v3.indicadores')).scalar_one()
    if n!=report['resumen']['contratos']:raise ValueError('Conteo AWS diferente del manifiesto.')
    classes=dict(c.execute(text('SELECT clasificacion,contratos FROM analytics_v3.clasificaciones')).all())
    if classes!=report['resumen']['clasificaciones']:raise ValueError('Clasificaciones AWS distintas.')
    dims=c.execute(text('SELECT dimension,total,evaluados,alertas,sin_senal,no_evaluables FROM analytics_v3.dimensiones')).all()
    if len(dims)!=7:raise ValueError('Faltan dimensiones en AWS.')
    for name,total,e,a,s,ne in dims:
        if dict(total=total,evaluados=e,alertas=a,sin_senal=s,no_evaluables=ne)!=report['resumen']['dimensiones'][name]:
            raise ValueError('Agregado AWS distinto para '+name)
    return {'filas':n,'clasificaciones':classes,'dimensiones_verificadas':7}


def publish(path,progress=print):
    from .alertas_aws import rds_engine,check_connection,nullable_type
    path,report,cfg=inspect_result(path);engine=rds_engine();n=report['resumen']['contratos']
    digest=report['huella_resultados'];sent=0
    try:
        check_connection(engine)
        with engine.begin() as c:
            c.execute(text("SET LOCAL lock_timeout='30s'"));c.execute(text("SET LOCAL statement_timeout='30min'"))
            c.execute(text('SELECT pg_advisory_xact_lock(73192027)'))
            c.execute(text('CREATE SCHEMA IF NOT EXISTS analytics_v3'));c.execute(text(ddl()))
            c.execute(text('''CREATE TABLE IF NOT EXISTS analytics_v3.publicacion (
                singleton boolean PRIMARY KEY CHECK(singleton),sha256 text,manifiesto jsonb,publicado timestamptz DEFAULT now())'''))
            prior=c.execute(text('SELECT sha256 FROM analytics_v3.publicacion WHERE singleton')).scalar_one_or_none()
            actual=c.execute(text('SELECT count(*) FROM analytics_v3.indicadores')).scalar_one()
            if prior!=digest or actual!=n:
                c.execute(text('CREATE TEMP TABLE v3_stage (LIKE analytics_v3.indicadores INCLUDING CONSTRAINTS) ON COMMIT DROP'))
                with pq.ParquetFile(path) as pf:
                    done=0
                    for batch in pf.iter_batches(batch_size=50000,columns=COMPACT,use_threads=False):
                        frame=batch.to_pandas(types_mapper=nullable_type);buf=io.StringIO();frame.to_csv(buf,index=False,header=False,na_rep='')
                        sent+=len(buf.getvalue().encode('utf-8'));buf.seek(0)
                        with c.connection.driver_connection.cursor() as cursor:
                            cursor.copy_expert('COPY v3_stage ('+','.join(COMPACT)+") FROM STDIN WITH(FORMAT CSV,NULL '')",buf,size=1024**2)
                        done+=len(frame);progress(f'AWS v3: {done:,}/{n:,} ({done/n:.1%}); {sent/1024**2:.1f} MiB')
                count,unique=c.execute(text('SELECT count(*),count(DISTINCT id_contrato) FROM v3_stage')).one()
                if count!=n or unique!=n:raise ValueError('Staging AWS incompleto o duplicado.')
                if sha(path)!=digest:raise RuntimeError('Resultado cambió durante publicación.')
                c.execute(text('DELETE FROM analytics_v3.indicadores'))
                c.execute(text('INSERT INTO analytics_v3.indicadores SELECT * FROM v3_stage ORDER BY id_contrato'))
            else:progress('AWS v3: mismos resultados, COPY omitido.')
            aggregates(c)
            evidence=verify_connection(c,report)
            c.execute(text('''INSERT INTO analytics_v3.publicacion VALUES(true,:sha,cast(:report AS jsonb),now())
                ON CONFLICT(singleton) DO UPDATE SET sha256=excluded.sha256,manifiesto=excluded.manifiesto,publicado=now()'''),
                {'sha':digest,'report':json.dumps(report,ensure_ascii=False)})
        return dict(estado='COMMIT',bytes_csv=sent,**evidence)
    finally:engine.dispose()


def prove(path):
    """Conexión AWS nueva, solo lectura; prueba de la publicación v3 real."""
    from .alertas_aws import rds_engine
    path,report,cfg=inspect_result(path);engine=rds_engine()
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ') as c:
            c.execute(text('SET TRANSACTION READ ONLY'));c.execute(text("SET LOCAL statement_timeout='120s'"))
            identity=c.execute(text('''SELECT current_database() base,clock_timestamp() verificado_en,
                (SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()) ssl''')).mappings().one()
            if not identity['ssl']:raise ValueError('Sesión sin SSL.')
            columns=c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='analytics_v3' AND table_name='indicadores' ORDER BY ordinal_position")).scalars().all()
            if columns!=COMPACT:raise ValueError('Columnas distintas de la proyección compacta v3.')
            if c.execute(text('SELECT sha256 FROM analytics_v3.publicacion WHERE singleton')).scalar_one()!=report['huella_resultados']:raise ValueError('AWS contiene otro lote.')
            # Releer la tabla real: no confiar solamente en agregados almacenados.
            real=dict(c.execute(text('SELECT clasificacion,count(*) FROM analytics_v3.indicadores GROUP BY 1')).all())
            if real!=report['resumen']['clasificaciones']:raise ValueError('Contenido real distinto del manifiesto.')
            for s in SIGNALS:
                e,a=c.execute(text(f'SELECT count({s}),count(*) FILTER(WHERE {s}=1) FROM analytics_v3.indicadores')).one()
                expected=report['resumen']['dimensiones'][s.upper()]
                if (e,a)!=(expected['evaluados'],expected['alertas']):raise ValueError('Contenido real distinto para '+s)
            return dict(estado='PRUEBA APROBADA',conexion=dict(identity),columnas=columns,**verify_connection(c,report))
    finally:engine.dispose()
