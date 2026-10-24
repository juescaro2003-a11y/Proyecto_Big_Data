"""Publicación idempotente mediante transacciones SQLAlchemy y COPY psycopg2."""
from __future__ import annotations

import io
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]


def rds_engine():
    load_dotenv(ROOT/'.env')
    load_dotenv(ROOT/'.env.aws',override=True)
    password=os.getenv('SECOP_RDS_PASSWORD')
    if not password:
        raise RuntimeError('Falta SECOP_RDS_PASSWORD. Configure la contraseña localmente; no la escriba en el notebook.')
    sslmode=os.getenv('SECOP_RDS_SSLMODE','require')
    if sslmode not in ('require','verify-ca','verify-full'):
        raise ValueError('RDS requiere SSL; utilice require, verify-ca o verify-full')
    options={'sslmode':sslmode,'connect_timeout':10,'application_name':'secop_alertas'}
    cert=os.getenv('SECOP_RDS_SSLROOTCERT')
    if cert: options['sslrootcert']=cert
    if sslmode.startswith('verify') and not cert:
        raise ValueError('Configure SECOP_RDS_SSLROOTCERT para verificar el certificado de RDS')
    url=URL.create('postgresql+psycopg2',username=os.getenv('SECOP_RDS_USER','postgres'),password=password,
        host=os.getenv('SECOP_RDS_HOST','proyectobd.c10pdetlifr0.us-east-1.rds.amazonaws.com'),
        port=int(os.getenv('SECOP_RDS_PORT','5432')),database=os.getenv('SECOP_RDS_DATABASE','basedatosproyectobd'))
    return create_engine(url,connect_args=options,pool_pre_ping=True,hide_parameters=True)


def check_connection(engine=None):
    owned=engine is None
    engine=engine or rds_engine()
    try:
        with engine.connect() as c:
            if c.execute(text('SELECT 1')).scalar_one()!=1: raise RuntimeError('SELECT 1 inesperado')
            ssl=c.execute(text('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()')).scalar_one_or_none()
            if ssl is not True: raise RuntimeError('La conexión de esta sesión no está cifrada con SSL')
        return {'conexion':'verificada','ssl':True}
    finally:
        if owned: engine.dispose()


# Solo estas siete columnas salen del Parquet hacia AWS.
COLUMNS = ['id_contrato','d1_concentracion','d2_valor_atipico','alerta_d3',
           'score_total','clasificacion','alerta_binaria']
TABLE = 'analytics.alertas_resumen'
DDL = '''CREATE TABLE IF NOT EXISTS analytics.alertas_resumen (
    id_contrato text PRIMARY KEY,
    d1_concentracion smallint CHECK(d1_concentracion IN (0,1)),
    d2_valor_atipico smallint CHECK(d2_valor_atipico IN (0,1)),
    alerta_d3 smallint CHECK(alerta_d3 IN (0,1)),
    score_total smallint NOT NULL CHECK(score_total BETWEEN 0 AND 3),
    clasificacion text NOT NULL CHECK(clasificacion IN ('ALERTA','NO_ALERTA','NO_EVALUABLE')),
    alerta_binaria smallint CHECK(alerta_binaria IN (0,1)))'''


def ddl_for(parquet=None):
    return 'CREATE SCHEMA IF NOT EXISTS analytics;\n'+DDL+';\n'


def nullable_type(t):
    if pa.types.is_signed_integer(t): return pd.Int64Dtype()
    if pa.types.is_unsigned_integer(t): return pd.UInt64Dtype()
    if pa.types.is_boolean(t): return pd.BooleanDtype()
    return None


def aggregate_compact(c):
    """Agregados globales de la fotografía publicada, sin importes ni datos del contrato."""
    c.execute(text('''CREATE TABLE IF NOT EXISTS analytics.resumen_clasificaciones (
        clasificacion text PRIMARY KEY,contratos bigint,porcentaje numeric)'''))
    c.execute(text('''CREATE TABLE IF NOT EXISTS analytics.resumen_dimensiones (
        dimension text PRIMARY KEY,total bigint,evaluados bigint,alertas bigint,
        sin_senal bigint,no_evaluables bigint,porcentaje_alertas_evaluados numeric,porcentaje_cobertura numeric)'''))
    c.execute(text('DELETE FROM analytics.resumen_clasificaciones'))
    c.execute(text('DELETE FROM analytics.resumen_dimensiones'))
    c.execute(text(f'''INSERT INTO analytics.resumen_clasificaciones
        SELECT clasificacion,count(*),100.0*count(*)/nullif(sum(count(*)) OVER(),0)
        FROM {TABLE} GROUP BY clasificacion'''))
    # Una lectura de la tabla; expansión a tres filas después de sumar.
    c.execute(text(f'''INSERT INTO analytics.resumen_dimensiones
        WITH g AS (SELECT count(*) n,
        count(d1_concentracion) e1,count(*) FILTER(WHERE d1_concentracion=1) a1,
        count(d2_valor_atipico) e2,count(*) FILTER(WHERE d2_valor_atipico=1) a2,
        count(alerta_d3) e3,count(*) FILTER(WHERE alerta_d3=1) a3 FROM {TABLE})
        SELECT d,n,e,a,e-a,n-e,100.0*a/nullif(e,0),100.0*e/nullif(n,0)
        FROM g CROSS JOIN LATERAL(VALUES ('D1',e1,a1),('D2',e2,a2),('D3',e3,a3)) x(d,e,a)'''))
    return {'clasificaciones':c.execute(text('SELECT count(*) FROM analytics.resumen_clasificaciones')).scalar_one(),
            'dimensiones':3}


def load_results_to_aws(parquet,cfg,model_hash,engine=None,progress=print):
    """Publica una fotografía compacta atómica. No transfiere columnas adicionales."""
    from .alertas_publicacion import fingerprint
    parquet=Path(parquet)
    with pq.ParquetFile(parquet) as pf:
        expected=pf.metadata.num_rows
        missing=set(COLUMNS)-set(pf.schema_arrow.names)
    if not expected or missing:raise ValueError(f'Resultado vacío o faltan columnas: {sorted(missing)}')
    digest=fingerprint(parquet)
    owned=engine is None
    engine=engine or rds_engine()
    try:
        check_connection(engine)
        with engine.begin() as c:
            c.execute(text("SET LOCAL lock_timeout='30s'"))
            c.execute(text("SET LOCAL statement_timeout='30min'"))
            c.execute(text('SELECT pg_advisory_xact_lock(73192027)'))
            c.execute(text('CREATE SCHEMA IF NOT EXISTS analytics'))
            c.execute(text(DDL))
            c.execute(text('''CREATE TABLE IF NOT EXISTS analytics.publicacion_resumen (
                singleton boolean PRIMARY KEY CHECK(singleton),sha256 text NOT NULL,
                fecha_corte date NOT NULL,version_modelo text NOT NULL,huella_reglas text NOT NULL,
                filas bigint NOT NULL,publicado timestamptz NOT NULL DEFAULT now())'''))
            prior=c.execute(text('SELECT sha256,fecha_corte,version_modelo,huella_reglas,filas FROM analytics.publicacion_resumen WHERE singleton')).first()
            if prior and prior[2]==cfg.version and prior[3]!=model_hash:
                raise ValueError('Misma versión con otras reglas: incremente la versión antes de publicar.')
            if prior and prior[0]==digest and str(prior[1])==cfg.cutoff and prior[2]==cfg.version and prior[4]==expected:
                n=c.execute(text(f'SELECT count(*) FROM {TABLE}')).scalar_one()
                if n==expected:
                    aggs=aggregate_compact(c)
                    progress('AWS: misma fotografía compacta; COPY omitido.')
                    return dict(insertados=0,actualizados=0,almacenados=n,transaccion='COMMIT',reutilizado=True,agregados=aggs)
            c.execute(text(f'CREATE TEMP TABLE alertas_compacto_stage (LIKE {TABLE} INCLUDING CONSTRAINTS) ON COMMIT DROP'))
            driver=c.connection.driver_connection
            done=0;bytes_sent=0
            with pq.ParquetFile(parquet) as pf:
                for batch in pf.iter_batches(batch_size=50000,columns=COLUMNS,use_threads=False):
                    frame=batch.to_pandas(types_mapper=nullable_type)
                    # Los NULL se representan con un campo CSV vacío.
                    # Se rechazan IDs vacíos para que no se confundan con un NULL.
                    if frame.id_contrato.isna().any() or frame.id_contrato.astype('string').str.strip().eq('').any():
                        raise ValueError('ID vacío/nulo: publicación rechazada.')
                    buf=io.StringIO()
                    frame.to_csv(buf,index=False,header=False,na_rep='')
                    bytes_sent+=len(buf.getvalue().encode('utf-8'));buf.seek(0)
                    with driver.cursor() as cursor:
                        cursor.copy_expert('COPY alertas_compacto_stage ('+','.join(COLUMNS)+") FROM STDIN WITH (FORMAT CSV, NULL '')",buf,size=1024*1024)
                    done+=len(frame)
                    progress(f'AWS compacto: {done:,}/{expected:,} ({100*done/expected:.1f}%); {bytes_sent/1024**2:.1f} MiB enviados')
            n,ids=c.execute(text('SELECT count(*),count(DISTINCT id_contrato) FROM alertas_compacto_stage')).one()
            if n!=expected or ids!=n:raise ValueError('Conteos o IDs duplicados en staging compacto.')
            # Nunca convertir NO_EVALUABLE en NO_ALERTA.
            minimum='coalesce(d1_concentracion,0)+coalesce(d2_valor_atipico,0)+coalesce(alerta_d3,0)'
            maximum=f'({minimum})+(d1_concentracion IS NULL)::int+(d2_valor_atipico IS NULL)::int+(alerta_d3 IS NULL)::int'
            invalid=c.execute(text(f'''SELECT count(*) FROM alertas_compacto_stage WHERE
                score_total IS DISTINCT FROM ({minimum}) OR
                clasificacion IS DISTINCT FROM CASE WHEN ({minimum})>=:threshold THEN 'ALERTA'
                    WHEN ({maximum})<:threshold THEN 'NO_ALERTA' ELSE 'NO_EVALUABLE' END OR
                alerta_binaria IS DISTINCT FROM CASE WHEN clasificacion='ALERTA' THEN 1 WHEN clasificacion='NO_ALERTA' THEN 0 ELSE NULL END'''),{'threshold':cfg.score_threshold}).scalar_one()
            if invalid:raise ValueError(f'{invalid} resultados compactos inconsistentes.')
            if fingerprint(parquet)!=digest:raise RuntimeError('El Parquet cambió durante la carga; se revierte.')
            progress('AWS: integrando fotografía compacta y agregados...')
            # Fotografía única: solo sustituye nuestra tabla compacta; no borra tablas anteriores ajenas.
            c.execute(text(f'DELETE FROM {TABLE}'))
            # Ordenar por la clave reduce accesos aleatorios al índice durante cargas grandes.
            c.execute(text(f'INSERT INTO {TABLE} SELECT * FROM alertas_compacto_stage ORDER BY id_contrato'))
            aggs=aggregate_compact(c)
            c.execute(text('''INSERT INTO analytics.publicacion_resumen VALUES
                (true,:sha,cast(:cut AS date),:version,:rules,:n,now())
                ON CONFLICT(singleton) DO UPDATE SET sha256=excluded.sha256,fecha_corte=excluded.fecha_corte,
                version_modelo=excluded.version_modelo,huella_reglas=excluded.huella_reglas,filas=excluded.filas,publicado=now()'''),
                dict(sha=digest,cut=cfg.cutoff,version=cfg.version,rules=model_hash,n=expected))
        return dict(insertados=expected,actualizados=0,almacenados=expected,transaccion='COMMIT',agregados=aggs,bytes_csv=bytes_sent)
    finally:
        if owned:engine.dispose()


def validate_aws_results(cfg,expected,engine=None):
    owned=engine is None;engine=engine or rds_engine()
    try:
        with engine.connect() as c:
            c.execute(text('SET TRANSACTION READ ONLY'))
            meta=c.execute(text('SELECT fecha_corte,version_modelo,filas FROM analytics.publicacion_resumen WHERE singleton')).one()
            if (str(meta[0]),meta[1],meta[2])!=(cfg.cutoff,cfg.version,expected):raise ValueError('Corte/modelo/conteo distinto del solicitado.')
            n=c.execute(text(f'SELECT count(*) FROM {TABLE}')).scalar_one()
            if n!=expected:raise ValueError('Conteo compacto distinto del esperado.')
            return {'almacenados':n,
                'primeros_10':pd.read_sql_query(text(f'SELECT * FROM {TABLE} ORDER BY id_contrato LIMIT 10'),c),
                'clasificaciones':pd.read_sql_query(text('SELECT * FROM analytics.resumen_clasificaciones ORDER BY clasificacion'),c),
                'dimensiones':pd.read_sql_query(text('SELECT * FROM analytics.resumen_dimensiones ORDER BY dimension'),c)}
    finally:
        if owned:engine.dispose()


def prove_aws_publication(result_path):
    """Prueba real de lectura en una conexión nueva; no publica ni modifica datos."""
    import json
    path=Path(result_path)
    report=json.loads(path.with_name('ejecucion.json').read_text(encoding='utf-8-sig'))
    if report.get('version')=='alertas-cd-3.0':
        from .indicador_v3_publicacion import prove
        return prove(path)
    if report.get('estado')!='COMPLETADA':raise AssertionError('El lote local no está completado.')
    expected_classes=report['resumen']['clasificaciones']
    expected=sum(expected_classes.values())
    engine=rds_engine()
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ') as c:
            c.execute(text('SET TRANSACTION READ ONLY'))
            c.execute(text("SET LOCAL statement_timeout='120s'"))
            connection=c.execute(text('''SELECT current_database() base,current_user usuario,
                inet_server_addr()::text servidor,inet_server_port() puerto,clock_timestamp() verificado_en,
                (SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()) ssl''')).mappings().one()
            if not connection['ssl']:raise AssertionError('La sesión AWS no usa SSL.')
            columns=c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='analytics' AND table_name='alertas_resumen' ORDER BY ordinal_position")).scalars().all()
            if columns!=COLUMNS:raise AssertionError(f'Columnas distintas: {columns}')
            meta=c.execute(text('SELECT fecha_corte,version_modelo,filas FROM analytics.publicacion_resumen WHERE singleton')).one()
            if (str(meta[0]),meta[1],meta[2])!=(report['parametros']['cutoff'],report['parametros']['version'],expected):
                raise AssertionError('Corte, versión o filas no corresponden al lote seleccionado.')
            n,unique=c.execute(text(f'SELECT count(*),count(DISTINCT id_contrato) FROM {TABLE}')).one()
            if n!=expected or unique!=n:raise AssertionError('Conteo incorrecto o IDs duplicados/nulos.')
            classes=dict(c.execute(text(f'SELECT clasificacion,count(*) FROM {TABLE} GROUP BY clasificacion')).all())
            stored_classes=dict(c.execute(text('SELECT clasificacion,contratos FROM analytics.resumen_clasificaciones')).all())
            if classes!=expected_classes or stored_classes!=classes:
                raise AssertionError('La clasificación real, el agregado y el manifiesto no coinciden.')
            invalid=c.execute(text(f'''SELECT count(*) FROM {TABLE} WHERE alerta_binaria IS DISTINCT FROM
                CASE WHEN clasificacion='ALERTA' THEN 1 WHEN clasificacion='NO_ALERTA' THEN 0 ELSE NULL END''')).scalar_one()
            if invalid:raise AssertionError('Alerta binaria inconsistente con la clasificación.')
            dims=c.execute(text(f'''WITH g AS (SELECT count(*) n,
                count(d1_concentracion) e1,count(*) FILTER(WHERE d1_concentracion=1) a1,
                count(d2_valor_atipico) e2,count(*) FILTER(WHERE d2_valor_atipico=1) a2,
                count(alerta_d3) e3,count(*) FILTER(WHERE alerta_d3=1) a3 FROM {TABLE})
                SELECT d,n,e,a,e-a,n-e FROM g CROSS JOIN LATERAL
                (VALUES ('D1',e1,a1),('D2',e2,a2),('D3',e3,a3)) x(d,e,a) ORDER BY d''')).all()
            stored_dims=c.execute(text('SELECT dimension,total,evaluados,alertas,sin_senal,no_evaluables FROM analytics.resumen_dimensiones ORDER BY dimension')).all()
            if dims!=stored_dims:raise AssertionError('Los agregados de dimensiones no coinciden con la tabla real.')
            sample=c.execute(text(f'SELECT * FROM {TABLE} ORDER BY id_contrato LIMIT 5')).mappings().all()
            return dict(estado='PRUEBA APROBADA',conexion=dict(connection),tabla=TABLE,
                filas=n,ids_unicos=unique,columnas=columns,corte=str(meta[0]),version=meta[1],
                clasificaciones=classes,agregados_dimensiones_verificados=len(dims),
                muestra=[dict(row) for row in sample])
    finally:
        engine.dispose()
