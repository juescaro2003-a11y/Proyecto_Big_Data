"""Publicación incremental de productos finales. Nunca ejecuta el ETL/modelo."""
from __future__ import annotations
from contextlib import contextmanager, ExitStack
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import math
import os
import tempfile
import time
import uuid

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import psutil

VERSION = 'publicacion-3.1'
GOLD_COLUMNS = ['id_contrato','fecha_de_firma','nombre_entidad','departamento','tipo_de_contrato',
                'proveedor_adjudicado','valor_del_contrato','objeto_del_contrato','urlproceso','fecha_corte']
PROCESS_COLUMNS = ['id_del_proceso','id_del_portafolio','fecha_de_publicacion_del','entidad',
                   'departamento_entidad','tipo_de_contrato','estado_del_procedimiento',
                   'precio_base','proveedores_invitados','respuestas_al_procedimiento']
ALERT_COLUMNS = ['id_contrato','firma','valor_contrato','clasificacion','d1_concentracion',
                 'd2_valor_atipico','alerta_d3','score_min','score_max','dimensiones_evaluadas',
                 'motivo_d1','motivo_d2','motivo_d3','participacion_proveedor','umbral_d1',
                 'zscore_robusto','umbral_d2','participacion_diciembre','umbral_d3','motivos_alerta',
                 'fecha_corte','version_modelo','huella_gold','huella_parametros','parametros_json',
                 'prefect_flow_run_id']


def lit(value):
    return "'"+str(value).replace("'", "''")+"'"


def ident(name):
    parts = name.split('.')
    if len(parts) != 2 or any(not p.replace('_','').isalnum() for p in parts):
        raise ValueError('Se requiere esquema.tabla con identificadores simples')
    return sql.Identifier(*parts)


class Monitor:
    def __init__(self):
        self.events = []
        self.started = time.perf_counter()

    def log(self, etapa, estado, **data):
        event = dict(etapa=etapa,estado=estado,transcurrido_s=round(time.perf_counter()-self.started,3),
                     rss_mb=round(psutil.Process().memory_info().rss/1024**2,1),**data)
        self.events.append(event)
        print(json.dumps(event,ensure_ascii=False,default=str),flush=True)

    @contextmanager
    def stage(self, name):
        start=time.perf_counter();self.log(name,'INICIO')
        try:
            yield
        except BaseException as exc:
            self.log(name,'ERROR',error_tipo=type(exc).__name__,segundos=time.perf_counter()-start)
            raise
        else:
            self.log(name,'COMPLETADA',segundos=time.perf_counter()-start)


@contextmanager
def source_locks(root):
    """Mismo protocolo de exclusión que los flujos. Nunca retira un lock ajeno."""
    acquired=[]
    try:
        for folder in (root, root/'salida/alertas'):
            folder.mkdir(parents=True,exist_ok=True)
            path=folder/'.pipeline.lock'
            try:
                with path.open('x',encoding='utf-8') as stream:
                    stream.write(str(os.getpid()))
            except FileExistsError:
                raise RuntimeError(f'PUBLICACION_LOCK: existe {path}; revise el monitor, no se elimina.') from None
            acquired.append(path)
        yield
    finally:
        for path in reversed(acquired):
            path.unlink()


def sha_file(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''):
            h.update(block)
    return h.hexdigest()


def product(paths, required):
    if not paths:
        raise FileNotFoundError('No hay archivos del producto Gold')
    evidence=[]
    for path in paths:
        before=path.stat()
        with pq.ParquetFile(path) as parquet:
            missing=set(required)-set(parquet.schema_arrow.names)
            if missing:raise ValueError(f'COLUMNAS: {path.name}: {sorted(missing)}')
            rows=parquet.metadata.num_rows
        digest=sha_file(path)
        after=path.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
            raise RuntimeError('FUENTE_CAMBIO: durante la lectura')
        evidence.append(dict(path=str(path.resolve()),sha256=digest,rows=rows,bytes=after.st_size,mtime=after.st_mtime_ns))
    identity=hashlib.sha256(json.dumps([(x['sha256'],x['rows']) for x in evidence]).encode()).hexdigest()
    return dict(hash=identity,files=evidence,rows=sum(x['rows'] for x in evidence))


def discover(root, result, config, monitor):
    report_path=result.parent/'ejecucion.json'
    report=json.loads(report_path.read_text(encoding='utf-8-sig'))
    if report.get('estado')!='COMPLETADA' or report.get('etapas',{}).get('classify_contracts',{}).get('estado')!='COMPLETADA':
        raise ValueError('LOTE_INCOMPLETO: se requiere ejecución y clasificación COMPLETADA')
    if report['parametros']['version'] not in config['model_versions']:
        raise ValueError('VERSION_MODELO: no autorizada en publicacion.json')
    with monitor.stage('huellas_y_metadatos'):
        from .seleccion_gold import gold_path,process_files,describe_processes
        gold=product([gold_path(root,config.get('contract_source','data/gold/contratos_control/contratos.parquet'))],GOLD_COLUMNS)
        alerts=product([result],ALERT_COLUMNS)
        if gold['files'][0]['sha256']!=report['huella_gold']:
            raise ValueError('GOLD_DISTINTO: el lote no fue calculado con estos bytes Gold')
        process_status='LEGADO_NO_PUBLICADO'
        processes=None
        if config.get('process_mode')=='gold_snapshot':
            processes=product(process_files(root,config),PROCESS_COLUMNS)
            summary=describe_processes(root,config)
            processes['selection']=summary
            processes['manifest_identity']=processes['hash']
            processes['cutoff']=summary['corte_declarado']
            processes['coverage_start']=None
            process_status='GOLD_EXISTENTE_SELECCIONADO'
        elif config.get('process_mode')=='legacy':
            processes=product(sorted((root/'data/gold/procesos_modelo').glob('*.parquet')),PROCESS_COLUMNS)
            process_status='LEGADO_HISTORICO_NO_VIGENTE'
        elif config.get('process_mode') not in ('disabled',None):
            raise ValueError('process_mode debe ser gold_snapshot, legacy o disabled.')
    return dict(gold=gold,alerts=alerts,processes=processes,process_status=process_status,report=report,
                report_hash=sha_file(report_path),version=VERSION)


def parquet_view(db,name,prod):
    paths=[p['path'].replace('\\','/') for p in prod['files']]
    db.execute(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({paths!r},union_by_name=true)')


def validate(db, evidence, monitor):
    """Una vez por nueva combinación de contenido; se valida, no se recalcula el modelo."""
    with monitor.stage('validacion_contenido'):
        for name,key in [('g','gold'),('a','alerts')]:
            parquet_view(db,name,evidence[key])
            bad=db.execute(f"SELECT count(*)-count(DISTINCT id_contrato),count(*) FILTER(WHERE trim(id_contrato)='') FROM {name}").fetchone()
            if any(bad):raise ValueError(f'IDS_INVALIDOS: {key}')
        report=evidence['report'];params=report['parametros']
        bad=db.execute('''SELECT count(*) FROM a WHERE version_modelo IS DISTINCT FROM ?
            OR cast(fecha_corte AS VARCHAR) IS DISTINCT FROM ? OR huella_gold IS DISTINCT FROM ?''',
            [params['version'],params['cutoff'],report['huella_gold']]).fetchone()[0]
        if bad:raise ValueError('TRAZABILIDAD_ALERTAS: versión, corte o huella inconsistentes')
        if db.execute('SELECT count(*) FROM g WHERE fecha_corte IS NULL OR cast(fecha_corte AS DATE) < cast(? AS DATE)',[params['cutoff']]).fetchone()[0]:
            raise ValueError('CORTE_GOLD: cobertura insuficiente')
        # Huella de bytes vincula el universo completo. Unión únicamente por ID único.
        bad=db.execute('''SELECT count(*) FROM a LEFT JOIN g USING(id_contrato)
            WHERE g.id_contrato IS NULL OR a.valor_contrato IS DISTINCT FROM g.valor_del_contrato
            OR a.firma IS DISTINCT FROM cast(g.fecha_de_firma AS DATE)''').fetchone()[0]
        if bad:raise ValueError('CORRESPONDENCIA: resultado ajeno al Gold o valores/fechas distintos')
        threshold=int(params['score_threshold'])
        bad=db.execute(f'''SELECT count(*) FROM a WHERE
            clasificacion IS NULL OR clasificacion NOT IN ('ALERTA','NO_ALERTA','NO_EVALUABLE')
            OR d1_concentracion NOT IN (0,1) OR d2_valor_atipico NOT IN (0,1) OR alerta_d3 NOT IN (0,1)
            OR score_min IS NULL OR score_max IS NULL OR dimensiones_evaluadas IS NULL
            OR dimensiones_evaluadas<>(d1_concentracion IS NOT NULL)::INT+(d2_valor_atipico IS NOT NULL)::INT+(alerta_d3 IS NOT NULL)::INT
            OR score_min<>coalesce(d1_concentracion,0)+coalesce(d2_valor_atipico,0)+coalesce(alerta_d3,0)
            OR score_max<>score_min+3-dimensiones_evaluadas
            OR clasificacion<>CASE WHEN score_min>={threshold} THEN 'ALERTA'
                WHEN score_max<{threshold} THEN 'NO_ALERTA' ELSE 'NO_EVALUABLE' END''').fetchone()[0]
        if bad:raise ValueError('SEMANTICA: señales, puntajes o clasificación inconsistentes; no se corrigen')
        classes=dict(db.execute('SELECT clasificacion,count(*) FROM a GROUP BY 1').fetchall())
        expected=report['resumen']
        if classes!=expected['clasificaciones'] or evidence['alerts']['rows']!=expected['contratos_resultado']:
            raise ValueError('CONTEOS: resultado no coincide con el resumen del modelo')
        for label,col in [('D1','d1_concentracion'),('D2','d2_valor_atipico'),('D3','alerta_d3')]:
            on,evaluated=db.execute(f'SELECT count(*) FILTER(WHERE {col}=1),count({col}) FROM a').fetchone()
            if (on,evaluated)!=(expected['dimensiones'][label]['alertas'],expected['dimensiones'][label]['evaluados']):
                raise ValueError('DIMENSION: conteos distintos del informe')
        if evidence['processes']:parquet_view(db,'p',evidence['processes'])
        return dict(contratos=evidence['gold']['rows'],resultados=evidence['alerts']['rows'],
                    sin_resultado=evidence['gold']['rows']-evidence['alerts']['rows'],clasificaciones=classes)


def pgtype(t):
    if pa.types.is_string(t) or pa.types.is_large_string(t):return 'text'
    if pa.types.is_integer(t):return 'bigint'
    if pa.types.is_floating(t):return 'double precision'
    if pa.types.is_boolean(t):return 'boolean'
    if pa.types.is_timestamp(t):return 'timestamp' if t.tz is None else 'timestamptz'
    if pa.types.is_date(t):return 'date'
    if pa.types.is_decimal(t):return 'numeric'
    raise TypeError(f'Tipo sin adaptación explícita: {t}')


def init_registry(conn):
    conn.execute('CREATE SCHEMA IF NOT EXISTS secop_panel')
    conn.execute('''CREATE TABLE IF NOT EXISTS secop_panel.publication_products (
        product text PRIMARY KEY, fingerprint text NOT NULL, relation text NOT NULL,
        oid bigint NOT NULL, dirty boolean NOT NULL DEFAULT false, rows bigint NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS secop_panel.publication_current (
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), identity text, evidence jsonb, metrics jsonb)''')
    conn.execute('''CREATE OR REPLACE FUNCTION secop_panel.mark_dirty() RETURNS trigger
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN
        UPDATE secop_panel.publication_products SET dirty=true
        WHERE relation=TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME; RETURN NULL; END $$''')


def existing_products(conn):
    if conn.execute("SELECT to_regclass('secop_panel.publication_products')").fetchone()[0] is None:return {}
    return {p:dict(hash=h,relation=r,valid=(not dirty and oid==current),rows=n)
            for p,h,r,oid,dirty,n,current in conn.execute('''SELECT *,to_regclass(relation)::oid::bigint
                FROM secop_panel.publication_products''')}


def canonical(row):
    def item(v):
        if isinstance(v,(date,datetime)):return v.isoformat()
        if isinstance(v,Decimal):return float(v)
        if isinstance(v,float) and not math.isfinite(v):return str(v)
        return v
    return json.dumps([item(v) for v in row],ensure_ascii=False,separators=(',',':'))


def matches_existing(conn, db, relation, view, columns, monitor):
    """Adopción inicial exacta y acotada en memoria; no copia filas a PostgreSQL."""
    if not relation or conn.execute('SELECT to_regclass(%s)',(relation,)).fetchone()[0] is None:return False
    known={r[0] for r in conn.execute('SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped',(relation,))}
    if not set(columns)<=known:return False
    # Ambas fuentes tienen ID contractual único. Nunca se adoptan procesos de legado por inferencia.
    with monitor.stage('verificar_tabla_existente_'+view):
        conn.execute(sql.SQL('LOCK TABLE {} IN SHARE MODE').format(ident(relation)))
        cols=sql.SQL(',').join(map(sql.Identifier,columns))
        local=db.execute('SELECT '+','.join('"'+c+'"' for c in columns)+f' FROM {view} ORDER BY id_contrato')
        with conn.cursor(name='verify_'+uuid.uuid4().hex) as cursor:
            cursor.execute(sql.SQL('SELECT {} FROM {} ORDER BY id_contrato').format(cols,ident(relation)))
            while True:
                left=local.fetchmany(4096);right=cursor.fetchmany(4096)
                if len(left)!=len(right):return False
                if not left:return True
                if any(canonical(a)!=canonical(b) for a,b in zip(left,right)):return False


def transfer(conn,prod,key,columns,monitor,batch_size=4096):
    relation='secop_panel.src_'+key+'_'+uuid.uuid4().hex[:16]
    schema=pq.read_schema(prod['files'][0]['path'])
    fields=sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(c),sql.SQL(pgtype(schema.field(c).type))) for c in columns)
    conn.execute(sql.SQL('CREATE TABLE {} ({})').format(ident(relation),fields))
    n=0;last=time.monotonic()
    with monitor.stage('transferencia_'+key):
        with conn.cursor().copy(sql.SQL('COPY {} ({}) FROM STDIN').format(ident(relation),sql.SQL(',').join(map(sql.Identifier,columns)))) as copy:
            for f in prod['files']:
                with pq.ParquetFile(f['path']) as pf:
                    for batch in pf.iter_batches(batch_size=batch_size,columns=columns,use_threads=False):
                        arrays=[col.to_pylist() for col in batch.columns]
                        for row in zip(*arrays):copy.write_row(row)
                        n+=batch.num_rows
                        if time.monotonic()-last>=5:
                            monitor.log(key,'COPY',filas=n,total=prod['rows']);last=time.monotonic()
        if n!=prod['rows']:raise ValueError('COPY_CONTEO: distinto de metadatos')
        if key in ('gold','alerts'):
            conn.execute(sql.SQL('CREATE UNIQUE INDEX ON {} (id_contrato)').format(ident(relation)))
        else:
            conn.execute(sql.SQL('ALTER TABLE {} ADD COLUMN dashboard_row_id bigint GENERATED ALWAYS AS IDENTITY').format(ident(relation)))
            conn.execute(sql.SQL('CREATE UNIQUE INDEX ON {} (dashboard_row_id)').format(ident(relation)))
        conn.execute(sql.SQL('ANALYZE {}').format(ident(relation)))
        monitor.log(key,'PUBLICADO',filas=n)
    return relation


def register(conn,key,prod,relation):
    oid=conn.execute('SELECT %s::regclass::oid::bigint',(relation,)).fetchone()[0]
    conn.execute('''INSERT INTO secop_panel.publication_products VALUES (%s,%s,%s,%s,false,%s)
        ON CONFLICT(product) DO UPDATE SET fingerprint=excluded.fingerprint,relation=excluded.relation,
        oid=excluded.oid,dirty=false,rows=excluded.rows''',(key,prod['hash'],relation,oid,prod['rows']))
    conn.execute(sql.SQL('CREATE OR REPLACE TRIGGER dashboard_source_dirty AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} FOR EACH STATEMENT EXECUTE FUNCTION secop_panel.mark_dirty()').format(ident(relation)))


def publish(conn,db,evidence,config,monitor,fail_hook=None):
    from .consultas import refresh_views
    identity=hashlib.sha256(json.dumps({k:evidence[k] for k in ('gold','alerts','processes','process_status','report_hash','version')},sort_keys=True).encode()).hexdigest()
    # Los mtime se conservan como evidencia, pero no se usan para decidir el contenido.
    key_identity=hashlib.sha256(json.dumps([VERSION,evidence['gold']['hash'],evidence['alerts']['hash'],
        (evidence['processes'] or {}).get('hash'),(evidence['processes'] or {}).get('cutoff'),evidence['process_status'],evidence['report_hash']]).encode()).hexdigest()
    with conn.transaction():
        conn.execute("SELECT set_config('lock_timeout',%s,true)",(str(config['lock_timeout_ms']),))
        conn.execute("SELECT set_config('statement_timeout',%s,true)",(str(config['statement_timeout_ms']),))
        conn.execute('SELECT pg_advisory_xact_lock(87346120)')
        # También serializa con load_all_to_postgres; no requiere modificar el modelo.
        conn.execute('SELECT pg_advisory_xact_lock(73192026)')
        init_registry(conn)
        old=existing_products(conn)
        needed={k:evidence[k] for k in ('gold','alerts','processes') if evidence[k] is not None}
        changed={k for k,p in needed.items() if k not in old or not old[k]['valid'] or old[k]['hash']!=p['hash']}
        # Una publicación anterior pudo conservar solo la proyección de procesos.
        if 'processes' in needed and 'processes' not in changed:
            names={row[0] for row in conn.execute('SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(%s) AND attnum>0 AND NOT attisdropped',(old['processes']['relation'],))}
            required=set(pq.read_schema(needed['processes']['files'][0]['path']).names)|{'dashboard_row_id'}
            if not required.issubset(names):changed.add('processes')
        previous=conn.execute('SELECT identity,evidence FROM secop_panel.publication_current').fetchone()
        if previous and previous[0]==key_identity and not changed:
            monitor.log('publicacion','OMITIDA_MISMO_CONTENIDO',filas_transferidas=0)
            return dict(estado='SIN_CAMBIOS',identity=key_identity,filas_transferidas=0)
        same_validation=(previous and previous[1].get('report_hash')==evidence['report_hash']
                         and not changed.intersection({'gold','alerts'}))
        if same_validation:
            monitor.log('validacion_contenido','OMITIDA_HUELLAS_Y_REGISTRO_VALIDOS')
            checks=dict(contratos=evidence['gold']['rows'],resultados=evidence['alerts']['rows'],
                        sin_resultado=evidence['gold']['rows']-evidence['alerts']['rows'],
                        clasificaciones=evidence['report']['resumen']['clasificaciones'])
        else:
            checks=validate(db,evidence,monitor)
        refs={};copied=0
        for k,p in needed.items():
            if k not in changed:
                refs[k]=old[k]['relation'];monitor.log(k,'OMITIDA_MISMO_CONTENIDO');continue
            cols=GOLD_COLUMNS if k=='gold' else pq.read_schema(p['files'][0]['path']).names
            candidate=config.get('existing_tables',{}).get(k)
            if k in ('gold','alerts') and matches_existing(conn,db,candidate,'g' if k=='gold' else 'a',cols,monitor):
                refs[k]=candidate;monitor.log(k,'REUTILIZADA_SIN_COPY',tabla=candidate)
            else:
                refs[k]=transfer(conn,p,k,cols,monitor);copied+=p['rows']
            register(conn,k,p,refs[k])
        with monitor.stage('vistas_y_agregados'):
            refresh_views(conn,refs,evidence,changed,previous is None,monitor)
        if fail_hook:fail_hook(conn)
        actual=dict(conn.execute('SELECT clasificacion,sum(n)::bigint FROM secop_panel.dash_contratos_resumen WHERE clasificacion<>%s GROUP BY 1',('SIN_RESULTADO_ALERTAS',)).fetchall())
        if actual!=checks['clasificaciones']:raise ValueError('PUBLICACION_CLASES: no coincide')
        if conn.execute('SELECT sum(n)::bigint FROM secop_panel.dash_contratos_resumen').fetchone()[0]!=checks['contratos']:
            raise ValueError('PUBLICACION_UNIVERSO: multiplicado o incompleto')
        for label,col in [('D1','d1'),('D2','d2'),('D3','d3')]:
            stats=conn.execute("SELECT sum(n) FILTER(WHERE estado='SEÑAL')::bigint,sum(n) FILTER(WHERE estado IN ('SEÑAL','SIN_SEÑAL'))::bigint FROM secop_panel.dash_dimensiones WHERE dimension=%s",(label,)).fetchone()
            exp=evidence['report']['resumen']['dimensiones'][label]
            if stats!=(exp['alertas'],exp['evaluados']):raise ValueError('PUBLICACION_DIMENSIONES')
        for prod in needed.values():
            for f in prod['files']:
                stat=Path(f['path']).stat()
                if (stat.st_size,stat.st_mtime_ns)!=(f['bytes'],f['mtime']):raise RuntimeError('FUENTE_CAMBIO_ANTES_COMMIT')
        conn.execute('''INSERT INTO secop_panel.publication_current VALUES (true,%s,%s,%s)
            ON CONFLICT(singleton) DO UPDATE SET identity=excluded.identity,evidence=excluded.evidence,metrics=excluded.metrics''',
            (key_identity,Jsonb(evidence),Jsonb(monitor.events)))
        role=config.get('reader_role','grafana_lectura')
        if conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,)).fetchone():
            conn.execute(sql.SQL('GRANT USAGE ON SCHEMA secop_panel TO {}').format(sql.Identifier(role)))
            conn.execute(sql.SQL('GRANT SELECT ON secop_panel.publication_products TO {}').format(sql.Identifier(role)))
            for key,relation in refs.items():
                conn.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(relation.split('.')[0]),sql.Identifier(role)))
                columns=GOLD_COLUMNS if key=='gold' else pq.read_schema(needed[key]['files'][0]['path']).names+(['dashboard_row_id'] if key=='processes' else [])
                conn.execute(sql.SQL('GRANT SELECT ({}) ON {} TO {}').format(sql.SQL(',').join(map(sql.Identifier,columns)),ident(relation),sql.Identifier(role)))
            for (view,) in conn.execute("SELECT viewname FROM pg_views WHERE schemaname='secop_panel' AND viewname LIKE 'dash_%'").fetchall():
                conn.execute(sql.SQL('GRANT SELECT ON {} TO {}').format(sql.Identifier('secop_panel',view),sql.Identifier(role)))
        else:monitor.log('permisos','LECTOR_PENDIENTE',rol=role)
    return dict(estado='PUBLICADA',identity=key_identity,filas_transferidas=copied,**checks)


def run(root,result,config,connection_factory,monitor=None):
    monitor=monitor or Monitor()
    try:
        with source_locks(root),tempfile.TemporaryDirectory(prefix='secop_publicacion_') as tmp:
            evidence=discover(root,result,config,monitor)
            with duckdb.connect() as db:
                db.execute('SET memory_limit='+lit(config.get('memory','1GB')))
                db.execute('SET threads=1');db.execute('SET temp_directory='+lit(tmp))
                with monitor.stage('conexion_postgres'):
                    conn=connection_factory()
                with conn:
                    answer=publish(conn,db,evidence,config,monitor)
            # Gold final lógico: referencia a bytes verificados; no duplica Parquet ni cambia IRCD.
            folder=root/'data/gold/dashboard_publicaciones';folder.mkdir(parents=True,exist_ok=True)
            manifest=folder/(answer['identity']+'.json')
            if not manifest.exists():
                stage=manifest.with_suffix('.tmp')
                stage.write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8');stage.replace(manifest)
            monitor.log('total',answer['estado'],filas_transferidas=answer['filas_transferidas'])
            return answer
    finally:
        logs=root/'salida/grafana';logs.mkdir(parents=True,exist_ok=True)
        (logs/(time.strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:8]+'.json')).write_text(
            json.dumps(monitor.events,ensure_ascii=False,indent=2),encoding='utf-8')
