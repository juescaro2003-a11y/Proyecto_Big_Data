"""Publicación idempotente mediante transacciones SQLAlchemy y COPY psycopg."""
from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, MetaData, Table, Text, UniqueConstraint, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.schema import CreateTable, CreateColumn
from sqlalchemy.dialects import postgresql

ROOT = Path(__file__).resolve().parents[1]
TABLE = 'analytics.alertas_contratacion_directa'
KEYS = ('id_contrato','fecha_corte','version_modelo')


def rds_engine():
    load_dotenv(ROOT/'.env')
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
    url=URL.create('postgresql+psycopg',username=os.getenv('SECOP_RDS_USER','Secop'),password=password,
        host=os.getenv('SECOP_RDS_HOST','secop.ccw10v99js6c.us-east-1.rds.amazonaws.com'),
        port=int(os.getenv('SECOP_RDS_PORT','5432')),database=os.getenv('SECOP_RDS_DATABASE','postgres'))
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


def quote(name):
    return '"'+name.replace('"','""')+'"'


def result_table(parquet):
    columns=[]
    for f in pq.ParquetFile(parquet).schema_arrow:
        t=f.type
        typ=(BigInteger if pa.types.is_integer(t) else Float if pa.types.is_floating(t) else
             Boolean if pa.types.is_boolean(t) else Date if pa.types.is_date(t) else DateTime if pa.types.is_timestamp(t) else Text)
        columns.append(Column(f.name,typ,nullable=f.name not in KEYS))
    return Table('alertas_contratacion_directa',MetaData(),*columns,
        UniqueConstraint(*KEYS,name='uq_alertas_contrato_corte_version'),schema='analytics')


def ddl_for(parquet):
    return 'CREATE SCHEMA IF NOT EXISTS analytics;\n'+str(CreateTable(result_table(parquet),if_not_exists=True).compile(dialect=postgresql.dialect()))+';\n'


def nullable_type(t):
    if pa.types.is_signed_integer(t): return pd.Int64Dtype()
    if pa.types.is_unsigned_integer(t): return pd.UInt64Dtype()
    if pa.types.is_boolean(t): return pd.BooleanDtype()
    return None


def load_results_to_aws(parquet,cfg,model_hash,engine=None,progress=print):
    parquet=Path(parquet)
    expected=pq.ParquetFile(parquet).metadata.num_rows
    if not expected: raise ValueError('Universo vacío: no se publicará una carga vacía en AWS')
    engine_owned=engine is None
    engine=engine or rds_engine()
    try:
        check_connection(engine)
        tbl=result_table(parquet)
        names=[x.name for x in tbl.columns]
        cols=', '.join(map(quote,names))
        keys=', '.join(map(quote,KEYS))
        params={'cut':cfg.cutoff,'version':cfg.version,'hash':model_hash}
        with engine.begin() as c:
            c.execute(text('SET LOCAL lock_timeout = \'30s\''))
            c.execute(text('SELECT pg_advisory_xact_lock(73192027)'))
            c.execute(text('CREATE SCHEMA IF NOT EXISTS analytics'))
            c.execute(text('CREATE TABLE IF NOT EXISTS analytics.versiones_alertas (version_modelo TEXT PRIMARY KEY, huella_definicion TEXT NOT NULL)'))
            existing=c.execute(text('SELECT huella_definicion FROM analytics.versiones_alertas WHERE version_modelo=:version'),params).scalar_one_or_none()
            if existing is not None and existing!=model_hash:
                raise ValueError('La versión ya existe con otra lógica/parámetros. Incremente version en la configuración.')
            c.execute(text('INSERT INTO analytics.versiones_alertas VALUES (:version,:hash) ON CONFLICT DO NOTHING'),params)
            tbl.create(c,checkfirst=True)
            # Evolución aditiva: conservar columnas/versiones históricas y añadir D3 nuevo.
            known=set(c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='analytics' AND table_name='alertas_contratacion_directa'")).scalars())
            for column in tbl.columns:
                if column.name not in known:
                    definition=str(CreateColumn(column).compile(dialect=postgresql.dialect()))
                    c.execute(text(f'ALTER TABLE {TABLE} ADD COLUMN {definition}'))
            c.execute(text(f'CREATE TEMP TABLE alertas_stage (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP'))
            driver=c.connection.driver_connection  # misma transacción del engine SQLAlchemy
            done=0
            for batch in pq.ParquetFile(parquet).iter_batches(batch_size=10000):
                frame=batch.to_pandas(types_mapper=nullable_type)
                # Un marcador aleatorio evita confundir texto literal con NULL.
                import uuid
                null_marker='__NULL_'+uuid.uuid4().hex+'__'
                buf=io.StringIO()
                frame.to_csv(buf,index=False,header=False,na_rep=null_marker,date_format='%Y-%m-%d %H:%M:%S.%f')
                buf.seek(0)
                with driver.cursor().copy(f"COPY alertas_stage ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '{null_marker}')") as copy:
                    for chunk in iter(lambda:buf.read(1024*1024),''): copy.write(chunk)
                done+=len(frame)
                progress(f'AWS staging: {done:,}/{expected:,} contratos')
            count=c.execute(text('SELECT count(*) FROM alertas_stage')).scalar_one()
            if count!=expected: raise ValueError('Conteo staging no coincide con Parquet')
            duplicates=c.execute(text(f'SELECT count(*) FROM (SELECT {keys} FROM alertas_stage GROUP BY {keys} HAVING count(*)>1) d')).scalar_one()
            if duplicates: raise ValueError('Clave compuesta duplicada antes de publicar')
            preexisting=c.execute(text(f'SELECT count(*) FROM {TABLE} t JOIN alertas_stage s USING ({keys})')).scalar_one()
            updates=', '.join(f'{quote(x)}=EXCLUDED.{quote(x)}' for x in names if x not in KEYS)
            c.execute(text(f'INSERT INTO {TABLE} ({cols}) SELECT {cols} FROM alertas_stage WHERE true ON CONFLICT ({keys}) DO UPDATE SET {updates}'))
            # Verificación exacta: datos y NULL deben coincidir con cada fila preparada.
            different=' OR '.join(f't.{quote(x)} IS DISTINCT FROM s.{quote(x)}' for x in names)
            mismatch=c.execute(text(f'SELECT count(*) FROM alertas_stage s LEFT JOIN {TABLE} t USING ({keys}) WHERE {different}')).scalar_one()
            actual=c.execute(text(f'SELECT count(*) FROM {TABLE} WHERE fecha_corte=cast(:cut AS date) AND version_modelo=:version'),params).scalar_one()
            if mismatch or actual!=expected:
                raise ValueError('No coincide contenido/conteo del corte. No se borra historia para forzar coincidencia; se revierte la carga.')
            # Una segunda petición sobre la misma clave actualiza; no añade registros.
        return {'insertados':expected-preexisting,'actualizados':preexisting,'almacenados':expected,'transaccion':'COMMIT'}
    finally:
        if engine_owned: engine.dispose()


def validate_aws_results(cfg,expected,engine=None):
    owned=engine is None
    engine=engine or rds_engine()
    params={'cut':cfg.cutoff,'version':cfg.version}
    where='fecha_corte=cast(:cut AS date) AND version_modelo=:version'
    try:
        with engine.connect() as c:
            c.execute(text('SET TRANSACTION READ ONLY'))
            n=c.execute(text(f'SELECT count(*) FROM {TABLE} WHERE {where}'),params).scalar_one()
            if n!=expected: raise ValueError(f'Conteo AWS {n} distinto de esperado {expected}')
            duplicated=c.execute(text(f'SELECT count(*) FROM (SELECT id_contrato FROM {TABLE} WHERE {where} GROUP BY id_contrato HAVING count(*)>1) d'),params).scalar_one()
            if duplicated: raise ValueError('AWS contiene duplicados de contrato/corte/versión')
            invalid=c.execute(text(f"""SELECT count(*) FROM {TABLE} WHERE {where} AND
                (d1_concentracion NOT IN (0,1) OR d2_valor_atipico NOT IN (0,1) OR alerta_d3 NOT IN (0,1)
                OR score_total<>coalesce(d1_concentracion,0)+coalesce(d2_valor_atipico,0)+coalesce(alerta_d3,0)
                OR clasificacion IS DISTINCT FROM CASE WHEN score_min>={cfg.score_threshold} THEN 'ALERTA'
                   WHEN score_max<{cfg.score_threshold} THEN 'NO_ALERTA' ELSE 'NO_EVALUABLE' END
                OR alerta_binaria IS DISTINCT FROM CASE WHEN score_min>={cfg.score_threshold} THEN 1
                   WHEN score_max<{cfg.score_threshold} THEN 0 END)"""),params).scalar_one()
            if invalid: raise ValueError(f'AWS tiene {invalid} filas inválidas')
            first=pd.read_sql_query(text(f'''SELECT id_contrato,codigo_entidad,codigo_proveedor,valor_contrato,
                participacion_proveedor,d1_concentracion,zscore_robusto,d2_valor_atipico,participacion_diciembre,
                umbral_d3,alerta_d3,score_total,nivel_alerta,alerta_binaria,clasificacion
                FROM {TABLE} WHERE {where} ORDER BY id_contrato LIMIT 10'''),c,params=params)
            distribution=pd.read_sql_query(text(f'SELECT clasificacion,count(*) AS total_contratos,avg(score_total) AS score_promedio FROM {TABLE} WHERE {where} GROUP BY 1 ORDER BY 2 DESC'),c,params=params)
            dimensions=pd.read_sql_query(text(f'''SELECT count(*) FILTER(WHERE d1_concentracion=1) AS alertas_concentracion,
                count(*) FILTER(WHERE d2_valor_atipico=1) AS alertas_valor_atipico,
                count(*) FILTER(WHERE alerta_d3=1) AS alertas_acumulacion_diciembre FROM {TABLE} WHERE {where}'''),c,params=params)
        return {'almacenados':n,'primeros_10':first,'clasificaciones':distribution,'dimensiones':dimensions}
    finally:
        if owned: engine.dispose()
