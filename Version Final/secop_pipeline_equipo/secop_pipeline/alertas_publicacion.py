"""Publicar un lote terminado y sus agregados sin ejecutar el modelo ni el ETL."""
from pathlib import Path
from dataclasses import fields
import hashlib
import json
import tempfile
import duckdb
from sqlalchemy import text


def fingerprint(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''):digest.update(block)
    return digest.hexdigest()


def inspect_completed_result(path):
    """Validación local acotada en memoria; no altera resultados ni checkpoints."""
    from .alertas import AlertConfig
    path=Path(path).resolve()
    report=json.loads(path.with_name('ejecucion.json').read_text(encoding='utf-8-sig'))
    if report.get('estado')!='COMPLETADA':raise ValueError('Seleccione un lote COMPLETADO.')
    root=Path(__file__).resolve().parents[1]
    if any(p.exists() for p in (root/'.pipeline.lock',root/'salida/alertas/.pipeline.lock')):
        raise RuntimeError('Hay un lock de ejecución. No se modifica ni elimina; espere o revise la ejecución activa.')
    cfg=AlertConfig(**{k:v for k,v in report['parametros'].items() if k in {f.name for f in fields(AlertConfig)}})
    with tempfile.TemporaryDirectory(prefix='secop_publicacion_') as temp:
        with duckdb.connect(config={'memory_limit':'1GB','threads':1,'temp_directory':temp}) as db:
            literal="'"+str(path).replace("'","''")+"'"
            db.execute(f'CREATE VIEW r AS SELECT * FROM read_parquet({literal})')
            n,ids,bad=db.execute('''SELECT count(*),count(DISTINCT id_contrato),
                count(*) FILTER(WHERE id_contrato IS NULL OR trim(id_contrato)='' OR
                version_modelo IS DISTINCT FROM ? OR cast(fecha_corte AS date) IS DISTINCT FROM cast(? AS date) OR huella_gold IS DISTINCT FROM ?)
                FROM r''',[cfg.version,cfg.cutoff,report['huella_gold']]).fetchone()
            classes=dict(db.execute('SELECT clasificacion,count(*) FROM r GROUP BY 1').fetchall())
            if not n or n!=ids or bad or classes!=report['resumen']['clasificaciones']:
                raise ValueError('Resultado inconsistente con su manifiesto, claves o clasificación.')
    return path,report,cfg


def publish_completed_result(path,progress=print):
    metadata=json.loads(Path(path).with_name('ejecucion.json').read_text(encoding='utf-8-sig'))
    if metadata.get('version')=='alertas-cd-3.0':
        from .indicador_v3_publicacion import publish,prove
        loaded=publish(path,progress=progress)
        return {'publicacion':loaded,'verificacion':prove(path)}
    from .alertas_aws import load_results_to_aws,validate_aws_results
    path,report,cfg=inspect_completed_result(path)
    # Identidad de las reglas del lote original; no usa código actual para reinterpretarlo.
    rules=cfg.evidence()
    for key in ('cutoff','reference_cutoff','coverage_start','memory_mb','threads'):rules.pop(key)
    model_hash=hashlib.sha256((report['huella_codigo']+json.dumps(rules,sort_keys=True)).encode()).hexdigest()
    loaded=load_results_to_aws(path,cfg,model_hash,progress=progress)
    verified=validate_aws_results(cfg,loaded['almacenados'])
    return {'publicacion':loaded,'verificacion':verified}
