"""Identidades por etapa y adopción controlada del caché anterior."""
import ast
import hashlib
import json
from pathlib import Path

LEGACY_HASH='813913b71b7cc524978f14275181aff83f432b5997f5d5861f6c1242ab577be1'


def hash_value(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()


def signatures(core,cfg,source_hash):
    tree=ast.parse(Path(core.__file__).read_text(encoding='utf-8-sig'))
    funcs={n.name:ast.dump(n,include_attributes=False) for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
    params=cfg.evidence()
    result={}
    def stage(name,deps=(),keys=(),helpers=(),extra=None):
        result[name]=hash_value([funcs[name],[result[d] for d in deps],{k:params[k] for k in keys},
                                 [funcs[x] for x in helpers],funcs['lit'],extra])
    stage('extract_gold',keys=['cutoff'],extra=[source_hash,core.SOURCE_FIELDS])
    stage('filter_direct_contracting',['extract_gold'],['cutoff','coverage_start'])
    stage('validate_gold',['filter_direct_contracting'])
    stage('calculate_concentration',['validate_gold'],[k for k in params if k.startswith('d1_')]+['coverage_start'])
    engine=Path(core.__file__).with_name('alertas_dask.py')
    stage('calculate_economic_anomalies',['validate_gold'],[k for k in params if k.startswith('d2_')]+['coverage_start'],
          extra=hashlib.sha256(engine.read_bytes()).hexdigest())
    stage('calculate_year_end_accumulation',['validate_gold'],[k for k in params if k.startswith('d3_')]+['cutoff','coverage_start'])
    stage('calculate_score',['calculate_concentration','calculate_economic_anomalies','calculate_year_end_accumulation'])
    stage('classify_contracts',['calculate_score'],[k for k in params if k not in ('memory_mb','threads')],
          ['validate_results','summary'])
    return result


def choose_work(output,source_hash,cfg):
    """Llamar después de tomar pipeline_lock: nunca adoptar una DB en uso."""
    target=Path(output)/hash_value(['stages-v2',source_hash])[:24]
    if target.exists(): return target,False
    # Solo este código auditado conservaba exactamente la misma preparación y D1.
    for path in sorted(Path(output).glob('*/ejecucion.json'),key=lambda p:p.stat().st_mtime,reverse=True):
        try:
            m=json.loads(path.read_text(encoding='utf-8'))
            if m.get('huella_gold')!=source_hash: continue
            if m.get('cache_schema')==2 and (path.parent/'analitica.duckdb').exists():
                return path.parent,False
            if m.get('huella_codigo')!=LEGACY_HASH: continue
            old={k:v for k,v in m['parametros'].items() if k in ('cutoff','coverage_start') or k.startswith('d1_')}
            current={k:v for k,v in cfg.evidence().items() if k in ('cutoff','coverage_start') or k.startswith('d1_')}
            if hash_value(old)!=hash_value(current): continue
            initial=['extract_gold','filter_direct_contracting','validate_gold','calculate_concentration']
            if not all(m['etapas'].get(k,{}).get('estado')=='COMPLETADA' for k in initial): continue
            import duckdb
            with duckdb.connect(str(path.parent/'analitica.duckdb'),read_only=True) as c:
                counts=c.execute('SELECT (SELECT count(*) FROM source),(SELECT count(*) FROM universe),(SELECT count(*) FROM d1),(SELECT count(DISTINCT id_contrato) FROM d1)').fetchone()
                if counts[0]!=m['etapas']['extract_gold']['resultado']['total_gold']: continue
                if counts[1]!=m['etapas']['validate_gold']['resultado']['contratos_universo']: continue
                if counts[1]!=counts[2] or counts[2]!=counts[3]: continue
            # Conservar la ruta original y registrar un alias evita copiar GB y conserva su evidencia.
            return path.parent,True
        except (OSError,ValueError,KeyError): continue
    return target,False


def stage_available(work,name):
    tables={'extract_gold':'source','filter_direct_contracting':'universe','validate_gold':'universe',
            'calculate_concentration':'d1','calculate_economic_anomalies':'d2',
            'calculate_year_end_accumulation':'d3','calculate_score':'scored','classify_contracts':'classified'}
    import duckdb
    db=Path(work)/'analitica.duckdb'
    if not db.exists(): return False
    with duckdb.connect(str(db),read_only=True) as c:
        exists=c.execute('SELECT count(*) FROM information_schema.tables WHERE table_name=?',[tables[name]]).fetchone()[0]>0
    if name=='calculate_year_end_accumulation' and not (Path(work)/'d3_entidad_anio.parquet').exists(): return False
    return exists and (name!='classify_contracts' or (Path(work)/'resultados.parquet').exists())
