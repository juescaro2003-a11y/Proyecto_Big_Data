"""Flujo posterior a Gold. No llama a extracción ni reconstruye capas previas."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

from prefect import flow,task,get_run_logger
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run
from secop_pipeline import alertas as core
from secop_pipeline import alertas_aws as aws
from secop_pipeline.storage import pipeline_lock
from secop_pipeline import alertas_cache as cache
from secop_pipeline import alertas_stats as stats


def transient_only(task,task_run,state):
    from sqlalchemy.exc import OperationalError
    import psycopg
    try:
        state.result()
    except (OperationalError,psycopg.OperationalError):
        return True
    except Exception:
        return False
    return False


extract_gold=task(name='extract_gold',log_prints=True,cache_policy=NO_CACHE)(core.extract_gold)
filter_direct_contracting=task(name='filter_direct_contracting',log_prints=True,cache_policy=NO_CACHE)(core.filter_direct_contracting)
validate_gold=task(name='validate_gold',log_prints=True,cache_policy=NO_CACHE)(core.validate_gold)
calculate_concentration=task(name='calculate_concentration',log_prints=True,cache_policy=NO_CACHE)(core.calculate_concentration)
calculate_economic_anomalies=task(name='calculate_economic_anomalies',log_prints=True,cache_policy=NO_CACHE)(core.calculate_economic_anomalies)
calculate_year_end_accumulation=task(name='calculate_year_end_accumulation',log_prints=True,cache_policy=NO_CACHE)(core.calculate_year_end_accumulation)
calculate_score=task(name='calculate_score',log_prints=True,cache_policy=NO_CACHE)(core.calculate_score)
classify_contracts=task(name='classify_contracts',log_prints=True,cache_policy=NO_CACHE)(core.classify_contracts)


@task(name='load_results_to_aws',retries=2,retry_delay_seconds=20,retry_condition_fn=transient_only,log_prints=True,cache_policy=NO_CACHE)
def load_results_to_aws(parquet,cfg,model_hash):
    return aws.load_results_to_aws(parquet,cfg,model_hash)


@task(name='validate_aws_results',retries=2,retry_delay_seconds=20,retry_condition_fn=transient_only,log_prints=True,cache_policy=NO_CACHE)
def validate_aws_results(cfg,expected):
    return aws.validate_aws_results(cfg,expected)


def configuration(path=None,cutoff=None):
    path=Path(path or ROOT/'alertas_config.json')
    values=json.loads(path.read_text(encoding='utf-8'))
    evidence_path=core.GOLD.parent/'auditoria_ircd.json'
    gold_params=json.loads(evidence_path.read_text(encoding='utf-8'))['parametros'] if evidence_path.exists() else {}
    values['cutoff']=cutoff or values.get('cutoff') or gold_params.get('cutoff')
    if not values['cutoff']: raise ValueError('Indique cutoff: no hay corte declarado en Gold')
    values['coverage_start']=values.get('coverage_start') or gold_params.get('coverage_start')
    if not values['coverage_start']: raise ValueError('Indique coverage_start con cobertura documentada')
    return core.AlertConfig(**values)


def code_hash():
    return hashlib.sha256(Path(core.__file__).read_bytes()+Path(core.__file__).with_name('alertas_dask.py').read_bytes()).hexdigest()


def model_hash(cfg):
    # El corte cambia por ejecución; las reglas no pueden cambiar bajo la misma versión.
    rules=cfg.evidence()
    for key in ('cutoff','reference_cutoff','coverage_start','memory_mb','threads'): rules.pop(key)
    return hashlib.sha256((code_hash()+json.dumps(rules,sort_keys=True)).encode()).hexdigest()


@flow(name='SECOP-II-ALERTAS-POST-GOLD',log_prints=True)
def alertas_flow(config_path=None,cutoff=None,publish_aws=False,only_load=False,scenarios=False,source=None,cache_namespace=None):
    # Un namespace nuevo empieza desde Gold; el mismo permite recuperar checkpoints.
    # Se conserva el lock global y el puntero del monitor para todas las ejecuciones.
    if cache_namespace is not None and not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', cache_namespace):
        raise ValueError('cache_namespace: use 1 a 48 letras minúsculas, números, guion o guion bajo')
    cfg=configuration(config_path,cutoff)
    source=Path(source or core.GOLD).resolve()
    logger=get_run_logger()
    # Fallar antes de cálculos largos si se pidió AWS y no hay conexión/credencial.
    if publish_aws: aws.check_connection()
    if only_load and not publish_aws: raise ValueError('only_load requiere publish_aws=True')
    identity=core.source_fingerprint(source)
    expected_signatures=cache.signatures(core,cfg,identity)
    output=ROOT/'salida/alertas'
    output.mkdir(parents=True,exist_ok=True)
    started=time.perf_counter()
    with pipeline_lock(output):
        cache_root=output if cache_namespace is None else output/('lote_'+cache_namespace)
        cache_root.mkdir(parents=True,exist_ok=True)
        work,adopted=cache.choose_work(cache_root,identity,cfg)
        manifest_path=work/'ejecucion.json'
        work.mkdir(parents=True,exist_ok=True)
        previous=json.loads(manifest_path.read_text(encoding='utf-8')) if manifest_path.exists() else {}
        completed=previous.get('etapas',{})
        if adopted:
            initial=('extract_gold','filter_direct_contracting','validate_gold','calculate_concentration')
            completed={k:dict(completed[k],signature=expected_signatures[k],origen='cache_anterior_validado') for k in initial}
            logger.info('Preparación y D1 anteriores validados; se conservarán en %s',work)
        run_id=str(flow_run.id)
        report={'cache_schema':2,'flow_run_id':run_id,'estado':'EN_CURSO','source':str(source),'huella_gold':identity,
                'cache_namespace':cache_namespace,'parametros':cfg.evidence(),'huella_codigo':code_hash(),'etapas':completed,'directorio':str(work)}
        report['hallazgos']={k:v for k,v in previous.get('hallazgos',{}).items()
            if k in stats.INDICATORS and v.get('signature')==expected_signatures[stats.INDICATORS[k][0]] and v.get('stats_version')==stats.version()}
        def save():
            report['segundos']=time.perf_counter()-started
            tmp=manifest_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
            tmp.replace(manifest_path)
            (output/'ultima_ejecucion.json').write_text(json.dumps({'directorio':str(work),'flow_run_id':run_id},indent=2),encoding='utf-8')
        def execute(name,fn,*args,reuse=True):
            signature=expected_signatures.get(name)
            if (reuse and signature and completed.get(name,{}).get('estado')=='COMPLETADA'
                and completed[name].get('signature')==signature and cache.stage_available(work,name)):
                logger.info('Reutilizando etapa %s; misma fuente, código y parámetros',name)
                return completed[name].get('resultado')
            completed[name]={'estado':'EN_CURSO'};save()
            t=time.perf_counter()
            try:
                value=fn(*args)
                completed[name]={'estado':'COMPLETADA','signature':signature,'segundos':time.perf_counter()-t,'resultado':value}
                save();return value
            except Exception as exc:
                completed[name]={'estado':'FALLIDA','segundos':time.perf_counter()-t,'error_tipo':type(exc).__name__}
                save();raise
        def record_indicator(label):
            stage=stats.INDICATORS[label][0]
            if label not in report['hallazgos']:
                report['hallazgos'][label]={'signature':expected_signatures[stage],
                    'stats_version':stats.version(),**stats.indicator_summary(work,cfg,label)}
            value=report['hallazgos'][label]
            logger.info('%s: %s alertas; %s evaluados; %s no evaluables',label,value['alertas'],value['evaluados'],value['no_evaluables'])
            save()
        try:
            save()
            if only_load:
                if not (work/'resultados.parquet').exists() or completed.get('classify_contracts',{}).get('estado')!='COMPLETADA' or completed.get('classify_contracts',{}).get('signature')!=expected_signatures['classify_contracts']:
                    raise ValueError('No hay resultado validado de la misma fuente, código y configuración; ejecute primero --local')
            else:
                execute('extract_gold',extract_gold,work,cfg,source)
                execute('filter_direct_contracting',filter_direct_contracting,work,cfg)
                execute('validate_gold',validate_gold,work,cfg)
                execute('calculate_concentration',calculate_concentration,work,cfg)
                record_indicator('D1')
                execute('calculate_economic_anomalies',calculate_economic_anomalies,work,cfg)
                record_indicator('D2')
                execute('calculate_year_end_accumulation',calculate_year_end_accumulation,work,cfg)
                record_indicator('D3')
                execute('calculate_score',calculate_score,work,cfg)
                execute('classify_contracts',classify_contracts,work,cfg,run_id,identity)
            report['resumen']=completed['classify_contracts']['resultado']
            if report['resumen'].get('estadisticas_version')!=stats.version():
                with core.connect(work,cfg) as connection:
                    report['resumen']=stats.final_summary(connection)
                completed['classify_contracts']['resultado']=report['resumen']
            save()
            if scenarios:
                report['escenarios']=core.evaluate_scenarios(work,cfg)
            # La fuente debe permanecer idéntica mientras se prepara una evaluación.
            if core.source_fingerprint(source)!=identity: raise RuntimeError('Gold cambió durante el análisis; no se publica')
            report['aws']={'estado':'NO_SOLICITADA'}
            if publish_aws:
                loaded=execute('load_results_to_aws',load_results_to_aws,work/'resultados.parquet',cfg,model_hash(cfg),reuse=False)
                remote=execute('validate_aws_results',validate_aws_results,cfg,loaded['almacenados'],reuse=False)
                report['aws']={'estado':'VERIFICADA',**loaded}
                for title in ('primeros_10','clasificaciones','dimensiones'):
                    print(f'Consulta real AWS: {title}')
                    print(remote[title].to_string(index=False))
                # DataFrames solo en retorno; el manifiesto conserva resultados tabulares JSON.
                completed['validate_aws_results']['resultado']={k:v.to_dict('records') if hasattr(v,'to_dict') else v for k,v in remote.items()}
            report['estado']='COMPLETADA';save()
            print(json.dumps(report['resumen'],ensure_ascii=False,indent=2))
            print(f'Estado: {report["estado"]}; AWS: {report["aws"]["estado"]}; {report["segundos"]:.1f} s; {work}')
            if publish_aws: report['consultas_aws']=remote
            return report
        except BaseException as exc:
            report['estado']='FALLIDA';report['error_tipo']=type(exc).__name__;save()
            raise
