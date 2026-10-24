"""Flujo independiente v3. No modifica resultados, puntero o checkpoints v2."""
from pathlib import Path
from dataclasses import asdict
from contextlib import ExitStack
import hashlib,json,time
from prefect import flow,task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run
from secop_pipeline import indicador_v3 as model
from secop_pipeline.storage import pipeline_lock


@task(name='indicador-v3-etapa',log_prints=True,cache_policy=NO_CACHE)
def calculate_stage(name,work,configuration,source):
    cfg=model.Config(**configuration)
    with model.connect(work,cfg) as c:
        c.execute('BEGIN')
        try:
            fn=model.STAGES[name]
            value=fn(c,cfg,source) if name=='preparar_gold' else fn(c,cfg)
            c.execute('COMMIT')
            return value
        except BaseException:
            c.execute('ROLLBACK');raise


@flow(name='SECOP-INDICADOR-V3-DESDE-GOLD',log_prints=True)
def indicador_v3_flow(config_path=None,source=None):
    cfg=model.configuration(config_path,source)
    source=Path(source or model.ROOT/'data/gold/contratos_control/contratos.parquet').resolve()
    base=model.ROOT/'salida/alertas';base.mkdir(parents=True,exist_ok=True)
    started=time.perf_counter()
    with ExitStack() as locks:
        locks.enter_context(pipeline_lock(model.ROOT));locks.enter_context(pipeline_lock(base))
        fingerprint=model.sha(source)
        code_hash=hashlib.sha256(Path(model.__file__).read_bytes()+Path(__file__).read_bytes()).hexdigest()
        identity=hashlib.sha256(json.dumps([fingerprint,code_hash,asdict(cfg)],sort_keys=True).encode()).hexdigest()
        work=base/'indicador_v3'/identity[:24];work.mkdir(parents=True,exist_ok=True)
        path=work/'ejecucion.json'
        report=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        if report.get('estado')=='COMPLETADA' and (work/'resultados.parquet').exists():
            if model.sha(work/'resultados.parquet')==report.get('huella_resultados'):
                model.save_json(base/'ultima_ejecucion_v3.json',{'directorio':str(work)})
                print('V3: mismos datos/código/parámetros; resultado completo reutilizado.')
                return report
            raise RuntimeError('Resultado cacheado modificado: no se sobrescribe. Revise el lote.')
        stages=report.get('etapas',{}) if (work/'indicador.duckdb').exists() else {}
        report=dict(estado='EN_CURSO',version=cfg.version,parametros=asdict(cfg),huella_gold=fingerprint,
            huella_codigo=code_hash,source=str(source),directorio=str(work),flow_run_id=str(flow_run.id),
            etapas=stages,metodologia='retrospectiva; C2 anual observado; V1 años completos previos; siete señales; pesos por tres familias; top K trimestral')
        model.save_json(path,report)
        # Puntero del monitor separado del último lote completo que usan los publicadores.
        model.save_json(base/'ejecucion_actual_v3.json',{'directorio':str(work)})
        try:
            for number,name in enumerate(model.STAGES,1):
                if stages.get(name,{}).get('estado')=='COMPLETADA':
                    print(f'{number}/5 {name}: checkpoint reutilizado');continue
                stages[name]={'estado':'EN_CURSO'};model.save_json(path,report)
                t=time.perf_counter();print(f'{number}/5 {name}: iniciando')
                value=calculate_stage(name,str(work),asdict(cfg),str(source))
                stages[name]={'estado':'COMPLETADA','segundos':time.perf_counter()-t,'resultado':value}
                model.save_json(path,report)
            if model.sha(source)!=fingerprint:raise RuntimeError('Gold cambió durante el cálculo; no se publica.')
            with model.connect(work,cfg) as c:
                for table,filename in [('results','resultados.parquet'),('red_aristas','red_aristas.parquet'),('red_proveedores','red_proveedores.parquet')]:
                    temporary=work/(filename+'.tmp')
                    c.execute(f'COPY {table} TO {model.lit(temporary)} (FORMAT PARQUET,COMPRESSION ZSTD)')
                    temporary.replace(work/filename)
                report['resumen']=model.summary(c)
            report.update(estado='COMPLETADA',segundos=time.perf_counter()-started,huella_resultados=model.sha(work/'resultados.parquet'))
            model.save_json(path,report)
            model.save_json(base/'ultima_ejecucion_v3.json',{'directorio':str(work)})
            print(json.dumps(report['resumen'],ensure_ascii=False,indent=2))
            return report
        except BaseException as exc:
            if 'name' in locals() and stages.get(name,{}).get('estado')=='EN_CURSO':
                stages[name].update(estado='FALLIDA',error_tipo=type(exc).__name__)
            report.update(estado='FALLIDA',error_tipo=type(exc).__name__,error=str(exc),segundos=time.perf_counter()-started)
            model.save_json(path,report);raise
