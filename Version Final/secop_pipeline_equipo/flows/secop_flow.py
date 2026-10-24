from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from prefect import flow, task

from secop_pipeline.config import BASES
from secop_pipeline.storage import pipeline_lock
from secop_pipeline.audit import RunAudit
from secop_pipeline.dask_jobs import build_gold
from secop_pipeline.extract import SocrataClient, extract_all, validate_api
from secop_pipeline.load import load_all_to_postgres
from secop_pipeline.quality import build_quality_report
from secop_pipeline.transform import transform_base

@task(retries=3, retry_delay_seconds=15, log_prints=True)
def validate_api_task():
    print("Validando columnas contra Datos.gov.co...")
    return validate_api(SocrataClient())

@task(retries=2, retry_delay_seconds=30, log_prints=True)
def extract_all_task(refresh=False, start=None, cutoff=None):
    print("Actualizando las cuatro bases desde la API..." if refresh else
          "Verificando y reutilizando Bronze local; sin descargar datos...")
    return extract_all(refresh=refresh, start=start, cutoff=cutoff)

@task(log_prints=True)
def transform_task(base: str):
    print(f"Transformando {base} a Parquet...")
    return transform_base(base)

@task(log_prints=True)
def quality_task():
    print("Construyendo reporte de calidad...")
    return build_quality_report().to_dict(orient="records")

@task(log_prints=True)
def gold_task(n_workers=None, memory_limit=None):
    print("Construyendo Gold contractual e IRCD con Dask...")
    audit = {}
    path = build_gold(n_workers=n_workers, memory_limit=memory_limit, audit=audit)
    return {'path': str(path), 'audit': audit}

@task(log_prints=True)
def postgres_task():
    print("Cargando PostgreSQL con COPY...")
    return load_all_to_postgres(replace=True)

@flow(name="SECOP-II-CONTROL-ETL", log_prints=True)
def secop_flow(
    load_postgres: bool | None = None,
    run_benchmark: bool | None = None,
    refresh: bool = False,
    start: str | None = None,
    cutoff: str | None = None,
    dask_workers: int | None = None,
    dask_memory: str | None = None,
):
    if load_postgres is None:
        load_postgres = os.getenv("LOAD_POSTGRES", "1") == "1"
    if run_benchmark is not None:
        print('run_benchmark es obsoleto: Gold se construye una vez en dos etapas con recursos distintos; no hay tarea de benchmark separada.')

    with pipeline_lock(ROOT):
        audit = RunAudit({'refresh': refresh, 'start_solicitado': start, 'cutoff_solicitado': cutoff,
                          'load_postgres': load_postgres, 'dask_modalidad': 'Normalización y analítica contractual secuenciales',
                          'dask_prep_workers': dask_workers if dask_workers is not None else int(os.getenv('DASK_PREP_WORKERS', '2')),
                          'dask_prep_memory': dask_memory or os.getenv('DASK_PREP_MEMORY') or 'automatica',
                          'dask_join_workers': int(os.getenv('DASK_JOIN_WORKERS', '1')),
                          'dask_join_memory': os.getenv('DASK_JOIN_MEMORY') or 'automatica'})
        audit.plan(['validacion_api', 'extraccion'] + ['transformar_' + base for base in BASES]
                   + ['calidad', 'gold', 'postgres'])
        try:
            if refresh:
                audit.call('validacion_api', validate_api_task)
            else:
                audit.skip('validacion_api')
            audit.call('extraccion', extract_all_task, refresh=refresh, start=start, cutoff=cutoff)
            for base in BASES:
                audit.call('transformar_' + base, transform_task, base)
            audit.call('calidad', quality_task)
            audit.call('gold', gold_task, n_workers=dask_workers, memory_limit=dask_memory)
            if load_postgres:
                result = audit.call('postgres', postgres_task)
            else:
                audit.skip('postgres')
                result = None
        except BaseException:
            try:
                audit.finish('FALLIDA')
                print(f'Informe de fallo: {audit.latest}')
            except Exception as report_error:
                print(f'No fue posible guardar el informe final: {type(report_error).__name__}')
            raise
        audit.finish('COMPLETADA')
        print(f'Informe ETL: {audit.latest}; histórico: {audit.history}')
        return result

if __name__ == "__main__":
    secop_flow()
