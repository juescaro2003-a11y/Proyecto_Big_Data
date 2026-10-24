from __future__ import annotations

import os
import shutil
import tempfile
import time
from pathlib import Path

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from dask.distributed import Client, LocalCluster
from dask.utils import parse_bytes

from .config import GOLD_DIR, OUTPUT_DIR, SILVER_DIR


def _read_path(path: Path, columns=None):
    return dd.read_parquet(str(path / '*.parquet'), engine='pyarrow', columns=columns,
                           split_row_groups='adaptive', blocksize='128MiB')


def _metadata(path: Path):
    files = list(path.glob('*.parquet'))
    return {'archivos': len(files), 'filas': sum(pq.ParquetFile(p).metadata.num_rows for p in files),
            'bytes': sum(p.stat().st_size for p in files)}


COVERAGE_FIELDS = ['tipodocproveedor', 'causal', 'segmento', 'proveedor_id',
                   'entidad_id', 'urlproceso', 'valor_del_contrato', 'fecha_de_firma']


def _prepare_file(path: str, target: str, batch_rows: int):
    """Una tarea lee, normaliza y escribe lotes; devuelve solo contadores pequeños."""
    from .control import FIELDS, normalize_contracts
    parquet = pq.ParquetFile(path)
    wanted = set(FIELDS) | {'raw__' + c for c in FIELDS}
    columns = [c for c in parquet.schema_arrow.names if c in wanted]
    counts = dict.fromkeys(COVERAGE_FIELDS, 0)
    rows = 0
    writer = None
    try:
        for batch in parquet.iter_batches(batch_size=batch_rows, columns=columns, use_threads=False):
            frame = batch.to_pandas()
            frame['fuente_archivo'] = Path(path).name
            normalized = normalize_contracts(frame)
            table = pa.Table.from_pandas(normalized, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(target, table.schema, compression='snappy')
            writer.write_table(table, row_group_size=batch_rows)
            rows += len(normalized)
            for col in COVERAGE_FIELDS:
                counts[col] += int(normalized[col].notna().sum())
            # Liberar referencias antes de descomprimir el lote siguiente.
            del frame, normalized, table, batch
        if writer is None:
            frame = parquet.schema_arrow.empty_table().select(columns).to_pandas()
            frame['fuente_archivo'] = Path(path).name
            table = pa.Table.from_pandas(normalize_contracts(frame), preserve_index=False)
            pq.write_table(table, target, compression='snappy')
    finally:
        if writer is not None:
            writer.close()
        parquet.close()
    return {'rows': rows, 'coverage': counts}


def _prepare(directory: Path, workers: int, evidence: dict):
    from dask.distributed import get_client
    batch_rows = int(os.getenv('DASK_PREP_BATCH_ROWS', '5000'))
    if not 1 <= batch_rows <= 10000:
        raise ValueError('DASK_PREP_BATCH_ROWS debe estar entre 1 y 10000')
    files = sorted((SILVER_DIR/'contratos').glob('*.parquet'))
    target = directory/'contratos'
    target.mkdir(parents=True, exist_ok=True)
    evidence['profiles'][-1].update(filas_por_lote=batch_rows,
        tareas_simultaneas_maximas=workers, lectura='Arrow iter_batches; columnas requeridas')
    counts = dict.fromkeys(COVERAGE_FIELDS, 0)
    rows = 0
    client = get_client()
    # Una tanda por número de workers: no acumular DataFrames ni futuras tareas en RAM.
    for offset in range(0, len(files), workers):
        futures = [client.submit(_prepare_file, str(path),
                   str(target/f'part-{offset+i:06d}.parquet'), batch_rows, pure=False)
                   for i, path in enumerate(files[offset:offset+workers])]
        try:
            results = client.gather(futures)
            for result in results:
                rows += result['rows']
                for col in counts:
                    counts[col] += result['coverage'][col]
        finally:
            for future in futures:
                future.release()
        print(f'A: {min(offset+workers, len(files))}/{len(files)} archivos; '
              f'{rows:,} contratos normalizados', flush=True)
    evidence['cobertura_campos'] = counts
    metadata = _metadata(target)
    input_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
    if rows != input_rows or metadata['filas'] != input_rows:
        raise RuntimeError('La preparación no conservó todas las filas de contratos')
    evidence['counts']['contratos_entrada'] = rows
    return {'operaciones': 'Dask con lectura y escritura por lotes; sin filtros destructivos',
            'filas_entrada_contratos': rows, 'filas_salida_preparadas': metadata['filas'],
            'bytes_salida': metadata['bytes']}


def _finish(prepared: Path, output: Path, evidence: dict):
    from .control_analytics import analyze_contracts
    from .control_config import ControlConfig
    config = ControlConfig(**{k:v for k,v in evidence['parametros'].items() if k != 'parameter_hash'})
    print('B: deduplicación contractual, C/R/A, referencias históricas e IRCD...', flush=True)
    # Una tarea Dask con DuckDB y spill permite ventanas ordenadas sin colectar
    # todos los contratos en pandas. Se reserva RAM para el proceso y Arrow.
    budget = int(evidence['profiles'][-1]['memoria_por_worker_GiB']*1024**3*.50)
    result = dask.delayed(analyze_contracts)(prepared/'contratos', output, config, budget).compute()
    evidence.update(result)
    return {'operaciones': 'Versiones, filtros de contratación directa, tres componentes e IRCD',
            'filas_salida_gold': result['counts']['contratos_gold'],
            'conservacion_contratos': True, 'bytes_salida': _metadata(output)['bytes']}


def _settings(n_workers, memory_limit, threads_per_worker, configs):
    auto_memory = configs is None and memory_limit is None and not os.getenv('DASK_PREP_MEMORY') and not os.getenv('DASK_JOIN_MEMORY')
    if configs is None:
        configs = [
            {'nombre': 'A_preparacion', 'n_workers': n_workers if n_workers is not None else int(os.getenv('DASK_PREP_WORKERS', '2')),
             'memory_limit': memory_limit or os.getenv('DASK_PREP_MEMORY') or '3GiB', 'threads_per_worker': threads_per_worker},
            {'nombre': 'B_analitica_control', 'n_workers': int(os.getenv('DASK_JOIN_WORKERS', '1')),
             'memory_limit': os.getenv('DASK_JOIN_MEMORY') or '6GiB', 'threads_per_worker': threads_per_worker},
        ]
    if len(configs) != 2 or len({c['nombre'] for c in configs}) != 2:
        raise ValueError('Se requieren dos configuraciones: primero preparación, después cruce')
    normalized = []
    for spec in configs:
        cfg = dict(spec)
        cfg['threads_per_worker'] = cfg.get('threads_per_worker', 1)
        cfg['memory_limit'] = parse_bytes(cfg['memory_limit'])
        cfg['auto_memory'] = auto_memory
        if cfg['n_workers'] < 1 or cfg['threads_per_worker'] < 1 or cfg['memory_limit'] <= 0:
            raise ValueError('Workers, hilos y memoria deben ser positivos')
        normalized.append(cfg)
    if (normalized[0]['n_workers'] == normalized[1]['n_workers']
            or normalized[0]['memory_limit'] == normalized[1]['memory_limit']):
        raise ValueError('Preparación y cruce deben diferir en workers y memoria por worker')
    if auto_memory:
        # Presupuesto común conservador: mitad de RAM disponible, como máximo 6 GiB.
        budget = min(6 * 1024**3, int(psutil.virtual_memory().available * .5))
        for cfg in normalized:
            cfg['memory_limit'] = (budget // cfg['n_workers'] // (64*1024**2)) * (64*1024**2)
            if cfg['memory_limit'] < 512*1024**2:
                raise ValueError('RAM insuficiente: se requieren al menos 512 MiB por worker; libere memoria')
    return normalized


def build_gold(overwrite: bool = True, *, n_workers: int | None = None,
               memory_limit: str | None = None, threads_per_worker: int = 1,
               audit: dict | None = None, configs: list[dict] | None = None,
               publish: bool = True, control_config=None) -> Path | None:
    """Dos etapas Dask con shuffle por tareas. publish=False verifica y descarta temporales."""
    from .storage import publish_directory
    from .control_config import ControlConfig
    control_config = control_config or ControlConfig.from_project()
    destination = GOLD_DIR/'contratos_control'
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    settings = _settings(n_workers, memory_limit, threads_per_worker, configs)
    evidence = audit if audit is not None else {}
    evidence.update(counts={}, profiles=[], publicado=False, enfoque='control_contractual',
        parametros=control_config.evidence(),
        rules={'universo': 'Contratación directa; filtro de personas configurable',
               'unidad': 'id_contrato único', 'componentes': ['concentracion','recurrencia','valor_atipico'],
               'deduplicacion': 'Última actualización; versiones ambiguas a exclusiones',
               'cruce': 'Agregados contractuales muchos-a-uno; no cruce con procesos'})
    evidence['hardware'] = {'cpu_logicos': psutil.cpu_count(), 'cpu_fisicos': psutil.cpu_count(logical=False),
                             'ram_total_bytes': psutil.virtual_memory().total,
                             'ram_disponible_inicio_bytes': psutil.virtual_memory().available}
    def source_state():
        return [(str(p), p.stat().st_size, p.stat().st_mtime_ns)
                for base in ('contratos',) for p in sorted((SILVER_DIR/base).glob('*.parquet'))]
    initial = source_state()
    if not initial:
        raise FileNotFoundError('No hay contratos Silver')
    work = Path(tempfile.mkdtemp(prefix='.gold-preparation-', dir=GOLD_DIR))
    output = Path(tempfile.mkdtemp(prefix='.gold-result-', dir=GOLD_DIR))
    spill = OUTPUT_DIR.parent/'.dask-spill'
    spill.mkdir(exist_ok=True)
    try:
        for index, configured in enumerate(settings):
            cfg = dict(configured)
            available = psutil.virtual_memory().available
            planned = cfg['memory_limit']
            if cfg['auto_memory']:
                ceiling = int(available*.5) // cfg['n_workers']
                cfg['memory_limit'] = min(planned, ceiling // (64*1024**2) * (64*1024**2))
            record = {'configuracion': cfg['nombre'], 'etapa': 'preparacion' if index == 0 else 'analitica_control',
                      'estado': 'EN_CURSO', 'shuffle': 'tasks', 'workers_solicitados': cfg['n_workers'],
                      'hilos_por_worker': cfg['threads_per_worker'], 'memoria_por_worker_GiB': cfg['memory_limit']/1024**3,
                      'presupuesto_total_GiB': cfg['n_workers']*cfg['memory_limit']/1024**3,
                      'memoria_planificada_por_worker_GiB': planned/1024**3,
                      'seleccion_memoria': 'automatica' if cfg['auto_memory'] else 'manual',
                      'memoria_target_spill_pause_terminate': '60% / 70% / 80% / 95%',
                      'ram_disponible_antes_GiB': available/1024**3}
            evidence['profiles'].append(record)
            if cfg['memory_limit'] < 512*1024**2:
                raise ValueError('RAM disponible demasiado baja: mínimo 512 MiB por worker; libere memoria')
            if index == 1 and cfg['auto_memory'] and cfg['memory_limit'] == int(evidence['profiles'][0]['memoria_por_worker_GiB']*1024**3):
                # Mantener evidencia de dos límites distintos sin aumentar recursos.
                cfg['memory_limit'] -= 64*1024**2
                if cfg['memory_limit'] < 512*1024**2:
                    raise ValueError('RAM insuficiente para dos configuraciones distintas')
                record['memoria_por_worker_GiB'] = cfg['memory_limit']/1024**3
                record['presupuesto_total_GiB'] = cfg['n_workers']*cfg['memory_limit']/1024**3
            if not cfg['auto_memory'] and cfg['n_workers']*cfg['memory_limit'] > available*.70:
                raise ValueError('La configuración MANUAL supera el 70% de RAM disponible. '
                                 'Reduzca DASK_PREP_MEMORY/DASK_JOIN_MEMORY o deje ambos vacíos para modo automático.')
            print(f"Iniciando {cfg['nombre']}: {cfg['n_workers']} workers, "
                  f"{cfg['memory_limit']/1024**3:.2f} GiB por worker; modo {record['seleccion_memoria']}")
            started = time.perf_counter()
            with dask.config.set({
                'dataframe.shuffle.method': 'tasks',
                'distributed.scheduler.worker-saturation': 1.0,
                'distributed.worker.memory.target': 0.60,
                'distributed.worker.memory.spill': 0.70,
                'distributed.worker.memory.pause': 0.80,
                'distributed.worker.memory.terminate': 0.95,
            }), LocalCluster(n_workers=cfg['n_workers'], threads_per_worker=cfg['threads_per_worker'],
                              memory_limit=cfg['memory_limit'], processes=True, dashboard_address=None,
                              local_directory=str(spill)) as cluster:
                with Client(cluster) as client:
                    client.wait_for_workers(cfg['n_workers'])
                    actual = list(client.scheduler_info()['workers'].values())
                    record['workers_observados'] = len(actual)
                    record['limites_efectivos_bytes'] = [w['memory_limit'] for w in actual]
                    if len(actual) != cfg['n_workers'] or any(w['memory_limit'] != cfg['memory_limit'] for w in actual):
                        raise RuntimeError('Recursos observados diferentes de los solicitados')
                    processing = time.perf_counter()
                    try:
                        result = _prepare(work, cfg['n_workers'], evidence) if index == 0 else _finish(work, output, evidence)
                    except BaseException as exc:
                        record['error_tipo'] = type(exc).__name__
                        record['error_mensaje'] = str(exc)
                        # Capturar antes de cerrar el clúster; un worker muerto puede no dejar logs.
                        try:
                            record['logs_nanny'] = client.get_worker_logs(n=30, nanny=True)
                        except Exception as log_error:
                            record['logs_no_disponibles'] = str(log_error)
                        raise
                    record.update(result)
                    record['segundos_proceso_y_auditoria'] = time.perf_counter()-processing
            record['segundos_total_etapa'] = time.perf_counter()-started
            record['estado'] = 'COMPLETADA'
            if source_state() != initial:
                raise RuntimeError('Silver cambió durante el procesamiento; no se publica Gold')
            print(f"{cfg['nombre']}: {record['workers_observados']} workers, "
                  f"{record['memoria_por_worker_GiB']:g} GiB/worker; {record['segundos_proceso_y_auditoria']:.2f} s")
        if not publish:
            evidence['verificacion_sin_publicar'] = True
            print('Verificación completa: temporales descartados; Gold publicado permanece intacto.', flush=True)
            return None
        publish_directory(output, destination)
        evidence['publicado'] = True
        return destination
    except BaseException as exc:
        if evidence['profiles'] and evidence['profiles'][-1]['estado'] == 'EN_CURSO':
            evidence['profiles'][-1]['estado'] = 'FALLIDA'
        # Prefect propaga la excepción; la auditoría puede conservar la etapa A completada.
        try:
            exc.etl_audit = evidence
        except Exception:
            pass
        raise
    finally:
        for path in (work, output):
            resolved = path.resolve()
            if resolved.parent != GOLD_DIR.resolve() or not resolved.name.startswith(('.gold-preparation-', '.gold-result-')):
                raise RuntimeError('Directorio temporal fuera de Gold')
            if resolved.exists():
                shutil.rmtree(resolved)


def benchmark_dask(configs: list[dict] | None = None) -> pd.DataFrame:
    """Alias obsoleto: construye Gold UNA vez, en dos etapas distintas; no compara velocidades."""
    import warnings
    warnings.warn('Use build_gold(audit=...); ahora se ejecutan normalización y analítica contractual, no un benchmark.',
                  FutureWarning, stacklevel=2)
    evidence = {}
    build_gold(configs=configs, audit=evidence)
    return pd.DataFrame([{k:v for k,v in row.items() if k != 'salidas'} for row in evidence['profiles']])
