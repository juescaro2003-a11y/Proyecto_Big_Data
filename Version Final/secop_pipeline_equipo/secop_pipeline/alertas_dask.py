"""D2 exacto por grupo/fecha, con workers aislados y checkpoints Parquet."""
from __future__ import annotations

import calendar
import hashlib
import json
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ENGINE_VERSION = 'd2-weighted-daily-v1'
SCHEMA = pa.schema([('gid', pa.string()), ('firma', pa.date32()),
    ('n_contratos_comparables', pa.int64()), ('mediana_grupo', pa.float64()),
    ('mad_grupo', pa.float64()), ('p95_log_grupo', pa.float64()),
    ('referencia_d2_min', pa.date32()), ('referencia_d2_max', pa.date32())])


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(8 * 1024**2), b''): h.update(b)
    return h.hexdigest()


def atomic_json(path, data):
    path = Path(path)
    tmp = path.with_name('.tmp-' + uuid.uuid4().hex[:12])
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    os.replace(tmp, path)


def literal(x): return "'" + str(x).replace("'", "''") + "'"


def month_start(day, months):
    y, m = divmod(day.year * 12 + day.month - 1 - months, 12)
    return date(y, m + 1, min(day.day, calendar.monthrange(y, m + 1)[1]))


def weighted_quantile(values, weights, q):
    """Equivalente a quantile_cont sobre cada observación original, sin expandirla."""
    total = int(weights.sum())
    if not total: return None
    cumulative = np.cumsum(weights, dtype=np.int64)
    pos = (total - 1) * q
    low, high = int(np.floor(pos)), int(np.ceil(pos))
    a = values[np.searchsorted(cumulative, low + 1)]
    b = values[np.searchsorted(cumulative, high + 1)]
    return float(a + (b - a) * (pos - low))


def daily_references(gid, dates, values, weights, targets, months, minimum, tick=None):
    """Ventana [fecha - meses, fecha). Pesos = contratos, nunca días."""
    order = np.argsort(dates, kind='stable')
    dates, values, weights = dates[order], values[order], weights[order]
    prices, codes = np.unique(values, return_inverse=True)
    counts = np.zeros(len(prices), dtype=np.int64)
    left = right = 0
    for j, target in enumerate(sorted(targets)):
        day = date.fromisoformat(str(target)[:10])
        lower = np.datetime64(month_start(day, months), 'D')
        upper = np.datetime64(day, 'D')
        new_left = int(np.searchsorted(dates, lower, side='left'))
        new_right = int(np.searchsorted(dates, upper, side='left'))
        # Inicializar una sola vez; después añadir/quitar exclusivamente los cambios.
        if j == 0:
            np.add.at(counts, codes[new_left:new_right], weights[new_left:new_right])
        else:
            np.add.at(counts, codes[right:new_right], weights[right:new_right])
            np.add.at(counts, codes[left:new_left], -weights[left:new_left])
        left, right = new_left, new_right
        n = int(counts.sum())
        med = mad = p95 = None
        if n >= minimum:
            active = counts > 0
            x, w = prices[active], counts[active]
            med = weighted_quantile(x, w, .5)
            p95 = weighted_quantile(x, w, .95)
            deviations = np.abs(x - med)
            idx = np.argsort(deviations, kind='stable')
            mad = weighted_quantile(deviations[idx], w[idx], .5)
        yield dict(gid=gid, firma=day, n_contratos_comparables=n,
            mediana_grupo=med, mad_grupo=mad, p95_log_grupo=p95,
            referencia_d2_min=dates[left].astype(object) if n else None,
            referencia_d2_max=dates[right-1].astype(object) if n else None)
        if tick: tick()


def valid_block(task):
    p = Path(task['output']); marker = p.with_suffix('.json')
    try:
        m = json.loads(marker.read_text(encoding='utf-8'))
        f = pq.ParquetFile(p)
        return (m['key'] == task['key'] and m['rows'] == task['targets_count']
                and f.metadata.num_rows == m['rows'] and f.schema_arrow.equals(SCHEMA)
                and m['sha256'] == digest(p))
    except (OSError, ValueError, KeyError, pa.ArrowException): return False


def compute_block(task):
    if valid_block(task): return {'key':task['key'], 'cached':True}
    started = time.monotonic()
    progress_path = Path(task['output']).with_suffix('.progress.json')
    done = 0; last = 0
    def tick():
        nonlocal done, last
        done += 1
        if time.monotonic() - last >= 5 or done == task['targets_count']:
            atomic_json(progress_path, {'key':task['key'],'done':done,'total':task['targets_count'],
                'pid':os.getpid(),'updated_at':datetime.now(timezone.utc).isoformat()})
            last = time.monotonic()
    table = pq.read_table(task['inputs'], columns=['gid','firma','log_valor','peso'])
    frame = table.to_pandas()
    del table
    results=[]
    grouped=frame.groupby('gid', sort=False)
    for gid, targets in task['targets'].items():
        g = grouped.get_group(gid)
        results.extend(daily_references(gid, g.firma.to_numpy(dtype='datetime64[D]'),
            g.log_valor.to_numpy(dtype=np.float64), g.peso.to_numpy(dtype=np.int64),
            targets, task['months'], task['minimum'], tick))
    if len(results) != task['targets_count']: raise ValueError('Bloque D2 incompleto')
    output = Path(task['output'])
    tmp = output.with_name('.tmp-'+uuid.uuid4().hex[:12])
    pq.write_table(pa.Table.from_pylist(results, schema=SCHEMA), tmp, compression='zstd')
    os.replace(tmp, output)
    atomic_json(output.with_suffix('.json'), {'key':task['key'],'rows':len(results),
        'sha256':digest(output),'seconds':time.monotonic()-started})
    return {'key':task['key'],'cached':False}


def runtime_config():
    import psutil
    path = Path(__file__).resolve().parents[1]/'alertas_dask_config.json'
    options = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    available = psutil.virtual_memory().available / 1024**3
    default_workers, default_memory = (4,2048) if available >= 12 else (2,1536) if available >= 6 else (1,1536)
    cpus = psutil.cpu_count(logical=False) or 1
    workers = int(options.get('workers') or min(default_workers, cpus))
    memory = int(options.get('memory_mb_per_worker') or default_memory)
    days = int(options.get('block_days',31))
    buckets = int(options.get('buckets',32))
    large = int(options.get('large_group_rows',100000))
    if min(workers,days,buckets,large) < 1 or memory < 512: raise ValueError('Configuración Dask inválida')
    if workers*memory/1024 + 2 > available:
        raise RuntimeError(f'Memoria disponible {available:.1f} GiB insuficiente para {workers} workers de {memory} MiB y reserva de 2 GiB')
    return dict(workers=workers,memory_mb_per_worker=memory,threads_per_worker=1,
                block_days=days,buckets=buckets,large_group_rows=large,
                available_gib=round(available,2))


def run_tasks(tasks, cache, runtime, prefix, state, progress):
    """Cola acotada: 2 tareas por worker; dashboard local y heartbeat cada 10 s."""
    import dask
    from distributed import Client, LocalCluster, wait
    state['level']=prefix
    complete=[t for t in tasks if valid_block(t)]
    completed_keys={t['key'] for t in complete}
    pending=[t for t in tasks if t['key'] not in completed_keys]
    state.update(total_blocks=len(tasks),cached_blocks=len(complete),completed_blocks=len(complete),
                 total_dates=sum(t['targets_count'] for t in tasks),cluster_active=False,
                 dashboard_url=None,status='EN_CURSO',eta_seconds=None,workers=[])
    started=time.monotonic(); initial_cost=sum(t['cost'] for t in complete)
    total_cost=sum(t['cost'] for t in tasks); finished_cost=initial_cost
    def update(client=None, active=None):
        elapsed=time.monotonic()-started
        done_dates=sum(t['targets_count'] for t in complete)
        # Solo las tareas actualmente activas pueden aportar avance parcial; ignorar intentos viejos.
        if active:
            for t in active.values():
                try:
                    p=json.loads(Path(t['output']).with_suffix('.progress.json').read_text(encoding='utf-8'))
                    done_dates+=min(int(p['done']),t['targets_count'])
                except (OSError,ValueError,KeyError): pass
        measured=finished_cost-initial_cost
        eta=(total_cost-finished_cost)/(measured/elapsed) if measured>0 and len(complete)-state['cached_blocks']>=2 else None
        if len(complete)==len(tasks): eta=0
        state.update(completed_blocks=len(complete),completed_dates=done_dates,
            percent=100*done_dates/state['total_dates'] if state['total_dates'] else 100,
            eta_seconds=eta,elapsed_seconds=elapsed,
            updated_at=datetime.now(timezone.utc).isoformat())
        if client:
            info=client.scheduler_info()['workers']
            state['workers']=[{'address':k,'status':v.get('status'),'memory_limit':v.get('memory_limit'),
                'memory_bytes':v.get('metrics',{}).get('memory'),'cpu_percent':v.get('metrics',{}).get('cpu'),
                'tasks':v.get('metrics',{}).get('task_counts',{})} for k,v in info.items()]
        atomic_json(progress,state)
        remaining='estimando' if eta is None else f'~{eta/60:.1f} min'
        print(f'D2 UNSPSC{prefix}: {len(complete)}/{len(tasks)} bloques; {state["percent"]:.1f}% fechas; restante nivel: {remaining}',flush=True)
    update()
    if not pending:
        state['status']='COMPLETADA';update();return
    active={}
    try:
        with dask.config.set({'distributed.worker.memory.target':.60,
                             'distributed.worker.memory.spill':.70,
                             'distributed.worker.memory.pause':.85,
                             'distributed.worker.memory.terminate':.95}):
            with LocalCluster(n_workers=runtime['workers'],threads_per_worker=1,processes=True,
                    memory_limit=f'{runtime["memory_mb_per_worker"]}MiB',host='127.0.0.1',
                    dashboard_address='127.0.0.1:0',local_directory=str(cache/'worker-spill')) as cluster:
                with Client(cluster) as client:
                    state.update(cluster_active=True,dashboard_url=client.dashboard_link,runtime=runtime)
                    print('Dashboard Dask: '+client.dashboard_link,flush=True)
                    queue=iter(sorted(pending,key=lambda t:t['cost'],reverse=True))
                    def submit():
                        while len(active)<runtime['workers']*2:
                            t=next(queue,None)
                            if t is None: break
                            Path(t['output']).with_suffix('.progress.json').unlink(missing_ok=True)
                            f=client.submit(compute_block,t,pure=False,retries=2)
                            active[f]=t
                    submit();update(client,active)
                    while active:
                        try: finished,_=wait(list(active),timeout=10,return_when='FIRST_COMPLETED')
                        except TimeoutError: finished=set()
                        for f in finished:
                            t=active.pop(f);f.result()
                            if not valid_block(t): raise ValueError('Checkpoint no válido después de calcular')
                            complete.append(t);finished_cost+=t['cost'];f.release()
                        submit();update(client,active)
                    client.retire_workers(workers=list(client.scheduler_info()['workers']),close_workers=True)
        state['status']='COMPLETADA'
    except BaseException as exc:
        state.update(status='FALLIDA',error_type=type(exc).__name__)
        raise
    finally:
        state['cluster_active']=False
        for worker in state.get('workers',[]): worker['status']='closed'
        update()


def build_references(c, work, cfg):
    """Solo el coordinador toca DuckDB; los workers leen Parquet y escriben bloques."""
    work=Path(work); runtime=runtime_config()
    c.execute("SET memory_limit='512MB'")
    atomic_json(work/'progreso_d2.json',{'status':'PREPARANDO','cluster_active':False,
        'updated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,
        'remaining_levels':list(cfg.d2_prefixes)})
    snapshot=work/'d2_entrada.parquet'
    tmp=snapshot.with_suffix('.tmp')
    c.execute(f"COPY (SELECT firma,unspsc_normalizado,tipo_normalizado,causal_normalizada,log_valor FROM economic ORDER BY ALL) TO {literal(tmp.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(tmp,snapshot)
    params={'input':digest(snapshot),'engine':digest(__file__),'months':cfg.d2_months,
        'minimum':cfg.d2_min_contracts,'prefixes':cfg.d2_prefixes,
        'block_days':runtime['block_days'],'buckets':runtime['buckets'],'large':runtime['large_group_rows']}
    key=hashlib.sha256(json.dumps(params,sort_keys=True).encode()).hexdigest()[:24]
    cache=work/'d2_blocks'/key;cache.mkdir(parents=True,exist_ok=True)
    state={'engine':ENGINE_VERSION,'cache':str(cache),'levels':list(cfg.d2_prefixes),'runtime':runtime}
    # Progreso público en work, independiente del directorio con identidad de caché.
    c.execute('CREATE OR REPLACE TEMP TABLE unresolved AS SELECT * FROM economic')
    refs=[]
    for priority,prefix in enumerate(cfg.d2_prefixes):
        atomic_json(work/'progreso_d2.json',{'status':'PREPARANDO','level':prefix,
            'cluster_active':False,'runtime':runtime,'remaining_levels':list(cfg.d2_prefixes[priority+1:]),
            'updated_at':datetime.now(timezone.utc).isoformat()})
        level=cache/str(prefix);level.mkdir(exist_ok=True)
        gid=f"sha256(to_json(list_value(left(unspsc_normalizado,{prefix}),tipo_normalizado,causal_normalizada)))"
        c.execute(f'CREATE OR REPLACE TEMP VIEW level_source AS SELECT *,{gid} gid FROM economic')
        c.execute(f'CREATE OR REPLACE TEMP TABLE targets AS SELECT DISTINCT {gid} gid,firma FROM unresolved')
        c.execute(f'''CREATE OR REPLACE TEMP TABLE grouped AS SELECT gid,firma,log_valor,count(*)::BIGINT peso
            FROM level_source WHERE gid IN (SELECT gid FROM targets) GROUP BY ALL''')
        # Windows: no usar el SHA de 64 caracteres como carpeta de partición.
        # dense_rank asigna rutas cortas sin colisiones; gid completo permanece en los datos.
        c.execute(f'''CREATE OR REPLACE TEMP TABLE group_sizes AS SELECT gid,sum(peso) n,count(*) compact_rows,
            CASE WHEN sum(peso)>{runtime['large_group_rows']} THEN 'g_'||cast(dense_rank() OVER (ORDER BY gid) AS VARCHAR) ELSE 'small_'||cast(hash(gid)%{runtime['buckets']} AS VARCHAR) END bucket
            FROM grouped GROUP BY gid''')
        plan=c.execute('SELECT t.gid,t.firma,s.bucket,s.n,s.compact_rows FROM targets t JOIN group_sizes s USING(gid) ORDER BY s.bucket,t.gid,t.firma').fetchall()
        input_dir=level/'input'
        marker=level/'input.json'
        prepared=False
        if marker.exists():
            try:
                meta=json.loads(marker.read_text(encoding='utf-8'))
                input_dir=level/meta.get('directory','input')
                prepared=meta['key']==key and all((input_dir/p).exists() and digest(input_dir/p)==h for p,h in meta['files'].items())
            except (OSError,ValueError,KeyError): pass
        if not prepared and plan:
            # Un directorio nuevo evita confundir particiones antiguas de una escritura fallida.
            input_dir=level/('i_'+uuid.uuid4().hex[:8])
            c.execute(f'''COPY (SELECT g.*,s.bucket FROM grouped g JOIN group_sizes s USING(gid) ORDER BY bucket,gid,firma)
                TO {literal(input_dir.as_posix())} (FORMAT PARQUET, PARTITION_BY(bucket), COMPRESSION ZSTD)''')
            atomic_json(marker,{'key':key,'directory':input_dir.name,
                'files':{p.relative_to(input_dir).as_posix():digest(p) for p in input_dir.rglob('*.parquet')}})
        elif prepared:
            input_dir=level/meta.get('directory','input')
        bybucket={}
        for group,day,bucket,n,compact in plan:
            bybucket.setdefault(bucket,{}).setdefault(group,{'days':[],'n':n,'compact':compact})['days'].append(day.isoformat())
        tasks=[]
        for bucket,groups in bybucket.items():
            inputs=[str(p) for p in (input_dir/('bucket='+bucket)).glob('*.parquet')]
            if not inputs: raise ValueError('Partición D2 sin archivo de entrada')
            batches=[]
            if bucket.startswith('small_'):
                batches=[{g:info['days'] for g,info in groups.items()}]
            else:
                for g,info in groups.items():
                    batches.extend({g:info['days'][i:i+runtime['block_days']]} for i in range(0,len(info['days']),runtime['block_days']))
            for targets in batches:
                task_key=hashlib.sha256(json.dumps([key,prefix,bucket,targets],sort_keys=True).encode()).hexdigest()
                tasks.append(dict(key=task_key,inputs=inputs,targets=targets,targets_count=sum(map(len,targets.values())),
                    months=cfg.d2_months,minimum=cfg.d2_min_contracts,output=str(level/(task_key[:24]+'.parquet')),
                    cost=sum(len(days)*groups[g]['compact'] for g,days in targets.items())))
        state['remaining_levels']=list(cfg.d2_prefixes[priority+1:])
        run_tasks(tasks,cache,runtime,prefix,state,work/'progreso_d2.json')
        # Copiar el heartbeat más reciente a una ruta estable para monitores externos.
        atomic_json(work/'progreso_d2.json',state)
        name=f'd2_ref_{prefix}'
        if tasks:
            paths='['+','.join(literal(t['output']) for t in tasks)+']'
            c.execute(f'''CREATE OR REPLACE TABLE {name} AS SELECT e.id_contrato,{priority} prioridad,
                concat('UNSPSC{prefix}:',left(e.unspsc_normalizado,{prefix}),'|',e.tipo_normalizado,'|',e.causal_normalizada) grupo_comparacion,
                r.* EXCLUDE(gid,firma) FROM level_source e JOIN read_parquet({paths}) r ON e.gid=r.gid AND e.firma=r.firma''')
        else:
            c.execute(f'''CREATE OR REPLACE TABLE {name} AS SELECT id_contrato,{priority} prioridad,''::VARCHAR grupo_comparacion,
                NULL::BIGINT n_contratos_comparables,NULL::DOUBLE mediana_grupo,NULL::DOUBLE mad_grupo,NULL::DOUBLE p95_log_grupo,
                NULL::DATE referencia_d2_min,NULL::DATE referencia_d2_max FROM economic WHERE false''')
        refs.append('SELECT * FROM '+name)
        c.execute(f'''CREATE OR REPLACE TEMP TABLE unresolved AS SELECT e.* FROM unresolved e
            LEFT JOIN {name} r USING(id_contrato) WHERE coalesce(r.n_contratos_comparables,0)<{cfg.d2_min_contracts}''')
    return refs
