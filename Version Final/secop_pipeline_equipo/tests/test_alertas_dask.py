import json
import tempfile
import unittest
from unittest.mock import patch
from dataclasses import replace
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from secop_pipeline import alertas_dask as d
from secop_pipeline import alertas as a
from secop_pipeline import alertas_cache as cache


class ExactDailyTests(unittest.TestCase):
    def test_threshold_change_reuses_statistical_blocks(self):
        with tempfile.TemporaryDirectory(prefix='d2-resume-') as folder:
            work=Path(folder);cfg=a.AlertConfig(cutoff='2025-12-31',coverage_start='2020-01-01',d2_min_contracts=1)
            rows=[]
            for i in range(4):
                row={k:None for k in a.SOURCE_FIELDS}
                row.update(id_contrato=str(i),codigo_proveedor='P',codigo_entidad='E',nit_entidad='E',documento_proveedor='P',
                    modalidad_de_contratacion='contratacion directa',tipo_de_contrato='servicio',justificacion_modalidad_de='causal',
                    codigo_de_categoria_principal='81101500',valor_del_contrato=float(i+10),fecha_de_firma=f'2025-01-0{i+1}')
                rows.append(row)
            source=work/'gold.parquet';pq.write_table(pa.Table.from_pylist(rows),source)
            a.extract_gold(work,cfg,source);a.filter_direct_contracting(work,cfg);a.validate_gold(work,cfg)
            def serial(tasks,*args):
                for t in tasks:d.compute_block(t)
            with patch.object(d,'run_tasks',serial):
                a.calculate_economic_anomalies(work,cfg)
                files={p:p.stat().st_mtime_ns for p in work.glob('d2_blocks/*/*/*.parquet')}
                self.assertTrue(files)
                a.calculate_economic_anomalies(work,replace(cfg,d2_threshold=4))
                self.assertEqual(files,{p:p.stat().st_mtime_ns for p in work.glob('d2_blocks/*/*/*.parquet')})

    def test_equivalent_to_sql_with_weights_leap_boundaries_and_future(self):
        rng=np.random.default_rng(23)
        days=np.array(['2022-02-28','2023-02-28','2024-02-29','2024-03-01','2024-05-01','2025-02-28','2025-03-01'],dtype='datetime64[D]')
        dates=np.repeat(days,15)
        values=rng.choice([0.,1.,2.,2.5,4.,20.],len(dates))
        weights=rng.integers(1,8,len(dates),dtype=np.int64)
        rows=[{'day':dt.astype(object),'x':float(x)} for dt,x,w in zip(dates,values,weights) for _ in range(int(w))]
        c=duckdb.connect();c.register('source',pa.Table.from_pylist(rows))
        got=list(d.daily_references('g',dates,values,weights,[str(x) for x in days],24,1))
        for row in got:
            expected=c.execute('SELECT count(*),median(x),mad(x),quantile_cont(x,.95),min(day),max(day) FROM source WHERE day>=?::DATE-INTERVAL 24 MONTHS AND day<?::DATE',[row['firma'],row['firma']]).fetchone()
            self.assertEqual(row['n_contratos_comparables'],expected[0])
            for key,val in zip(['mediana_grupo','mad_grupo','p95_log_grupo'],expected[1:4]):
                if val is None:self.assertIsNone(row[key])
                else:self.assertAlmostEqual(row[key],val,places=12)
            self.assertEqual((row['referencia_d2_min'],row['referencia_d2_max']),expected[4:])
        c.close()

    def test_stage_signatures_separate_resources_and_thresholds(self):
        cfg=a.AlertConfig(cutoff='2025-12-31')
        x=cache.signatures(a,cfg,'source')
        y=cache.signatures(a,replace(cfg,memory_mb=4096,threads=2),'source')
        self.assertEqual(x,y)
        z=cache.signatures(a,replace(cfg,d2_threshold=4),'source')
        self.assertEqual(x['calculate_concentration'],z['calculate_concentration'])
        self.assertEqual(x['validate_gold'],z['validate_gold'])
        self.assertNotEqual(x['calculate_economic_anomalies'],z['calculate_economic_anomalies'])

    def test_two_process_workers_checkpoint_and_corruption_recovery(self):
        with tempfile.TemporaryDirectory(prefix='d2-test-') as folder:
            root=Path(folder)
            inp=root/'input.parquet'
            pq.write_table(pa.Table.from_pylist([{'gid':'g','firma':date(2024,1,1),'log_valor':2.,'peso':10},
                {'gid':'g','firma':date(2024,2,1),'log_valor':3.,'peso':5}]),inp)
            tasks=[]
            for i,day in enumerate(['2024-03-01','2024-04-01']):
                tasks.append(dict(key=str(i),inputs=[str(inp)],targets={'g':[day]},targets_count=1,
                    months=24,minimum=3,output=str(root/f'{i}.parquet'),cost=2))
            runtime={'workers':2,'memory_mb_per_worker':768,'threads_per_worker':1}
            state={}
            dashboard_checked=[]
            writer=d.atomic_json
            def observe(path,data):
                writer(path,data)
                if data.get('cluster_active') and data.get('dashboard_url') and not dashboard_checked:
                    from urllib.request import urlopen
                    with urlopen(data['dashboard_url'],timeout=10) as response:
                        dashboard_checked.append(response.status)
            with patch.object(d,'atomic_json',observe):
                d.run_tasks(tasks,root,runtime,8,state,root/'progreso.json')
            self.assertEqual(dashboard_checked,[200])
            self.assertTrue(all(d.valid_block(t) for t in tasks))
            self.assertEqual(state['percent'],100)
            self.assertIsNotNone(state['dashboard_url'])
            self.assertFalse(state['cluster_active'])
            stamps=[Path(t['output']).stat().st_mtime_ns for t in tasks]
            d.run_tasks(tasks,root,runtime,8,state,root/'progreso.json')
            self.assertEqual(state['cached_blocks'],2)
            self.assertEqual(stamps,[Path(t['output']).stat().st_mtime_ns for t in tasks])
            Path(tasks[0]['output']).write_bytes(b'broken')
            self.assertFalse(d.valid_block(tasks[0]))
            d.compute_block(tasks[0])
            self.assertTrue(d.valid_block(tasks[0]))

    def test_legacy_cache_adoption_checks_counts_and_source(self):
        with tempfile.TemporaryDirectory(prefix='d2-legacy-') as folder:
            out=Path(folder);work=out/'old';work.mkdir()
            cfg=a.AlertConfig(cutoff='2025-12-31')
            with duckdb.connect(str(work/'analitica.duckdb')) as c:
                for table in ('source','universe','d1'):
                    c.execute(f"CREATE TABLE {table} AS SELECT 'a' id_contrato")
            stages={k:{'estado':'COMPLETADA'} for k in ('extract_gold','filter_direct_contracting','validate_gold','calculate_concentration')}
            stages['extract_gold']['resultado']={'total_gold':1}
            stages['validate_gold']['resultado']={'contratos_universo':1}
            m={'huella_gold':'abc','huella_codigo':cache.LEGACY_HASH,'parametros':cfg.evidence(),'etapas':stages}
            (work/'ejecucion.json').write_text(json.dumps(m),encoding='utf-8')
            self.assertEqual(cache.choose_work(out,'abc',cfg),(work,True))
            self.assertFalse(cache.choose_work(out,'other',cfg)[1])
            m['cache_schema']=2;m['huella_codigo']='new'
            (work/'ejecucion.json').write_text(json.dumps(m),encoding='utf-8')
            self.assertEqual(cache.choose_work(out,'abc',cfg),(work,False))


if __name__=='__main__': unittest.main()
