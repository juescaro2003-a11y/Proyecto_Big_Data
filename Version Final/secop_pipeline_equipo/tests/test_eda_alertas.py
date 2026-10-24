import json
import unittest
from pathlib import Path
from unittest.mock import patch

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import test_alertas as fixtures
from secop_pipeline import alertas as a,alertas_dask as d,alertas_cache as cache


class EdaIntegrationTests(unittest.TestCase):
    def test_new_eda_sections_and_small_cache(self):
        fixture=fixtures.AnalyticsTests();fixture.setUp()
        try:
            rows=[]
            for year in range(2022,2026):
                for i in range(10):
                    month=12 if i<(5 if year==2025 else 1) else 1
                    rows.append(fixtures.contract(f'{year}-{i}',provider=f'P{i%3}',value=100+i,day=f'{year}-{month:02d}-10'))
            fixture.setup_data(rows)
            def serial(tasks,*args):
                for t in tasks:d.compute_block(t)
            with patch.object(d,'run_tasks',serial): result=fixture.finish()
            self.assertTrue(cache.stage_available(fixture.work,'calculate_year_end_accumulation'))
            with a.connect(fixture.work,fixture.cfg) as con:
                con.execute('CREATE VIEW alertas AS SELECT * FROM classified')
                def query(sql):return con.execute(sql).df()
                monthly=query("SELECT date_trunc('month',firma) mes,count(*) contratos FROM universe GROUP BY 1").set_index('mes')
                namespace={'pd':pd,'np':np,'plt':plt,'con':con,'query':query,'lit':a.lit,
                    'display':lambda *args:None,'conclusion':lambda text:None,'monthly':monthly,
                    'audit':{'parametros':fixture.cfg.evidence()},'ALERTAS_LISTAS':True,
                    'folder':fixture.work,'report':{'parametros':fixture.cfg.evidence(),'resumen':result}}
                nb=json.loads((Path(__file__).resolve().parents[1]/'notebooks/10_inventario_trazabilidad_eda.ipynb').read_text(encoding='utf-8'))
                selected=[c for c in nb['cells'] if c['cell_type']=='code' and (''.join(c['source']).startswith('season =') or ''.join(c['source']).startswith('if ALERTAS_LISTAS:'))]
                self.assertEqual(len(selected),3)
                for cell in selected:exec(''.join(cell['source']),namespace)
                self.assertEqual(len(namespace['years']),4)
                self.assertEqual(namespace['eligible'].alerta_entidad_anio.sum(),1)
                self.assertEqual(len(namespace['association']),1)
            (fixture.work/'d3_entidad_anio.parquet').unlink()
            self.assertFalse(cache.stage_available(fixture.work,'calculate_year_end_accumulation'))
        finally:
            plt.close('all');fixture.tearDown()

if __name__=='__main__':unittest.main()
