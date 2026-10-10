"""Integración real de los dos clústeres sobre archivos exclusivamente sintéticos."""
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from secop_pipeline.config import ROOT
from secop_pipeline.control_config import ControlConfig
from secop_pipeline.transform import transform_dataframe
from secop_pipeline import dask_jobs
from test_control import contract


class Pipeline(unittest.TestCase):
    def test_two_clusters_publish_synthetic(self):
        # Temporal dentro del proyecto: publish_directory verifica ese límite.
        with tempfile.TemporaryDirectory(prefix='.test-ircd-',dir=ROOT) as folder:
            root=Path(folder); silver=root/'silver'; gold=root/'gold'
            (silver/'contratos').mkdir(parents=True);gold.mkdir()
            raw=pd.DataFrame([contract('A'),contract('B')]).astype('string')
            clean=transform_dataframe(raw,'contratos')
            self.assertEqual(clean.iloc[0]['raw__valor_del_contrato'],'1000.0')
            clean.to_parquet(silver/'contratos'/'part.parquet',index=False)
            audit={}
            with patch.object(dask_jobs,'SILVER_DIR',silver),patch.object(dask_jobs,'GOLD_DIR',gold),patch.object(dask_jobs,'OUTPUT_DIR',root/'salida'):
                result=dask_jobs.build_gold(audit=audit,control_config=ControlConfig())
            self.assertEqual(len(pd.read_parquet(result/'contratos.parquet')),2)
            self.assertTrue(audit['publicado'])
            self.assertEqual([p['workers_observados'] for p in audit['profiles']],[2,1])
            self.assertNotEqual(audit['profiles'][0]['memoria_por_worker_GiB'],audit['profiles'][1]['memoria_por_worker_GiB'])
            self.assertTrue(audit['conservacion_contratos'])
            # Ejecutar las celdas del tablero contra la salida sintética.
            import matplotlib
            matplotlib.use('Agg')
            from secop_pipeline import config as project_config
            from secop_pipeline.audit import notebook
            nb=json.loads((ROOT/'notebooks'/'06_control_contratacion_directa.ipynb').read_text(encoding='utf-8'))
            scope={}
            with patch.object(project_config,'GOLD_DIR',gold),patch('IPython.display.display'),patch('matplotlib.pyplot.show'):
                for cell in nb['cells']:
                    if cell['cell_type']=='code':
                        exec(compile(''.join(cell['source']),'notebook_control','exec'),scope)
            scope['con'].close()
            import matplotlib.pyplot as plt
            plt.close('all')
            report=notebook({'run_id':'synthetic','status':'COMPLETADA','started_at':'2026-01-01',
                'parameters':{},'code':[], 'stages':[{'name':'gold','status':'COMPLETADA',
                'result':{'audit':audit}}]})
            self.assertIn('Gold contractual publicado',json.dumps(report,ensure_ascii=False))


if __name__=='__main__':
    unittest.main()
