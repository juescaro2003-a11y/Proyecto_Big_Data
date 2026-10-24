import itertools
import json
import math
from pathlib import Path
import tempfile
import unittest
from datetime import date,timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from secop_pipeline import alertas as a
from secop_pipeline.alertas_aws import ddl_for


def contract(i,provider='P1',value=100,day='2025-06-01',category='v1.81101500',entity='E1',**changes):
    row={k:None for k in a.SOURCE_FIELDS}
    row.update(id_contrato=str(i),codigo_entidad=entity,nit_entidad=entity,codigo_proveedor=provider,
               documento_proveedor=provider,tipodocproveedor='NIT',nombre_entidad='Entidad',proveedor_adjudicado='Proveedor',
               modalidad_de_contratacion=' CONTRATACIÓN   DIRECTA ',justificacion_modalidad_de='Servicios profesionales',
               tipo_de_contrato='Prestación de servicios',codigo_de_categoria_principal=category,
               fecha_de_firma=day,valor_del_contrato=float(value) if value is not None else None,
               fecha_corte='2025-12-31')
    row.update(changes)
    return row


class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='alertas-test-',dir=Path(__file__).resolve().parent)
        self.root=Path(self.temp.name)
        self.work=self.root/'work'
        self.source=self.root/'gold.parquet'
        self.cfg=a.AlertConfig(cutoff='2025-12-31',coverage_start='2020-01-01',d1_min_contracts=3,
            d1_min_suppliers=2,d2_min_contracts=3,d3_min_contracts=2,d3_min_years=2,memory_mb=512)

    def tearDown(self):
        if self.root.resolve().parent != Path(__file__).resolve().parent:
            raise RuntimeError('Temporal fuera del directorio de pruebas')
        self.temp.cleanup()

    def setup_data(self,rows):
        pq.write_table(pa.Table.from_pylist(rows),self.source)
        self.original=a.source_fingerprint(self.source)
        a.extract_gold(self.work,self.cfg,self.source)
        a.filter_direct_contracting(self.work,self.cfg)
        return a.validate_gold(self.work,self.cfg)

    def sql(self,query):
        with a.connect(self.work,self.cfg) as c: return c.execute(query).fetchall()

    def finish(self):
        a.calculate_concentration(self.work,self.cfg)
        a.calculate_economic_anomalies(self.work,self.cfg)
        a.calculate_year_end_accumulation(self.work,self.cfg)
        a.calculate_score(self.work,self.cfg)
        result=a.classify_contracts(self.work,self.cfg,'test-flow',self.original)
        self.assertEqual(self.original,a.source_fingerprint(self.source))
        return result

    def test_normalization_and_exact_modality(self):
        rows=[contract('ok'),contract('variant',modalidad_de_contratacion='Contratación directa con ofertas'),
              contract('other',modalidad_de_contratacion='Licitación pública'),contract('invalid',category='8110150'),
              contract('numeric',category='81101500'),contract('zero',category='00000000')]
        self.setup_data(rows)
        self.assertEqual(self.sql('SELECT id_contrato,segmento_unspsc FROM universe ORDER BY id_contrato'),
                         [('invalid',None),('numeric','81'),('ok','81'),('zero',None)])

    def test_duplicates_fail_without_dropping(self):
        with self.assertRaisesRegex(ValueError,'duplicados'):
            self.setup_data([contract('x'),contract('x')])

    def test_conflicting_identity_is_not_scored(self):
        self.setup_data([contract(1,documento_proveedor='A'),contract(2,documento_proveedor='B')])
        self.assertEqual(self.sql('SELECT bool_or(proveedor_consistente) FROM universe')[0][0],False)

    def test_d1_includes_today_excludes_future_and_old(self):
        self.setup_data([contract(1,'A',80),contract(2,'B',10),contract(3,'C',10),
                         contract(4,'B',10000,day='2025-06-02'),contract(5,'A',10000,day='2023-01-01')])
        a.calculate_concentration(self.work,self.cfg)
        p,n,flag=self.sql("SELECT participacion_proveedor,n_contratos_entidad_365d,d1_concentracion FROM d1 WHERE id_contrato='1'")[0]
        self.assertAlmostEqual(p,.8);self.assertEqual(n,3);self.assertEqual(flag,1)

    def test_d1_coverage_and_invalid_amount(self):
        self.setup_data([contract(1,'A',40),contract(2,'B',40),contract(3,None,20)])
        a.calculate_concentration(self.work,self.cfg)
        self.assertEqual(self.sql("SELECT motivo_d1 FROM d1 WHERE id_contrato='1'")[0][0],'baja_cobertura_proveedores')

    def test_d2_mad_correct_and_no_self_same_day_future(self):
        rows=[contract(i,value=math.exp(x),day=f'2025-01-0{i+1}') for i,x in enumerate((1,2,3))]
        rows += [contract('target',value=math.exp(10),day='2025-06-01'),
                 contract('peer',value=math.exp(20),day='2025-06-01'),contract('future',value=math.exp(30),day='2025-06-02')]
        self.setup_data(rows)
        a.calculate_economic_anomalies(self.work,self.cfg)
        median,mad,n,z=self.sql("SELECT mediana_grupo,mad_grupo,n_contratos_comparables,zscore_robusto FROM d2 WHERE id_contrato='target'")[0]
        self.assertAlmostEqual(median,2);self.assertAlmostEqual(mad,1);self.assertEqual(n,3)
        self.assertAlmostEqual(z,8/1.4826)

    def test_d2_mad_zero_p95_fallback(self):
        self.setup_data([contract(i,value=100,day='2025-01-01') for i in range(3)]+[contract('target',value=200)])
        a.calculate_economic_anomalies(self.work,self.cfg)
        method,z,flag=self.sql("SELECT metodo_comparacion,zscore_robusto,d2_valor_atipico FROM d2 WHERE id_contrato='target'")[0]
        self.assertEqual(method,'P95_LOG_MAD_CERO');self.assertIsNone(z);self.assertEqual(flag,1)

    def test_d2_hierarchy_preserves_causal(self):
        rows=[contract(i,category='81101600',day='2025-01-01',value=100+i) for i in range(3)]
        rows += [contract('target',category='81101500'),contract('other',category='81101500',justificacion_modalidad_de='otra causal')]
        self.setup_data(rows);a.calculate_economic_anomalies(self.work,self.cfg)
        self.assertTrue(self.sql("SELECT grupo_comparacion FROM d2 WHERE id_contrato='target'")[0][0].startswith('UNSPSC4'))
        self.assertIsNone(self.sql("SELECT d2_valor_atipico FROM d2 WHERE id_contrato='other'")[0][0])

    def test_d3_december_compared_to_prior_entity_years(self):
        rows=[]
        for year in (2022,2023,2024,2025):
            for i in range(10):
                month=12 if i < (5 if year==2025 else 1) else 1
                rows.append(contract(f'{year}-{i}',day=f'{year}-{month:02d}-10'))
        self.setup_data(rows);a.calculate_year_end_accumulation(self.work,self.cfg)
        share,n,flag,last=self.sql("SELECT participacion_diciembre,n_anios_referencia_d3,alerta_d3,ultimo_anio_referencia_d3 FROM d3 WHERE id_contrato='2025-0'")[0]
        self.assertAlmostEqual(share,.5);self.assertEqual((n,flag,last),(3,1,2024))
        self.assertEqual(self.sql("SELECT alerta_d3 FROM d3 WHERE id_contrato='2025-9'")[0][0],0)
        self.assertEqual(self.sql('SELECT count(*) FROM d3')[0][0],40)

    def test_d3_partial_year_stays_unknown(self):
        from dataclasses import replace
        self.cfg=replace(self.cfg,cutoff='2026-09-21')
        rows=[contract(f'{year}-{i}',day=f'{year}-01-10',fecha_corte='2026-09-21')
              for year in (2022,2023,2024,2025,2026) for i in range(3)]
        self.setup_data(rows);a.calculate_year_end_accumulation(self.work,self.cfg)
        self.assertEqual(self.sql("SELECT alerta_d3,motivo_d3 FROM d3 WHERE id_contrato='2026-0'")[0],(None,'anio_incompleto'))

    def test_all_missing_and_small_groups(self):
        self.setup_data([contract('x',provider=None,entity=None,value=None,day=None,category=None)])
        self.finish()
        self.assertEqual(self.sql('SELECT score_total,dimensiones_no_evaluables,clasificacion,alerta_binaria FROM classified')[0],(0,3,'NO_EVALUABLE',None))

    def test_all_27_classifications(self):
        for values in itertools.product((None,0,1),repeat=3):
            s,m,label=a.classify_values(values)
            possibilities={sum(v if v is not None else fill[i] for i,v in enumerate(values))>=2 for fill in itertools.product((0,1),repeat=3)}
            expected='ALERTA' if possibilities=={True} else 'NO_ALERTA' if possibilities=={False} else 'NO_EVALUABLE'
            self.assertEqual(label,expected,(values,s,m))

    def test_output_schema_and_scenarios(self):
        self.setup_data([contract(i,provider='P'+str(i%3),value=100+i,day='2025-01-01' if i<6 else '2025-06-01') for i in range(9)])
        result=self.finish()
        self.assertEqual(result['contratos_resultado'],9)
        scenarios=a.evaluate_scenarios(self.work,self.cfg)
        self.assertEqual(len(scenarios),27)
        ddl=ddl_for(self.work/'resultados.parquet')
        self.assertIn('UNIQUE (id_contrato, fecha_corte, version_modelo)',ddl)
        self.assertNotIn('DROP TABLE',ddl)

    def test_invalid_config(self):
        with self.assertRaises(ValueError): a.AlertConfig(cutoff='2025-12-31',reference_cutoff='2025-12-31')
        with self.assertRaises(ValueError): a.AlertConfig(cutoff='2025-12-31',d1_threshold=2)

    def test_d1_threshold_is_strict(self):
        self.setup_data([contract(1,'A',40),contract(2,'B',30),contract(3,'C',30)])
        a.calculate_concentration(self.work,self.cfg)
        self.assertEqual(self.sql("SELECT d1_concentracion FROM d1 WHERE id_contrato='1'")[0][0],0)

    def test_d3_insufficient_reference_keeps_null(self):
        self.setup_data([contract(1,'A'),contract(2,'A')])
        a.calculate_year_end_accumulation(self.work,self.cfg)
        self.assertEqual(self.sql("SELECT alerta_d3,umbral_d3,motivo_d3 FROM d3 WHERE id_contrato='1'")[0],
                         (None,None,'historia_entidad_insuficiente'))

    def test_flow_cache_wiring_without_network(self):
        from unittest.mock import patch
        from contextlib import ExitStack
        import logging
        from flows import alertas_flow as f
        rows=[contract(i,provider='P'+str(i%3),value=100+i) for i in range(9)]
        pq.write_table(pa.Table.from_pylist(rows),self.source)
        config=self.root/'config.json'
        config.write_text(json.dumps(self.cfg.evidence()),encoding='utf-8')
        names=['extract_gold','filter_direct_contracting','validate_gold','calculate_concentration',
               'calculate_economic_anomalies','calculate_year_end_accumulation','calculate_score','classify_contracts']
        with ExitStack() as stack:
            stack.enter_context(patch.object(f,'ROOT',self.root))
            stack.enter_context(patch.object(f,'get_run_logger',return_value=logging.getLogger('test')))
            for name in names: stack.enter_context(patch.object(f,name,getattr(f,name).fn))
            result=f.alertas_flow.fn(config_path=config,source=self.source)
            output=Path(result['directorio'])/'resultados.parquet'
            before=output.stat().st_mtime_ns
            again=f.alertas_flow.fn(config_path=config,source=self.source)
            self.assertEqual(output.stat().st_mtime_ns,before)
            self.assertEqual(again['estado'],'COMPLETADA')
            self.assertEqual(again['aws']['estado'],'NO_SOLICITADA')
            self.assertFalse((self.root/'data/gold/contratos_control/contratos.parquet').exists())

    def test_missing_aws_password_fails_before_network(self):
        import os
        from unittest.mock import patch
        from secop_pipeline import alertas_aws
        with patch.dict(os.environ,{'SECOP_RDS_PASSWORD':''}),patch.object(alertas_aws,'load_dotenv'):
            with self.assertRaisesRegex(RuntimeError,'SECOP_RDS_PASSWORD'):
                alertas_aws.rds_engine()


if __name__=='__main__': unittest.main()
