"""Aceptación sintética. No descarga ni modifica PostgreSQL o datos publicados."""
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import pandas as pd

from secop_pipeline.control import normalize_contracts, score_concentration, score_recurrence, score_value, compose
from secop_pipeline.control_config import ControlConfig
from secop_pipeline.control_analytics import analyze_contracts


def contract(id='C1', **kwargs):
    row = dict(id_contrato=id,nit_entidad='800.123.456-7',documento_proveedor='123456',
        tipodocproveedor='CC', codigo_proveedor='P1', modalidad_de_contratacion='Contratación directa',
        justificacion_modalidad_de='Servicios profesionales',tipo_de_contrato='Prestación de servicios',
        codigo_de_categoria_principal='80111600',departamento='Bogotá',valor_del_contrato=1000.,
        fecha_de_firma='2025-06-01',fecha_de_inicio_del_contrato='2025-06-01',
        fecha_de_fin_del_contrato='2025-06-30',ultima_actualizacion='2026-01-01')
    row.update(kwargs)
    if 'codigo_proveedor' not in kwargs:
        row['codigo_proveedor']='P'+str(row['documento_proveedor'])
    return row


def execute(rows, cfg=None):
    with tempfile.TemporaryDirectory() as folder:
        path=Path(folder); prepared=path/'in'; prepared.mkdir()
        normalize_contracts(pd.DataFrame(rows)).to_parquet(prepared/'part.parquet',index=False)
        evidence=analyze_contracts(prepared,path/'out',cfg or ControlConfig())
        return pd.read_parquet(path/'out'/'contratos.parquet'), evidence


class Scores(unittest.TestCase):
    def test_concentration_boundaries(self):
        self.assertEqual([score_concentration(v,.3,.5) for v in (.2,.3,.5)], [0,1,2])
        self.assertIsNone(score_concentration(.2,None,.5))

    def test_recurrence(self):
        self.assertEqual(score_recurrence(1,1,3),0)
        self.assertEqual(score_recurrence(3,2,3),2)
        self.assertIsNone(score_recurrence(3,1,1))

    def test_value_ratio(self):
        self.assertEqual(score_value(120,100,110,120),1)
        self.assertEqual(score_value(170,100,110,120),2)
        self.assertIsNone(score_value(170,None,110,120))

    def test_compose(self):
        self.assertEqual(compose(2,1,2),(5,'completo','revision prioritaria'))
        self.assertEqual(compose(2,None,0)[0:2],(None,'incompleto'))

    def test_nit_unknown(self):
        row=normalize_contracts(pd.DataFrame([contract(tipodocproveedor='NIT')])).iloc[0]
        self.assertEqual(row.clase_persona,'indeterminado')

    def test_config_validation(self):
        with self.assertRaises(ValueError): ControlConfig(person_filter='juridica')

    def test_silver_semantics_and_decimal(self):
        from secop_pipeline.transform import transform_dataframe
        df=pd.DataFrame([contract(valor_del_contrato='1.234',estado_contrato='no aplica')]).astype('string')
        out=transform_dataframe(df,'contratos')
        self.assertEqual(out.iloc[0].valor_del_contrato,1.234)
        self.assertEqual(out.iloc[0].estado_contrato,'no aplica')
        self.assertEqual(out.iloc[0]['raw__valor_del_contrato'],'1.234')


class Integration(unittest.TestCase):
    def test_filters_dedup_window_entities_and_cutoff(self):
        rows=[contract('OLD',fecha_de_firma='2023-01-01'),contract('A',fecha_de_firma='2025-01-01'),
              contract('B',fecha_de_firma='2025-03-01'),contract('C'),contract('C'),
              contract('OTHER',nit_entidad='9001'),contract('FUTURE',fecha_de_firma='2027-01-01'),
              contract('LIC',modalidad_de_contratacion='Licitación pública')]
        df,ev=execute(rows)
        self.assertEqual(len(df),5)
        self.assertEqual(df.set_index('id_contrato').loc['C','recurrencia'],3)
        self.assertEqual(df.set_index('id_contrato').loc['OTHER','recurrencia'],1)
        self.assertEqual(ev['exclusiones'],{'duplicado_exacto':1,'posterior_al_corte':1,'otra_modalidad':1})
        self.assertTrue(df.ircd.isna().all())

    def test_latest_and_ambiguous(self):
        rows=[contract('A',valor_del_contrato=200,ultima_actualizacion='2024-01-01'), contract('A'),
              contract('B'),contract('B',valor_del_contrato=1001),contract(None)]
        df,ev=execute(rows)
        self.assertEqual(list(df.id_contrato),['A'])
        self.assertEqual(df.iloc[0].valor_del_contrato,1000)
        self.assertEqual(ev['exclusiones']['version_ambigua'],2)
        self.assertEqual(ev['exclusiones']['sin_id_contrato'],1)

    def test_person_filter_same_denominator(self):
        rows=[contract('A'),contract('B',tipodocproveedor='NIT')]
        df,ev=execute(rows,ControlConfig(person_filter='natural'))
        self.assertEqual(len(df),1)
        self.assertEqual(df.iloc[0].denominador_entidad,1000)

    def test_no_monthly_assumption(self):
        df,_=execute([contract()])
        self.assertEqual(df.iloc[0].motivo_valor_atipico,'comparabilidad_mensual_no_validada')
        self.assertTrue(pd.isna(df.iloc[0].score_valor_atipico))

    def test_missing_dates(self):
        df,_=execute([contract(fecha_de_fin_del_contrato=None)])
        self.assertEqual(df.iloc[0].motivo_valor_atipico,'faltan_fechas')
        self.assertEqual(df.iloc[0].estado_indice,'incompleto')

    def test_window_boundaries_same_day(self):
        rows=[contract('A',fecha_de_firma='2024-06-01'),contract('B',fecha_de_firma='2024-06-02'),
              contract('C',fecha_de_firma='2025-06-02'),contract('D',fecha_de_firma='2025-06-02')]
        df,_=execute(rows)
        self.assertEqual(df.set_index('id_contrato').loc['C','recurrencia'],3)
        self.assertEqual(df.set_index('id_contrato').loc['D','recurrencia'],3)

    def test_calibration_and_scores(self):
        rows=[]
        for ent in range(3):
            for i in range(32):
                rows.append(contract(f'H{ent}-{i}',nit_entidad=str(800+ent),
                    documento_proveedor=str(100+i),valor_del_contrato=100+i*10,
                    fecha_de_firma='2024-06-01'))
        rows += [contract(f'T{i}',nit_entidad='800',documento_proveedor=str(100+i),valor_del_contrato=100+i*10) for i in range(32)]
        cfg=ControlConfig(monthly_validated_categories=('80111600',),accept_recurrence_ties=True)
        df,ev=execute(rows,cfg)
        targets=df[df.id_contrato.str.startswith('T')]
        self.assertTrue(targets.ircd.notna().all())
        self.assertTrue(df[df.id_contrato.str.startswith('H')].ircd.isna().all())
        for _,r in targets.iterrows():
            self.assertEqual(r.score_concentracion,score_concentration(r.concentracion,r.p90_c,r.p95_c))
            self.assertEqual(r.score_recurrencia,score_recurrence(r.recurrencia,r.p90_r,r.p95_r,True))
            self.assertEqual(r.score_valor_atipico,score_value(r.valor_mensual,r.mediana_a,r.p90_a,r.p95_a))
        self.assertEqual(ev['counts']['indice_completo'],32)

    def test_old_columns_missing(self):
        row=contract(); row.pop('justificacion_modalidad_de');row.pop('tipodocproveedor')
        df,_=execute([row])
        self.assertEqual(df.iloc[0].clase_persona,'indeterminado')
        self.assertEqual(df.iloc[0].motivo_concentracion,'sin_causal_o_tipo')

    def test_empty_universe(self):
        df,ev=execute([contract(modalidad_de_contratacion='Otra')])
        self.assertEqual(len(df),0)
        self.assertEqual(ev['counts']['contratos_gold'],0)

    def test_invalid_value_not_zero(self):
        rows=[contract('A',valor_del_contrato=-1),contract('B')]
        df,_=execute(rows,ControlConfig(min_entity_contracts=2))
        self.assertTrue(df.score_concentracion.isna().all())
        self.assertTrue(df.motivo_concentracion.eq('universo_con_valores_o_proveedores_incompletos').all())


if __name__=='__main__':
    unittest.main()
