import itertools
import unittest
import duckdb
import pyarrow as pa
from secop_pipeline.alertas import classify_values
from secop_pipeline.alertas_stats import final_summary


class SummaryTests(unittest.TestCase):
    def test_all_combinations_reconcile_without_double_counting(self):
        rows=[]
        for values in itertools.product((None,0,1),repeat=3):
            score,missing,label=classify_values(values)
            rows.append(dict(d1_concentracion=values[0],d2_valor_atipico=values[1],alerta_d3=values[2],
                clasificacion=label,score_total=score,dimensiones_evaluadas=3-missing))
        with duckdb.connect() as c:
            c.register('classified',pa.Table.from_pylist(rows))
            result=final_summary(c)
        self.assertEqual(result['contratos_resultado'],27)
        self.assertEqual(result['contratos_evaluados_completos'],8)
        self.assertEqual(sum(result['clasificaciones'].values()),27)
        self.assertEqual(sum(r['contratos'] for r in result['combinaciones']),27)
        self.assertEqual(result['total_senales'],27)
        self.assertEqual(result['contratos_con_alguna_senal'],19)
        self.assertEqual(result['contratos_sin_senales_con_tres_indicadores_evaluados'],1)
        self.assertEqual(result['coincidencias']['D1_D2'],3)
        self.assertEqual(result['coincidencias']['D1_D2_D3'],1)
        for d in result['dimensiones'].values():
            self.assertEqual((d['alertas'],d['sin_alerta'],d['no_evaluables']),(9,9,9))
            self.assertEqual(d['alertas_sobre_evaluados_pct'],50)

    def test_zero_evaluable_percentage_is_unknown(self):
        with duckdb.connect() as c:
            c.execute("CREATE TABLE classified AS SELECT NULL::INT d1_concentracion,NULL::INT d2_valor_atipico,NULL::INT alerta_d3,'NO_EVALUABLE' clasificacion,0 score_total,0 dimensiones_evaluadas")
            result=final_summary(c)
            for d in result['dimensiones'].values():
                self.assertIsNone(d['alertas_sobre_evaluados_pct'])
                self.assertEqual(d['no_evaluables'],1)
            c.execute('DELETE FROM classified')
            result=final_summary(c)
            self.assertEqual(result['contratos_resultado'],0)
            self.assertIsNone(result['dimensiones']['D1']['cobertura_pct'])

if __name__=='__main__':unittest.main()
