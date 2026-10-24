"""Pruebas pequeñas. SOLO PostgreSQL efímero localhost:55439/fixture."""
import copy
from datetime import date,datetime
import json
from pathlib import Path
import tempfile
import unittest
import uuid
import time
import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg import sql

from .publicacion import run,sha_file,product,discover,Monitor,publish,source_locks,pgtype
from .generar_dashboards import dashboard

BASE=dict(host='127.0.0.1',port=55439,user='postgres',password='fixture-local-only',autocommit=True,connect_timeout=5)


def fixture(root):
    gold=root/'data/gold/contratos_control/contratos.parquet';gold.parent.mkdir(parents=True)
    rows=[dict(id_contrato=f'C{i}',fecha_de_firma=datetime(2020,1,i+1),nombre_entidad='Entidad',
               departamento='Bogotá',tipo_de_contrato='Servicios',proveedor_adjudicado='Proveedor',
               valor_del_contrato=float(100*(i+1)),objeto_del_contrato="Texto con 'comillas'",urlproceso='https://example.org',fecha_corte=date(2020,12,31)) for i in range(5)]
    pq.write_table(pa.Table.from_pylist(rows),gold)
    result=root/'salida/alertas/lote/resultados.parquet';result.parent.mkdir(parents=True)
    flags=[(1,1,0),(0,0,None),(1,None,None),(None,None,None)]
    records=[]
    for i,(d1,d2,d3) in enumerate(flags):
        lo=sum(x or 0 for x in (d1,d2,d3));n=sum(x is not None for x in (d1,d2,d3));hi=lo+3-n
        records.append(dict(id_contrato=f'C{i}',firma=rows[i]['fecha_de_firma'].date(),valor_contrato=rows[i]['valor_del_contrato'],
            clasificacion='ALERTA' if lo>=2 else ('NO_ALERTA' if hi<2 else 'NO_EVALUABLE'),
            d1_concentracion=d1,d2_valor_atipico=d2,alerta_d3=d3,score_min=lo,score_max=hi,dimensiones_evaluadas=n,
            motivo_d1='motivo',motivo_d2='motivo',motivo_d3='motivo',participacion_proveedor=.5,umbral_d1=.4,
            zscore_robusto=4.,umbral_d2=3.5,participacion_diciembre=.4,umbral_d3=.3,motivos_alerta='revisar',
            fecha_corte=date(2020,12,31),version_modelo='alertas-cd-2.0',huella_gold=sha_file(gold),huella_parametros='parametros',
            parametros_json='{"score_threshold":2}',prefect_flow_run_id='fixture-run'))
    save_result(result,records)
    config=dict(database_mode='existing',model_versions=['alertas-cd-2.0'],process_mode='disabled',existing_tables={},
                lock_timeout_ms=500,statement_timeout_ms=20000,memory='256MB')
    return gold,result,records,config


def save_result(path,rows):
    table=pa.Table.from_pylist(rows)
    pq.write_table(table,path)
    classes={}
    for r in rows:classes[r['clasificacion']]=classes.get(r['clasificacion'],0)+1
    dimensions={k:dict(alertas=sum(r[c]==1 for r in rows),evaluados=sum(r[c] is not None for r in rows))
                for k,c in [('D1','d1_concentracion'),('D2','d2_valor_atipico'),('D3','alerta_d3')]}
    report=dict(estado='COMPLETADA',flow_run_id='fixture-run',huella_gold=rows[0]['huella_gold'],
        parametros=dict(version='alertas-cd-2.0',cutoff='2020-12-31',score_threshold=2),
        etapas={'classify_contracts':{'estado':'COMPLETADA'}},
        resumen=dict(contratos_resultado=len(rows),clasificaciones=classes,dimensiones=dimensions))
    path.with_name('ejecucion.json').write_text(json.dumps(report),encoding='utf-8')


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.gold,self.result,self.rows,self.cfg=fixture(self.root)
        self.db='test_'+uuid.uuid4().hex[:12]
        with psycopg.connect(dbname='fixture',**BASE) as c:
            c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.db)))
        self.factory=lambda:psycopg.connect(dbname=self.db,**BASE)

    def publish(self):return run(self.root,self.result,self.cfg,self.factory)

    def test_idempotence_dimensions_and_incremental(self):
        first=self.publish();self.assertEqual(first['filas_transferidas'],9)
        again=self.publish();self.assertEqual(again['filas_transferidas'],0);self.assertEqual(again['estado'],'SIN_CAMBIOS')
        with self.factory() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM secop_panel.dash_contratos').fetchone()[0],5)
            self.assertEqual(c.execute("SELECT count(*) FROM secop_panel.dash_contratos WHERE clasificacion='SIN_RESULTADO_ALERTAS'").fetchone()[0],1)
            dims=c.execute("SELECT estado,sum(n) FROM secop_panel.dash_dimensiones WHERE dimension='D1' GROUP BY estado ORDER BY estado").fetchall()
            self.assertEqual(dict(dims),{'NO_EVALUABLE':1,'SEÑAL':2,'SIN_RESULTADO_ALERTAS':1,'SIN_SEÑAL':1})
            self.assertEqual(c.execute('SELECT sum(valor_reportado) FROM secop_panel.dash_contratos_resumen').fetchone()[0],1500)
        self.rows[0]['motivo_d2']='detalle nuevo';save_result(self.result,self.rows)
        updated=self.publish();self.assertEqual(updated['filas_transferidas'],4)

    def test_failure_rolls_back(self):
        first=self.publish();self.rows[0]['motivo_d2']='nuevo';save_result(self.result,self.rows)
        def fail(c):raise RuntimeError('fallo simulado antes del commit')
        with source_locks(self.root),duckdb.connect() as db,self.factory() as c:
            mon=Monitor();e=discover(self.root,self.result,self.cfg,mon)
            with self.assertRaises(RuntimeError):publish(c,db,e,self.cfg,mon,fail_hook=fail)
            self.assertEqual(c.execute('SELECT identity FROM secop_panel.publication_current').fetchone()[0],first['identity'])
            self.assertEqual(c.execute("SELECT motivo_d2 FROM secop_panel.dash_contratos WHERE id_contrato='C0'").fetchone()[0],'motivo')

    def test_rejects_invalid_inputs(self):
        lock=self.root/'.pipeline.lock';lock.write_text('otra ejecución')
        with self.assertRaises(RuntimeError):self.publish()
        self.assertTrue(lock.exists());lock.unlink()
        report=self.result.with_name('ejecucion.json');old=report.read_text();report.write_text(old.replace('COMPLETADA','EN_CURSO'))
        with self.assertRaises(ValueError):self.publish()
        report.write_text(old)
        self.rows[1]['id_contrato']='C0';save_result(self.result,self.rows)
        with self.assertRaisesRegex(ValueError,'IDS_INVALIDOS'):self.publish()

    def test_gold_mismatch_same_size_mtime(self):
        stat=self.gold.stat();data=self.gold.read_bytes();self.gold.write_bytes(data[:-8]+b'1234'+data[-4:])
        import os
        os.utime(self.gold,ns=(stat.st_atime_ns,stat.st_mtime_ns))
        with self.assertRaises(Exception):self.publish()

    def test_adopts_existing_no_gold_copy(self):
        table=pq.read_table(self.gold);fields=','.join(f'"{f.name}" {pgtype(f.type)}' for f in table.schema)
        with self.factory() as c:
            c.execute('CREATE SCHEMA gold');c.execute('CREATE TABLE gold.contratos_control ('+fields+')')
            with c.cursor().copy('COPY gold.contratos_control FROM STDIN') as cp:
                for row in table.to_pylist():cp.write_row(tuple(row.values()))
        self.cfg['existing_tables']={'gold':'gold.contratos_control'}
        answer=self.publish();self.assertEqual(answer['filas_transferidas'],4)
        with self.factory() as c:
            c.execute("UPDATE gold.contratos_control SET valor_del_contrato=999 WHERE id_contrato='C0'")
            self.assertTrue(c.execute("SELECT dirty FROM secop_panel.publication_products WHERE product='gold'").fetchone()[0])
            self.assertFalse(c.execute('SELECT fuentes_vigentes FROM secop_panel.dash_metadatos').fetchone()[0])
            # Simular la sustitución que realiza load.py sin cambiar el ETL.
            c.execute('CREATE TABLE gold.preparada AS SELECT * FROM gold.contratos_control')
            c.execute("UPDATE gold.preparada SET valor_del_contrato=100 WHERE id_contrato='C0'")
            c.execute('DROP TABLE gold.contratos_control')
            c.execute('ALTER TABLE gold.preparada RENAME TO contratos_control')
        self.assertEqual(self.publish()['filas_transferidas'],0)

    def test_legacy_process_explicit_and_incremental(self):
        self.publish()
        folder=self.root/'data/gold/procesos_modelo';folder.mkdir(parents=True)
        # Repetición de ID deliberada: se conserva cada fila y se navega por row_id.
        rows=[dict(id_del_proceso='P1',id_del_portafolio='F1',fecha_de_publicacion_del=datetime(2019,1,1),
            entidad='Entidad',departamento_entidad='Bogotá',tipo_de_contrato='Tipo',estado_del_procedimiento='Cerrado',
            precio_base=10.,proveedores_invitados=2.,respuestas_al_procedimiento=1.)]*3
        pq.write_table(pa.Table.from_pylist(rows),folder/'part.parquet')
        self.cfg['process_mode']='legacy';answer=self.publish();self.assertEqual(answer['filas_transferidas'],3)
        with self.factory() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM secop_panel.dash_procesos').fetchone()[0],3)
            self.assertEqual(c.execute('SELECT repeticiones_id FROM secop_panel.dash_procesos_calidad').fetchone()[0],2)
        self.assertEqual(self.publish()['filas_transferidas'],0)

    def test_dashboard_sql_and_pagination(self):
        self.publish();measurements=[]
        replacements={'anio':"'Todos'",'buscar':"''",'pagina':"'1'",'clasificacion':"'ALERTA','NO_ALERTA','NO_EVALUABLE','SIN_RESULTADO_ALERTAS'"}
        with self.factory() as c:
            for kind in ('contratos','procesos'):
                dash=dashboard(kind)
                for panel in dash['panels']:
                    for target in panel.get('targets',[]):
                        query=target['rawSql']
                        for name,val in replacements.items():query=query.replace('${'+name+':sqlstring}',val)
                        start=time.perf_counter();c.execute(query).fetchall()
                        measurements.append(dict(dashboard=kind,panel=panel['title'],seconds=time.perf_counter()-start))
                        if 'página SQL' in panel['title']:
                            self.assertIn('LIMIT 200 OFFSET',query)
                for variable in dash['templating']['list']:
                    if variable['type']=='query':c.execute(variable['query']).fetchall()
            plan=c.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM secop_panel.dash_contratos WHERE id_contrato='C1' ORDER BY id_contrato LIMIT 200").fetchone()[0]
            self.assertNotIn('Function Scan',json.dumps(plan))
            for name,val in [('anio',"'2020'"),('buscar',"'C1'"),('pagina',"'2'"),('buscar',"'C'' OR 1=1 --'")]:
                values=dict(replacements);values[name]=val
                for panel in dashboard('contratos')['panels']:
                    for target in panel.get('targets',[]):
                        query=target['rawSql']
                        for k,v in values.items():query=query.replace('${'+k+':sqlstring}',v)
                        c.execute(query).fetchall()
        artifact=Path(__file__).parent/'validacion_consultas.json'
        artifact.write_text(json.dumps({'fixture_rows':5,'queries':measurements,'plan':plan},indent=2,default=str),encoding='utf-8')

    def test_pagination_covers_all_records(self):
        gold_rows=pq.read_table(self.gold).to_pylist()
        expanded=[];alerts=[]
        for i in range(451):
            g=copy.deepcopy(gold_rows[i%5]);g['id_contrato']=f'ID{i:05d}';expanded.append(g)
        pq.write_table(pa.Table.from_pylist(expanded),self.gold)
        digest=sha_file(self.gold)
        for g in expanded:
            a=copy.deepcopy(self.rows[0]);a.update(id_contrato=g['id_contrato'],firma=g['fecha_de_firma'].date(),valor_contrato=g['valor_del_contrato'],huella_gold=digest);alerts.append(a)
        save_result(self.result,alerts);self.publish()
        panel=next(p for p in dashboard('contratos')['panels'] if 'página SQL' in p['title'])
        ids=[]
        with self.factory() as c:
            for page in (1,2,3):
                query=panel['targets'][0]['rawSql']
                for k,v in {'anio':"'Todos'",'buscar':"''",'pagina':f"'{page}'",'clasificacion':"'ALERTA'"}.items():
                    query=query.replace('${'+k+':sqlstring}',v)
                batch=c.execute(query).fetchall();self.assertLessEqual(len(batch),200);ids.extend(r[0] for r in batch)
        self.assertEqual(len(ids),451);self.assertEqual(len(set(ids)),451)

    def test_reader_permissions_and_lock_timeout(self):
        role='reader_'+uuid.uuid4().hex[:8];self.cfg['reader_role']=role
        with self.factory() as c:c.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        self.publish()
        with self.factory() as c:
            c.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            self.assertEqual(c.execute('SELECT count(*) FROM secop_panel.dash_contratos').fetchone()[0],5)
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):c.execute('DELETE FROM secop_panel.publication_products')
        with self.factory() as blocking:
            blocking.execute('SELECT pg_advisory_lock(87346120)')
            start=time.monotonic()
            with self.assertRaises(psycopg.errors.LockNotAvailable):self.publish()
            self.assertLess(time.monotonic()-start,4)


if __name__=='__main__':unittest.main()
