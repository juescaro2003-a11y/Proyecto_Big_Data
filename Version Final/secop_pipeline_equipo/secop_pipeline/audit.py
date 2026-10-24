"""Informe de ejecución autocontenido en notebooks; no ejecuta el EDA."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import time
import uuid

import pyarrow.parquet as pq

from .config import ROOT, BASES, BRONZE_DIR, SILVER_DIR, GOLD_DIR, OUTPUT_DIR, TIPOS


def now():
    return datetime.now(timezone.utc).isoformat()


def inventory():
    """Solo metadatos locales. Nunca consulta la API ni PostgreSQL."""
    records = []
    for layer, directory, bases, extension in [
        ('Bronze', BRONZE_DIR, BASES, '*.csv'),
        ('Silver', SILVER_DIR, BASES, '*.parquet'),
        ('Gold', GOLD_DIR, ['contratos_control'], '*.parquet'),
    ]:
        for base in bases:
            files = sorted((directory / base).glob(extension))
            rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files) if layer != 'Bronze' else None
            size = sum(p.stat().st_size for p in files)
            records.append({'capa': layer, 'base': base, 'archivos': len(files),
                            'bytes': size, 'MiB': size / 1024**2, 'filas': rows})
    return records


def table(rows):
    if not rows:
        return '*Sin medición disponible para esta ejecución.*'
    keys = list(dict.fromkeys(k for row in rows for k in row))
    def escape(value):
        if value is None:
            return 'No medido'
        if isinstance(value, float):
            return f'{value:,.4f}'
        return str(value).replace('|', '\\|').replace('\n', ' ')
    return '\n'.join(['| ' + ' | '.join(keys) + ' |', '| ' + ' | '.join(['---']*len(keys)) + ' |']
                     + ['| ' + ' | '.join(escape(row.get(k)) for k in keys) + ' |' for row in rows])


def markdown(text):
    return {'cell_type': 'markdown', 'id': uuid.uuid4().hex[:12], 'metadata': {}, 'source': text}


def notebook(data):
    stages = data.get('stages', [])
    results = {s['name']: s.get('result') for s in stages if s['status'] == 'COMPLETADA'}
    gold = (results.get('gold') or {}).get('audit', {})
    if not gold:
        gold = next((s.get('partial_audit', {}) for s in stages if s['name']=='gold'), {})
    manifest = results.get('extraccion') or data.get('manifest_before') or {}
    cells = [markdown('# Informe de ejecución — control contractual SECOP II\n\n'
        + f"Ejecución: {data['run_id']} · Estado: **{data['status']}**\n\n"
        + f"Inicio UTC: {data['started_at']} · Fin: {data.get('finished_at', 'en curso')}\n\n"
        + 'Unidad Gold: contrato de contratación directa. Solo tres componentes: concentración, recurrencia y valor atípico. '
        + 'Un indicador nulo significa evidencia insuficiente; una alerta requiere revisión, no demuestra irregularidad.')]
    def section(title, rows, explanation=''):
        cells.append(markdown('## '+title+'\n\n'+table(rows)+'\n\n'+explanation))
    section('1. Parámetros', [{'parámetro':k,'valor':v} for k,v in data['parameters'].items()])
    section('2. Etapas y tiempos', [{k:s.get(k) for k in ('name','status','seconds','error_type')} for s in stages])
    section('3. Entradas', [{'base':k,'filas':v['rows'],'archivos':len(v['files']),
        'bytes':sum(f.get('bytes',0) for f in v['files'])} for k,v in manifest.get('bases',{}).items()],
        f"Cobertura declarada: {manifest.get('start_date')} a {manifest.get('cutoff_date')}. "
        + 'La reutilización verifica hashes; no descarga nuevos datos. Los catálogos no tienen filtro de fechas.')
    section('4. Inventario anterior', data.get('before',[]))
    section('5. Inventario final', data.get('after',[]), 'Tamaños de archivos; no incluyen índices de PostgreSQL.')
    changes=[]
    metrics=[]
    for base in BASES:
        rows=results.get('transformar_'+base,[]) or []
        statuses=Counter(r['status'] for r in rows)
        changes.append({'base':base, **dict(statuses)})
        totals={}
        for row in rows:
            for col, values in row.get('metrics',{}).items():
                key=(row['status'],col)
                totals.setdefault(key,Counter()).update(values)
        metrics.extend({'base':base,'procedencia':status,'columna':col,**dict(value)} for (status,col),value in totals.items())
    section('6. Transformaciones Silver',changes,'No filtran filas. Contratos conserva columnas raw__ para auditar la normalización. '
        + 'No aplica y no adjudicado conservan su significado. existing indica métricas de una transformación previa.')
    section('Cambios por columna',metrics,'Los conteos son celdas, no filas distintas; no se suman como registros eliminados.')
    section('Cobertura de entrada a Gold',[{'campo':k,'filas_con_dato':v} for k,v in gold.get('cobertura_campos',{}).items()])
    section('7. Conteos Gold',[{'métrica':k,'valor':v} for k,v in gold.get('counts',{}).items()])
    section('Exclusiones',[{'motivo':k,'filas':v} for k,v in gold.get('exclusiones',{}).items()],
        'Cada fila de entrada tiene un destino: contrato conservado o una exclusión. Versiones anteriores, duplicados exactos '
        + 'y versiones ambiguas se distinguen. Las filas originales permanecen en Bronze. Las exclusiones se guardan aparte de la tabla analítica.')
    section('8. Reglas y referencias', [{'parámetro':k,'valor':v} for k,v in gold.get('parametros',{}).items()],
        'C: participación entidad–proveedor–año–segmento. R: contratos distintos en [firma−365 días, firma]. '
        + 'A: valor mensual solo cuando la remuneración y categoría están validadas. Referencias calculadas desde contratos, '
        + 'no umbrales hipotéticos. Interpretación: foto anual al corte; no alerta disponible al firmar.')
    section('Diagnóstico de componentes',[{'métrica':k,'resultado':v} for k,v in gold.get('diagnosticos',{}).items()])
    cells.append(markdown('## 9. Relaciones y conservación\n\n'
        + 'Gold utiliza contratos; procesos, proveedores y grupos se conservan como bases auxiliares independientes. '
        + 'Los agregados C/R/A se cruzan con contratos por claves y segmentos, sin unir por nombres. '
        + 'Se comprueba unicidad de id_contrato y conservación del número de contratos tras los cruces. '
        + 'IRCD solo suma si los tres scores existen. La falta de causal o de comparabilidad no se convierte en cero.'))
    section('10. Recursos Dask',[{k:v for k,v in r.items() if k!='salidas'} for r in gold.get('profiles',[])],
        'A normaliza y escribe por lotes pequeños con dos workers; B calcula con una tarea Dask y DuckDB con spill a disco. '
        + 'B limita DuckDB al 50% del límite del worker. Se registran recursos efectivos; no se afirma que sean óptimos.')
    section('11. Calidad',results.get('calidad',[]) or [])
    section('12. PostgreSQL',results.get('postgres',[]) or [],
        'Solo COMPLETADA en la etapa postgres confirma publicación transaccional de las tablas nuevas. '
        + 'Las tablas históricas del enfoque anterior no se borran automáticamente.')
    section('13. Limitaciones',[{'limitación':v} for v in gold.get('limitations',[])])
    section('14. Trazabilidad de código',data.get('code',[]))
    section('Versiones',data.get('versions',[]))
    cells.append(markdown('## Estado de publicación\n\n'+
        ('Gold contractual publicado.' if gold.get('publicado') else 'No se confirma publicación de Gold contractual en esta ejecución.')+
        '\n\n'+'; '.join(data.get('warnings',[]))))
    cells.append({'cell_type':'code','id':uuid.uuid4().hex[:12],'metadata':{},'execution_count':None,'outputs':[],
        'source':'import json\nevidencia = json.loads('+repr(json.dumps(data,ensure_ascii=False,default=str))+')\n'})
    return {'nbformat':4,'nbformat_minor':5,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'},
        'language_info':{'name':'python'},'etl_audit':{'run_id':data['run_id'],'status':data['status']}},'cells':cells}


class RunAudit:
    def __init__(self, parameters, *, initial=False):
        from importlib.metadata import version, PackageNotFoundError
        started = now()
        self.started_clock = time.perf_counter()
        code = []
        for folder in ('secop_pipeline', 'flows'):
            for path in sorted((ROOT / folder).glob('*.py')):
                code.append({'archivo': str(path.relative_to(ROOT)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        versions = [{'paquete': 'Python', 'versión': platform.python_version()}]
        for package in ('pandas', 'pyarrow', 'dask', 'distributed', 'prefect', 'psycopg', 'duckdb'):
            try:
                versions.append({'paquete': package, 'versión': version(package)})
            except PackageNotFoundError:
                versions.append({'paquete': package, 'versión': 'No instalado'})
        self.data = {'run_id': datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8],
                     'started_at': started, 'updated_at': started, 'status': 'INVENTARIO_PREVIO' if initial else 'EN_CURSO',
                     'parameters': parameters, 'code': code, 'versions': versions, 'stages': [], 'warnings': []}
        self.history = ROOT / 'notebooks' / 'informes_etl' / (self.data['run_id'] + '.ipynb')
        self.latest = ROOT / 'notebooks' / '07_informe_ejecucion_etl.ipynb'
        try:
            self.data['before'] = inventory()
        except Exception as exc:
            self.data['before'] = []
            self.data['warnings'].append('Inventario inicial no disponible: ' + type(exc).__name__)
        self.data['after'] = self.data['before'] if initial else []
        manifest_path = OUTPUT_DIR / 'manifest_extraccion.json'
        if manifest_path.exists():
            try:
                self.data['manifest_before'] = json.loads(manifest_path.read_text(encoding='utf-8'))
            except (ValueError, OSError) as exc:
                self.data['warnings'].append('Manifiesto anterior no legible: ' + type(exc).__name__)

    def plan(self, names):
        self.data['stages'] = [{'name': name, 'status': 'PENDIENTE'} for name in names]

    def save(self):
        self.data['updated_at'] = now()
        payload = json.dumps(notebook(self.data), ensure_ascii=False, indent=1, default=str)
        for path in (self.history, self.latest):
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_suffix('.ipynb.partial')
            temp.write_text(payload, encoding='utf-8')
            temp.replace(path)

    def call(self, name, function, *args, **kwargs):
        stage = next(s for s in self.data['stages'] if s['name'] == name)
        stage.update(status='EN_CURSO', started_at=now())
        self.save()
        start = time.perf_counter()
        try:
            result = function(*args, **kwargs)
            stage.update(status='COMPLETADA', result=result)
            return result
        except BaseException as exc:
            stage.update(status='FALLIDA', error_type=type(exc).__name__)
            if hasattr(exc, 'etl_audit'):
                stage['partial_audit'] = exc.etl_audit
            raise
        finally:
            stage['seconds'] = time.perf_counter() - start
            try:
                self.save()
            except Exception as exc:
                # No ocultar una excepción original por un fallo del checkpoint.
                self.data['warnings'].append('No se guardó checkpoint: ' + type(exc).__name__)

    def skip(self, name):
        next(s for s in self.data['stages'] if s['name'] == name)['status'] = 'OMITIDA'

    def finish(self, status):
        self.data['status'] = status
        self.data['finished_at'] = now()
        self.data['total_seconds'] = time.perf_counter() - self.started_clock
        for stage in self.data['stages']:
            if stage['status'] == 'PENDIENTE':
                stage['status'] = 'NO_EJECUTADA'
        try:
            self.data['after'] = inventory()
        except Exception as exc:
            self.data['warnings'].append('No se pudo actualizar inventario final: ' + type(exc).__name__)
            self.data['after'] = []
        self.save()


def initial_report():
    """Inventario inicial honesto: no reconstruye una ejecución que no fue instrumentada."""
    report = RunAudit({'modo': 'Inventario local de solo lectura; ETL no ejecutado'}, initial=True)
    path = OUTPUT_DIR / 'manifest_extraccion.json'
    if path.exists():
        report.data['manifest_reference'] = json.loads(path.read_text(encoding='utf-8'))
    report.data['notice'] = ('**Inventario previo, no informe de una nueva ejecución.** '
                             'Los tiempos, cambios por variable, filtros y cruces se medirán al ejecutar el flujo instrumentado.')
    report.save()
    return report.latest
