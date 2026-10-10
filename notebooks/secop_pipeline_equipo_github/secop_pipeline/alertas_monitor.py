"""Monitor de solo lectura: no importa Prefect ni abre la base DuckDB."""
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


class JsonReadCache:
    """Caché acotada para monitores: relee solo archivos cuya firma cambió.

    Una lectura incompleta no sustituye la última entrada válida y se propaga
    al panel para que muestre el reintento; no se presenta como estado fresco.
    """
    def __init__(self, max_files=8):
        self.entries = {}
        self.max_files = max_files

    def __call__(self, path):
        path = Path(path)
        stat = path.stat()
        signature = (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)
        previous = self.entries.get(path)
        if previous is not None and previous[0] == signature:
            return previous[1]
        value = json.loads(path.read_text(encoding='utf-8'))
        after = path.stat()
        if signature != (after.st_mtime_ns, after.st_ctime_ns, after.st_size):
            raise OSError('Archivo cambió durante la lectura; reintentar')
        if path not in self.entries and len(self.entries) >= self.max_files:
            self.entries.pop(next(iter(self.entries)))
        self.entries[path] = (signature, value)
        return value


def snapshot(root=ROOT, reader=None):
    read = reader or (lambda p: json.loads(p.read_text(encoding='utf-8')))
    pointer=Path(root)/'salida/alertas/ultima_ejecucion.json'
    if not pointer.exists(): return {'estado':'SIN_EJECUCION'}
    p=read(pointer)
    work=Path(p['directorio'])
    report=read(work/'ejecucion.json')
    steps=report.get('etapas',{})
    active=[k for k,v in steps.items() if v.get('estado')=='EN_CURSO']
    progress=work/'progreso_d2.json'
    d2=read(progress) if progress.exists() else {}
    age=None
    if d2.get('updated_at'):
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(d2['updated_at'])).total_seconds()
    return {'estado':report.get('estado'),'version':report.get('parametros',{}).get('version'),
        'parametros':report.get('parametros',{}),'flow_run_id':report.get('flow_run_id'),
        'directorio':str(work),'etapa_actual':active,'etapas_completadas':sum(v.get('estado')=='COMPLETADA' for v in steps.values()),
        'hallazgos':report.get('hallazgos',{}),'resumen':report.get('resumen'),'etapas':steps,'d2':d2,'heartbeat_age_seconds':age,
        'dashboard_url':d2.get('dashboard_url') if d2.get('cluster_active') and age is not None and age<60 and report.get('estado')=='EN_CURSO' and 'calculate_economic_anomalies' in active else None}


def text_status(s):
    lines=[f"Flujo: {s['estado']}"]
    if 'etapas' not in s: return '\n'.join(lines)
    lines.append('Etapa actual: '+(', '.join(s['etapa_actual']) or 'ninguna'))
    lines.append(f"Etapas completadas: {s['etapas_completadas']} (no representa porcentaje de tiempo)")
    d=s['d2']
    if d:
        lines.append(f"D2 nivel UNSPSC {d.get('level','preparación')}: {d.get('completed_blocks',0)}/{d.get('total_blocks','?')} bloques; {d.get('cached_blocks',0)} desde caché")
        lines.append(f"Fechas calculadas: {d.get('percent',0):.1f}% — {d.get('completed_dates',0)}/{d.get('total_dates','?')}")
        eta=d.get('eta_seconds')
        lines.append('Tiempo restante del nivel: '+(f'~{eta/60:.1f} minutos (estimado)' if eta is not None else 'estimando; requiere bloques terminados'))
        lines.append('Niveles posteriores: '+str(d.get('remaining_levels',[]))+'; después faltan integración de D2 y las siguientes etapas del flujo.')
        if s['heartbeat_age_seconds'] is not None and s['heartbeat_age_seconds']>60:
            lines.append('Heartbeat sin actualizar hace %.0f s; confirmar actividad antes de interpretar el avance.'%s['heartbeat_age_seconds'])
        for w in d.get('workers',[]):
            lines.append(f"Worker {w['address']}: {w['status']}; CPU {w.get('cpu_percent')}; RAM {(w.get('memory_bytes') or 0)/2**20:.0f} MiB; tareas {w.get('tasks')}")
    else: lines.append('Esta ejecución aún no publica avance por bloques (código anterior o preparación).')
    findings=(s.get('resumen') or {}).get('dimensiones') or s.get('hallazgos',{})
    if findings:
        lines.append('HALLAZGOS GLOBALES (conteos de contratos; indicador terminado)')
        for label in ('D1','D2','D3'):
            item=findings.get(label)
            if item is None:
                lines.append(label+': pendiente de terminar');continue
            rate=item.get('alertas_sobre_evaluados_pct')
            pct='no calculable' if rate is None else f'{rate:.2f}%'
            lines.append(f"{label}: {item['alertas']:,} alertas / {item['evaluados']:,} evaluados ({pct}); {item.get('no_evaluables','?')} no evaluables")
    summary=s.get('resumen')
    if summary:
        lines.append('Clasificación final: '+str(summary.get('clasificaciones',{})))
        if summary.get('d3_entidades_anio'): lines.append('D3 entidades-año: '+str(summary['d3_entidades_anio']))
        lines.append(f"Contratos únicos con alguna señal: {summary.get('contratos_con_alguna_senal','?')}; señales sumadas: {summary.get('total_senales','?')} (pueden coexistir en un contrato)")
    if s.get('dashboard_url'):lines.append('Dashboard: '+s['dashboard_url'])
    return '\n'.join(lines)
