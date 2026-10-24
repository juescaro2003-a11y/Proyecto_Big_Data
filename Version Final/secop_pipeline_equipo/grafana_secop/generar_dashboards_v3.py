"""Diseño v3 independiente; conserva los tableros y datos históricos v2."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from grafana_secop.generar_dashboards import dashboard,target,FILTER,PAGE,DS
from secop_pipeline.indicador_v3 import SIGNALS


def build(kind):
    doc=dashboard(kind)
    # Mantener navegación y paginación, cambiando todas las fuentes a v3.
    doc=json.loads(json.dumps(doc).replace('secop_panel.','secop_v3.'))
    for panel in doc['panels']:
        field=panel.get('fieldConfig',{})
        overrides=field.get('overrides',[])
        legacy=[x for x in overrides if x.get('matcher',{}).get('options') in ('d1','d2','d3')]
        field['overrides']=[x for x in overrides if x not in legacy]
        if kind=='contratos' and legacy:
            for s in SIGNALS:
                item=json.loads(json.dumps(legacy[0]));item['matcher']['options']=s
                field['overrides'].append(item)
    doc['uid']='secop-'+kind+'-v3';doc['title']='SECOP II · '+('Indicador v3 — contratos' if kind=='contratos' else 'Procesos Gold — v3')
    doc['links']=[{'title':'Indicador v3','url':'/d/secop-contratos-v3','type':'link'},{'title':'Procesos Gold','url':'/d/secop-procesos-v3','type':'link'}]
    if kind=='procesos':
        doc['panels'][0]['options']['content']='**Procesos del Gold existente.** Su universo y fechas son distintos de contratos. Las siete señales C1/C2/C3/V1/V2/P1/P2 pertenecen al tablero contractual. Se conservan todos los registros y se navega en páginas SQL de 200. El filtro Año controla los gráficos; el selector temporal superior no se usa.'
        return doc
    note='''**Indicador descriptivo v3 — siete señales, tres familias.** Puntaje 0–1: promedio de las familias C, V y P con igual peso. No es una probabilidad de corrupción. Se exige que las siete señales sean evaluables; si falta alguna, el puntaje final y la alerta son NULL y la clasificación es NO_EVALUABLE.

**ALERTA significa priorizado dentro del cupo trimestral**, con puntaje positivo; no significa dos de tres ni incumplimiento probado. Los empates se resuelven por ID para respetar el cupo, no porque un ID represente más riesgo. Las medidas de red no entran al puntaje. C2 usa el año observado hasta el corte: este análisis es retrospectivo.

Año controla los gráficos. Clasificación e ID filtran únicamente el detalle. Página permite recorrer todos los registros. El selector temporal superior no se usa. Consulte versión, corte y cupo en Trazabilidad.'''
    doc['panels'][0]['options']['content']=note
    for p in doc['panels']:
        title=p['title']
        if title=='Clasificación y calidad monetaria':
            p['title']='Clasificación — universo completo'
            p['targets']=[target(f'SELECT clasificacion,sum(n) contratos FROM secop_v3.dash_contratos_resumen WHERE {FILTER} GROUP BY 1')]
            p['description']='Los contratos con importe inválido permanecen, pero V1 no es evaluable.'
        elif title=='Señales y no evaluables por dimensión':
            p['targets']=[target(f'SELECT dimension,sum(alertas) señales,sum(sin_senal) sin_señal,sum(no_evaluables) no_evaluables FROM secop_v3.dash_dimensiones WHERE {FILTER} GROUP BY 1 ORDER BY 1')]
        elif title=='Cobertura y porcentaje de señal entre evaluados':
            p['targets']=[target(f'SELECT dimension,sum(total) contratos,sum(evaluados) evaluados,round(100.0*sum(evaluados)/nullif(sum(total),0),2) cobertura_pct,round(100.0*sum(alertas)/nullif(sum(evaluados),0),2) señal_entre_evaluados_pct FROM secop_v3.dash_dimensiones WHERE {FILTER} GROUP BY 1 ORDER BY 1')]
        elif title=='Contratos — página SQL de 200 filas':
            p['targets']=[target(f"SELECT * FROM secop_v3.dash_contratos WHERE {FILTER} AND clasificacion IN (${{clasificacion:sqlstring}}) AND (${{buscar:sqlstring}}='' OR id_contrato=${{buscar:sqlstring}}) ORDER BY id_contrato LIMIT 200 OFFSET (({PAGE}-1)*200)")]
            overrides=p['fieldConfig']['overrides'];template=overrides[0] if overrides else None
            p['fieldConfig']['overrides']=[]
            if template:
                for s in SIGNALS:
                    t=json.loads(json.dumps(template));t['matcher']['options']=s;p['fieldConfig']['overrides'].append(t)
        elif title=='Definición de las dimensiones':
            p['options']['content']='''**C1:** proporción de contratos del proveedor en la entidad, 365 días. **C2:** HHI anual por número de contratos. **C3:** proporción de contratos del proveedor que provienen de esa entidad, 365 días.

**V1:** valores previamente validados, logaritmo y mediana/MAD de dos años completos anteriores del mismo tipo, causal y categoría UNSPSC de 4 dígitos; percentil 95 si MAD es cero. **V2:** repetición entidad–proveedor–tipo–categoría en ventana corta; no confirma fraccionamiento.

**P1:** primera aparición observada del proveedor, con cobertura histórica suficiente; no significa empresa recién creada. **P2:** diversidad de segmentos UNSPSC del proveedor; no demuestra incompatibilidad de actividades.

Los umbrales son parámetros operativos iniciales pendientes de validación externa. El detalle muestra métricas y motivos. Los intervalos puntaje_min/puntaje_max conservan la incertidumbre de las señales ausentes.'''
    p={'id':max(x['id'] for x in doc['panels'])+1,'title':'Distribución del puntaje evaluable',
       'type':'barchart','gridPos':{'x':0,'y':sum(x['gridPos']['h'] for x in doc['panels']),'w':24,'h':8},
       'datasource':DS,'targets':[target(f"SELECT (intervalo/10.0)::text || ' a ' || ((intervalo+1)/10.0)::text intervalo,sum(n) contratos FROM secop_v3.dash_puntajes WHERE {FILTER} GROUP BY intervalo ORDER BY intervalo")],
       'fieldConfig':{'defaults':{},'overrides':[]},'options':{'orientation':'vertical'}}
    doc['panels'].append(p)
    return doc


def main():
    out=ROOT/'grafana_secop/grafana/dashboards';out.mkdir(parents=True,exist_ok=True)
    for kind in ('contratos','procesos'):
        (out/(kind+'_v3.json')).write_text(json.dumps(build(kind),ensure_ascii=False,indent=2),encoding='utf-8')
        historical=out/(kind+'.json')
        if historical.exists():
            previous=json.loads(historical.read_text(encoding='utf-8'))
            if not previous['title'].startswith('Histórico v2 · '):previous['title']='Histórico v2 · '+previous['title']
            historical.write_text(json.dumps(previous,ensure_ascii=False,indent=2),encoding='utf-8')
    old=ROOT/'grafana_secop/grafana/provisioning/alerting/riesgo.json'
    rule=json.loads(old.read_text(encoding='utf-8'))
    group=rule['groups'][0];group['name']='SECOP v3 — priorización trimestral'
    r=group['rules'][0];r['uid']='secop-riesgo-v3';r['title']='SECOP v3: contratos priorizados para revisión'
    r['isPaused']=False
    r['annotations']={'summary':'La publicación v3 contiene contratos priorizados. No es una probabilidad de corrupción.','__dashboardUid__':'secop-contratos-v3','__panelId__':'3'}
    r['data'][0]['model']=target("SELECT now() AS time,coalesce(sum(n) FILTER(WHERE clasificacion='ALERTA'),0)::double precision AS alertas FROM secop_v3.dash_contratos_resumen HAVING count(*)>0 AND coalesce((SELECT fuentes_vigentes FROM secop_v3.dash_metadatos),false)",'time_series')
    (old.parent/'riesgo_v3.json').write_text(json.dumps(rule,ensure_ascii=False,indent=2),encoding='utf-8')
    historical=json.loads(old.read_text(encoding='utf-8'))
    for group in historical['groups']:
        for r in group['rules']:
            r['isPaused']=True
            if not r['title'].startswith('Histórico v2 · '):r['title']='Histórico v2 · '+r['title']
    old.write_text(json.dumps(historical,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':main()
