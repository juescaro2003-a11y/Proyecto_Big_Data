"""Genera los JSON provisionados; no necesita conexión ni modifica datos."""
import json
from pathlib import Path

ROOT=Path(__file__).parent
DS={'type':'grafana-postgresql-datasource','uid':'secop-postgres'}
FILTER="(${anio:sqlstring} = 'Todos' OR anio::text = ${anio:sqlstring})"
PAGE="(CASE WHEN ${pagina:sqlstring} ~ '^[1-9][0-9]{0,8}$' THEN (${pagina:sqlstring})::bigint ELSE 1 END)"


def target(query, fmt='table', ref='A'):
    return {'refId':ref,'datasource':DS,'rawSql':query,'format':fmt,'rawQuery':True,'editorMode':'code'}


def dashboard(kind):
    title='Contratos y riesgo' if kind=='contratos' else 'Procesos de Gold'
    panels=[]
    def panel(title,query=None,type='table',height=8,description='',risk=False,text=None):
        y=sum(p['gridPos']['h'] for p in panels)
        p={'id':len(panels)+1,'title':title,'type':type,'gridPos':{'x':0,'y':y,'w':24,'h':height},
           'description':description,'fieldConfig':{'defaults':{'custom':{},'noValue':'Sin dato'},'overrides':[]},'options':{}}
        if query:
            p.update(datasource=DS,targets=[target(query,'time_series' if type=='timeseries' else 'table')])
        if type=='text': p['options']={'mode':'markdown','content':text}
        if type=='stat':
            p['options']={'reduceOptions':{'calcs':['lastNotNull'],'values':False},'colorMode':'background','graphMode':'none'}
            p['fieldConfig']['defaults']['thresholds']={'mode':'absolute','steps':[{'color':'green','value':None},{'color':'red','value':1}]} if risk else {'mode':'absolute','steps':[{'color':'blue','value':None}]}
        if type=='barchart':
            p['options']={'orientation':'horizontal','showValue':'auto','legend':{'displayMode':'list','placement':'bottom'}}
        if type=='table':
            p['options']={'showHeader':True,'cellHeight':'sm','enablePagination':False}
            for name in ['d1','d2','d3']:
                p['fieldConfig']['overrides'].append({'matcher':{'id':'byName','options':name},'properties':[
                    {'id':'mappings','value':[{'type':'value','options':{'0':{'text':'SIN SEÑAL','color':'green'},'1':{'text':'SEÑAL','color':'red'}}},{'type':'special','options':{'match':'null','result':{'text':'NO EVALUABLE','color':'orange'}}}]},
                    {'id':'custom.cellOptions','value':{'type':'color-text'}}]})
        panels.append(p)
    note=('**Contratos de Gold · resultados de alertas del corte seleccionado. SIN_RESULTADO_ALERTAS indica que falta un resultado coincidente para esa fila de Gold.** ALERTA: al menos dos señales confirmadas. '
          'NO EVALUABLE: la información disponible no permite decidir. Una señal no demuestra irregularidad. '
          '**Los importes de la versión 2.0 pueden contener errores extremos; las sumas son valores reportados, no validados.** '
          'La tabla usa paginación SQL; cambia Página para recorrer todo el universo y Buscar ID para buscar un identificador exacto. Restablece Página a 1 al cambiar filtros. Clasificación y Buscar ID afectan solo al detalle; los denominadores globales conservan Año. El filtro Año controla los paneles; el selector temporal superior no se aplica. Año 0 = fecha ausente.')
    if kind=='procesos':
        note=('**Procesos de Gold.** Las cifras cuentan registros y muestran por separado identificadores distintos. '
              'D1, D2 y D3 pertenecen a contratos: consulte el otro dashboard. Los universos y sus fechas son diferentes; no sume procesos y contratos. '
              'Se muestran todas las filas de la versión Gold de procesos seleccionada. Sus fechas observadas no equivalen al corte contractual ni acreditan un corte de extracción. Consulta estado_procesos y fuentes_vigentes. Si fuentes_vigentes es falso, los agregados corresponden al corte anterior y debes republicar. Página y Buscar ID controlan solo el detalle. Filtro Año de publicación; año 0 = sin fecha. El selector temporal superior no se aplica.')
    panel('Cómo interpretar este tablero',type='text',height=4,text=note)
    panel('Trazabilidad del corte contractual', 'SELECT * FROM secop_panel.dash_metadatos',height=4,
          description='Fecha de cálculo y publicación son distintas. En procesos, la fuente es Gold al momento de cargar.')
    if kind=='contratos':
        panel('Contratos en ALERTA — revisar',f"SELECT coalesce(sum(n),0) AS alertas FROM secop_panel.dash_contratos_resumen WHERE {FILTER} AND clasificacion='ALERTA'",'stat',4,risk=True)
        panel('Clasificación y calidad monetaria',f'SELECT clasificacion,sum(n) contratos,sum(valor_reportado) cop_reportados,sum(valor_no_positivo) valores_nulos_no_finitos_o_no_positivos,sum(valores_extremos) valores_mayores_10_billones FROM secop_panel.dash_contratos_resumen WHERE {FILTER} GROUP BY clasificacion',height=6,
              description='El diagnóstico >10 billones no cambia señales ni excluye filas. No implementa la propuesta V2 del factor 1000.')
        panel('Señales y no evaluables por dimensión',f"SELECT dimension,sum(n) FILTER(WHERE estado='SEÑAL') señales,sum(n) FILTER(WHERE estado='SIN_SEÑAL') sin_señal,sum(n) FILTER(WHERE estado='NO_EVALUABLE') no_evaluables,sum(n) FILTER(WHERE estado='SIN_RESULTADO_ALERTAS') sin_resultado FROM secop_panel.dash_dimensiones WHERE {FILTER} GROUP BY dimension ORDER BY dimension",'barchart')
        panel('Cobertura y porcentaje de señal entre evaluados',f"SELECT dimension,sum(n) total,sum(n) FILTER(WHERE estado IN ('SEÑAL','SIN_SEÑAL')) evaluados,round(100.0*sum(n) FILTER(WHERE estado IN ('SEÑAL','SIN_SEÑAL'))/nullif(sum(n),0),2) cobertura_pct,round(100.0*sum(n) FILTER(WHERE estado='SEÑAL')/nullif(sum(n) FILTER(WHERE estado IN ('SEÑAL','SIN_SEÑAL')),0),2) señal_entre_evaluados_pct FROM secop_panel.dash_dimensiones WHERE {FILTER} GROUP BY dimension ORDER BY dimension",height=6)
        panel('Entidades — universo completo',f'SELECT entidad,sum(n) contratos,sum(alertas) alertas,sum(no_evaluables) no_evaluables,sum(sin_resultado) sin_resultado,round(100.0*sum(alertas)/nullif(sum(n),0),2) alertas_sobre_total_pct FROM secop_panel.dash_entidades WHERE {FILTER} GROUP BY entidad ORDER BY alertas DESC,contratos DESC',height=10,
              description='El número absoluto favorece entidades grandes. Compare también el porcentaje y la cobertura; no es un ranking de corrupción.')
        panel('Contratos — página SQL de 200 filas',f"SELECT id_contrato,firma,entidad,proveedor,tipo,departamento,valor,clasificacion,d1,d2,d3,motivo_d1,motivo_d2,motivo_d3,participacion_proveedor,umbral_d1,zscore_robusto,umbral_d2,participacion_diciembre,umbral_d3,objeto_del_contrato,urlproceso FROM secop_panel.dash_contratos WHERE {FILTER} AND clasificacion IN (${ '{clasificacion:sqlstring}' }) AND (${{buscar:sqlstring}}='' OR id_contrato=${{buscar:sqlstring}}) ORDER BY id_contrato LIMIT 200 OFFSET (({PAGE}-1)*200)",height=12)
        panel('Definición de las dimensiones',type='text',height=7,text='**D1 — Concentración monetaria:** participación del proveedor en el valor contratado por la entidad en la ventana de 365 días. Umbral configurado, normalmente 40 %, sujeto a cobertura y tamaño mínimo.\n\n**D2 — Valor atípico:** comparación del logaritmo del valor con mediana y MAD de contratos comparables previos; usa percentil 95 si MAD es cero. No equivale a sobreprecio probado.\n\n**D3 — Acumulación de diciembre:** proporción anual de contratos firmados en diciembre frente al historial de la entidad. Exige años completos y referencia suficiente.\n\nLos umbrales exactos y motivos se muestran en el detalle. NULL conserva el estado no evaluable. Una señal por sí sola no genera ALERTA final.')
    else:
        panel('Cobertura y duplicación de procesos — universo completo', 'SELECT * FROM secop_panel.dash_procesos_calidad',height=5,
              description='Conteo global, independiente del filtro Año. Un ID repetido no implica automáticamente que toda la fila sea un duplicado.')
        panel('Registros por estado',f'SELECT estado,sum(n) registros,sum(ids_distintos) ids_distintos_por_anio_estado,sum(sin_id) sin_identificador,sum(presupuesto_reportado) presupuesto_reportado FROM secop_panel.dash_procesos_resumen WHERE {FILTER} GROUP BY estado',height=8,
              description='Los IDs distintos son por grupo año/estado; no representan un conteo global deduplicado. No se eliminan registros para maquillar duplicados.')
        panel('Procesos — página SQL de 200 filas',f"SELECT id_del_proceso,id_del_portafolio,fecha,entidad,departamento,tipo,estado,precio_base,proveedores_invitados,respuestas FROM secop_panel.dash_procesos WHERE {FILTER} AND (${{buscar:sqlstring}}='' OR id_del_proceso=${{buscar:sqlstring}}) ORDER BY dashboard_row_id LIMIT 200 OFFSET (({PAGE}-1)*200)",height=12)
    panel('Actividad mensual — '+('firma' if kind=='contratos' else 'publicación'),f'SELECT mes AS time,sum(n)::double precision AS registros FROM secop_panel.dash_{kind}_mensual WHERE {FILTER} GROUP BY mes ORDER BY mes','timeseries')
    panel('Tipos de contrato — todos',f'SELECT tipo,sum(n) registros FROM secop_panel.dash_{kind}_tipos WHERE {FILTER} GROUP BY tipo ORDER BY registros DESC','barchart')
    variables=[{'name':'anio','label':'Año','type':'query','datasource':DS,'query':f"SELECT 'Todos' AS __text,'Todos' AS __value UNION ALL SELECT DISTINCT anio::text,anio::text FROM secop_panel.dash_{kind}_mensual UNION SELECT 'Sin fecha','0'",'refresh':1,'multi':False,'includeAll':False,'current':{'text':'Todos','value':'Todos'}}]
    variables += [{'name':'pagina','label':'Página SQL (desde 1)','type':'textbox','query':'1','current':{'text':'1','value':'1'}},
                  {'name':'buscar','label':'Buscar ID exacto (solo detalle)','type':'textbox','query':'','current':{'text':'','value':''}}]
    if kind=='contratos':
        variables.append({'name':'clasificacion','label':'Clasificación (solo detalle)','type':'custom','query':'ALERTA,NO_ALERTA,NO_EVALUABLE,SIN_RESULTADO_ALERTAS','multi':True,'includeAll':False,'current':{'text':['ALERTA','NO_ALERTA','NO_EVALUABLE','SIN_RESULTADO_ALERTAS'],'value':['ALERTA','NO_ALERTA','NO_EVALUABLE','SIN_RESULTADO_ALERTAS']}})
    table = 'contratos' if kind=='contratos' else 'procesos'
    idcol = 'id_contrato' if kind=='contratos' else 'id_del_proceso'
    extra = ' AND clasificacion IN (${clasificacion:sqlstring})' if kind=='contratos' else ''
    total = f"SELECT coalesce(sum(n),0) FROM secop_panel.dash_{table}_resumen WHERE {FILTER}"+extra
    exact = f"SELECT count(*) FROM secop_panel.dash_{table} WHERE {FILTER}"+extra+f" AND {idcol}=${{buscar:sqlstring}}"
    query = f"SELECT {PAGE} pagina,200 filas_por_pagina,total_filtrado,ceil(total_filtrado/200.0) paginas FROM (SELECT CASE WHEN ${{buscar:sqlstring}}='' THEN ({total}) ELSE ({exact}) END total_filtrado) t"
    panel('Navegación SQL — cambia Página; al filtrar vuelve a 1',query,height=4)
    return {'uid':'secop-'+kind,'title':'SECOP II · '+title,'schemaVersion':41,'version':1,'tags':['SECOP II','Contratación directa'],
            'editable':False,'timezone':'America/Bogota','refresh':'','time':{'from':'2017-01-01T00:00:00Z','to':'now'},
            'templating':{'list':variables},'panels':panels,'links':[{'title':'Contratos y riesgo','url':'/d/secop-contratos','type':'link'}, {'title':'Procesos','url':'/d/secop-procesos','type':'link'}]}


def main():
    out=ROOT/'grafana/dashboards'; out.mkdir(parents=True,exist_ok=True)
    for kind in ['contratos','procesos']:
        (out/(kind+'.json')).write_text(json.dumps(dashboard(kind),ensure_ascii=False,indent=2),encoding='utf-8')
    rule={'uid':'secop-riesgo-corte','title':'SECOP: contratos con riesgo en el corte publicado','condition':'C','for':'1m','noDataState':'NoData','execErrState':'Error',
          'annotations':{'summary':'El corte publicado contiene contratos en ALERTA. Revisar el dashboard de contratos.','__dashboardUid__':'secop-contratos','__panelId__':'3'},
          'labels':{'proyecto':'SECOP','severidad':'revision'},'data':[
              {'refId':'A','datasourceUid':DS['uid'],'relativeTimeRange':{'from':600,'to':0},'model':target("SELECT now() AS time,coalesce(sum(n) FILTER(WHERE clasificacion='ALERTA'),0)::double precision AS alertas FROM secop_panel.dash_contratos_resumen HAVING count(*)>0 AND coalesce((SELECT fuentes_vigentes FROM secop_panel.dash_metadatos),false)",'time_series')},
              {'refId':'B','datasourceUid':'__expr__','relativeTimeRange':{'from':600,'to':0},'model':{'refId':'B','type':'reduce','expression':'A','reducer':'last','settings':{'mode':'strict'}}},
              {'refId':'C','datasourceUid':'__expr__','relativeTimeRange':{'from':600,'to':0},'model':{'refId':'C','type':'threshold','expression':'B','conditions':[{'type':'query','evaluator':{'type':'gt','params':[0]},'operator':{'type':'and'},'reducer':{'type':'last','params':[]},'query':{'params':['C']}}]}}]}
    path=ROOT/'grafana/provisioning/alerting';path.mkdir(parents=True,exist_ok=True)
    (path/'riesgo.json').write_text(json.dumps({'apiVersion':1,'groups':[{'orgId':1,'name':'SECOP - corte publicado','folder':'SECOP II','interval':'5m','rules':[rule]}]},ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':
    main()
