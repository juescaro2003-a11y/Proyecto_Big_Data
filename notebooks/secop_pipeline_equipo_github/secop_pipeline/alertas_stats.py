"""Estadísticas globales sin recalcular indicadores; solo agregaciones de columnas pequeñas."""
import hashlib
from pathlib import Path

INDICATORS={'D1':('calculate_concentration','d1','d1_concentracion','Concentración'),
            'D2':('calculate_economic_anomalies','d2','d2_valor_atipico','Valor atípico'),
            'D3':('calculate_year_end_accumulation','d3','alerta_d3','Acumulación de diciembre')}


def version(): return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def metric(alerts,evaluated,total):
    return {'alertas':int(alerts),'sin_alerta':int(evaluated-alerts),'evaluados':int(evaluated),
            'no_evaluables':int(total-evaluated),'total':int(total),
            'cobertura_pct':100*evaluated/total if total else None,
            'alertas_sobre_evaluados_pct':100*alerts/evaluated if evaluated else None,
            'alertas_sobre_total_pct':100*alerts/total if total else None}


def indicator_summary(work,cfg,label):
    from .alertas import connect
    _,table,col,_=INDICATORS[label]
    with connect(work,cfg) as c:
        n,e,a=c.execute(f'SELECT count(*),count({col}),count(*) FILTER(WHERE {col}=1) FROM {table}').fetchone()
    return metric(a,e,n)


def final_summary(c):
    # Una lectura de seis columnas; como máximo 27 combinaciones de señales para una regla fija.
    rows=c.execute('''SELECT d1_concentracion,d2_valor_atipico,alerta_d3,clasificacion,
        score_total,dimensiones_evaluadas,count(*) n FROM classified GROUP BY ALL''').fetchall()
    total=sum(r[6] for r in rows)
    result={'estadisticas_version':version(),'contratos_resultado':total,
        'contratos_evaluados_completos':sum(r[6] for r in rows if r[5]==3),
        'clasificaciones':dict.fromkeys(['ALERTA','NO_ALERTA','NO_EVALUABLE'],0),
        'puntajes':dict.fromkeys(range(4),0),'dimensiones':{},'combinaciones':[],
        'contratos_con_alguna_senal':0,'contratos_sin_senales_observadas':0,
        'contratos_sin_senales_con_tres_indicadores_evaluados':0,
        'coincidencias':dict.fromkeys(['D1_D2','D1_D3','D2_D3','D1_D2_D3'],0)}
    result['contratos_informacion_insuficiente']=total-result['contratos_evaluados_completos']
    for j,label in enumerate(INDICATORS):
        result['dimensiones'][label]=metric(sum(r[6] for r in rows if r[j]==1),sum(r[6] for r in rows if r[j] is not None),total)
    for d1,d2,d3,classification,score,evaluated,n in rows:
        result['clasificaciones'][classification]+=n
        result['puntajes'][score]+=n
        result['combinaciones'].append({'D1':d1,'D2':d2,'D3':d3,'contratos':n,'clasificacion':classification,
                                       'porcentaje_total':100*n/total if total else None})
        if 1 in (d1,d2,d3):result['contratos_con_alguna_senal']+=n
        else:
            result['contratos_sin_senales_observadas']+=n
            if evaluated==3:result['contratos_sin_senales_con_tres_indicadores_evaluados']+=n
        for key,condition in [('D1_D2',d1==d2==1),('D1_D3',d1==d3==1),('D2_D3',d2==d3==1),('D1_D2_D3',d1==d2==d3==1)]:
            if condition:result['coincidencias'][key]+=n
    result['combinaciones'].sort(key=lambda r:r['contratos'],reverse=True)
    result['total_senales']=sum(d['alertas'] for d in result['dimensiones'].values())
    if c.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='d3_year_scores'").fetchone()[0]:
        n,e,a=c.execute("SELECT count(*),count(*) FILTER(WHERE motivo_entidad_anio_d3 IS NULL),count(*) FILTER(WHERE motivo_entidad_anio_d3 IS NULL AND participacion_diciembre>umbral_d3) FROM d3_year_scores").fetchone()
        result['d3_entidades_anio']={'total':n,'evaluables':e,'alertadas':a}
    return result
