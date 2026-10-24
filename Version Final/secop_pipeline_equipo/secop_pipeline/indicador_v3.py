"""Indicador descriptivo retrospectivo v3, calculado únicamente desde Gold.

No es probabilidad de corrupción ni modelo entrenado. Mantiene todas las filas;
la red básica se calcula como complemento y no entra al puntaje.
"""
from pathlib import Path
from dataclasses import dataclass,asdict
from datetime import date
import hashlib,json,math,time
import duckdb

ROOT=Path(__file__).resolve().parents[1]
SIGNALS=('c1','c2','c3','v1','v2','p1','p2')
FAMILIES={'C':('c1','c2','c3'),'V':('v1','v2'),'P':('p1','p2')}
COMPACT=['id_contrato',*SIGNALS,'puntaje','clasificacion','alerta_binaria']


@dataclass(frozen=True)
class Config:
    cutoff:str
    coverage_start:str
    version:str='alertas-cd-3.0'
    memory_mb:int=2048
    threads:int=1
    history_days:int=365
    min_entity_contracts:int=10
    min_supplier_contracts:int=5
    c1_threshold:float=.4
    c2_threshold:float=.25
    c3_threshold:float=.8
    absolute_value_cap:float=1e13
    median_value_factor:float=1000
    v1_reference_years:int=2
    v1_min_reference:int=30
    v1_z_threshold:float=3.5
    v2_days:int=30
    v2_min_contracts:int=3
    p2_min_segments:int=3
    review_top_k_per_quarter:int=2000
    minimum_priority_score:float=0

    def __post_init__(self):
        if self.version!='alertas-cd-3.0':raise ValueError('Este motor implementa alertas-cd-3.0.')
        if date.fromisoformat(self.coverage_start)>date.fromisoformat(self.cutoff):raise ValueError('Cobertura posterior al corte.')
        for k in ('threads','history_days','min_entity_contracts','min_supplier_contracts','v1_reference_years','v1_min_reference','v2_days','v2_min_contracts','p2_min_segments','review_top_k_per_quarter'):
            if type(getattr(self,k)) is not int or getattr(self,k)<1:raise ValueError(k+' debe ser entero positivo.')
        if self.memory_mb<512 or self.v1_reference_years>5:raise ValueError('Memoria >=512MB y referencia <=5 años.')
        for k in ('c1_threshold','c2_threshold','c3_threshold','minimum_priority_score'):
            if not 0<=getattr(self,k)<=1:raise ValueError(k+' fuera de [0,1].')
        for k in ('absolute_value_cap','median_value_factor','v1_z_threshold'):
            if not math.isfinite(getattr(self,k)) or getattr(self,k)<=0:raise ValueError(k+' inválido.')


def lit(x):return "'"+str(x).replace("'","''")+"'"


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(8*1024**2),b''):h.update(chunk)
    return h.hexdigest()


def configuration(path=None,source=None):
    source=Path(source or ROOT/'data/gold/contratos_control/contratos.parquet')
    vals=json.loads(Path(path or ROOT/'indicador_v3_config.json').read_text(encoding='utf-8'))
    audit=source.parent/'auditoria_ircd.json'
    declared=json.loads(audit.read_text(encoding='utf-8')).get('parametros',{}) if audit.exists() else {}
    for key in ('cutoff','coverage_start'):
        vals[key]=vals.get(key) or declared.get(key)
        if not vals[key]:raise ValueError('Declare '+key+'; no se infiere cobertura de la fecha mínima observada.')
    return Config(**vals)


def connect(work,cfg):
    work=Path(work);(work/'spill').mkdir(parents=True,exist_ok=True)
    return duckdb.connect(str(work/'indicador.duckdb'),config={
        'memory_limit':f'{cfg.memory_mb}MB','threads':cfg.threads,
        'preserve_insertion_order':False,'temp_directory':str(work/'spill')})


def prepare(c,cfg,source):
    c.execute(f'''CREATE OR REPLACE TABLE base AS SELECT id_contrato,
        nullif(trim(entidad_id),'') entidad,nullif(trim(proveedor_id),'') proveedor,
        cast(fecha_de_firma AS DATE) firma,extract(year FROM fecha_de_firma)::int anio,
        nullif(trim(tipo_de_contrato),'') tipo,
        CASE WHEN lower(trim(causal)) IN ('sin_causal','sin informacion','no definido','') THEN NULL ELSE trim(causal) END causal,
        CASE WHEN regexp_full_match(regexp_replace(codigo_de_categoria_principal,'^v[0-9]+[.]',''),'[0-9]{{8}}')
             THEN regexp_replace(codigo_de_categoria_principal,'^v[0-9]+[.]','') END unspsc,
        valor_del_contrato::DOUBLE valor
        FROM read_parquet({lit(source)})''')
    n,ids,empty,late=c.execute(f'''SELECT count(*),count(DISTINCT id_contrato),
        count(*) FILTER(WHERE trim(id_contrato)=''),count(*) FILTER(WHERE firma>DATE {lit(cfg.cutoff)}) FROM base''').fetchone()
    if not n or ids!=n or empty or late:raise ValueError('Gold vacío, IDs inválidos o fechas posteriores al corte.')
    c.execute(f'''CREATE OR REPLACE TABLE value_baseline AS SELECT tipo,anio,median(valor) mediana_tipo_anio
        FROM base WHERE isfinite(valor) AND valor>0 AND valor<={cfg.absolute_value_cap} GROUP BY tipo,anio''')
    c.execute(f'''CREATE OR REPLACE TABLE universe AS SELECT b.*,m.mediana_tipo_anio,
        CASE WHEN valor IS NULL THEN 'nulo' WHEN NOT isfinite(valor) THEN 'no_finito'
             WHEN valor<=0 THEN 'no_positivo' WHEN valor>{cfg.absolute_value_cap} THEN 'supera_tope_absoluto'
             WHEN valor>{cfg.median_value_factor}*mediana_tipo_anio THEN 'supera_factor_mediana' END motivo_valor_invalido,
        firma-INTERVAL {cfg.history_days} DAYS>=DATE {lit(cfg.coverage_start)} historia_completa,
        make_date(b.anio,12,31)<=DATE {lit(cfg.cutoff)} anio_completo
        FROM base b LEFT JOIN value_baseline m ON b.tipo=m.tipo AND b.anio=m.anio''')
    return {'filas':n,'excluidos_del_universo':0,'montos_invalidos':dict(c.execute("SELECT motivo_valor_invalido,count(*) FROM universe WHERE motivo_valor_invalido IS NOT NULL GROUP BY 1").fetchall())}


def relations(c,cfg):
    frame=f'ORDER BY firma RANGE BETWEEN INTERVAL {cfg.history_days-1} DAYS PRECEDING AND CURRENT ROW'
    c.execute(f'''CREATE OR REPLACE TABLE windows AS SELECT id_contrato,
        count(*) OVER e n_entidad,count(*) OVER p n_proveedor,count(*) OVER ep n_pareja,
        count(DISTINCT left(unspsc,2)) OVER p n_segmentos,
        count(*) FILTER(WHERE unspsc IS NULL) OVER p n_sin_categoria,
        count(*) OVER (PARTITION BY entidad,proveedor,left(unspsc,4),tipo ORDER BY firma
            RANGE BETWEEN INTERVAL {cfg.v2_days-1} DAYS PRECEDING AND CURRENT ROW) n_repetidos,
        min(firma) OVER (PARTITION BY proveedor) primera_firma
        FROM universe WHERE firma IS NOT NULL
        WINDOW e AS (PARTITION BY entidad {frame}),p AS(PARTITION BY proveedor {frame}),
        ep AS(PARTITION BY entidad,proveedor {frame})''')
    c.execute('''CREATE OR REPLACE TABLE entity_year AS WITH p AS (
        SELECT entidad,anio,proveedor,count(*) n FROM universe
        WHERE entidad IS NOT NULL AND firma IS NOT NULL GROUP BY ALL)
        SELECT entidad,anio,sum(n)::BIGINT n_anual,
        coalesce(sum(n) FILTER(WHERE proveedor IS NULL),0)::BIGINT n_sin_proveedor,
        sum(n::DOUBLE*n) FILTER(WHERE proveedor IS NOT NULL)/power(sum(n),2) hhi
        FROM p GROUP BY entidad,anio''')
    reasons={
        'c1':f"WHEN NOT u.historia_completa THEN 'historia_insuficiente' WHEN w.n_entidad<{cfg.min_entity_contracts} THEN 'pocos_contratos_entidad'",
        'c2':f"WHEN make_date(u.anio,1,1)<DATE {lit(cfg.coverage_start)} THEN 'inicio_anual_sin_cobertura' WHEN y.n_sin_proveedor>0 THEN 'proveedores_anuales_no_identificados' WHEN y.n_anual<{cfg.min_entity_contracts} THEN 'pocos_contratos_entidad'",
        'c3':f"WHEN NOT u.historia_completa THEN 'historia_insuficiente' WHEN w.n_proveedor<{cfg.min_supplier_contracts} THEN 'pocos_contratos_proveedor'",
        'v2':f"WHEN u.unspsc IS NULL OR u.tipo IS NULL THEN 'sin_categoria_o_tipo' WHEN u.firma-INTERVAL {cfg.v2_days} DAYS<DATE {lit(cfg.coverage_start)} THEN 'historia_insuficiente'",
        'p1':"WHEN NOT u.historia_completa THEN 'historia_insuficiente'",
        'p2':f"WHEN u.unspsc IS NULL THEN 'sin_categoria' WHEN w.n_sin_categoria>0 THEN 'categorias_incompletas_historia' WHEN NOT u.historia_completa THEN 'historia_insuficiente' WHEN w.n_proveedor<{cfg.min_supplier_contracts} THEN 'pocos_contratos_proveedor'"}
    expr=', '.join(f"CASE WHEN u.entidad IS NULL OR u.proveedor IS NULL THEN 'sin_identificador' WHEN u.firma IS NULL THEN 'sin_fecha' {v} END motivo_{k}" for k,v in reasons.items())
    c.execute(f'''CREATE OR REPLACE TABLE relation_reasons AS SELECT u.id_contrato,w.* EXCLUDE(id_contrato),y.hhi,y.n_anual,
        w.n_pareja::DOUBLE/nullif(w.n_entidad,0) participacion_entidad,
        w.n_pareja::DOUBLE/nullif(w.n_proveedor,0) dependencia_proveedor,{expr}
        FROM universe u LEFT JOIN windows w USING(id_contrato) LEFT JOIN entity_year y ON u.entidad=y.entidad AND u.anio=y.anio''')
    c.execute(f'''CREATE OR REPLACE TABLE relation_signals AS SELECT r.*,
        CASE WHEN motivo_c1 IS NULL THEN (participacion_entidad>{cfg.c1_threshold})::SMALLINT END c1,
        CASE WHEN motivo_c2 IS NULL THEN (hhi>{cfg.c2_threshold})::SMALLINT END c2,
        CASE WHEN motivo_c3 IS NULL THEN (dependencia_proveedor>{cfg.c3_threshold})::SMALLINT END c3,
        CASE WHEN motivo_v2 IS NULL THEN (n_repetidos>={cfg.v2_min_contracts})::SMALLINT END v2,
        CASE WHEN motivo_p1 IS NULL THEN (u.firma=primera_firma)::SMALLINT END p1,
        CASE WHEN motivo_p2 IS NULL THEN (n_segmentos>={cfg.p2_min_segments})::SMALLINT END p2
        FROM relation_reasons r JOIN universe u USING(id_contrato)''')
    return {'filas':c.execute('SELECT count(*) FROM relation_signals').fetchone()[0]}


def values(c,cfg):
    # Solo años anteriores completos: ventana reproducible, sin comparación fila-a-fila.
    c.execute(f'''CREATE OR REPLACE TABLE ref_values AS SELECT u.id_contrato,u.tipo,u.causal,left(u.unspsc,4) categoria,
        u.anio+d.delta anio_objetivo,ln(u.valor) log_valor FROM universe u
        CROSS JOIN range(1,{cfg.v1_reference_years+1}) d(delta)
        WHERE motivo_valor_invalido IS NULL AND tipo IS NOT NULL AND causal IS NOT NULL AND unspsc IS NOT NULL
        AND make_date(u.anio,1,1)>=DATE {lit(cfg.coverage_start)}
        AND make_date(u.anio,12,31)<=DATE {lit(cfg.cutoff)}
        AND u.anio+d.delta<=year(DATE {lit(cfg.cutoff)})''')
    c.execute('''CREATE OR REPLACE TABLE ref_median AS SELECT tipo,causal,categoria,anio_objetivo,
        count(*) n_ref,median(log_valor) mediana_log,quantile_cont(log_valor,.95) p95_log
        FROM ref_values GROUP BY ALL''')
    c.execute('''CREATE OR REPLACE TABLE references_v1 AS SELECT m.*,median(abs(v.log_valor-m.mediana_log)) mad_log
        FROM ref_values v JOIN ref_median m USING(tipo,causal,categoria,anio_objetivo)
        GROUP BY ALL''')
    c.execute(f'''CREATE OR REPLACE TABLE value_reasons AS SELECT u.id_contrato,r.* EXCLUDE(tipo,causal,categoria,anio_objetivo),
        CASE WHEN u.motivo_valor_invalido IS NOT NULL THEN u.motivo_valor_invalido
            WHEN u.firma IS NULL THEN 'sin_fecha' WHEN u.tipo IS NULL OR u.causal IS NULL OR u.unspsc IS NULL THEN 'sin_grupo_comparable'
            WHEN coalesce(r.n_ref,0)<{cfg.v1_min_reference} THEN 'referencia_insuficiente' END motivo_v1,
        CASE WHEN u.valor>0 AND isfinite(u.valor) THEN ln(u.valor) END log_valor
        FROM universe u LEFT JOIN references_v1 r ON u.tipo=r.tipo AND u.causal=r.causal
            AND left(u.unspsc,4)=r.categoria AND u.anio=r.anio_objetivo''')
    c.execute(f'''CREATE OR REPLACE TABLE value_signal AS SELECT *,
        CASE WHEN motivo_v1 IS NULL AND mad_log>0 THEN .67448975*(log_valor-mediana_log)/mad_log END z_robusto,
        CASE WHEN motivo_v1 IS NULL THEN CASE WHEN mad_log>0
            THEN (.67448975*(log_valor-mediana_log)/mad_log>{cfg.v1_z_threshold})::SMALLINT
            ELSE (log_valor>p95_log)::SMALLINT END END v1,
        CASE WHEN motivo_v1 IS NULL THEN CASE WHEN mad_log>0 THEN 'mediana_mad' ELSE 'percentil95_mad_cero' END END metodo_v1
        FROM value_reasons''')
    return {'referencias':c.execute('SELECT count(*) FROM references_v1').fetchone()[0]}


def network(c,cfg):
    c.execute('''CREATE OR REPLACE TABLE red_aristas AS SELECT entidad,proveedor,anio,count(*) contratos,
        count(DISTINCT date_trunc('week',firma)) semanas_activas,
        count(*)::DOUBLE/count(DISTINCT date_trunc('week',firma)) contratos_por_semana
        FROM universe WHERE entidad IS NOT NULL AND proveedor IS NOT NULL AND firma IS NOT NULL GROUP BY ALL''')
    c.execute('''CREATE OR REPLACE TABLE red_proveedores AS SELECT proveedor,anio,count(*) grado_entidades,
        sum(contratos)::BIGINT grado_ponderado FROM red_aristas GROUP BY proveedor,anio''')
    return {'aristas':c.execute('SELECT count(*) FROM red_aristas').fetchone()[0], 'entra_al_puntaje':False}


def scoring(c,cfg):
    c.execute('''CREATE OR REPLACE TABLE signals AS SELECT u.*,r.* EXCLUDE(id_contrato),v.* EXCLUDE(id_contrato)
        FROM universe u LEFT JOIN relation_signals r USING(id_contrato) LEFT JOIN value_signal v USING(id_contrato)''')
    count='+'.join(f'({s} IS NOT NULL)::INT' for s in SIGNALS)
    lo='+'.join('('+ '+'.join(f'coalesce({s},0)' for s in cols)+f')/{len(cols)}.0' for cols in FAMILIES.values())
    hi='+'.join('('+ '+'.join(f'coalesce({s},1)' for s in cols)+f')/{len(cols)}.0' for cols in FAMILIES.values())
    c.execute(f'''CREATE OR REPLACE TABLE scores AS SELECT *,({count}) dimensiones_evaluadas,
        (c1+c2+c3)/3.0 familia_c,(v1+v2)/2.0 familia_v,(p1+p2)/2.0 familia_p,
        ({lo})/3.0 puntaje_min,({hi})/3.0 puntaje_max,
        CASE WHEN ({count})=7 THEN ({lo})/3.0 END puntaje,
        date_trunc('quarter',firma)::DATE periodo_revision
        FROM signals''')
    c.execute(f'''CREATE OR REPLACE TABLE ranking AS SELECT id_contrato,
        row_number() OVER(PARTITION BY periodo_revision ORDER BY puntaje DESC,id_contrato) rango_revision
        FROM scores WHERE puntaje IS NOT NULL AND puntaje>{cfg.minimum_priority_score}''')
    c.execute(f'''CREATE OR REPLACE TABLE results AS SELECT s.*,r.rango_revision,
        CASE WHEN s.puntaje IS NULL THEN 'NO_EVALUABLE'
             WHEN r.rango_revision<={cfg.review_top_k_per_quarter} THEN 'ALERTA' ELSE 'NO_ALERTA' END clasificacion,
        CASE WHEN s.puntaje IS NULL THEN NULL WHEN r.rango_revision<={cfg.review_top_k_per_quarter} THEN 1 ELSE 0 END::SMALLINT alerta_binaria,
        {lit(cfg.version)} version_modelo,DATE {lit(cfg.cutoff)} fecha_corte
        FROM scores s LEFT JOIN ranking r USING(id_contrato)''')
    # Invariantes antes de publicar. No son un entrenamiento ni validación externa de riesgo.
    n,ids=c.execute('SELECT count(*),count(DISTINCT id_contrato) FROM results').fetchone()
    if n!=ids or n!=c.execute('SELECT count(*) FROM base').fetchone()[0]:raise ValueError('Pérdida o multiplicación de contratos.')
    return summary(c)


def summary(c):
    dims={}
    for s in SIGNALS:
        n,e,a=c.execute(f'SELECT count(*),count({s}),count(*) FILTER(WHERE {s}=1) FROM results').fetchone()
        dims[s.upper()]={'total':n,'evaluados':e,'alertas':a,'sin_senal':e-a,'no_evaluables':n-e}
    return {'contratos':n,'clasificaciones':dict(c.execute('SELECT clasificacion,count(*) FROM results GROUP BY 1').fetchall()),
        'dimensiones':dims,'valores_invalidos':dict(c.execute('SELECT motivo_valor_invalido,count(*) FROM results WHERE motivo_valor_invalido IS NOT NULL GROUP BY 1').fetchall())}


STAGES={'preparar_gold':prepare,'relaciones':relations,'valores':values,'red_complementaria':network,'puntuar':scoring}


def save_json(path,obj):
    path=Path(path);tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding='utf-8');tmp.replace(path)


def latest_result(root=ROOT):
    p=Path(root)/'salida/alertas/ultima_ejecucion_v3.json'
    if not p.exists():raise FileNotFoundError('Primero ejecute el notebook 09 (indicador v3). No se usa el lote v2 automáticamente.')
    return Path(json.loads(p.read_text(encoding='utf-8'))['directorio'])/'resultados.parquet'
