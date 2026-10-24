"""Analítica posterior a Gold. Cada función materializa una etapa en DuckDB local."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, timedelta
import hashlib
import json
import math
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / 'data/gold/contratos_control/contratos.parquet'
SOURCE_FIELDS = ['id_contrato', 'nit_entidad', 'codigo_entidad', 'nombre_entidad',
    'documento_proveedor', 'codigo_proveedor', 'tipodocproveedor', 'proveedor_adjudicado',
    'modalidad_de_contratacion', 'justificacion_modalidad_de', 'tipo_de_contrato',
    'codigo_de_categoria_principal', 'departamento', 'descripcion_del_proceso',
    'objeto_del_contrato', 'urlproceso', 'referencia_del_contrato', 'valor_del_contrato',
    'fecha_de_firma', 'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato',
    'ultima_actualizacion']


def lit(value):
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class AlertConfig:
    cutoff: str
    coverage_start: str = '2017-01-01'
    reference_cutoff: str | None = None
    version: str = 'alertas-cd-2.0'
    d1_days: int = 365
    d1_threshold: float = .40
    d1_min_contracts: int = 10
    d1_min_suppliers: int = 3
    d1_min_coverage: float = .95
    d2_months: int = 24
    d2_min_contracts: int = 30
    d2_threshold: float = 3.5
    # Ampliar solo UNSPSC; siempre preservar tipo y causal.
    d2_prefixes: tuple[int, ...] = (8, 4)
    d3_min_contracts: int = 30
    d3_min_years: int = 3
    d3_min_share: float = .30
    d3_iqr_factor: float = 1.5
    score_threshold: int = 2
    memory_mb: int = 3072
    threads: int = 1

    def __post_init__(self):
        cut, start = date.fromisoformat(self.cutoff), date.fromisoformat(self.coverage_start)
        ref = date.fromisoformat(self.reference_cutoff) if self.reference_cutoff else cut-timedelta(days=1)
        object.__setattr__(self, 'reference_cutoff', ref.isoformat())
        object.__setattr__(self, 'd2_prefixes', tuple(self.d2_prefixes))
        if not start <= ref < cut:
            raise ValueError('Se exige inicio de cobertura <= corte D3 de referencia < corte evaluado')
        if not self.version or len(self.version) > 100:
            raise ValueError('Versión de modelo inválida')
        for v in (self.d1_days, self.d1_min_contracts, self.d1_min_suppliers,
                  self.d2_months, self.d2_min_contracts, self.d3_min_contracts,
                  self.d3_min_years, self.threads):
            if not isinstance(v, int) or v < 1:
                raise ValueError('Ventanas, mínimos e hilos deben ser enteros positivos')
        if self.memory_mb < 256 or not 1 <= self.score_threshold <= 3:
            raise ValueError('Memoria o umbral de score inválido')
        if not 0 <= self.d1_threshold <= 1 or not 0 < self.d1_min_coverage <= 1 or not 0 <= self.d3_min_share <= 1:
            raise ValueError('Participación/cobertura fuera de rango')
        if not math.isfinite(self.d2_threshold) or self.d2_threshold <= 0 or not math.isfinite(self.d3_iqr_factor) or self.d3_iqr_factor < 0:
            raise ValueError('Umbrales no válidos')
        if self.d2_prefixes not in ((8,), (8, 4), (8, 4, 2)):
            raise ValueError('Jerarquía permitida: [8], [8,4] o [8,4,2]')

    def evidence(self):
        return asdict(self)

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.evidence(), sort_keys=True).encode()).hexdigest()


def source_fingerprint(path):
    """Huella de bytes; detecta cambios aunque se conserve tamaño/fecha."""
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8*1024*1024), b''):
            h.update(block)
    return h.hexdigest()


@contextmanager
def connect(work, cfg):
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(work/'analitica.duckdb'))
    try:
        con.execute(f"SET memory_limit='{cfg.memory_mb}MB'")
        con.execute(f'SET threads={cfg.threads}')
        con.execute('SET preserve_insertion_order=false')
        con.execute(f"SET temp_directory={lit((work/'spill').as_posix())}")
        yield con
    finally:
        con.close()


def extract_gold(work, cfg, source=GOLD):
    source = Path(source).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    with connect(work, cfg) as c:
        c.execute(f'CREATE OR REPLACE VIEW gold_source AS SELECT * FROM read_parquet({lit(source.as_posix())})')
        columns = {r[0] for r in c.execute('DESCRIBE gold_source').fetchall()}
        missing = set(SOURCE_FIELDS)-columns
        if missing:
            raise ValueError(f'Faltan columnas Gold: {sorted(missing)}')
        if 'fecha_corte' in columns:
            cuts = c.execute('SELECT min(fecha_corte),max(fecha_corte),count(*) FILTER(WHERE fecha_corte IS NULL) FROM gold_source').fetchone()
            if cuts[0] != cuts[1] or cuts[2] or cuts[0] is None or str(cuts[0])[:10] < cfg.cutoff:
                raise ValueError('El snapshot Gold no acredita cobertura homogénea hasta el corte solicitado')
        fields=[('"'+x+'"') if x=='valor_del_contrato' or x.startswith('fecha_') or x=='ultima_actualizacion'
                else f'cast("{x}" AS VARCHAR) AS "{x}"' for x in SOURCE_FIELDS]
        c.execute('CREATE OR REPLACE TABLE source AS SELECT '+','.join(fields)+' FROM gold_source')
        return {'total_gold': c.execute('SELECT count(*) FROM source').fetchone()[0], 'source':str(source)}


def filter_direct_contracting(work, cfg):
    with connect(work, cfg) as c:
        c.execute("""CREATE OR REPLACE MACRO norm(x) AS
            nullif(regexp_replace(strip_accents(lower(trim(cast(x AS VARCHAR)))), '\\s+', ' ', 'g'),'')""")
        c.execute("""CREATE OR REPLACE MACRO code(x) AS CASE
            WHEN norm(x) IN ('no definido','no definido.','no aplica','n/a','null','none','0','-') THEN NULL
            WHEN regexp_full_match(upper(trim(cast(x AS VARCHAR))), '[A-Z0-9_-]+') THEN upper(trim(cast(x AS VARCHAR))) END""")
        c.execute("""CREATE OR REPLACE TABLE filtered AS SELECT *,
            norm(modalidad_de_contratacion) AS modalidad_normalizada,
            code(codigo_entidad) AS entidad_clave, code(codigo_proveedor) AS proveedor_clave,
            norm(tipo_de_contrato) AS tipo_normalizado,
            CASE WHEN norm(justificacion_modalidad_de) IN ('no definido','no definida','no aplica','no aplica/no pertenece','n/a','null')
                 THEN NULL ELSE norm(justificacion_modalidad_de) END AS causal_normalizada,
            CASE WHEN regexp_full_match(lower(trim(codigo_de_categoria_principal)), '(v1\\.)?[0-9]{8}')
                 THEN regexp_replace(lower(trim(codigo_de_categoria_principal)), '^v1\\.', '') END AS unspsc_candidato,
            try_cast(fecha_de_firma AS DATE) AS firma,
            try_cast(valor_del_contrato AS DOUBLE) AS valor_contrato
            FROM source WHERE norm(modalidad_de_contratacion)='contratacion directa'""")
        direct = c.execute('SELECT count(*) FROM filtered').fetchone()[0]
        total = c.execute('SELECT count(*) FROM source').fetchone()[0]
        # Contratos sin fecha permanecen para informar NO_EVALUABLE.
        c.execute(f"CREATE OR REPLACE TABLE universe AS SELECT * FROM filtered WHERE firma IS NULL OR firma BETWEEN DATE {lit(cfg.coverage_start)} AND DATE {lit(cfg.cutoff)}")
        return {'total_directa':direct, 'excluidos_modalidad':total-direct,
                'excluidos_periodo':direct-c.execute('SELECT count(*) FROM universe').fetchone()[0]}


def validate_gold(work, cfg):
    with connect(work, cfg) as c:
        bad = c.execute("SELECT count(*)-count(DISTINCT id_contrato),count(*) FILTER(WHERE id_contrato IS NULL OR trim(id_contrato)='') FROM universe").fetchone()
        if any(bad):
            raise ValueError('Gold contiene IDs duplicados o vacíos. No se consolidan versiones arbitrariamente.')
        # Conflictos código-documento: no inventar una identidad. El nombre no es clave.
        for key, doc, table in [('entidad_clave','nit_entidad','entidades_conflictivas'),('proveedor_clave','documento_proveedor','proveedores_conflictivos')]:
            c.execute(f"""CREATE OR REPLACE TABLE {table} AS SELECT {key} AS clave FROM universe
                WHERE {key} IS NOT NULL AND {doc} IS NOT NULL AND norm({doc}) NOT IN ('no definido','no aplica','0','n/a','-')
                GROUP BY {key} HAVING count(DISTINCT regexp_replace(upper(trim({doc})), '[.\\s-]', '', 'g'))>1""")
        c.execute('ALTER TABLE universe ADD COLUMN IF NOT EXISTS entidad_consistente BOOLEAN')
        c.execute('ALTER TABLE universe ADD COLUMN IF NOT EXISTS proveedor_consistente BOOLEAN')
        c.execute('ALTER TABLE universe ADD COLUMN IF NOT EXISTS unspsc_normalizado VARCHAR')
        c.execute("""UPDATE universe SET entidad_consistente=entidad_clave IS NOT NULL AND entidad_clave NOT IN (SELECT clave FROM entidades_conflictivas),
            proveedor_consistente=proveedor_clave IS NOT NULL AND proveedor_clave NOT IN (SELECT clave FROM proveedores_conflictivos),
            unspsc_normalizado=CASE WHEN unspsc_candidato <> '00000000' AND left(unspsc_candidato,2)<>'00' THEN unspsc_candidato END""")
        c.execute('ALTER TABLE universe ADD COLUMN IF NOT EXISTS segmento_unspsc VARCHAR')
        c.execute('UPDATE universe SET segmento_unspsc=left(unspsc_normalizado,2)')
        row=c.execute("""SELECT count(*),count(*) FILTER(WHERE NOT entidad_consistente),count(*) FILTER(WHERE NOT proveedor_consistente),
            count(*) FILTER(WHERE valor_contrato IS NULL OR NOT isfinite(valor_contrato) OR valor_contrato<=0),
            count(*) FILTER(WHERE firma IS NULL),count(*) FILTER(WHERE codigo_de_categoria_principal IS NULL),
            count(*) FILTER(WHERE codigo_de_categoria_principal IS NOT NULL AND unspsc_normalizado IS NULL)
            FROM universe""").fetchone()
        return dict(zip(['contratos_universo','entidad_no_fiable','proveedor_no_fiable','valor_no_positivo_valido',
                         'sin_firma','sin_categoria','categoria_mal_formada'],row))


def calculate_concentration(work, cfg):
    with connect(work,cfg) as c:
        # RANGE respeta fecha: incluye todo el día y contrato actual, nunca fechas posteriores.
        frame=f'ORDER BY firma RANGE BETWEEN INTERVAL {cfg.d1_days} DAYS PRECEDING AND CURRENT ROW'
        c.execute(f"""CREATE OR REPLACE TABLE d1_windows AS SELECT id_contrato,
            sum(valor_contrato) FILTER(WHERE isfinite(valor_contrato) AND valor_contrato>=0) OVER e AS valor_entidad_365d,
            sum(valor_contrato) FILTER(WHERE isfinite(valor_contrato) AND valor_contrato>=0 AND proveedor_consistente) OVER p AS valor_proveedor_365d,
            count(*) OVER e AS n_contratos_entidad_365d,
            count(DISTINCT proveedor_clave) FILTER(WHERE proveedor_consistente) OVER e AS n_proveedores_entidad_365d,
            sum(valor_contrato) FILTER(WHERE isfinite(valor_contrato) AND valor_contrato>=0 AND proveedor_consistente) OVER e AS valor_identificado,
            count(*) FILTER(WHERE valor_contrato IS NULL OR NOT isfinite(valor_contrato) OR valor_contrato<0) OVER e AS valores_invalidos_ventana
            FROM universe WHERE firma IS NOT NULL AND entidad_consistente
            WINDOW e AS (PARTITION BY entidad_clave {frame}), p AS (PARTITION BY entidad_clave,proveedor_clave {frame})""")
        c.execute(f"""CREATE OR REPLACE TABLE d1_reasons AS SELECT u.id_contrato,w.* EXCLUDE(id_contrato),
            w.valor_identificado/nullif(w.valor_entidad_365d,0) AS cobertura_valor_referencia,
            CASE WHEN NOT u.entidad_consistente OR NOT u.proveedor_consistente THEN 'identificador_no_fiable'
                 WHEN u.firma IS NULL THEN 'sin_fecha_firma'
                 WHEN u.firma-INTERVAL {cfg.d1_days} DAYS<DATE {lit(cfg.coverage_start)} THEN 'historia_insuficiente'
                 WHEN w.valores_invalidos_ventana>0 THEN 'valores_invalidos_en_denominador'
                 WHEN w.valor_entidad_365d IS NULL OR w.valor_entidad_365d<=0 THEN 'denominador_no_valido'
                 WHEN w.n_contratos_entidad_365d<{cfg.d1_min_contracts} THEN 'pocos_contratos'
                 WHEN w.n_proveedores_entidad_365d<{cfg.d1_min_suppliers} THEN 'pocos_proveedores'
                 WHEN coalesce(w.valor_identificado/nullif(w.valor_entidad_365d,0),0)<{cfg.d1_min_coverage} THEN 'baja_cobertura_proveedores'
                 END AS motivo_d1
            FROM universe u LEFT JOIN d1_windows w USING(id_contrato)""")
        c.execute(f"""CREATE OR REPLACE TABLE d1 AS SELECT *,{cfg.d1_threshold}::DOUBLE AS umbral_d1,
            CASE WHEN motivo_d1 IS NULL THEN valor_proveedor_365d/valor_entidad_365d END AS participacion_proveedor,
            CASE WHEN motivo_d1 IS NULL THEN (valor_proveedor_365d/valor_entidad_365d>{cfg.d1_threshold})::SMALLINT END AS d1_concentracion
            FROM d1_reasons""")
        return c.execute('SELECT count(*) FROM d1').fetchone()[0]


def calculate_economic_anomalies(work,cfg):
    with connect(work,cfg) as c:
        # Ventanas compartidas por grupo y día. MAD calcula desviaciones respecto
        # de la mediana DE ESA VENTANA, no una segunda ventana sobre residuos ajenos.
        c.execute("""CREATE OR REPLACE TABLE economic AS SELECT id_contrato,firma,unspsc_normalizado,tipo_normalizado,causal_normalizada,
            ln(valor_contrato) AS log_valor FROM universe WHERE firma IS NOT NULL AND valor_contrato>0 AND isfinite(valor_contrato)
            AND unspsc_normalizado IS NOT NULL AND tipo_normalizado IS NOT NULL AND causal_normalizada IS NOT NULL""")
        from .alertas_dask import build_references
        # Fase 1: materializar referencias y cerrar conexión al acabar.
        # build_references cierra sus clusters antes de devolver tablas persistentes.
        # La conexión se cierra también si falla, liberando sus tablas temporales.
        refs=build_references(c,work,cfg)

    # Fase 2: conexión nueva, presupuesto original y sin temporales de preparación.
    # Los checkpoints de bloques se mantienen: el motor Dask no cambia.
    with connect(work,cfg) as c:
        print(f'D2 integración por claves: {cfg.memory_mb} MB, {cfg.threads} hilo(s), '
              'conexión nueva; workers Dask cerrados', flush=True)
        # Prioridad mínima entre referencias EVALUABLES. Se agrupan solo dos
        # columnas; no ordenar todas las columnas con row_number sobre millones.
        c.execute(f"""CREATE OR REPLACE TEMP VIEW d2_candidates AS
            SELECT * FROM ({' UNION ALL '.join(refs)})
            WHERE n_contratos_comparables>={cfg.d2_min_contracts}""")
        c.execute("""CREATE OR REPLACE TABLE d2_priority AS
            SELECT id_contrato,min(prioridad) AS prioridad
            FROM d2_candidates GROUP BY id_contrato""")
        # Materializar separadamente la selección y el join reduce operadores
        # bloqueantes simultáneos. Todas las columnas pertenecen a la misma fila.
        c.execute("""CREATE OR REPLACE TABLE d2_chosen AS
            SELECT r.* FROM d2_candidates r
            INNER JOIN d2_priority p USING(id_contrato,prioridad)""")
        total,unique=c.execute('SELECT count(*),count(DISTINCT id_contrato) FROM d2_chosen').fetchone()
        if total!=unique:
            raise ValueError('D2: referencias duplicadas para la prioridad elegida; no se multiplican contratos')
        c.execute(f"""CREATE OR REPLACE TABLE d2_reasons AS SELECT u.id_contrato,r.* EXCLUDE(id_contrato),
            CASE WHEN u.firma IS NULL THEN 'sin_fecha_firma'
                 WHEN u.valor_contrato IS NULL OR NOT isfinite(u.valor_contrato) OR u.valor_contrato<=0 THEN 'valor_no_positivo_valido'
                 WHEN u.unspsc_normalizado IS NULL OR u.tipo_normalizado IS NULL OR u.causal_normalizada IS NULL THEN 'grupo_incompleto'
                 WHEN u.firma-INTERVAL {cfg.d2_months} MONTHS<DATE {lit(cfg.coverage_start)} THEN 'historia_insuficiente'
                 WHEN r.id_contrato IS NULL THEN 'referencia_insuficiente' END AS motivo_d2,
            CASE WHEN u.valor_contrato>0 AND isfinite(u.valor_contrato) THEN ln(u.valor_contrato) END AS log_valor
            FROM universe u LEFT JOIN d2_chosen r USING(id_contrato)""")
        c.execute(f"""CREATE OR REPLACE TABLE d2 AS SELECT *,{cfg.d2_threshold}::DOUBLE AS umbral_d2,
            CASE WHEN motivo_d2 IS NOT NULL THEN NULL WHEN mad_grupo=0 THEN 'P95_LOG_MAD_CERO' ELSE 'Z_ROBUSTO_LOG' END AS metodo_comparacion,
            CASE WHEN motivo_d2 IS NULL AND mad_grupo>0 THEN (log_valor-mediana_grupo)/(1.4826*mad_grupo) END AS zscore_robusto,
            CASE WHEN motivo_d2 IS NOT NULL THEN NULL WHEN mad_grupo=0 THEN (log_valor>p95_log_grupo)::SMALLINT
                 ELSE ((log_valor-mediana_grupo)/(1.4826*mad_grupo)>{cfg.d2_threshold})::SMALLINT END AS d2_valor_atipico
            FROM d2_reasons""")
        return c.execute('SELECT count(*) FROM d2').fetchone()[0]


def calculate_year_end_accumulation(work,cfg):
    """Señal retrospectiva entidad-año; se asigna a contratos de diciembre."""
    with connect(work,cfg) as c:
        c.execute(f"""CREATE OR REPLACE TABLE d3_entity_year AS
            SELECT entidad_clave,year(firma)::INTEGER AS d3_anio,
                count(*)::BIGINT AS n_contratos_entidad_anio,
                count(*) FILTER(WHERE month(firma)=12)::BIGINT AS n_contratos_diciembre,
                count(*) FILTER(WHERE month(firma)=12)::DOUBLE/count(*) AS participacion_diciembre,
                make_date(year(firma)::INTEGER,1,1)>=DATE {lit(cfg.coverage_start)}
                AND make_date(year(firma)::INTEGER,12,31)<=DATE {lit(cfg.cutoff)} AS anio_completo_d3
            FROM universe WHERE entidad_consistente AND firma IS NOT NULL
            GROUP BY entidad_clave,year(firma)""")
        c.execute(f"""CREATE OR REPLACE TABLE d3_year_reference AS
            SELECT y.entidad_clave,y.d3_anio,count(h.d3_anio)::BIGINT AS n_anios_referencia_d3,
                min(h.d3_anio) AS primer_anio_referencia_d3,max(h.d3_anio) AS ultimo_anio_referencia_d3,
                quantile_cont(h.participacion_diciembre,.25) AS q1_diciembre,
                quantile_cont(h.participacion_diciembre,.75) AS q3_diciembre
            FROM d3_entity_year y LEFT JOIN d3_entity_year h
              ON y.entidad_clave=h.entidad_clave AND h.d3_anio<y.d3_anio
              AND h.anio_completo_d3 AND h.n_contratos_entidad_anio>={cfg.d3_min_contracts}
            GROUP BY y.entidad_clave,y.d3_anio""")
        c.execute(f"""CREATE OR REPLACE TABLE d3_year_scores AS
            SELECT y.*,r.* EXCLUDE(entidad_clave,d3_anio),
                q3_diciembre-q1_diciembre AS iqr_diciembre,
                CASE WHEN n_anios_referencia_d3>={cfg.d3_min_years}
                    THEN greatest({cfg.d3_min_share},q3_diciembre+{cfg.d3_iqr_factor}*(q3_diciembre-q1_diciembre)) END AS umbral_d3,
                CASE WHEN NOT anio_completo_d3 THEN 'anio_incompleto'
                     WHEN n_contratos_entidad_anio<{cfg.d3_min_contracts} THEN 'pocos_contratos_entidad_anio'
                     WHEN n_anios_referencia_d3<{cfg.d3_min_years} THEN 'historia_entidad_insuficiente' END AS motivo_entidad_anio_d3
            FROM d3_entity_year y LEFT JOIN d3_year_reference r USING(entidad_clave,d3_anio)""")
        c.execute("""CREATE OR REPLACE TABLE d3 AS
            SELECT u.id_contrato,s.* EXCLUDE(entidad_clave,motivo_entidad_anio_d3),
                CASE WHEN NOT u.entidad_consistente THEN 'identificador_entidad_no_fiable'
                     WHEN u.firma IS NULL THEN 'sin_fecha_firma'
                     WHEN s.d3_anio IS NULL THEN 'sin_historia_entidad'
                     ELSE s.motivo_entidad_anio_d3 END AS motivo_d3,
                CASE WHEN u.entidad_consistente AND u.firma IS NOT NULL
                     AND s.d3_anio IS NOT NULL AND s.motivo_entidad_anio_d3 IS NULL
                     THEN (month(u.firma)=12 AND s.participacion_diciembre>s.umbral_d3)::SMALLINT END AS alerta_d3
            FROM universe u LEFT JOIN d3_year_scores s
              ON u.entidad_clave=s.entidad_clave AND year(u.firma)=s.d3_anio""")
        result=c.execute("""SELECT count(*) AS entidades_anio,
            count(*) FILTER(WHERE motivo_entidad_anio_d3 IS NULL) AS entidades_anio_evaluables,
            count(*) FILTER(WHERE motivo_entidad_anio_d3 IS NULL AND participacion_diciembre>umbral_d3) AS entidades_anio_con_alerta
            FROM d3_year_scores""").fetchdf().to_dict('records')[0]
        # Caché analítica pequeña para EDA: una fila por entidad-año, sin repetir contratos.
        yearly_path=Path(work)/'d3_entidad_anio.parquet'
        yearly_temp=yearly_path.with_suffix('.partial')
        c.execute(f"COPY d3_year_scores TO {lit(yearly_temp.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)")
        yearly_temp.replace(yearly_path)
        result.update(definicion='Proporción de contratos firmados en diciembre frente a años completos anteriores de la misma entidad',
                      asignacion='Solo contratos de diciembre en entidades-año alertadas',corte=cfg.cutoff)
        return result

def classify_values(values, threshold=2):
    if any(v not in (None,0,1) for v in values) or len(values)!=3:
        raise ValueError('Se requieren tres binarias 0/1/None')
    observed=sum(v for v in values if v is not None)
    missing=sum(v is None for v in values)
    return observed, missing, ('ALERTA' if observed>=threshold else 'NO_ALERTA' if observed+missing<threshold else 'NO_EVALUABLE')


def calculate_score(work,cfg):
    with connect(work,cfg) as c:
        c.execute("""CREATE OR REPLACE TABLE scored AS SELECT u.*,a.* EXCLUDE(id_contrato),b.* EXCLUDE(id_contrato),d.* EXCLUDE(id_contrato),
            (coalesce(a.d1_concentracion,0)+coalesce(b.d2_valor_atipico,0)+coalesce(d.alerta_d3,0))::SMALLINT AS score_total,
            ((a.d1_concentracion IS NOT NULL)::INT+(b.d2_valor_atipico IS NOT NULL)::INT+(d.alerta_d3 IS NOT NULL)::INT)::SMALLINT AS dimensiones_evaluadas
            FROM universe u LEFT JOIN d1 a USING(id_contrato) LEFT JOIN d2 b USING(id_contrato) LEFT JOIN d3 d USING(id_contrato)""")
        n,unique=c.execute('SELECT count(*),count(DISTINCT id_contrato) FROM scored').fetchone()
        if n!=unique or n!=c.execute('SELECT count(*) FROM universe').fetchone()[0]:
            raise ValueError('Un join multiplicó o perdió contratos')


def classify_contracts(work,cfg,run_id,source_hash):
    with connect(work,cfg) as c:
        c.execute(f"""CREATE OR REPLACE TABLE classified AS SELECT *,
            (3-dimensiones_evaluadas)::SMALLINT AS dimensiones_no_evaluables,
            score_total AS score_min, (score_total+3-dimensiones_evaluadas)::SMALLINT AS score_max,
            CASE WHEN score_total>={cfg.score_threshold} THEN 'ALERTA'
                 WHEN score_total+3-dimensiones_evaluadas<{cfg.score_threshold} THEN 'NO_ALERTA' ELSE 'NO_EVALUABLE' END AS clasificacion,
            CASE WHEN score_total>={cfg.score_threshold} THEN 1 WHEN score_total+3-dimensiones_evaluadas<{cfg.score_threshold} THEN 0 END::SMALLINT AS alerta_binaria,
            concat(CASE score_total WHEN 0 THEN 'Sin señales' WHEN 1 THEN 'Bajo' WHEN 2 THEN 'Medio' ELSE 'Alto' END,
                   CASE WHEN dimensiones_evaluadas<3 THEN ' (provisional)' ELSE '' END) AS nivel_alerta,
            concat_ws('; ',CASE WHEN d1_concentracion=1 THEN 'D1: concentración' END,
                CASE WHEN d2_valor_atipico=1 THEN 'D2: valor atípico' END,CASE WHEN alerta_d3=1 THEN 'D3: acumulación de diciembre' END) AS motivos_alerta,
            CASE WHEN motivo_d1 IS NULL THEN 'EVALUADA' ELSE 'NO_EVALUABLE' END AS estado_d1,
            CASE WHEN motivo_d2 IS NULL THEN 'EVALUADA' ELSE 'NO_EVALUABLE' END AS estado_d2,
            CASE WHEN motivo_d3 IS NULL THEN 'EVALUADA' ELSE 'NO_EVALUABLE' END AS estado_d3,
            DATE {lit(cfg.cutoff)} AS fecha_corte, current_timestamp::TIMESTAMP AS fecha_procesamiento,
            {lit(cfg.version)} AS version_modelo,{lit(run_id)} AS prefect_flow_run_id,
            {lit(cfg.fingerprint())} AS huella_parametros,{lit(source_hash)} AS huella_gold,
            {lit(json.dumps(cfg.evidence(),sort_keys=True))} AS parametros_json,
            DATE {lit(cfg.coverage_start)} AS historia_inicio,CASE WHEN ultimo_anio_referencia_d3 IS NOT NULL THEN make_date(ultimo_anio_referencia_d3::INTEGER,12,31) END AS corte_referencia_d3,
            'D1/D2 ordenados por firma en snapshot actual; D3 retrospectivo entidad-año completo, conocido al cierre del año' AS limitacion_temporal,
            'UNSPSC validado sintácticamente; prefijo v1 explícito; sin catálogo maestro de códigos vigentes' AS validacion_unspsc,
            'valor total reportado; no acreditado como inicial' AS definicion_valor
            FROM scored""")
        validate_results(c,cfg)
        c.execute(f"COPY classified TO {lit((Path(work)/'resultados.parquet').as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)")
        return summary(c)


def validate_results(c,cfg):
    bad=c.execute(f"""SELECT count(*) FROM classified WHERE
        modalidad_normalizada<>'contratacion directa'
        OR d1_concentracion NOT IN (0,1) OR d2_valor_atipico NOT IN (0,1) OR alerta_d3 NOT IN (0,1)
        OR participacion_proveedor NOT BETWEEN 0 AND 1
        OR (d2_valor_atipico IS NOT NULL AND (referencia_d2_max>=firma OR n_contratos_comparables<{cfg.d2_min_contracts}))
        OR (alerta_d3 IS NOT NULL AND (n_anios_referencia_d3<{cfg.d3_min_years} OR NOT anio_completo_d3 OR ultimo_anio_referencia_d3>=d3_anio OR participacion_diciembre NOT BETWEEN 0 AND 1 OR (alerta_d3=1 AND (month(firma)<>12 OR participacion_diciembre<=umbral_d3))))
        OR score_total<>coalesce(d1_concentracion,0)+coalesce(d2_valor_atipico,0)+coalesce(alerta_d3,0)
        OR score_max<>score_total+dimensiones_no_evaluables
        OR (clasificacion='ALERTA' AND (score_min<{cfg.score_threshold} OR alerta_binaria IS DISTINCT FROM 1))
        OR (clasificacion='NO_ALERTA' AND (score_max>={cfg.score_threshold} OR alerta_binaria IS DISTINCT FROM 0))
        OR (clasificacion='NO_EVALUABLE' AND (score_min>={cfg.score_threshold} OR score_max<{cfg.score_threshold} OR alerta_binaria IS NOT NULL))""").fetchone()[0]
    n,u=c.execute('SELECT count(*),count(DISTINCT id_contrato) FROM classified').fetchone()
    if bad or n!=u or n!=c.execute('SELECT count(*) FROM universe').fetchone()[0]:
        raise ValueError(f'Validación analítica fallida: {bad} inválidos; filas={n}; IDs={u}')


def summary(c):
    from .alertas_stats import final_summary
    return final_summary(c)


def evaluate_scenarios(work,cfg):
    """27 escenarios sobre indicadores ya calculados; no recalibra el histórico."""
    from itertools import product
    rows=[]
    with connect(work,cfg) as c:
        for t1,t2,factor in product((.30,.40,.50),(3.,3.5,4.),(1.,1.5,2.)):
            row=c.execute(f"""WITH a AS (SELECT
                CASE WHEN d1_concentracion IS NOT NULL THEN (participacion_proveedor>{t1})::INT END AS a1,
                CASE WHEN d2_valor_atipico IS NULL THEN NULL WHEN metodo_comparacion='P95_LOG_MAD_CERO' THEN d2_valor_atipico ELSE (zscore_robusto>{t2})::INT END AS a2,
                CASE WHEN alerta_d3 IS NOT NULL THEN (month(firma)=12 AND participacion_diciembre>greatest({cfg.d3_min_share},q3_diciembre+{factor}*iqr_diciembre))::INT END AS a3
                FROM classified), b AS (SELECT *,coalesce(a1,0)+coalesce(a2,0)+coalesce(a3,0) AS s,
                (a1 IS NULL)::INT+(a2 IS NULL)::INT+(a3 IS NULL)::INT AS m FROM a)
                SELECT count(*) FILTER(WHERE a1=1),count(*) FILTER(WHERE a2=1),count(*) FILTER(WHERE a3=1),
                count(a1),count(a2),count(a3),count(*) FILTER(WHERE s>={cfg.score_threshold}),
                count(*) FILTER(WHERE s+m<{cfg.score_threshold}),count(*) FILTER(WHERE s<{cfg.score_threshold} AND s+m>={cfg.score_threshold}),
                count(*) FILTER(WHERE a1=1 AND a2=1),count(*) FILTER(WHERE a1=1 AND a3=1),count(*) FILTER(WHERE a2=1 AND a3=1),count(*) FROM b""").fetchone()
            d=dict(zip(['alertas_d1','alertas_d2','alertas_d3','evaluados_d1','evaluados_d2','evaluados_d3','ALERTA','NO_ALERTA','NO_EVALUABLE','coinciden_d1_d2','coinciden_d1_d3','coinciden_d2_d3','total'],row))
            d.update(umbral_d1=t1,umbral_d2=t2,factor_d3=factor,clasificados_pct=100*(d['ALERTA']+d['NO_ALERTA'])/max(d['total'],1))
            for dimension in ('d1','d2','d3'):
                d['cobertura_'+dimension+'_pct']=100*d['evaluados_'+dimension]/max(d['total'],1)
            rows.append(d)
    return rows
