"""IRCD mediante SQL con spill a disco, ejecutado dentro del worker B de Dask.

La unidad es contrato. Las referencias se estiman de datos históricos, no de
constantes monetarias. La interpretación es fotografía anual del snapshot.
"""
import json
from pathlib import Path

import duckdb

from .control_config import ControlConfig


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def analyze_contracts(prepared, output, config: ControlConfig, memory_bytes=1024**3):
    prepared, output = Path(prepared), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    spill = output / '_spill'
    spill.mkdir(exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit={literal(str(int(memory_bytes)) + 'B')}")
        con.execute('SET threads=1')
        con.execute('SET preserve_insertion_order=false')
        con.execute(f'SET temp_directory={literal(spill.as_posix())}')
        con.execute(f"CREATE VIEW source AS SELECT * FROM read_parquet({literal((prepared/'*.parquet').as_posix())}, union_by_name=true)")
        def run(sql):
            con.execute(sql)
        def count(table, where='true'):
            return con.execute(f'SELECT count(*) FROM {table} WHERE {where}').fetchone()[0]
        cutoff = literal(config.cutoff)
        baseline_start, baseline_end = literal(config.baseline_start), literal(config.baseline_end)
        # Primero elegir versión; filtrar modalidad antes podría recuperar una versión obsoleta.
        run('''CREATE TEMP TABLE ranked AS
            SELECT *, dense_rank() OVER (PARTITION BY id_contrato ORDER BY ultima_actualizacion DESC NULLS LAST) AS version_rank,
                   count(*) OVER (PARTITION BY id_contrato) AS filas_mismo_id
            FROM source''')
        run('''CREATE TEMP TABLE conflict AS SELECT id_contrato,
               count(DISTINCT huella_registro) FILTER (WHERE version_rank=1) AS variantes_ultima,
               count(DISTINCT huella_registro) AS variantes_total,
               count(*) FILTER (WHERE ultima_actualizacion IS NULL) AS sin_actualizacion
            FROM ranked GROUP BY id_contrato''')
        run('''CREATE TEMP TABLE decisions AS SELECT r.*, c.variantes_ultima,
            CASE WHEN r.id_contrato IS NULL THEN 'sin_id_contrato'
                 WHEN c.variantes_ultima>1 OR (c.variantes_total>1 AND c.sin_actualizacion>0)
                   THEN 'version_ambigua'
                 WHEN r.version_rank>1 THEN 'version_anterior'
                 ELSE NULL END AS motivo_previo,
            row_number() OVER(PARTITION BY r.id_contrato, r.version_rank ORDER BY r.huella_registro, r.fuente_archivo) AS repeticion
            FROM ranked r LEFT JOIN conflict c ON r.id_contrato IS NOT DISTINCT FROM c.id_contrato''')
        person_rule = 'false' if config.person_filter == 'todos' else f'clase_persona <> {literal(config.person_filter)}'
        run(f'''CREATE TEMP TABLE classified AS SELECT *,
            CASE WHEN motivo_previo IS NOT NULL THEN motivo_previo
                 WHEN repeticion>1 THEN 'duplicado_exacto'
                 WHEN modalidad_de_contratacion IS DISTINCT FROM 'contratacion directa' THEN 'otra_modalidad'
                 WHEN fecha_de_firma::DATE > DATE {cutoff} THEN 'posterior_al_corte'
                 WHEN {person_rule} THEN 'fuera_filtro_persona'
                 ELSE NULL END AS motivo_exclusion
            FROM decisions''')
        run('''CREATE TEMP TABLE universe AS SELECT *, year(fecha_de_firma)::INTEGER AS anio,
                (fecha_de_fin_del_contrato::DATE-fecha_de_inicio_del_contrato::DATE+1)::INTEGER AS duracion_dias
            FROM classified WHERE motivo_exclusion IS NULL''')
        # C usa el mismo universo del filtro de personas en numerador y denominador.
        run('''CREATE TEMP TABLE entity_year AS SELECT entidad_id, segmento, anio,
            count(*) AS n_entidad_segmento, sum(valor_del_contrato) FILTER (WHERE valor_del_contrato>=0 AND isfinite(valor_del_contrato)) AS denominador_entidad,
            count(*) FILTER (WHERE valor_del_contrato IS NULL OR NOT isfinite(valor_del_contrato) OR valor_del_contrato<0 OR proveedor_id IS NULL) AS n_no_calculables,
            CASE WHEN count(*)<100 THEN '30-99' WHEN count(*)<1000 THEN '100-999' ELSE '1000+' END AS estrato_volumen
            FROM universe GROUP BY entidad_id, segmento, anio''')
        run('''CREATE TEMP TABLE pairs AS SELECT entidad_id, proveedor_id, segmento, anio,
            sum(valor_del_contrato) FILTER(WHERE valor_del_contrato>=0 AND isfinite(valor_del_contrato)) AS numerador_proveedor
            FROM universe GROUP BY entidad_id, proveedor_id, segmento, anio''')
        run(f'''CREATE TEMP TABLE shares AS SELECT p.*, e.* EXCLUDE(entidad_id, segmento, anio),
            CASE WHEN e.n_no_calculables=0 AND e.denominador_entidad>0 AND e.n_entidad_segmento>={config.min_entity_contracts}
                  AND p.entidad_id IS NOT NULL AND p.proveedor_id IS NOT NULL AND p.segmento IS NOT NULL
                 THEN p.numerador_proveedor/e.denominador_entidad END AS concentracion
            FROM pairs p LEFT JOIN entity_year e USING(entidad_id, segmento, anio)''')
        # Se utilizan años completos de referencia; nunca el año del contrato evaluado.
        run(f'''CREATE TEMP TABLE cref AS SELECT segmento, estrato_volumen, count(*) AS n_ref_c,
            quantile_cont(concentracion,{config.p90}) AS p90_c,
            quantile_cont(concentracion,{config.p95}) AS p95_c
            FROM shares WHERE concentracion IS NOT NULL
              AND make_date(anio,1,1)>=DATE {baseline_start}
              AND make_date(anio,12,31)<=DATE {baseline_end}
              AND make_date(anio,12,31)<=DATE {cutoff}
            GROUP BY segmento, estrato_volumen''')
        # Agregación por día: todos los contratos de la misma fecha se incluyen.
        run('''CREATE TEMP TABLE days AS SELECT entidad_id, proveedor_id, segmento,
               fecha_de_firma::DATE AS dia, count(*) AS n
            FROM universe WHERE entidad_id IS NOT NULL AND proveedor_id IS NOT NULL AND segmento IS NOT NULL
              AND fecha_de_firma IS NOT NULL GROUP BY entidad_id, proveedor_id, segmento, dia''')
        run('''CREATE TEMP TABLE recurrence AS SELECT *,
            sum(n) OVER (PARTITION BY entidad_id,proveedor_id,segmento ORDER BY dia
                         RANGE BETWEEN INTERVAL 365 DAYS PRECEDING AND CURRENT ROW)::BIGINT AS recurrencia
            FROM days''')
        run(f'''CREATE TEMP TABLE pair_year_r AS SELECT r.entidad_id,r.proveedor_id,r.segmento,year(r.dia) AS anio,
            e.estrato_volumen, max(r.recurrencia) AS recurrencia
            FROM recurrence r JOIN entity_year e ON r.entidad_id=e.entidad_id AND r.segmento=e.segmento AND year(r.dia)=e.anio
            WHERE r.dia BETWEEN DATE {baseline_start} AND DATE {baseline_end}
              AND r.dia<=DATE {cutoff} AND r.dia-INTERVAL 365 DAYS>=DATE {literal(config.coverage_start)}
            GROUP BY r.entidad_id,r.proveedor_id,r.segmento,year(r.dia),e.estrato_volumen''')
        run(f'''CREATE TEMP TABLE rref AS SELECT segmento,estrato_volumen,count(*) AS n_ref_r,
            quantile_cont(recurrencia,{config.p90}) AS p90_r,
            quantile_cont(recurrencia,{config.p95}) AS p95_r
            FROM pair_year_r GROUP BY segmento,estrato_volumen''')
        cats = ','.join(literal(x) for x in config.monthly_validated_categories) or "'__ninguna_validada__'"
        run(f'''CREATE TEMP TABLE monthly AS SELECT *,
            CASE WHEN fecha_de_inicio_del_contrato IS NULL OR fecha_de_fin_del_contrato IS NULL THEN 'faltan_fechas'
                 WHEN duracion_dias NOT BETWEEN 1 AND {config.max_duration_days} THEN 'duracion_no_validada'
                 WHEN valor_del_contrato IS NULL OR NOT isfinite(valor_del_contrato) OR valor_del_contrato<=0 THEN 'valor_no_valido'
                 WHEN tipo_de_contrato IS DISTINCT FROM 'prestacion de servicios' THEN 'remuneracion_no_validada'
                 WHEN codigo_de_categoria_principal IS NULL OR codigo_de_categoria_principal NOT IN ({cats}) THEN 'comparabilidad_mensual_no_validada'
                 WHEN segmento IS NULL OR departamento IS NULL THEN 'faltan_comparables'
                 ELSE NULL END AS motivo_mensual
            FROM universe''')
        run('''CREATE TEMP TABLE values_monthly AS SELECT *,
            CASE WHEN motivo_mensual IS NULL THEN valor_del_contrato*30.44/duracion_dias END AS valor_mensual
            FROM monthly''')
        # Referencia A: último año completo de la línea base, sin mezclar años nominales.
        # Su aplicación a años posteriores requiere validar comparabilidad económica.
        run(f'''CREATE TEMP TABLE aref AS SELECT segmento,codigo_de_categoria_principal,departamento,
            count(*) AS n_ref_a, median(valor_mensual) AS mediana_a,
            quantile_cont(valor_mensual,{config.p90}) AS p90_a, quantile_cont(valor_mensual,{config.p95}) AS p95_a
            FROM values_monthly WHERE motivo_mensual IS NULL
              AND anio=year(DATE {baseline_end})
              AND fecha_de_firma::DATE BETWEEN DATE {baseline_start} AND DATE {baseline_end}
              AND fecha_de_firma::DATE<=DATE {cutoff}
            GROUP BY segmento,codigo_de_categoria_principal,departamento''')
        run(f'''CREATE TEMP TABLE evidence AS SELECT u.*,
            s.numerador_proveedor,s.denominador_entidad,s.n_entidad_segmento,s.n_no_calculables,s.estrato_volumen,s.concentracion,
            c.n_ref_c,c.p90_c,c.p95_c,r.recurrencia,rr.n_ref_r,rr.p90_r,rr.p95_r,
            a.n_ref_a,a.mediana_a,a.p90_a,a.p95_a, u.valor_mensual/nullif(a.mediana_a,0) AS ratio_valor,
            (u.fecha_de_firma::DATE-INTERVAL 365 DAYS)::DATE AS ventana_inicio,
            u.fecha_de_firma::DATE AS ventana_fin,
            CASE WHEN u.fecha_de_firma IS NULL THEN 'sin_fecha_firma'
                 WHEN u.anio<=year(DATE {baseline_end}) THEN 'periodo_calibracion_no_puntuado'
                 WHEN DATE {baseline_start}<DATE {literal(config.coverage_start)} THEN 'historia_insuficiente'
                 ELSE NULL END AS motivo_temporal
            FROM values_monthly u
            LEFT JOIN shares s ON u.entidad_id=s.entidad_id AND u.proveedor_id=s.proveedor_id AND u.segmento=s.segmento AND u.anio=s.anio
            LEFT JOIN cref c ON u.segmento=c.segmento AND s.estrato_volumen=c.estrato_volumen
            LEFT JOIN recurrence r ON u.entidad_id=r.entidad_id AND u.proveedor_id=r.proveedor_id AND u.segmento=r.segmento AND u.fecha_de_firma::DATE=r.dia
            LEFT JOIN rref rr ON u.segmento=rr.segmento AND s.estrato_volumen=rr.estrato_volumen
            LEFT JOIN aref a ON u.segmento=a.segmento AND u.codigo_de_categoria_principal=a.codigo_de_categoria_principal AND u.departamento=a.departamento''')
        ties = 'false' if config.accept_recurrence_ties else 'p90_r=p95_r'
        run(f'''CREATE TEMP TABLE reasons AS SELECT *,
            coalesce(motivo_temporal, CASE WHEN entidad_id IS NULL OR proveedor_id IS NULL THEN 'sin_identificador_fiable'
                WHEN segmento IS NULL THEN 'sin_causal_o_tipo'
                WHEN n_entidad_segmento<{config.min_entity_contracts} THEN 'evidencia_insuficiente_entidad'
                WHEN n_no_calculables>0 THEN 'universo_con_valores_o_proveedores_incompletos'
                WHEN concentracion IS NULL THEN 'denominador_no_valido'
                WHEN coalesce(n_ref_c,0)<{config.min_reference} THEN 'referencia_insuficiente' END) AS motivo_concentracion,
            coalesce(motivo_temporal, CASE WHEN entidad_id IS NULL OR proveedor_id IS NULL THEN 'sin_identificador_fiable'
                WHEN segmento IS NULL THEN 'sin_causal_o_tipo'
                WHEN ventana_inicio<DATE {literal(config.coverage_start)} THEN 'ventana_sin_cobertura_completa'
                WHEN coalesce(n_ref_r,0)<{config.min_reference} THEN 'referencia_insuficiente'
                WHEN {ties} THEN 'empate_percentiles_pendiente_calibracion' END) AS motivo_recurrencia,
            coalesce(motivo_temporal,motivo_mensual, CASE WHEN coalesce(n_ref_a,0)<{config.min_value_group} THEN 'referencia_insuficiente'
                WHEN mediana_a<=0 OR mediana_a IS NULL THEN 'mediana_no_valida' END) AS motivo_valor_atipico
            FROM evidence''')
        run(f'''CREATE TEMP TABLE scores AS SELECT *,
            CASE WHEN motivo_concentracion IS NOT NULL THEN NULL WHEN concentracion>=p95_c THEN 2 WHEN concentracion>=p90_c THEN 1 ELSE 0 END::SMALLINT AS score_concentracion,
            CASE WHEN motivo_recurrencia IS NOT NULL THEN NULL WHEN recurrencia<2 THEN 0 WHEN recurrencia>=p95_r THEN 2 WHEN recurrencia>=p90_r THEN 1 ELSE 0 END::SMALLINT AS score_recurrencia,
            CASE WHEN motivo_valor_atipico IS NOT NULL THEN NULL WHEN valor_mensual>=p95_a AND ratio_valor>={config.high_ratio} THEN 2 WHEN valor_mensual>=p90_a THEN 1 ELSE 0 END::SMALLINT AS score_valor_atipico
            FROM reasons''')
        params = config.evidence()
        run(f'''CREATE TEMP TABLE final AS WITH composed AS (SELECT *,
            (score_concentracion+score_recurrencia+score_valor_atipico)::SMALLINT AS ircd FROM scores)
            SELECT *, CASE WHEN ircd IS NULL THEN 'incompleto' ELSE 'completo' END AS estado_indice,
              CASE WHEN ircd IS NULL THEN 'componentes parciales; requiere verificar cobertura'
                   WHEN ircd>={config.priority_from} THEN 'revision prioritaria'
                   WHEN ircd>={config.preliminary_from} THEN 'revision preliminar' ELSE 'seguimiento ordinario' END AS nivel_revision,
              DATE {cutoff} AS fecha_corte, current_timestamp::TIMESTAMP AS fecha_calculo,
              {literal(config.version)} AS version_parametros, {literal(params['parameter_hash'])} AS huella_parametros,
              {literal(config.temporal_mode)} AS modo_temporal, {literal(config.value_definition)} AS definicion_valor,
              {literal(config.person_filter)} AS filtro_personas,
              DATE {baseline_start} AS referencia_inicio, DATE {baseline_end} AS referencia_fin,
              year(DATE {baseline_end}) AS anio_referencia_valor,
              CASE WHEN anio BETWEEN year(DATE {baseline_start}) AND year(DATE {baseline_end}) THEN 'calibracion'
                   WHEN anio={config.validation_year} THEN 'validacion'
                   WHEN anio={config.monitoring_year} THEN 'seguimiento' ELSE 'otro_periodo' END AS rol_periodo,
              'SECOP II jbjy-vk9h; snapshot actual, sin versiones historicas as-of' AS fuente_referencia
            FROM composed''')
        n = count('final')
        unique = con.execute('SELECT count(DISTINCT id_contrato) FROM final').fetchone()[0]
        if n != unique or n != count('universe'):
            raise ValueError('No se conserva una fila por contrato')
        run(f"COPY final TO {literal((output/'contratos.parquet').as_posix())} (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 50000)")
        # Subcarpeta aparte: nunca se carga como si fueran contratos analizados.
        rejected = output/'exclusiones'
        rejected.mkdir(exist_ok=True)
        run(f"COPY (SELECT id_contrato,huella_registro,fuente_archivo,motivo_exclusion FROM classified WHERE motivo_exclusion IS NOT NULL) TO {literal((rejected/'exclusiones.parquet').as_posix())} (FORMAT PARQUET, COMPRESSION SNAPPY)")
        counts = {'contratos_entrada': count('source'), 'contratos_gold': n,
                  'indice_completo': count('final', "estado_indice='completo'"),
                  'indice_incompleto': count('final', "estado_indice='incompleto'")}
        exclusions = dict(con.execute('SELECT motivo_exclusion,count(*) FROM classified WHERE motivo_exclusion IS NOT NULL GROUP BY 1').fetchall())
        if n+sum(exclusions.values()) != counts['contratos_entrada']:
            raise ValueError('No se reconcilian entradas, exclusiones y salida')
        diagnostics = {}
        for comp in ('concentracion', 'recurrencia', 'valor_atipico'):
            diagnostics[comp] = {'calculables':count('final',f'score_{comp} IS NOT NULL'),
                'alertas':count('final',f'score_{comp}>0'),
                'motivos_null': dict(con.execute(f'SELECT motivo_{comp},count(*) FROM final WHERE motivo_{comp} IS NOT NULL GROUP BY 1').fetchall())}
        diagnostics['intersecciones'] = [dict(zip(['c','r','a','contratos'], row)) for row in
            con.execute('SELECT score_concentracion>0,score_recurrencia>0,score_valor_atipico>0,count(*) FROM final GROUP BY 1,2,3').fetchall()]
        diagnostics['correlacion_C_R_scores'] = con.execute('SELECT corr(score_concentracion,score_recurrencia) FROM final').fetchone()[0]
        diagnostics['contratos_empate_R'] = count('final','p90_r=p95_r')
        diagnostics['contratos_alertables_R_si_aceptar_empates'] = count('final','p90_r=p95_r AND recurrencia>=2 AND recurrencia>=p90_r')
        result = {'counts': counts, 'exclusiones': exclusions, 'diagnosticos': diagnostics,
                  'parametros': params, 'conservacion_contratos':True,
                  'limitations':['Foto anual del snapshot, no reconstrucción de información disponible al firmar.',
                     'Valor de la fuente definido como total; no acreditado como monto inicial.',
                     'A requiere categorías revisadas y validación de remuneración/comparabilidad entre años.',
                     'Las referencias y umbrales son diagnósticos iniciales; falta revisión de expedientes.']}
        (output/'auditoria_ircd.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        return result
    finally:
        con.close()
        # DuckDB libera su spill al cerrar. rmdir solo elimina el directorio vacío.
        if spill.exists() and not any(spill.iterdir()):
            spill.rmdir()
