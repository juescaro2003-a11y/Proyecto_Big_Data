"""Proyecciones y dos agregados materiales. No contiene cálculo del modelo."""
from psycopg import sql
from .publicacion import ident


def view(c,name,query):
    c.execute(sql.SQL('CREATE OR REPLACE VIEW {} AS ').format(sql.Identifier('secop_panel','dash_'+name))+sql.SQL(query))


def refresh_views(c, refs, evidence, changed, first, monitor):
    contractual=first or bool(changed & {'gold','alerts'})
    if contractual:
        gold=ident(refs['gold']).as_string(c);alerts=ident(refs['alerts']).as_string(c)
        query=f'''SELECT g.id_contrato,cast(g.fecha_de_firma AS date) firma,
            coalesce(extract(year FROM g.fecha_de_firma),0)::int anio,g.nombre_entidad entidad,
            g.departamento,g.tipo_de_contrato tipo,g.proveedor_adjudicado proveedor,
            g.valor_del_contrato valor,coalesce(a.clasificacion,'SIN_RESULTADO_ALERTAS') clasificacion,
            a.d1_concentracion::bigint d1,a.d2_valor_atipico::bigint d2,a.alerta_d3::bigint d3,
            a.motivo_d1,a.motivo_d2,a.motivo_d3,a.participacion_proveedor,a.umbral_d1,
            a.zscore_robusto,a.umbral_d2,a.participacion_diciembre,a.umbral_d3,
            a.dimensiones_evaluadas::bigint,a.motivos_alerta,g.objeto_del_contrato,g.urlproceso,
            a.score_min::bigint,a.score_max::bigint,a.version_modelo,a.fecha_corte,a.huella_gold,
            a.huella_parametros,a.parametros_json,a.prefect_flow_run_id
            FROM {gold} g LEFT JOIN {alerts} a USING(id_contrato)
            WHERE NOT EXISTS (SELECT 1 FROM secop_panel.publication_products
                WHERE product IN ('gold','alerts') AND (dirty OR oid IS DISTINCT FROM to_regclass(relation)::oid::bigint))'''
        # Cuerpo SQL en texto: PostgreSQL no fija dependencia con el OID de Gold.
        # STABLE + SECURITY INVOKER permite inlining y pushdown de ID/LIMIT.
        # Así load.py puede reemplazar su tabla sin cambiarse ni usar CASCADE.
        returns='''id_contrato text,firma date,anio int,entidad text,departamento text,tipo text,
            proveedor text,valor double precision,clasificacion text,d1 bigint,d2 bigint,d3 bigint,
            motivo_d1 text,motivo_d2 text,motivo_d3 text,participacion_proveedor double precision,
            umbral_d1 double precision,zscore_robusto double precision,umbral_d2 double precision,
            participacion_diciembre double precision,umbral_d3 double precision,dimensiones_evaluadas bigint,
            motivos_alerta text,objeto_del_contrato text,urlproceso text,score_min bigint,score_max bigint,
            version_modelo text,fecha_corte date,huella_gold text,huella_parametros text,parametros_json text,
            prefect_flow_run_id text'''
        c.execute(sql.SQL('CREATE OR REPLACE FUNCTION secop_panel.contracts_rows() RETURNS TABLE ('+returns+') LANGUAGE sql STABLE SECURITY INVOKER AS {}').format(sql.Literal(query)))
        view(c,'contratos','SELECT * FROM secop_panel.contracts_rows()')
        # El detalle completo de las métricas del modelo permanece disponible, sin perder columnas.
        columns=c.execute('''SELECT attname,format_type(atttypid,atttypmod) FROM pg_attribute
            WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum''',(refs['alerts'],)).fetchall()
        columns=[(n,'bigint' if t in ('smallint','integer','bigint') else t) for n,t in columns]
        returns_model=sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(n),sql.SQL(t)) for n,t in columns)
        selected=sql.SQL(',').join(sql.SQL('{}::{}').format(sql.Identifier(n),sql.SQL(t)) for n,t in columns).as_string(c)
        c.execute(sql.SQL('CREATE OR REPLACE FUNCTION secop_panel.model_rows() RETURNS TABLE ({}) LANGUAGE sql STABLE SECURITY INVOKER AS {}').format(
            returns_model,sql.Literal(f'SELECT {selected} FROM {alerts}')))
        view(c,'alertas_modelo','SELECT * FROM secop_panel.model_rows()')
        cube='''SELECT anio,clasificacion,entidad,tipo,date_trunc('month',firma) mes,
            grouping(clasificacion) gc,grouping(entidad) ge,grouping(tipo) gt,grouping(date_trunc('month',firma)) gm,
            count(*) n,sum(CASE WHEN valor>0 AND valor<'Infinity'::float8 THEN valor ELSE 0 END) valor_reportado,
            count(*) FILTER(WHERE valor IS NULL OR NOT(valor>0 AND valor<'Infinity'::float8)) valor_no_positivo,
            count(*) FILTER(WHERE valor>1e13 AND valor<'Infinity'::float8) valores_extremos,
            count(*) FILTER(WHERE clasificacion='ALERTA') alertas,
            count(*) FILTER(WHERE clasificacion='NO_EVALUABLE') no_evaluables,
            count(*) FILTER(WHERE clasificacion='SIN_RESULTADO_ALERTAS') sin_resultado,
            count(*) FILTER(WHERE d1=1) a1,count(d1) e1,
            count(*) FILTER(WHERE d2=1) a2,count(d2) e2,
            count(*) FILTER(WHERE d3=1) a3,count(d3) e3
            FROM secop_panel.dash_contratos GROUP BY GROUPING SETS
            ((anio,clasificacion),(anio,entidad),(anio,tipo),(anio,date_trunc('month',firma)))'''
        materialize(c,'cubo_contratos',cube)
    else:monitor.log('agregados_contratos','OMITIDA_MISMO_CONTENIDO')
    view(c,'contratos_resumen','SELECT anio,clasificacion,n,valor_reportado,valor_no_positivo,valores_extremos FROM secop_panel.cubo_contratos WHERE gc=0')
    view(c,'entidades',"SELECT anio,coalesce(entidad,'Sin información') entidad,n,alertas,no_evaluables,sin_resultado FROM secop_panel.cubo_contratos WHERE ge=0")
    view(c,'contratos_tipos',"SELECT anio,coalesce(tipo,'Sin información') tipo,n FROM secop_panel.cubo_contratos WHERE gt=0")
    view(c,'contratos_mensual','SELECT anio,mes,n FROM secop_panel.cubo_contratos WHERE gm=0 AND mes IS NOT NULL')
    view(c,'dimensiones', '''SELECT anio,dimension,estado,sum(cantidad)::bigint n FROM secop_panel.cubo_contratos,
        LATERAL (VALUES ('D1','SEÑAL',a1),('D1','SIN_SEÑAL',e1-a1),('D1','NO_EVALUABLE',n-e1-sin_resultado),
        ('D1','SIN_RESULTADO_ALERTAS',sin_resultado),('D2','SEÑAL',a2),('D2','SIN_SEÑAL',e2-a2),
        ('D2','NO_EVALUABLE',n-e2-sin_resultado),('D2','SIN_RESULTADO_ALERTAS',sin_resultado),
        ('D3','SEÑAL',a3),('D3','SIN_SEÑAL',e3-a3),('D3','NO_EVALUABLE',n-e3-sin_resultado),
        ('D3','SIN_RESULTADO_ALERTAS',sin_resultado)) v(dimension,estado,cantidad)
        WHERE gc=0 GROUP BY anio,dimension,estado''')
    if 'processes' in refs:
        p=ident(refs['processes']).as_string(c)
        view(c,'procesos_modelo',f'SELECT * FROM {p}')
        view(c,'procesos',f'''SELECT dashboard_row_id,id_del_proceso,id_del_portafolio,
            cast(fecha_de_publicacion_del AS date) fecha,
            coalesce(extract(year FROM fecha_de_publicacion_del),0)::int anio,
            entidad,departamento_entidad departamento,tipo_de_contrato tipo,
            estado_del_procedimiento estado,precio_base,proveedores_invitados,
            respuestas_al_procedimiento respuestas FROM {p}''')
    else:
        view(c,'procesos','''SELECT NULL::bigint dashboard_row_id,NULL::text id_del_proceso,NULL::text id_del_portafolio,
            NULL::date fecha,NULL::int anio,NULL::text entidad,NULL::text departamento,NULL::text tipo,
            NULL::text estado,NULL::float8 precio_base,NULL::float8 proveedores_invitados,NULL::float8 respuestas WHERE false''')
    # Incluye el estado en identidad: desactivar legado elimina su visibilidad, no borra la fuente.
    status_changed=True
    prior=c.execute('SELECT evidence->>\'process_status\' FROM secop_panel.publication_current').fetchone()
    if prior:status_changed=prior[0]!=evidence['process_status']
    if first or 'processes' in changed or status_changed:
        cube='''SELECT anio,estado,tipo,date_trunc('month',fecha) mes,
            grouping(estado) ge,grouping(tipo) gt,grouping(date_trunc('month',fecha)) gm,grouping(anio) ga,
            count(*) n,count(DISTINCT id_del_proceso) ids_distintos,
            count(*) FILTER(WHERE id_del_proceso IS NULL) sin_id,
            count(id_del_proceso)-count(DISTINCT id_del_proceso) repeticiones_id,
            min(fecha) primera_publicacion,max(fecha) ultima_publicacion,
            sum(CASE WHEN precio_base>0 AND precio_base<'Infinity'::float8 THEN precio_base ELSE 0 END) presupuesto_reportado
            FROM secop_panel.dash_procesos GROUP BY GROUPING SETS
            ((anio,estado),(anio,tipo),(anio,date_trunc('month',fecha)),())'''
        materialize(c,'cubo_procesos',cube)
    else:monitor.log('agregados_procesos','OMITIDA_MISMO_CONTENIDO')
    view(c,'procesos_resumen',"SELECT anio,coalesce(estado,'Sin información') estado,n,ids_distintos,sin_id,presupuesto_reportado FROM secop_panel.cubo_procesos WHERE ge=0")
    view(c,'procesos_tipos',"SELECT anio,coalesce(tipo,'Sin información') tipo,n FROM secop_panel.cubo_procesos WHERE gt=0")
    view(c,'procesos_mensual','SELECT anio,mes,n FROM secop_panel.cubo_procesos WHERE gm=0 AND mes IS NOT NULL')
    view(c,'procesos_calidad','SELECT n registros,ids_distintos,sin_id,repeticiones_id,primera_publicacion,ultima_publicacion FROM secop_panel.cubo_procesos WHERE ga=1')
    view(c,'metadatos', '''SELECT identity publicacion,evidence->>'process_status' estado_procesos,
        evidence->'report'->'parametros'->>'version' version_modelo,
        evidence->'report'->'parametros'->>'cutoff' fecha_corte,
        evidence->'report'->>'flow_run_id' ejecucion,
        (evidence->'gold'->>'rows')::bigint contratos_gold,
        (evidence->'alerts'->>'rows')::bigint contratos_resultado,
        evidence->'gold'->>'hash' huella_producto_gold,
        NOT EXISTS (SELECT 1 FROM secop_panel.publication_products WHERE product IN ('gold','alerts')
            AND (dirty OR oid IS DISTINCT FROM to_regclass(relation)::oid::bigint)) fuentes_vigentes
        ,evidence->'processes'->>'cutoff' corte_procesos,
        evidence->'processes'->>'coverage_start' inicio_procesos,
        (evidence->'processes'->>'rows')::bigint filas_procesos,
        evidence->'processes'->>'manifest_identity' identidad_gold_procesos,
        evidence->'processes'->'selection'->>'fecha_min_observada_footer' primera_publicacion_procesos,
        evidence->'processes'->'selection'->>'fecha_max_observada_footer' ultima_publicacion_procesos
        FROM secop_panel.publication_current''')


def materialize(c,name,query):
    if c.execute('SELECT to_regclass(%s)',('secop_panel.'+name,)).fetchone()[0] is None:
        c.execute(sql.SQL('CREATE MATERIALIZED VIEW {} AS ').format(sql.Identifier('secop_panel',name))+sql.SQL(query))
    else:
        c.execute(sql.SQL('REFRESH MATERIALIZED VIEW {}').format(sql.Identifier('secop_panel',name)))
    c.execute(sql.SQL('ANALYZE {}').format(sql.Identifier('secop_panel',name)))
