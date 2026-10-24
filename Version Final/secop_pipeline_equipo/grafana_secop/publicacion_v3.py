"""Publicación local incremental v3. Reutiliza Gold; no ejecuta cálculos de riesgo."""
from pathlib import Path
import hashlib,json,tempfile
import duckdb
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.types.json import Jsonb
from secop_pipeline.indicador_v3 import SIGNALS
from secop_pipeline.indicador_v3_publicacion import inspect_result
from .publicacion import Monitor,product,transfer,ident,GOLD_COLUMNS,existing_products,source_locks,matches_existing,parquet_view
from .seleccion_gold import gold_path,process_files,describe_processes


def view(c,name,query):
    # Los agregados materializados tampoco deben mostrar datos de fuentes alteradas.
    if name!='metadatos':query=f'SELECT * FROM ({query}) vigente WHERE {VALID}'
    c.execute(sql.SQL('CREATE OR REPLACE VIEW {} AS ').format(sql.Identifier('secop_v3','dash_'+name))+sql.SQL(query))


def materialize(c,name,query):
    rel='secop_v3.'+name
    if c.execute('SELECT to_regclass(%s)',(rel,)).fetchone()[0] is None:
        c.execute(sql.SQL('CREATE MATERIALIZED VIEW {} AS ').format(ident(rel))+sql.SQL(query))
    else:c.execute(sql.SQL('REFRESH MATERIALIZED VIEW {}').format(ident(rel)))
    c.execute(sql.SQL('ANALYZE {}').format(ident(rel)))


VALID="NOT EXISTS(SELECT 1 FROM secop_v3.productos WHERE dirty OR oid IS DISTINCT FROM to_regclass(relation)::oid::bigint)"


def refresh(c,refs,changed):
    g,a,p=[ident(refs[k]).as_string(c) for k in ('gold','alerts','processes')]
    flags=','.join(f'a.{s}::bigint {s}' for s in SIGNALS)
    reasons=','.join(f'a.motivo_{s}' for s in SIGNALS)
    query=f'''SELECT g.id_contrato,cast(g.fecha_de_firma AS date) firma,extract(year FROM g.fecha_de_firma)::int anio,
        g.nombre_entidad entidad,g.proveedor_adjudicado proveedor,g.departamento,g.tipo_de_contrato tipo,
        g.valor_del_contrato valor,a.puntaje,a.puntaje_min,a.puntaje_max,
        coalesce(a.clasificacion,'SIN_RESULTADO_ALERTAS') clasificacion,a.alerta_binaria::bigint,
        {flags},a.dimensiones_evaluadas::bigint,{reasons},a.motivo_valor_invalido,
        a.participacion_entidad,a.hhi,a.dependencia_proveedor,a.z_robusto,
        a.n_repetidos::bigint,a.n_segmentos::bigint,a.periodo_revision,a.rango_revision::bigint,
        a.anio_completo,g.objeto_del_contrato,g.urlproceso
        FROM {g} g LEFT JOIN {a} a USING(id_contrato) WHERE {VALID}'''
    returns='''id_contrato text,firma date,anio int,entidad text,proveedor text,departamento text,tipo text,
        valor double precision,puntaje double precision,puntaje_min double precision,puntaje_max double precision,
        clasificacion text,alerta_binaria bigint,'''+','.join(f'{s} bigint' for s in SIGNALS)+',dimensiones_evaluadas bigint,'+','.join(f'motivo_{s} text' for s in SIGNALS)+''',motivo_valor_invalido text,
        participacion_entidad double precision,hhi double precision,dependencia_proveedor double precision,z_robusto double precision,
        n_repetidos bigint,n_segmentos bigint,periodo_revision date,rango_revision bigint,anio_completo boolean,
        objeto_del_contrato text,urlproceso text'''
    c.execute(sql.SQL('CREATE OR REPLACE FUNCTION secop_v3.contracts_rows() RETURNS TABLE ('+returns+') LANGUAGE sql STABLE SECURITY INVOKER AS {}').format(sql.Literal(query)))
    view(c,'contratos','SELECT * FROM secop_v3.contracts_rows()')
    view(c,'alertas_modelo',f'SELECT * FROM {a}')
    if changed & {'gold','alerts'}:
        counts=','.join(f'count({s}) e_{s},count(*) FILTER(WHERE {s}=1) a_{s}' for s in SIGNALS)
        materialize(c,'cubo_contratos',f'''SELECT anio,clasificacion,entidad,tipo,date_trunc('month',firma) mes,
            grouping(clasificacion) gc,grouping(entidad) ge,grouping(tipo) gt,grouping(date_trunc('month',firma)) gm,
            count(*) n,count(*) FILTER(WHERE clasificacion='ALERTA') alertas,
            count(*) FILTER(WHERE clasificacion='NO_EVALUABLE') no_evaluables,
            count(*) FILTER(WHERE clasificacion='SIN_RESULTADO_ALERTAS') sin_resultado,
            {counts} FROM secop_v3.dash_contratos GROUP BY GROUPING SETS
            ((anio,clasificacion),(anio,entidad),(anio,tipo),(anio,date_trunc('month',firma)))''')
        materialize(c,'puntajes',"SELECT anio,least(9,floor(puntaje*10)::int) intervalo,count(*) n FROM secop_v3.dash_contratos WHERE puntaje IS NOT NULL GROUP BY 1,2")
    view(c,'contratos_resumen','SELECT anio,clasificacion,n FROM secop_v3.cubo_contratos WHERE gc=0')
    view(c,'entidades','SELECT anio,entidad,n,alertas,no_evaluables,sin_resultado FROM secop_v3.cubo_contratos WHERE ge=0')
    view(c,'contratos_tipos',"SELECT anio,coalesce(tipo,'Sin información') tipo,n FROM secop_v3.cubo_contratos WHERE gt=0")
    view(c,'contratos_mensual','SELECT anio,mes,n FROM secop_v3.cubo_contratos WHERE gm=0 AND mes IS NOT NULL')
    rows=','.join(f"('{s.upper()}',e_{s},a_{s})" for s in SIGNALS)
    view(c,'dimensiones',f'''SELECT anio,dimension,sum(n)::bigint total,sum(e)::bigint evaluados,sum(a)::bigint alertas,
        sum(e-a)::bigint sin_senal,sum(n-e-sin_resultado)::bigint no_evaluables,sum(sin_resultado)::bigint sin_resultado
        FROM secop_v3.cubo_contratos CROSS JOIN LATERAL(VALUES {rows}) d(dimension,e,a)
        WHERE gc=0 GROUP BY anio,dimension''')
    view(c,'puntajes','SELECT * FROM secop_v3.puntajes')
    view(c,'procesos_modelo',f'SELECT * FROM {p}')
    view(c,'procesos',f'''SELECT dashboard_row_id,id_del_proceso,id_del_portafolio,
        cast(fecha_de_publicacion_del AS date) fecha,coalesce(extract(year FROM fecha_de_publicacion_del),0)::int anio,
        entidad,departamento_entidad departamento,tipo_de_contrato tipo,estado_del_procedimiento estado,
        precio_base,proveedores_invitados,respuestas_al_procedimiento respuestas FROM {p}''')
    if 'processes' in changed:
        materialize(c,'cubo_procesos', '''SELECT anio,estado,tipo,date_trunc('month',fecha) mes,
            grouping(estado) ge,grouping(tipo) gt,grouping(date_trunc('month',fecha)) gm,grouping(anio) ga,
            count(*) n,count(DISTINCT id_del_proceso) ids_distintos,count(*) FILTER(WHERE id_del_proceso IS NULL) sin_id,
            count(id_del_proceso)-count(DISTINCT id_del_proceso) repeticiones_id,min(fecha) primera_publicacion,max(fecha) ultima_publicacion,
            sum(CASE WHEN precio_base>0 AND precio_base<'Infinity'::float8 THEN precio_base ELSE 0 END) presupuesto_reportado
            FROM secop_v3.dash_procesos GROUP BY GROUPING SETS ((anio,estado),(anio,tipo),(anio,date_trunc('month',fecha)),())''')
    view(c,'procesos_calidad','SELECT n registros,ids_distintos,sin_id,repeticiones_id,primera_publicacion,ultima_publicacion FROM secop_v3.cubo_procesos WHERE ga=1')
    view(c,'procesos_resumen',"SELECT anio,coalesce(estado,'Sin información') estado,n,ids_distintos,sin_id,presupuesto_reportado FROM secop_v3.cubo_procesos WHERE ge=0")
    view(c,'procesos_tipos',"SELECT anio,coalesce(tipo,'Sin información') tipo,n FROM secop_v3.cubo_procesos WHERE gt=0")
    view(c,'procesos_mensual','SELECT anio,mes,n FROM secop_v3.cubo_procesos WHERE gm=0 AND mes IS NOT NULL')
    view(c,'metadatos',f'''SELECT identity publicacion,report->'parametros'->>'version' version_modelo,
        report->'parametros'->>'cutoff' fecha_corte,report->'parametros'->>'review_top_k_per_quarter' cupo_trimestral,
        report->'resumen'->>'contratos' contratos_gold,procesos->>'filas' filas_procesos,
        procesos->>'corte_declarado' corte_procesos,procesos->>'fecha_min_observada_footer' primera_publicacion_procesos,
        procesos->>'fecha_max_observada_footer' ultima_publicacion_procesos,
        'GOLD_EXISTENTE_SELECCIONADO'::text estado_procesos,{VALID} fuentes_vigentes FROM secop_v3.publicacion''')


def run(root,result,config,connection_factory):
    root=Path(root);result,report,cfg=inspect_result(result);monitor=Monitor()
    with source_locks(root):
        sources={'gold':product([gold_path(root,config['contract_source'])],GOLD_COLUMNS),
                 'alerts':product([result],['id_contrato',*SIGNALS,'puntaje','clasificacion']),
                 'processes':product(process_files(root,config),['id_del_proceso'])}
        if sources['gold']['files'][0]['sha256']!=report['huella_gold']:raise ValueError('Gold distinto del usado por v3.')
        if sources['alerts']['files'][0]['sha256']!=report['huella_resultados']:raise ValueError('Resultados v3 modificados después de su validación.')
        identity=hashlib.sha256(json.dumps([report['huella_resultados'],sources['gold']['hash'],sources['processes']['hash'],'grafana-v3.0',config.get('process_cutoff')]).encode()).hexdigest()
        info=describe_processes(root,config)
        with connection_factory() as c:
            with c.transaction():
                c.execute("SELECT set_config('lock_timeout',%s,true)",(str(config['lock_timeout_ms']),))
                c.execute('SELECT pg_advisory_xact_lock(87346120)');c.execute('SELECT pg_advisory_xact_lock(73192026)')
                c.execute('CREATE SCHEMA IF NOT EXISTS secop_v3');c.execute('CREATE SCHEMA IF NOT EXISTS secop_panel')
                c.execute('''CREATE TABLE IF NOT EXISTS secop_v3.productos(product text PRIMARY KEY,hash text,relation text,oid bigint,dirty boolean DEFAULT false)''')
                c.execute('''CREATE TABLE IF NOT EXISTS secop_v3.publicacion(singleton boolean PRIMARY KEY CHECK(singleton),identity text,report jsonb,procesos jsonb)''')
                c.execute('''CREATE OR REPLACE FUNCTION secop_v3.mark_dirty() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
                    SET search_path=pg_catalog AS $$ BEGIN UPDATE secop_v3.productos SET dirty=true
                    WHERE relation=TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME;RETURN NULL;END $$''')
                old={k:(h,r,not d and oid==cur) for k,h,r,oid,d,cur in c.execute('SELECT *,to_regclass(relation)::oid::bigint FROM secop_v3.productos')}
                prior=c.execute('SELECT identity FROM secop_v3.publicacion WHERE singleton').fetchone()
                changed={k for k,p in sources.items() if k not in old or old[k][0]!=p['hash'] or not old[k][2]}
                legacy=existing_products(c);refs={};copied=0
                for k,p in sources.items():
                    if k not in changed:refs[k]=old[k][1];continue
                    cols=GOLD_COLUMNS if k=='gold' else pq.read_schema(p['files'][0]['path']).names
                    legacy_match=k in ('gold','processes') and legacy.get(k,{}).get('valid') and legacy[k]['hash']==p['hash']
                    if legacy_match:
                        present={row[0] for row in c.execute('SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(%s) AND attnum>0 AND NOT attisdropped',(legacy[k]['relation'],))}
                        legacy_match=set(cols).issubset(present)
                    adopted=False
                    if k=='gold' and not legacy_match:
                        candidate=config.get('existing_tables',{}).get('gold')
                        with tempfile.TemporaryDirectory(prefix='secop_v3_adopt_') as tmp:
                            with duckdb.connect(config={'memory_limit':'1GB','threads':1,'temp_directory':tmp}) as db:
                                parquet_view(db,'g',p)
                                adopted=matches_existing(c,db,candidate,'g',cols,monitor)
                        if adopted:refs[k]=candidate;monitor.log(k,'ADOPTADO_SIN_COPY')
                    if legacy_match:
                        refs[k]=legacy[k]['relation'];monitor.log(k,'REUTILIZADO')
                    elif not adopted:refs[k]=transfer(c,p,k,cols,monitor);copied+=p['rows']
                    oid=c.execute('SELECT %s::regclass::oid::bigint',(refs[k],)).fetchone()[0]
                    c.execute('''INSERT INTO secop_v3.productos VALUES(%s,%s,%s,%s,false) ON CONFLICT(product)
                        DO UPDATE SET hash=excluded.hash,relation=excluded.relation,oid=excluded.oid,dirty=false''',(k,p['hash'],refs[k],oid))
                    c.execute(sql.SQL('CREATE OR REPLACE TRIGGER dashboard_v3_dirty AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} FOR EACH STATEMENT EXECUTE FUNCTION secop_v3.mark_dirty()').format(ident(refs[k])))
                if not prior:changed=set(sources)
                refresh(c,refs,changed)
                actual=dict(c.execute('SELECT clasificacion,sum(n)::bigint FROM secop_v3.dash_contratos_resumen GROUP BY 1').fetchall())
                if actual!=report['resumen']['clasificaciones']:raise ValueError('Publicación local no coincide con el lote.')
                for name,e,a in c.execute('SELECT dimension,sum(evaluados)::bigint,sum(alertas)::bigint FROM secop_v3.dash_dimensiones GROUP BY 1'):
                    expected=report['resumen']['dimensiones'][name]
                    if (e,a)!=(expected['evaluados'],expected['alertas']):raise ValueError('Agregados locales no coinciden: '+name)
                for p in sources.values():
                    for f in p['files']:
                        st=Path(f['path']).stat()
                        if (st.st_size,st.st_mtime_ns)!=(f['bytes'],f['mtime']):raise RuntimeError('Fuente modificada durante la publicación.')
                c.execute('''INSERT INTO secop_v3.publicacion VALUES(true,%s,%s,%s) ON CONFLICT(singleton)
                    DO UPDATE SET identity=excluded.identity,report=excluded.report,procesos=excluded.procesos''',(identity,Jsonb(report),Jsonb(info)))
                role=config['reader_role']
                if not c.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,)).fetchone():raise ValueError('Ejecute configure_reader antes de publicar.')
                c.execute(sql.SQL('GRANT USAGE ON SCHEMA secop_v3,secop_panel TO {}').format(sql.Identifier(role)))
                c.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA secop_v3 TO {}').format(sql.Identifier(role)))
                for k,rel in refs.items():
                    cols=GOLD_COLUMNS if k=='gold' else pq.read_schema(sources[k]['files'][0]['path']).names
                    c.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(rel.split('.')[0]),sql.Identifier(role)))
                    c.execute(sql.SQL('GRANT SELECT ({}) ON {} TO {}').format(sql.SQL(',').join(map(sql.Identifier,cols)),ident(rel),sql.Identifier(role)))
        return {'estado':'PUBLICADA' if copied else 'SIN_TRANSFERENCIA','version':cfg.version,'filas_transferidas':copied,'contratos':report['resumen']['contratos'],'procesos':info['filas']}
