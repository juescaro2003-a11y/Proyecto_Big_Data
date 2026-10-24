"""Configuración local, tiempos máximos y ejecución de servicios; no instala paquetes."""
from pathlib import Path
from contextlib import contextmanager
import json
import os
import queue
import subprocess
import threading
import time
from dotenv import dotenv_values
import psycopg
from psycopg import sql


def config(root):
    return json.loads((root/'grafana_secop/publicacion.json').read_text(encoding='utf-8'))


def connection_args(root,cfg):
    if cfg['database_mode']=='existing':
        env=dotenv_values(root/'.env')
        args=dict(host=env.get('POSTGRES_HOST','localhost'),port=int(env.get('POSTGRES_PORT','5432')),
                  dbname=env.get('POSTGRES_DB','secop'),user=env.get('POSTGRES_USER','secop_user'),
                  password=env.get('POSTGRES_PASSWORD',''))
    elif cfg['database_mode']=='isolated':
        env=dotenv_values(root/'grafana_secop/.env')
        args=dict(host='localhost',port=int(env.get('PG_PORT','5434')),dbname='secop_dashboard',
                  user='secop_admin',password=env.get('PG_PASSWORD',''))
    else:raise ValueError('database_mode: existing o isolated')
    if args['host'] not in ('localhost','127.0.0.1','::1'):
        raise ValueError('SOLO_LOCAL: esta publicación no admite AWS ni hosts remotos')
    args['connect_timeout']=int(cfg['connect_timeout'])
    args['options']=f"-c statement_timeout={int(cfg['statement_timeout_ms'])} -c lock_timeout={int(cfg['lock_timeout_ms'])}"
    args['autocommit']=True
    return args


def inspect_database(root,cfg):
    args=connection_args(root,cfg)
    with psycopg.connect(**args) as c:
        with c.transaction():
            c.execute('SET TRANSACTION READ ONLY')
            return {'database':args['dbname'],'port':args['port'],
                'tables':c.execute("SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY 1,2").fetchall(),
                'published':c.execute("SELECT to_regclass('secop_panel.publication_current')").fetchone()[0]}


def configure_reader(root,cfg):
    env=dotenv_values(root/'grafana_secop/.env')
    password=env.get('GRAFANA_DB_PASSWORD')
    if not password or password.startswith('REEMPLAZAR'):raise ValueError('Configure GRAFANA_DB_PASSWORD en grafana_secop/.env')
    role=cfg['reader_role']
    with psycopg.connect(**connection_args(root,cfg)) as c:
        if not c.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,)).fetchone():
            c.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(sql.Identifier(role),sql.Literal(password)))
            c.execute(sql.SQL("ALTER ROLE {} SET statement_timeout='30s'").format(sql.Identifier(role)))
            c.execute(sql.SQL('ALTER ROLE {} SET default_transaction_read_only=on').format(sql.Identifier(role)))
        # No cambia la contraseña de un rol existente ni otorga acceso a tablas ajenas.
        if c.execute("SELECT to_regnamespace('secop_panel')").fetchone()[0]:
            c.execute(sql.SQL('GRANT USAGE ON SCHEMA secop_panel TO {}').format(sql.Identifier(role)))
            if c.execute("SELECT to_regclass('secop_panel.publication_products')").fetchone()[0]:
                from .publicacion import GOLD_COLUMNS,ident
                c.execute(sql.SQL('GRANT SELECT ON secop_panel.publication_products TO {}').format(sql.Identifier(role)))
                for product,relation in c.execute('SELECT product,relation FROM secop_panel.publication_products').fetchall():
                    names=[r[0] for r in c.execute('SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(%s) AND attnum>0 AND NOT attisdropped',(relation,))]
                    columns=[n for n in names if product!='gold' or n in GOLD_COLUMNS]
                    if columns:
                        c.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(relation.split('.')[0]),sql.Identifier(role)))
                        c.execute(sql.SQL('GRANT SELECT ({}) ON {} TO {}').format(sql.SQL(',').join(map(sql.Identifier,columns)),ident(relation),sql.Identifier(role)))
            for (view,) in c.execute("SELECT viewname FROM pg_views WHERE schemaname='secop_panel' AND viewname LIKE 'dash_%'").fetchall():
                c.execute(sql.SQL('GRANT SELECT ON {} TO {}').format(sql.Identifier('secop_panel',view),sql.Identifier(role)))
        if c.execute("SELECT to_regclass('secop_v3.productos')").fetchone()[0]:
            from .publicacion import GOLD_COLUMNS,ident
            c.execute(sql.SQL('GRANT USAGE ON SCHEMA secop_v3 TO {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA secop_v3 TO {}').format(sql.Identifier(role)))
            for product,relation in c.execute('SELECT product,relation FROM secop_v3.productos').fetchall():
                names=[r[0] for r in c.execute('SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(%s) AND attnum>0 AND NOT attisdropped',(relation,))]
                columns=[n for n in names if product!='gold' or n in GOLD_COLUMNS]
                if columns:
                    c.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(relation.split('.')[0]),sql.Identifier(role)))
                    c.execute(sql.SQL('GRANT SELECT ({}) ON {} TO {}').format(sql.SQL(',').join(map(sql.Identifier,columns)),ident(relation),sql.Identifier(role)))


def execute(command,cwd,timeout,env=None):
    """Timeout también cuando el subproceso no imprime ninguna línea."""
    started=time.monotonic();messages=queue.Queue()
    process=subprocess.Popen(command,cwd=cwd,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                             text=True,encoding='utf-8',errors='replace')
    def reader():
        for line in process.stdout:messages.put(line)
    worker=threading.Thread(target=reader,daemon=True);worker.start()
    try:
        while process.poll() is None or not messages.empty():
            if time.monotonic()-started>timeout:raise TimeoutError('TIEMPO_MAXIMO: subproceso detenido')
            try:print(messages.get(timeout=.2),end='',flush=True)
            except queue.Empty:pass
        worker.join(timeout=2)
        while not messages.empty():print(messages.get_nowait(),end='')
        if process.returncode:raise RuntimeError(f'SUBPROCESO: código {process.returncode}')
    finally:
        if process.poll() is None:
            process.terminate()
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        process.stdout.close()
    print(f'Finalizado en {time.monotonic()-started:.1f} s',flush=True)


def docker_up(root,cfg):
    args=connection_args(root,cfg)
    env=os.environ.copy()
    env.update(GRAFANA_DB_HOST='host.docker.internal',GRAFANA_DB_PORT=str(args['port']),
               GRAFANA_DB_NAME=args['dbname'],GRAFANA_DB_USER=cfg['reader_role'])
    command=['docker','compose']
    if cfg['database_mode']=='isolated':command+=['--profile','isolated']
    command+=['up','-d','--wait','--wait-timeout',str(cfg['docker_timeout'])]
    execute(command,root/'grafana_secop',cfg['docker_timeout']+15,env)


def latest_result(root):
    pointer=json.loads((root/'salida/alertas/ultima_ejecucion.json').read_text(encoding='utf-8-sig'))
    return Path(pointer['directorio'])/'resultados.parquet'
