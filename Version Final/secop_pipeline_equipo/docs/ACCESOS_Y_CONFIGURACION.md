# Accesos, usuarios, puertos y contraseñas

[Inicio](../README.md) · [Operación](GUIA_OPERACION.md) · [Mapa](MAPA_PROYECTO.md)

Configuración local revisada el **23 de octubre de 2026**. Esta es una guía para encontrar credenciales, no una comprobación de que los servicios estén levantados. Las contraseñas están configuradas en los archivos locales indicados; no se duplican aquí.

## 1. Accesos principales

| Servicio | Dirección desde este computador | Usuario / base | Dónde consultar la contraseña |
|---|---|---|---|
| Grafana web | http://localhost:3001 | Usuario web inicial `admin` | [`grafana_secop/.env`](../grafana_secop/.env), variable `GRAFANA_ADMIN_PASSWORD` |
| PostgreSQL local del ETL y tableros | `localhost:5432` | Usuario `secop_user`; base `secop` | [`.env`](../.env), variable `POSTGRES_PASSWORD` |
| PostgreSQL local: lector de Grafana | Misma base `secop`, puerto 5432 | `grafana_lectura` | [`grafana_secop/.env`](../grafana_secop/.env), `GRAFANA_DB_PASSWORD` |
| PostgreSQL aislado opcional | `localhost:5434` | Usuario `secop_admin`; base `secop_dashboard` | [`grafana_secop/.env`](../grafana_secop/.env), `PG_PASSWORD` |
| AWS RDS | `proyectobd.c10pdetlifr0.us-east-1.rds.amazonaws.com:5432` | Usuario `postgres`; base `basedatosproyectobd` | [`.env.aws`](../.env.aws), `SECOP_RDS_PASSWORD` |
| JupyterLab | URL que imprime `python -m jupyter lab`; normalmente puerto 8888, puede cambiar | Acceso mediante token/configuración de Jupyter | Consultar la URL de la terminal; no hay contraseña fija definida por este proyecto |
| Prefect | URL que registra el flujo según el perfil activo | Cuenta/perfil Prefect configurado en el equipo | Sesión/perfil de Prefect; no hay una contraseña de consola definida por este repo |
| API datos.gov.co | Recursos públicos definidos en `secop_pipeline/config.py` | Sin usuario PostgreSQL | `DATA_GOV_APP_TOKEN` en `.env`; actualmente vacío, es opcional para el cliente |

No hay credenciales de consola AWS/IAM definidas aquí. El usuario `postgres` pertenece a la base RDS, no permite entrar a la consola AWS.

## 2. Cómo encontrar cada contraseña

Abrir el archivo indicado en un editor local y buscar el nombre exacto de la variable. Por ejemplo, en `.env.aws` buscar `SECOP_RDS_PASSWORD=` y copiar solo su valor al campo Contraseña de DBeaver. No copiar la línea completa ni compartir capturas del archivo.

Los enlaces a `.env` pueden no abrir en un visor web; los archivos deben existir en la copia local. Si recibiste el proyecto sin ellos, pide al responsable los valores por un canal privado y usa las plantillas `.env.example`/`.env.aws.example`/`grafana_secop/.env.example` para crear los archivos. No usar los valores de ejemplo como prueba de acceso.

El acceso a estos archivos da acceso a sus secretos. `.gitignore` impide incorporaciones nuevas habituales, pero no es cifrado ni elimina archivos que ya estaban rastreados. No incluir los `.env` reales al compartir ZIP o GitHub.

## 3. Diferencias que suelen causar confusión

- **Grafana admin**: permite entrar a la web y administrar paneles.
- **grafana_lectura**: permite a Grafana consultar PostgreSQL; no sirve para entrar a la web.
- **secop_user**: realiza operaciones locales del proyecto; no es el usuario RDS.
- **postgres**: usuario RDS configurado para la publicación AWS.
- **secop_admin**: solo se utiliza en el modo PostgreSQL aislado de Grafana.

El modo actual de `grafana_secop/publicacion.json` es `existing`: usa PostgreSQL local existente. El servicio aislado de puerto 5434 es opcional y no se inicia por defecto en ese modo.

## 4. Conexión con DBeaver u otro cliente SQL

### Local

Seleccionar PostgreSQL, host `localhost`, puerto `5432`, base `secop`, usuario `secop_user` y contraseña de `.env`. Los resultados de tableros nuevos están en `secop_v3`; Silver original en `secop` y Gold original en `gold`. Las tablas de transferencia internas pueden estar en `secop_panel`; no asumir que todo ese esquema está obsoleto.

### AWS

Seleccionar PostgreSQL, host RDS de la tabla, puerto `5432`, base `basedatosproyectobd`, usuario `postgres` y contraseña de `.env.aws`. SSL debe estar habilitado: el código exige `require`, `verify-ca` o `verify-full` (actual `require`). Para ver resultados nuevos abrir `analytics_v3.indicadores`, `analytics_v3.dimensiones`, `analytics_v3.clasificaciones` y `analytics_v3.publicacion`.

Un timeout se investiga como conectividad, ruta de red y reglas RDS antes de atribuirlo a autenticación. No abrir el puerto indiscriminadamente: se necesita una ruta autorizada desde el equipo o red que ejecuta el notebook.

## 5. Docker y contraseñas persistidas

El notebook 12 ya contiene los comandos para iniciar los servicios. Abrir Docker Desktop antes. No es obligatorio hacer otro Compose manual si se ejecuta esa celda.

Dentro del contenedor Grafana se usa `host.docker.internal` para llegar al PostgreSQL del computador. `localhost` dentro de un contenedor sería el propio contenedor, no Windows. `runtime.py` pasa host/puerto/base/usuario a Compose.

Cambiar una contraseña en `.env` **no cambia automáticamente** la contraseña del usuario de una base inicializada. `configure_reader` crea el rol si no existe, pero no cambia la contraseña del rol existente. Lo mismo aplica a la contraseña inicial de Grafana cuando ya hay un volumen persistente. Si el archivo y la base no coinciden, el responsable debe sincronizarlos; no borrar volúmenes para intentar solucionarlo.

## 6. Variables de configuración

| Archivo | Variables principales |
|---|---|
| `.env` | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `START_DATE`, `CUTOFF_DATE`, `DATA_GOV_APP_TOKEN` |
| `.env.aws` | `SECOP_RDS_HOST`, `SECOP_RDS_PORT`, `SECOP_RDS_USER`, `SECOP_RDS_DATABASE`, `SECOP_RDS_PASSWORD`, `SECOP_RDS_SSLMODE`; certificado opcional `SECOP_RDS_SSLROOTCERT` |
| `grafana_secop/.env` | `GRAFANA_PORT`, `GRAFANA_ADMIN_PASSWORD`, `GRAFANA_DB_PASSWORD`; modo aislado `PG_PORT`, `PG_PASSWORD` |
| `indicador_v3_config.json` | Reglas, corte/cobertura, memoria, hilos y cupo. No contiene contraseñas. |
| `grafana_secop/publicacion.json` | Modo local, fuentes Gold, rol lector, límites de tiempo. No contiene contraseñas. |

AWS carga `.env.aws` con prioridad sobre `.env`. Si `SECOP_RDS_DATABASE` no está definido usa `basedatosproyectobd` según el código. Una variable de entorno externa también puede afectar configuración; esta tabla refleja los archivos locales, no una sesión remota.

El proyecto no fija un puerto de dashboard Dask para v3 porque no usa un clúster Dask. En el ETL original consultar la URL registrada por el clúster, sin asumir que siempre sea 8787.
