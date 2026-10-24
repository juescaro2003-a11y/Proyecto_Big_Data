# Mapa completo del proyecto

[Inicio](../README.md) · [Operación](GUIA_OPERACION.md) · [Accesos](ACCESOS_Y_CONFIGURACION.md)

## Carpetas

| Carpeta | Contenido y uso |
|---|---|
| `secop_pipeline/` | Funciones del ETL, indicadores, auditoría y publicación. Los notebooks llaman estos módulos. |
| `flows/` | Orden de tareas Prefect, estados, checkpoints y manejo de fallos. |
| `scripts/` | Entradas por terminal; no contienen un segundo modelo independiente. |
| `notebooks/` | Interfaz de trabajo: configuración, ejecución, inspección y gráficos. |
| `data/bronze/` | CSV originales por base. |
| `data/silver/` | Parquet con tipos/normalización y trazabilidad de valores originales. |
| `data/gold/contratos_control/` | Universo contractual preparado y auditoría. Entrada de v3. |
| `data/gold/procesos_modelo/` | Snapshot existente utilizado por el tablero de procesos. |
| `data/gold/dashboard_publicaciones/` | Manifiestos de publicaciones de la implementación anterior. |
| `salida/alertas/indicador_v3/` | Lotes nuevos, Parquet, DuckDB, spill y manifiestos del indicador. |
| `salida/alertas/` | Punteros v2/v3, lotes históricos y actuales. No borrar en bloque. |
| `salida/grafana/` | Evidencias de publicaciones anteriores. |
| `docs/` | Guías actuales y presentación histórica identificada. |
| `grafana_secop/` | Publicación local, contenedores, tableros y configuración de datasource/alertas. |
| `tests/` | Pruebas existentes del ETL y motores anteriores; no equivalen a cobertura completa v3. |
| `sql/` | Consulta de referencia para inspeccionar PostgreSQL. |
| `versiones anteriores/` | Material archivado, respaldos y su inventario de procedencia. |
| `.dask-spill/` | Temporales de Dask; no son una base final. |
| `.vscode/` | Preferencias del editor local. |
| `__pycache__/`, `.ipynb_checkpoints/` | Cachés Python y copias automáticas de notebooks, donde existan. |

## Scripts del paquete: ETL y Gold

| Archivo en `secop_pipeline/` | Qué hace y quién lo usa |
|---|---|
| `__init__.py` | Declara el paquete importable. |
| `config.py` | Centraliza rutas, recursos de API, columnas, tipos, fechas y parámetros de PostgreSQL. |
| `extract.py` | Cliente Socrata, extracción/reutilización Bronze, manifiestos y huellas. Llamado por 01 y el flujo ETL. |
| `transform.py` | Limpieza de textos y conversión numérica/temporal por archivo; crea Silver y métricas. Llamado por 02/ETL. |
| `quality.py` | Reportes de calidad y estructura de Silver. |
| `control.py` | Normaliza contratos, identificadores y categorías; incluye reglas del IRCD original. |
| `control_config.py` | Valida parámetros/cobertura/filtros del productor contractual. Lee `ircd_config.json`. |
| `control_analytics.py` | Resuelve versiones/duplicados, selecciona universo, calcula IRCD original y auditoría Gold mediante DuckDB. |
| `dask_jobs.py` | Configura etapas A/B, distribuye preparación y ejecuta construcción Gold; `benchmark_dask` es compatibilidad obsoleta. |
| `audit.py` | Registra parámetros, eventos, tiempos, entradas/salidas y genera informes de ejecución. |
| `storage.py` | Locks y publicación de directorios para evitar ejecución concurrente o resultados parciales. |
| `load.py` | COPY de Silver/Gold original a PostgreSQL; no es el publicador compacto v3. |
| `db.py` | Conexión, disponibilidad y creación de esquemas PostgreSQL del ETL. |
| `models.py` | Mapeo SQLAlchemy de tablas/columnas para consultas ORM. |
| `orm.py` | Consultas con los modelos; usado por notebook 04. |

## Scripts del paquete: indicador y compatibilidad

| Archivo | Qué hace / estado |
|---|---|
| `indicador_v3.py` | Motor vigente: cinco etapas, siete señales, red, puntaje, clasificación, resumen y configuración. |
| `indicador_v3_publicacion.py` | Valida lote completo, publica 11 columnas y agregados AWS, comprueba la publicación real. |
| `alertas_aws.py` | Conexión RDS psycopg2, tipos y funciones AWS anteriores. V3 reutiliza utilidades; sigue activo. |
| `alertas_publicacion.py` | Despacha publicación según versión; el CLI compartido lo usa. |
| `alertas.py` | Motor anterior D1–D3. Conservado por compatibilidad y pruebas. |
| `alertas_dask.py` | Referencias distribuidas del D2 anterior. No configura V1 actual. |
| `alertas_cache.py` | Checkpoints/huellas del motor anterior. |
| `alertas_stats.py` | Agregados D1–D3. |
| `alertas_monitor.py` | Lector del manifiesto anterior; el notebook 11 v3 no depende de él. |

## Orquestadores y entradas por terminal

| Archivo | Uso |
|---|---|
| `flows/secop_flow.py` | ETL Bronze/Silver/Gold, validación/auditoría y carga original opcional. |
| `flows/indicador_v3_flow.py` | Flujo vigente desde Gold; locks, checkpoints y publicación del lote local completo. |
| `flows/alertas_flow.py` | Orquestación anterior D1–D3, conservada por pruebas/compatibilidad. |
| `scripts/run_pipeline.py` | CLI del ETL. Actualizar API requiere `--refresh-bronze`. |
| `scripts/run_indicador_v3.py` | CLI del cálculo nuevo, con `--config` y `--gold`. |
| `scripts/publicar_alertas_aws.py` | Publica un lote completo; por defecto busca v3. No reconstruye el ETL. |
| `scripts/db_status.py` | Consulta disponibilidad/estado de PostgreSQL local. |
| `scripts/compare_manifests.py` | Compara manifiestos para revisar diferencias de ejecuciones. |

Los CLI anteriores `run_alertas.py` y `monitor_alertas.py` están archivados: no ejecutarlos directamente desde `versiones anteriores/`.

## Notebooks: cuál abrir

| Notebook | Entrada / función / precaución |
|---|---|
| `00_verificar_entorno` | Revisa librerías y accesos; puede consultar API y PostgreSQL. No instala paquetes. |
| `01_extraccion_api` | API → Bronze; solo cuando se necesita la extracción/reutilización de entrada. |
| `02_transformacion_silver` | Bronze → Silver y reporte de calidad. |
| `03_dask_gold` | Silver → Gold con dos configuraciones Dask. Reconstruye el productor contractual original. |
| `04_postgres_orm` | Carga original Silver/Gold a PostgreSQL y consulta ORM; puede transferir mucho más que v3. |
| `05_prefect_pipeline` | Orquesta ETL. Revisar `MODO`: puede seleccionar solo carga, reprocesamiento o actualización. |
| `06_control_contratacion_directa` | Consulta IRCD original de Gold; permanece porque una prueba lo ejecuta. No representa v3. |
| `07_informe_ejecucion_etl` | Informe generado de una ejecución específica; no confundir con monitor en vivo. |
| `08_eda_fedesarrollo_control` | EDA complementario sobre Silver/Gold inspirado en el estudio. No es cálculo v3. |
| `09_dimensiones_indicador` | Inicio habitual: configura y ejecuta v3 desde Gold. |
| `10_agregados_indicadores` | Análisis del lote v3 completo, tablas y gráficos dentro del notebook. |
| `11_monitor_alertas` | Solo lectura de avance/errores del lote v3; abrir en otro kernel. |
| `12_dashboard_grafana` | Configura servicios, publica localmente y muestra enlaces Grafana. |
| `13_publicar_alertas_aws` | Publicación compacta RDS y evidencia de lectura desde una conexión nueva. |
| `informes_etl/*.ipynb` | Informes históricos por ejecución. |

## Grafana: cada archivo

| Archivo bajo `grafana_secop/` | Responsabilidad |
|---|---|
| `__init__.py` | Paquete Python. |
| `cargar_datos.py` | CLI que selecciona publicador según versión del lote. |
| `publicacion_v3.py` | Transferencia/adopción incremental local, vistas y agregados `secop_v3`. |
| `publicacion.py` | Publicador anterior y utilidades compartidas que v3 importa. |
| `consultas.py` | Vistas SQL de la publicación anterior. |
| `seleccion_gold.py` | Resuelve rutas Gold y describe cobertura de procesos. |
| `runtime.py` | Conexión local, rol lector, límites de tiempo, subprocesos y Docker Compose. |
| `generar_dashboards_v3.py` | Genera diseños nuevos, etiqueta históricos y pausa regla antigua. |
| `generar_dashboards.py` | Plantillas anteriores importadas por v3. No borrar. |
| `publicacion.json` | Modo de base, fuentes Gold, rol lector y timeouts. |
| `docker-compose.yml` | Grafana y PostgreSQL aislado opcional. |
| `postgres/init.sh` | Inicializa rol lector en la base aislada al crear un volumen nuevo. |
| `grafana/dashboards/*_v3.json` | Paneles actuales de contratos/procesos. |
| `grafana/dashboards/contratos.json`, `procesos.json` | Paneles históricos v2. |
| `grafana/provisioning/datasources/postgres.yml` | Datasource SQL y contraseña inyectada por entorno. |
| `grafana/provisioning/dashboards/tableros.yml` | Carga automática de diseños JSON. |
| `grafana/provisioning/alerting/riesgo_v3.json` | Regla de priorización actual. |
| `grafana/provisioning/alerting/riesgo.json` | Regla histórica pausada y plantilla leída por el generador. |
| `test_publicacion.py` | Suite anterior con base temporal; no se ejecuta como parte normal del notebook. |
| `VALIDACION.md`, `validacion_consultas.json` | Evidencias de aquella suite, no certificación de v3. |
| `requirements.txt` | Subconjunto histórico para el laboratorio local. El entorno del proyecto se instala desde el requirements raíz. |
| `.env`, `.env.example` | Valores locales y plantilla sin secretos reales. |
| `README.md` | Guía específica; contiene información histórica claramente separada por el aviso inicial. |

## Configuración y archivos raíz

| Archivo | Qué controla |
|---|---|
| `environment.yml` | Nombre `secop`, Python 3.12.6, canal Conda y lectura de requirements. |
| `requirements.txt` | Dependencias Python y rangos de versiones. No es un lock de todas las dependencias transitivas. |
| `indicador_v3_config.json` | Parámetros vigentes del indicador, memoria y cupo de revisión. |
| `ircd_config.json` | Productor Gold y reglas IRCD originales; no configura v3. |
| `alertas_config.json`, `alertas_dask_config.json` | Configuración del motor D1–D3 conservado. |
| `.env` | Fechas/API y PostgreSQL local; revisar variables históricas antes de asumir que controlan v3. |
| `.env.aws` | Credenciales/host RDS; prevalece sobre `.env` para conexión AWS. |
| `.env.example`, `.env.aws.example` | Plantillas para otro computador; no garantizan conexión hasta configurarlas. |
| `docker-compose.yml` | PostgreSQL local del ETL, puerto 5432 por defecto, volumen persistente. |
| `.gitignore` | Exclusiones de archivos locales, secretos y datos. No elimina un secreto ya rastreado en Git. |
| `sql/00_info.sql` | Consulta de referencia de base/usuario y tablas `secop`/`gold`; no incluye automáticamente `secop_v3` ni `analytics_v3`. |

## Librerías y para qué se usan

| Dependencia | Función en este proyecto |
|---|---|
| pandas / NumPy | Transformaciones por lote, tipos y análisis numérico. |
| requests | Consultas HTTP a Socrata. |
| pyarrow | Esquemas, lectura/escritura Parquet y lotes de publicación. |
| Dask / distributed | Dos etapas del productor Gold; workers, planificación y límites. |
| DuckDB | SQL analítico local, ventanas, agregaciones, medianas y spill. |
| Prefect | Orquestación y registro de estados/tareas. No reemplaza al motor analítico. |
| SQLAlchemy | Conexiones, transacciones y ORM. |
| psycopg (v3) | PostgreSQL local y COPY del ETL/Grafana. |
| psycopg2-binary | Driver solicitado para publicación AWS. No es el mismo paquete que psycopg. |
| python-dotenv | Lee archivos de variables locales. |
| psutil | RAM/procesos y decisiones de recursos. |
| JupyterLab / ipykernel | Interfaz y kernel de notebooks. |
| matplotlib | Gráficos del EDA. |
| tqdm | Indicadores de progreso donde el código lo utiliza. |
| bokeh | Dashboard de Dask del motor anterior; no crea un dashboard para DuckDB v3. |

Docker, PostgreSQL y Grafana son servicios/herramientas externos; no se instalan mediante pip. Los módulos estándar de Python (pathlib, json, hashlib, datetime, etc.) vienen con Python.

## Pruebas y documentos

`test_control.py` y `test_control_pipeline.py` cubren normalización/productor Gold. `test_alertas.py`, `test_alertas_dask.py`, `test_alertas_stats.py` y `test_eda_alertas.py` corresponden principalmente al motor anterior. No ejecutarlas en la operación rutinaria ni presentar su resultado histórico como validación v3.

La especificación actual es `INDICADOR_V3.md`. `EJECUTAR_GRAFANA_Y_AWS.md` conserva instrucciones anteriores con aviso de versión; para incorporarse usar la guía de operación nueva. `PRESENTACION_ETL_SECOP_II.md` es una fotografía histórica útil del ETL. El inventario de `versiones anteriores/` explica cada traslado y permite recuperar su ruta original.

## Lectura de negocio y entrega externa

[Perspectiva de negocio](PERSPECTIVA_NEGOCIO.md) explica decisiones y límites; [Entrega GitHub](ENTREGA_GITHUB.md) distingue archivos incluidos y recursos externos. Los README de data y salida explican qué debe recibirse y qué se genera. El archivo histórico y sus respaldos solo están en la copia local.
