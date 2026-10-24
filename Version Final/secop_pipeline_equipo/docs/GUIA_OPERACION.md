# Guía de operación de principio a fin

[Inicio](../README.md) · [Mapa](MAPA_PROYECTO.md) · [Accesos](ACCESOS_Y_CONFIGURACION.md) · [Indicador](INDICADOR_V3.md)

## 1. Preparar el computador

Se necesita Conda, el proyecto completo y sus datos Gold. Para Grafana se necesita Docker Desktop; para AWS, acceso de red al RDS. Un clon de GitHub no necesariamente incluye los datos ni los secretos locales: deben recibirse por separado.

Desde una terminal en la raíz del proyecto:

```powershell
conda env create -f environment.yml
conda activate secop
python -m jupyter lab
```

Si el entorno ya existe, activarlo es suficiente. Para actualizar sus dependencias explícitamente se usa `conda env update -f environment.yml`. La instalación ocurre al preparar el entorno; no insertar `pip install` en scripts ni notebooks. Elegir el kernel del entorno `secop`. Si no aparece, registrarlo durante la preparación con `python -m ipykernel install --user --name secop --display-name "Python (secop)"`.

Abrir los notebooks dentro de la carpeta del proyecto: buscan `secop_pipeline` en la carpeta actual o sus padres. Reiniciar el kernel después de modificar módulos para no conservar código importado anteriormente.

## 2. Elegir el recorrido

| Situación | Acción |
|---|---|
| Gold ya existe y solo necesitas indicador/tableros | 09 → 10 → 12 y/o 13; 11 monitorea |
| Necesitas actualizar desde la API | Configurar fechas y ejecutar 05 en modo de actualización explícita; luego 09 |
| Necesitas reconstruir desde Bronze local | 05 en modo reprocesamiento local; luego 09 |
| Solo falta la carga original del ETL a PostgreSQL | 04 o modo solo_postgres de 05; no son la carga compacta v3 |
| Solo falta Grafana o AWS y 09 está completo | 12 o 13; no volver a construir Gold |

No ejecutar todos los notebooks 01–05 por rutina: 05 orquesta tareas que también pueden ejecutarse individualmente en 01–04.

## 3. Extracción: API → Bronze

`extract.py` consulta los recursos de Socrata definidos en `config.py`: procesos, contratos, proveedores y grupos. Las fechas y el tamaño de lotes se configuran en `.env`. Los CSV se guardan bajo `data/bronze/<base>/`; los manifiestos registran procedencia, filas, archivos y huellas SHA-256.

SHA-256 identifica el contenido para detectar modificaciones y reutilizar resultados. No cifra los datos ni valida que los valores sean correctos. Reutilizar Bronze significa conservar el snapshot disponible; no equivale a incorporar contratos nuevos de la API. La actualización debe solicitarse explícitamente.

## 4. Transformación: Bronze → Silver

`transform.py` procesa archivos por base, limpia espacios y representaciones vacías, tipa identificadores/textos, números y fechas, y guarda Parquet. Los números priorizan el formato numérico de la API y después aplican limpieza de separadores. Los valores que no se convierten quedan nulos. Las fechas se interpretan en UTC y se almacenan sin zona horaria.

En contratos se preservan columnas `raw__` para contrastar con los valores originales; se incorporan campos opcionales ausentes como nulos. Esto no recupera información que no vino en Bronze. `quality.py` informa sobre estructura y calidad; `audit.py` integra evidencias e informes de la ejecución. La conversión de tipo no es, por sí sola, validación de plausibilidad monetaria.

## 5. Construcción de Gold contractual

`dask_jobs.py` ejecuta dos etapas secuenciales: A prepara/normaliza particiones; B ejecuta `control_analytics.py` dentro de Dask para consolidar contratos y producir Gold. No son dos ejecuciones equivalentes para benchmarking.

La selección de versiones ocurre antes de filtrar modalidad. Se identifican filas sin ID, versiones ambiguas, versiones anteriores y duplicados exactos; después se aplican contratación directa, corte y filtro de persona configurado. Las exclusiones se documentan en la auditoría. No debe confundirse una reducción por selección del universo con pérdida accidental de filas.

El productor todavía calcula el IRCD original: concentración, recurrencia y valor atípico. Estos campos son históricos respecto de v3. **V3 lee las variables base del Gold y calcula su propio indicador; no suma ni reutiliza los scores antiguos.**

Entradas y salidas principales:

- Entrada: Silver de contratos y configuración `ircd_config.json`.
- Salida contractual: `data/gold/contratos_control/contratos.parquet`.
- Auditoría: `data/gold/contratos_control/auditoria_ircd.json`.
- Gold de procesos existente: `data/gold/procesos_modelo/`; el productor contractual actual no garantiza su actualización simultánea.

La llave del indicador es `id_contrato`; `entidad_id` y `proveedor_id` definen agrupaciones, no llaves únicas por fila. No unir procesos y contratos por nombres: una relación requiere comprobar las claves y su cardinalidad. Los tableros actuales mantienen esos universos separados.

## 6. Indicador v3 desde Gold: notebook 09

El flujo `indicador_v3_flow` tiene cinco etapas con transacciones y checkpoints:

1. **preparar_gold:** selecciona campos, valida IDs/corte, normaliza categoría y marca importes no utilizables. Conserva todas las filas del Gold aceptado.
2. **relaciones:** calcula ventanas, participaciones, HHI, repetición, primera aparición y diversidad de categorías para C1/C2/C3/V2/P1/P2.
3. **valores:** calcula referencias robustas de V1 con logaritmos, mediana y MAD de años previos completos. Los importes inválidos no participan en referencias ni reciben señal V1.
4. **red_complementaria:** agrega vínculos entidad–proveedor, grados y actividad; no entra al puntaje.
5. **puntuar:** integra señales por ID, calcula familias, cobertura, puntaje, orden trimestral y clasificación.

La especificación completa está en [INDICADOR_V3.md](INDICADOR_V3.md). Configuración: `indicador_v3_config.json`. `cutoff` y `coverage_start` nulos se resuelven desde la auditoría de Gold; si no están declarados, se requiere configurarlos. No se infieren de la fecha mínima observada.

El resultado va a `salida/alertas/indicador_v3/<huella>/`. Incluye `resultados.parquet`, `red_aristas.parquet`, `red_proveedores.parquet`, `ejecucion.json`, base DuckDB y temporales. La huella depende de datos, código y parámetros: si cambian, se crea otro lote. Los checkpoints completos se reutilizan si corresponden al mismo lote. No cambiar el nombre o mover solo un Parquet fuera de su manifiesto.

## 7. Recursos, monitor y recuperación

- ETL Dask: por defecto A tiene 2 workers y B tiene 1, con 1 hilo por worker. Cuando la memoria es automática, reparte un presupuesto por etapa de hasta 6 GiB y hasta la mitad de la RAM disponible; las etapas no corren simultáneamente. Overrides: `DASK_PREP_WORKERS`, `DASK_PREP_MEMORY`, `DASK_JOIN_WORKERS`, `DASK_JOIN_MEMORY`.
- Indicador v3: DuckDB, por defecto `memory_mb=2048` y `threads=1`, con spill a disco. No arranca un clúster Dask ni tiene dashboard de workers. El límite del motor no equivale a un límite absoluto de memoria de todo Python.
- Notebook 11: lee `ejecucion_actual_v3.json` y el manifiesto del lote. Su porcentaje es etapas finalizadas, no estimación de tiempo restante.
- Publicadores: usan `ultima_ejecucion_v3.json`, que apunta al último lote completo. Mientras hay un lote nuevo fallido/en curso, el anterior puede seguir siendo el último completo.
- Un fallo no exige borrar resultados. Revisar `error`, la etapa fallida, RAM y espacio libre; cerrar la ejecución y reejecutar 09 con los mismos parámetros para aprovechar checkpoints.
- Un lock exige comprobar primero si hay proceso activo. No eliminarlo automáticamente. No editar una base `.duckdb` o `.wal` de una ejecución activa.

## 8. Analizar, visualizar y publicar

**10:** lee resultados y presenta clases, cobertura por señal, distribución del puntaje, selección trimestral, motivos monetarios, relaciones binarias y red. No publica en AWS ni exporta gráficos.

**12:** abre servicios locales mediante Docker Compose, prepara el usuario lector y publica Gold/resultados en PostgreSQL. La publicación usa huellas, intenta reutilizar tablas coincidentes y crea vistas/agregados en `secop_v3`. Los detalles se consultan por páginas SQL; se conserva todo el universo. Si se modifican las fuentes registradas, las vistas impiden presentar agregados como vigentes. El notebook recarga el diseño v3. Grafana consulta PostgreSQL local, no el Parquet directamente ni el RDS de AWS.

**13:** usa `psycopg2` con SSL para publicar en `analytics_v3`: `indicadores` tiene ID, siete señales, puntaje, clasificación y alerta; `clasificaciones` y `dimensiones` contienen agregados; `publicacion` contiene el manifiesto. COPY se realiza por lotes dentro de una transacción. Una carga repetida coincidente puede omitir COPY; una publicación diferente reemplaza la fotografía vigente v3. La última celda abre otra conexión y comprueba filas, columnas y agregados. Conexión exitosa por sí sola no demuestra carga completa.

Las tablas v2 se conservan por separado. La alerta de Grafana señala existencia de contratos priorizados, no detección validada de corrupción. NoData/Error no son equivalentes a cero alertas.

## 9. Problemas frecuentes

| Síntoma | Qué revisar |
|---|---|
| No encuentra Gold o manifiesto | Ruta del proyecto y archivos entregados; ejecutar 09 antes de 10/12/13 |
| Notebook usa reglas antiguas | Kernel reiniciado y notebook 09 v3, no ejecutores archivados |
| Error de memoria | `memory_mb`, `threads`, RAM libre y espacio para spill; no multiplicar workers por rutina |
| Timeout AWS | Host/puerto, conectividad RDS, VPC/acceso público o ruta privada y regla de entrada para el cliente; no asumir contraseña incorrecta |
| Login Grafana falla | `GRAFANA_ADMIN_PASSWORD`; un volumen existente puede conservar la contraseña anterior |
| Grafana abre pero SQL falla | Rol `grafana_lectura`, su contraseña, base local y publicación 12 |
| Cambiar `.env` no cambia una contraseña PostgreSQL | Variables de inicialización no rotan automáticamente usuarios de un volumen ya creado |
| Aviso WebSocket de Prefect | Revisar el estado del flujo y sus manifiestos; el mensaje solo no determina éxito o fallo del cálculo |

## 10. Alternativas por terminal

Desde la raíz, con `secop` activado:

```powershell
# Solo cálculo v3 desde Gold existente
python scripts/run_indicador_v3.py
# Consultar opciones de publicación, sin ejecutar una carga
python scripts/publicar_alertas_aws.py --help
# ETL desde Bronze local, sin carga original a PostgreSQL (reconstruye Gold)
python scripts/run_pipeline.py --reuse-bronze --no-postgres
```

Para el trabajo habitual usar notebooks. El último comando es una operación distinta, no un paso obligatorio previo al cálculo v3.
