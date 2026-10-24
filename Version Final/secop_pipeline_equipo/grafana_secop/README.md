> **Versión activa del indicador: alertas-cd-3.0.** Ejecuta notebooks 09 → 10 → 12 → 13; 11 monitorea. La guía vigente es `docs/INDICADOR_V3.md`: siete señales C1/C2/C3/V1/V2/P1/P2, puntaje por tres familias, top K trimestral, AWS compacto de 11 columnas en `analytics_v3`. Los apartados anteriores sobre D1/D2/D3 y siete columnas describen v2, conservada para comparación. No se debe presentar una prueba histórica v2 como evidencia de carga v3.

# Publicación local SECOP II — versión 3.1

## Ejecutar

1. Abre Docker Desktop. El notebook puede iniciar PostgreSQL y Grafana con Compose; no necesitas repetir el comando en terminal.
2. Revisa `publicacion.json`: `database_mode: existing` reutiliza el PostgreSQL de `.env` raíz. `isolated` conserva la opción del servicio independiente anterior. No hay fallback automático ni conexión a AWS.
3. Conserva las claves de `grafana_secop/.env`; si no existe, crea el archivo desde `.env.example`. No se sobrescriben credenciales ni se reinstalan paquetes. Dependencias únicamente mediante `environment.yml` del proyecto.
4. Reinicia el kernel y abre `notebooks/12_dashboard_grafana.ipynb`. El notebook inicia Grafana con Compose, verifica la base y prepara el lector.
5. Para la primera publicación o un nuevo corte, activa `PUBLICAR = True`. Para abrir Grafana o cambiar diseño, deja `PUBLICAR = False`.
6. Accede a http://localhost:3001. Revisa Trazabilidad antes de interpretar resultados.

No ejecutes el notebook 04 ni el ETL completo para actualizar el dashboard.

## Fuentes y responsabilidades

SECOP II → Bronze (CSV originales) → Silver (tipado/limpieza) → Gold contractual (IRCD del modelo anterior) → notebook 09 (D1/D2/D3 y clasificación) → publicación local → Grafana.

- `03_dask_gold`/`dask_jobs.build_gold` produce `data/gold/contratos_control/contratos.parquet`.
- `04_postgres_orm`/`load_all_to_postgres` publica Silver y `gold.contratos_control`, reemplazando tablas transaccionalmente.
- `05_prefect_pipeline` orquesta el ETL; no se llama desde el publicador.
- `09_dimensiones_indicador`/`flows.alertas_flow` produce `resultados.parquet` y `ejecucion.json`. No se cambia su metodología.
- `10_agregados_indicadores` genera agregados temporales para explorar en el notebook, no un producto persistente reutilizable por Grafana.
- `11_monitor_alertas` observa la ejecución, sin producir datos nuevos.
- `12_dashboard_grafana` coordina publicación/configuración. No contiene Base64, scripts duplicados ni instalaciones de dependencias.

El producto Gold final de visualización es **lógico**: un manifiesto inmutable en `data/gold/dashboard_publicaciones/<huella>.json` referencia los bytes de Gold contractual y el resultado final. No se duplica el Parquet ni se sobrescribe el Gold anterior. Los archivos referenciados deben conservarse; el manifiesto no es una copia de seguridad.

### Selección de las versiones Gold existentes

La configuración predeterminada publica **ambos Gold existentes**, sin leer Silver ni ejecutar productores:

| Parámetro | Valor |
|---|---|
| `contract_source` | `data/gold/contratos_control/contratos.parquet` |
| `process_source` | `data/gold/procesos_modelo` |
| `process_mode` | `gold_snapshot` |
| `process_cutoff` | `null`: corte de extracción no declarado |

En la inspección de los metadatos de estos archivos había **4.353.720 contratos** y **626.821 procesos en 8 Parquet**. El notebook muestra los conteos y tamaños reales de la selección cada vez que lo ejecutas. Para elegir otra versión cambia las rutas en `publicacion.json`, siempre dentro de `data/gold`; selecciona una carpeta concreta sin mezclar generaciones.

La publicación conserva todas las filas de procesos y todas sus columnas en `secop_panel.dash_procesos_modelo`. Las fechas mínima/máxima observadas se muestran separadas del corte contractual. No se inventa una fecha de extracción para archivos que no la declaran. Los indicadores se leen de un lote COMPLETADO cuya huella corresponda exactamente al Gold contractual seleccionado. Selecciona `RESULTADOS` en el notebook si el último lote no corresponde a ese Gold.

`seleccion_gold.py` resuelve las rutas permitidas, enumera los archivos de la versión y lee filas/tamaños/fechas de sus metadatos. No transforma ni genera datos. Los modos antiguos `legacy` y `disabled` siguen disponibles por compatibilidad, pero no son la configuración utilizada por este notebook.

## Qué se eliminó

- Unión de 22 columnas en cada publicación: reemplazada por ID único tras validar SHA-256 del Gold, manifiesto COMPLETADA, versión/corte, correspondencia y conteos.
- Preparación completa en tablas DuckDB y CSV en disco: reemplazada por vistas de validación y COPY por lotes Arrow de 4096 filas. No se crea un CSV completo.
- Recarga indiscriminada de todos los productos: registro por producto y huella de contenido.
- Doce tablas físicas de detalle/resúmenes: dos agregados materiales, vistas y fuentes necesarias. Las tablas anteriores no se borran.
- Recuperación de millones de filas en Grafana: detalle con `LIMIT 200 OFFSET ...`, página configurable y búsqueda por ID exacto.
- Instalación automática y extracción Base64 del notebook: eliminadas.

## Validación e idempotencia

Se rechazan locks existentes, ejecución incompleta, versión no autorizada, Gold distinto del utilizado por el modelo, IDs nulos/vacíos/repetidos, resultados ajenos, fechas/valores inconsistentes y conteos diferentes del informe. Se verifica la regla de clasificación, pero no se recalcula ni se corrige ningún resultado.

Los SHA-256 se calculan leyendo secuencialmente los archivos. No se confía solamente en tamaño/mtime. Una publicación idéntica omite validación analítica adicional, COPY y actualización de agregados. Abrir Grafana con `PUBLICAR=False` ni siquiera recorre estos archivos.

Si una tabla existente no tiene evidencia de publicación, se compara exactamente su contenido necesario contra Parquet en bloques ordenados por ID antes de adoptarla. Esta primera comprobación puede ser costosa; evita copiar datos que ya están correctos. Si no coincide, se crea una fuente propia sin destruir la tabla existente. Después se registran OID, huella y un trigger de cambios por sentencia.

| Cambio | Acción |
|---|---|
| Ninguno | Cero filas transferidas; no refrescar agregados |
| Alertas | Publicar/adoptar alertas y refrescar agregado contractual |
| Gold contractual | Validar lote correspondiente, publicar/adoptar Gold y actualizar dependencias |
| Gold de procesos | Solo procesos y su agregado |
| Diseño | Regenerar JSON explícitamente; ninguna publicación |

## Compatibilidad con PostgreSQL del ETL

Las proyecciones contractuales usan funciones SQL STABLE con cuerpo en texto. Evitan fijar dependencia al OID de una tabla que `load.py` reemplaza, sin modificar el ETL. El plan de pruebas confirmó inlining (sin Function Scan), permitiendo aplicar filtros e índices. Se conserva el protocolo de advisory locks del cargador.

Si el ETL cambia la tabla, su OID o un trigger de escritura invalidan la evidencia. `fuentes_vigentes` pasa a falso; el detalle contractual no mezcla las nuevas filas con alertas antiguas. Los resúmenes conservan el último corte publicado y la regla de riesgo devuelve NoData hasta republicar. Si se cambia el esquema y la función deja de resolverlo, puede aparecer Error: nunca se interpreta como ausencia de riesgo.

La actualización de vistas, productos, dos agregados y registro se hace en una transacción. Ante excepción se conserva el último corte. No se usa CASCADE ni se borran volúmenes. Se conservan fuentes físicas anteriores; su retención consume espacio y la limpieza debe ser una operación administrativa separada.

El usuario `grafana_lectura` recibe lectura de vistas, registro de productos y columnas necesarias de las fuentes utilizadas por funciones SQL; nunca escritura. Se necesita un usuario publicador con permisos para crear objetos, triggers y conceder lectura. No se modifica la contraseña de un rol ya existente.

## Consultas y límites

- Todos los contratos Gold se conservan. Alertas ausentes quedan `SIN_RESULTADO_ALERTAS`, separado de `NO_EVALUABLE` también en las dimensiones.
- Gráficos globales: Año controla el denominador. Clasificación y Buscar ID afectan únicamente al detalle.
- El panel Navegación devuelve páginas y total. Cambia Página y vuelve a 1 después de cambiar filtros. Es paginación SQL, no paginación visual de filas previamente descargadas.
- Grafana usa interpolación escapada `sqlstring`, no parámetros bind de psycopg. Página se valida con expresión regular antes del cast. El lector tiene permisos limitados y timeout.
- OFFSET profundo sigue teniendo costo. La búsqueda exacta por ID usa la clave/indexación. No se promete navegación instantánea en millones de filas.
- Dos agregados materiales se justifican por las consultas repetitivas de conteos, grupos y series; los índices de ID aceleran detalle, pero no evitan recorrer millones de filas para cada agrupación global. No se añadieron índices especulativos de todas las combinaciones de filtros.
- `dash_alertas_modelo` conserva todas las métricas originales de alertas. No se convierten NULL en cero, no se elimina información de motivos/umbrales y no se cambian D1/D2/D3.

## Operación y mediciones

`publicacion.json` controla conexión (5 s), bloqueo (10 s), consultas (300 s), Docker (180 s), publicación (7200 s) y memoria DuckDB (1 GB). La salida identifica etapa, inicio/fin, segundos, filas y RSS. `salida/grafana` conserva registros JSON. No hay porcentajes ficticios.

El timeout del subproceso termina el publicador; PostgreSQL revierte la transacción. Una terminación forzada puede dejar un lock local: se debe comprobar que el proceso terminó antes de retirarlo manualmente. Nunca se borran locks ajenos automáticamente.

La primera carga y el refresco de vistas pueden bloquear consultas brevemente. Cambiar exclusivamente el diseño no afecta las tablas. No hay contactos externos de notificación configurados.

## Archivos principales

- `publicacion.py`: huellas, validación, adopción/COPY, transacción, registro y logs.
- `consultas.py`: proyecciones y dos agregados materiales.
- `runtime.py`: conexiones exclusivamente locales, permisos y ejecución con timeout.
- `cargar_datos.py`: CLI (`--inspect` solo consulta catálogo).
- `generar_dashboards.py`: fuente única del diseño; los JSON son artefactos generados.
- `publicacion.json`: fuente única de políticas de publicación.
- `docker-compose.yml`: Grafana y PostgreSQL opcional por perfil `isolated`.
- `test_publicacion.py`: pruebas sintéticas, exclusivamente puerto 55439.
- `VALIDACION.md`: resultados y pendientes reales.
