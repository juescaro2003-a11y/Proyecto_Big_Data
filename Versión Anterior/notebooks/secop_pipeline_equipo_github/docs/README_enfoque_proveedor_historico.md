> **Configuración Dask vigente:** preparación con 2 workers y cruce con 1 worker.
> Memoria automática: presupuesto por etapa hasta 4 GiB y hasta el 50% de RAM disponible.
> Normalmente 2 GiB/worker en A y 4 GiB/worker en B. Los límites efectivos se registran.
> El fallo anterior era el rechazo preventivo de 8 GiB, antes de arrancar workers.

# SECOP II — guía de funcionamiento del ETL

Este proyecto prepara datos de contratación pública para consultarlos en PostgreSQL y analizarlos en notebooks. **ETL** significa extraer, transformar y cargar. Prefect coordina las etapas; Dask realiza la construcción de Gold en dos etapas; pandas y PyArrow preparan Silver; psycopg carga PostgreSQL.

Esta guía describe el **código actual**, incluidas las correcciones de actualización y el informe de auditoría. Las salidas antiguas de los notebooks pueden corresponder a versiones anteriores. Documentar una función no demuestra que su última versión ya haya completado una ejecución.

## Contenido

1. Objetivo, capas y recorrido de los datos.
2. Bases de origen y selección de columnas.
3. Carpetas y archivos del proyecto.
4. Extracción y modos de actualización.
5. Transformaciones de Silver.
6. Calidad de datos.
7. Gold: filtros, deduplicación, cruces y variables.
8. Carga de PostgreSQL y acceso ORM.
9. Prefect y comportamiento ante fallos.
10. Informe de transparencia.
11. Función de cada script y notebook.
12. Uso de las librerías.
13. Configuración y formas de ejecución.
14. Escalabilidad, límites y solución de problemas.
15. Anexo de columnas y reglas de tipado.

## 1. Objetivo y recorrido de los datos

La iniciativa de negocio busca ayudar a empresas a encontrar oportunidades que encajen con su perfil. El ETL proporciona los datos para explorar ese problema; **Gold no es por sí solo un recomendador ni una estimación de probabilidad de ganar**.

```text
API de Datos.gov.co                         Datos locales existentes
      │ actualización explícita                     │ modo predeterminado
      └───────────────────┬─────────────────────────┘
                          ▼
                 Bronze: archivos CSV
                          │ limpieza y conversión de tipos
                          ▼
                 Silver: archivos Parquet
                          │ revisión de columnas
              ┌───────────┴───────────────────┐
              │                               │
    Procesos + Contratos                      │ cuatro bases Silver
              │ filtros + deduplicación       │
              │ + LEFT JOIN con Dask          │
              ▼                               │
      Gold: procesos_modelo                   │
              └───────────────┬───────────────┘
                              ▼
                    PostgreSQL mediante COPY
                              │
             consultas SQL / ORM / notebook EDA

Prefect: coordina extracción → Silver → calidad → Gold en dos etapas → carga.
Auditoría: registra las etapas en el notebook 07 y conserva históricos.
EDA: notebook 06 independiente; Prefect no lo ejecuta.
```

| Capa | Qué contiene | Por qué existe |
| --- | --- | --- |
| Bronze | CSV de las columnas solicitadas a la API | Conservar la entrada para repetir la transformación sin descargar otra vez |
| Silver | Las cuatro bases limpias y tipadas en Parquet | Reducir almacenamiento, estabilizar tipos y facilitar lecturas por columnas |
| Gold | Procesos filtrados, deduplicados y enriquecidos con evidencia de contrato | Disponer de una tabla a nivel de proceso para análisis históricos |
| PostgreSQL | Cuatro tablas `secop.*` y una tabla `gold.procesos_modelo` | Consultar, relacionar y consumir datos desde SQL o Python |

Bronze conserva una entrada **seleccionada**: no es una copia de todas las columnas disponibles en la fuente. Gold y PostgreSQL son productos reconstruibles a partir de las capas anteriores.

## 2. Bases de origen

Las fuentes están declaradas en `secop_pipeline/config.py`. El cliente consulta rutas del portal configurado mediante el identificador de cada conjunto.

| Base | ID de fuente | Unidad de registro esperada | Fecha usada en extracción | Partición |
| --- | --- | --- | --- | --- |
| `procesos` | `p6dx-8zbt` | Registro de un proceso; un ID puede repetirse en la fuente | `fecha_de_publicacion_del` | Mes |
| `contratos` | `jbjy-vk9h` | Registro de contrato; la extracción no elimina IDs repetidos | `fecha_de_firma` | Mes |
| `proveedores` | `qmzu-gj57` | Registro de proveedor | No se aplica filtro temporal al catálogo | Páginas numeradas |
| `grupos` | `ceth-n4bn` | Relación grupo–participante; varias filas pueden pertenecer al mismo grupo | No se aplica filtro temporal al catálogo | Páginas numeradas |

`COLUMNAS` establece qué campos se solicitan. `TIPOS` los clasifica en llaves, etiquetas, números y fechas. La clasificación es explícita porque inferir tipos desde una muestra puede convertir identificadores en números o producir esquemas distintos entre particiones.

El intervalo `START_DATE`–`CUTOFF_DATE` limita procesos y contratos según sus respectivas fechas. No representa una fotografía histórica de los atributos: una consulta actual puede devolver el estado actualizado de un registro antiguo. Los registros sin una fecha que satisfaga el filtro quedan fuera de esas consultas mensuales.

## 3. Estructura del proyecto

```text
secop_pipeline_equipo/
├── secop_pipeline/       Funciones reutilizables del ETL
├── flows/               Orquestación con Prefect
├── scripts/             Entradas por terminal y utilidades
├── notebooks/           Trabajo por etapas, EDA e informe
│   └── informes_etl/    Un notebook por ejecución auditada
├── data/
│   ├── bronze/          CSV por base
│   ├── silver/          Parquet por base + metadatos de transformación
│   └── gold/            Parquet de procesos_modelo
├── salida/              Manifiesto, reporte de calidad y benchmark
├── sql/                 Consultas de referencia
├── .dask-spill/          Espacio de trabajo temporal de Dask
├── .env                 Configuración local y credenciales
├── .env.example         Plantilla de configuración, sin datos privados
├── .pipeline.lock       Bloqueo temporal mientras corre el flujo
├── docker-compose.yml   Servicio PostgreSQL y volumen persistente
├── environment.yml      Entorno Conda
├── requirements.txt     Dependencias Python
└── README.md            Esta guía
```

`salida/manifest_extraccion.json` registra fuentes, intervalo, filas, tamaño y SHA-256 de cada archivo Bronze. `reporte_calidad.csv` resume columnas y volumen de Silver. `banco_dask.csv` conserva la comparación histórica de configuraciones (mecanismo retirado). Estos archivos operativos son diferentes de los resultados del EDA, que permanecen en su notebook.

Los archivos `.partial` sirven para preparar escrituras antes de reemplazar el archivo definitivo. Los directorios `.previous-*` pueden aparecer al publicar una capa. Una interrupción abrupta puede requerir revisar esos elementos; no debe asumirse que son descartables mientras haya una ejecución activa.

## 4. Extracción: qué entra y cuándo se descarga

### 4.1 Modo predeterminado: reutilizar Bronze

`extract_all()` y `secop_flow()` usan `refresh=False` por defecto:

1. Leen el manifiesto local.
2. Conservan su intervalo cuando no se suministran fechas explícitas.
3. Verifican las bases, identificadores de fuente, conjunto de archivos y hashes.
4. Devuelven el manifiesto para continuar el procesamiento.

No descargan novedades. Si el manifiesto falta o los archivos no coinciden, se detienen con un error. No cambian automáticamente al modo de descarga. Las fechas explícitas deben coincidir con el intervalo del manifiesto.

**Reutilizar Bronze no convierte al flujo completo en solo lectura:** posteriormente puede regenerar Silver, Gold y las tablas de PostgreSQL.

### 4.2 Actualización explícita

`refresh=True` o `--refresh-bronze` solicita una descarga completa:

1. `validate_api()` consulta una pequeña muestra y comprueba que existan las columnas solicitadas.
2. `SocrataClient.request_csv()` envía `$select`, `$limit`, `$offset` y `$order=:id`; añade `$where` para las fechas cuando corresponde.
3. La paginación usa `SOCRATA_BATCH_SIZE`, por defecto 50.000 filas por solicitud.
4. Procesos y contratos se descargan mes a mes, respetando el inicio y el corte inclusive. El límite superior de la consulta es exclusivo: día siguiente al corte.
5. Proveedores y grupos se descargan como catálogos completos paginados.
6. La nueva extracción se prepara en otro directorio y se publica al terminar las cuatro bases.
7. Se escribe el manifiesto, incluida una copia junto a Bronze.

Los valores se leen inicialmente como texto para aplazar la interpretación de tipos a Silver. El cliente HTTP tiene reintentos; además, Prefect puede reintentar la tarea de extracción completa.

**Por qué refrescar también meses antiguos:** un proceso puede cambiar de estado después de publicarse y un contrato puede recibir correcciones. Descargar solo meses nuevos no recoge esos cambios. La solución actual es una recarga completa; no implementa una marca incremental de última modificación ni UPSERT.

La API puede cambiar entre páginas. Ordenar por `:id` ayuda a estabilizar la paginación, pero no garantiza una transacción ni una instantánea única de la fuente remota.

### 4.3 Funciones individuales

`download_month()`, `download_catalog()` y `extract_base()` permiten trabajar por base o partición. Conservan comportamiento de reutilización de archivos existentes. La actualización completa segura se coordina desde `extract_all(refresh=True)`, que trabaja en un directorio nuevo. Invocar una función individual no equivale a ejecutar el flujo auditado completo.

## 5. Silver: transformaciones y motivos

`transform.py` aplica reglas por columna sin filtrar observaciones ni deduplicar registros.

### 5.1 Textos, llaves y valores ausentes

`clean_text()` convierte a texto pandas, recorta espacios al principio y al final y compara una versión en minúsculas con `VACIOS`. Las coincidencias se convierten en `pd.NA`.

Ejemplos: `"  Bogotá  "` queda `"Bogotá"`; `"sin dato"` queda nulo. No se estandarizan sistemáticamente tildes, mayúsculas, nombres territoriales ni sinónimos.

La regla de vacíos es global: también contiene expresiones como `no adjudicado` y `no aplica`. Su conversión a nulo elimina esa representación original de Silver, aunque permanezca en Bronze. Es una decisión de limpieza que puede perder significado en algunas columnas y debe revisarse si cambia el caso de uso. Nulo no demuestra una respuesta negativa.

Los NIT, códigos y llaves se conservan como texto porque identifican entidades; no representan magnitudes para sumar o correlacionar. La lista completa de centinelas y columnas aparece al final de esta guía.

### 5.2 Números

`to_number()` normaliza texto, retira espacios, espacios no separables y `$`, y aplica reglas de separadores:

| Entrada de ejemplo | Interpretación prevista |
| --- | --- |
| `1.234.567,89` | `1234567.89` |
| `15.693.000` | `15693000` |
| `15,693,000` | `15693000` |
| `1234,5` | `1234.5` |
| Texto no interpretable | Nulo por `errors="coerce"` |

Después se convierte a `float64`, incluso en columnas que conceptualmente contienen conteos. Esto da un esquema homogéneo entre archivos y admite valores ausentes.

**Límite de la regla:** no reconoce todos los formatos internacionales. Un valor como `1.234` se interpreta como miles; un formato mixto como `1,234.56` puede interpretarse incorrectamente porque la regla con ambos separadores supone punto de miles y coma decimal. Convertir sin error no certifica que el importe sea correcto.

Silver no elimina automáticamente importes negativos, ceros ni valores extremos. Los conserva para que puedan investigarse. `float64` tampoco ofrece exactitud decimal contable para todos los valores.

### 5.3 Fechas

`to_date()` usa `pd.to_datetime(..., errors="coerce", format="mixed", utc=True)` y después elimina la información de zona con `tz_convert(None)`.

- Valores no interpretables: nulo temporal.
- Valores con zona: normalizados a UTC antes de almacenarse sin zona.
- Valores sin zona: interpretados como UTC; no se infiere automáticamente hora colombiana.

La elección de zona puede afectar análisis de hora o día. Tampoco se valida aquí que la firma sea posterior a publicación o que el cierre sea posterior al inicio.

### 5.4 Escritura por bloques y reutilización

`transform_file()` lee bloques de `TRANSFORM_CHUNK_ROWS`, por defecto 50.000 filas. Construye un esquema Arrow explícito —texto, `float64` o timestamp— y escribe Parquet con compresión Snappy. El archivo definitivo se sustituye cuando termina la escritura temporal.

La decisión de reutilizar un Parquet depende de una huella que combina:

- SHA-256 del CSV.
- SHA-256 del código de transformación.
- Reglas `TIPOS` de esa base.
- Lista de valores vacíos.

La huella y las métricas quedan en `.parquet.meta.json`. Si cambia la entrada o una regla, se vuelve a transformar. Cambiar el código puede regenerar todos los archivos aunque algunos resultados sean iguales. La huella no incluye todas las versiones de dependencias ni verifica el contenido del Parquet existente mediante otro hash.

`transform_base()` retira Parquet cuya partición ya no exista en Bronze, después de procesar los archivos de la base. Eso evita conservar particiones fuera del snapshot activo. El retiro de un archivo obsoleto es distinto de filtrar filas por una regla de negocio.

### 5.5 Qué reducción ocurre aquí

La reducción esperada es **física**, por formato columnar, tipos y compresión. El número de filas debe conservarse entre la entrada Bronze activa y Silver. El ahorro porcentual se calcula como:

```text
100 × (1 − bytes_Silver / bytes_Bronze)
```

Un porcentaje menor o incluso negativo es posible; no se fuerza un ahorro. La eliminación de procesos por modalidad, estado e ID repetido ocurre después, en Gold.

## 6. Control de calidad

`quality.py` lee metadatos de los Parquet, suma filas y tamaños y compara las columnas de **todos** los archivos con `COLUMNAS`.

Si faltan o sobran columnas, escribe el reporte y lanza un error para detener el flujo antes de Gold y de la carga. La unidad histórica `gb_parquet` usa divisiones por 1024, aunque el nombre no diga GiB.

Este control no garantiza:

- Unicidad de IDs o ausencia de duplicados.
- Tipos semánticamente correctos en cada campo.
- Montos razonables, fechas coherentes o categorías válidas.
- Integridad referencial entre bases.
- Correspondencia exclusiva entre contrato y proceso.

`UNIQUE_KEYS` declara algunas llaves en el módulo, pero no ejecuta por sí mismo una validación de unicidad. La auditoría mide efectos adicionales; medirlos no implica que todos bloqueen el flujo.

## 7. Gold: qué se quita, por qué y cómo se conecta

### 7.1 Filtros secuenciales

| Paso | Regla actual | Motivo analítico | Consecuencia |
| --- | --- | --- | --- |
| Modalidad | Excluir las cuatro cadenas de `SIN_COMPETENCIA` | Acotar el análisis histórico hacia procesos de interés competitivo | No representa todo SECOP ni garantiza que todo lo conservado sea abierto a cualquier empresa |
| Estado | Conservar únicamente `Seleccionado` y `Cancelado` | Trabajar con resultados de procesos terminados | Las ofertas abiertas quedan fuera de Gold |
| ID | `drop_duplicates(subset=["id_del_proceso"])` | Obtener una fila por ID de proceso | Se pierde multiplicidad; no se elige explícitamente la actualización más reciente |

Modalidades excluidas exactamente:

- `Contratación directa`.
- `Contratación régimen especial`.
- `Solicitud de información a los Proveedores`.
- `Subasta de prueba`.

La comparación es por valores exactos después de la limpieza textual: no busca prefijos ni normaliza toda la modalidad a minúsculas. Por eso una variante `... con ofertas` no queda excluida simplemente por contener parte del nombre. Un estado nulo no satisface el filtro de terminados; una modalidad nula no coincide con la lista de exclusión y puede superar ese primer filtro.

La deduplicación conserva la primera aparición según el procesamiento, sin resolver conflictos de atributos. Si hay varios IDs nulos pueden colapsarse en una fila. Estas limitaciones se documentan para no confundir deduplicación técnica con reconciliación de versiones.

**Para buscar oportunidades vigentes hace falta otro conjunto de candidatos:** la tabla Gold actual solo conserva los estados terminados indicados.

### 7.2 Preparación de contratos y cruce

De Silver contratos se toma únicamente `proceso_de_compra`, se eliminan claves nulas y se conservan claves distintas. A cada una se le asigna `tiene_contrato=1`.

```text
procesos.id_del_portafolio
              │ LEFT JOIN
              ▼
contratos.proceso_de_compra — reducido a claves únicas y no nulas
```

**Por qué usar LEFT JOIN:** conservar todos los procesos seleccionados aunque no se encuentre un contrato. Después del cruce, la ausencia de marca se rellena con cero.

**Por qué deduplicar primero contratos:** varios contratos pueden pertenecer a un portafolio. Unir todas sus filas multiplicaría procesos. Aquí solo se busca evidencia de existencia, no detalle contractual ni suma de sus importes.

La relación técnica es muchos-a-uno respecto a la clave derecha. Si varios procesos comparten portafolio, todos pueden recibir la misma evidencia de contrato. No se comprueba que cada contrato pertenezca exclusivamente a una fase del proceso. Un cero indica falta de coincidencia en los datos cargados, no prueba definitiva de que nunca hubo contrato.

Proveedores y grupos **no participan en este JOIN**. Se conservan como tablas separadas. Las relaciones contratos–proveedores y grupos–participantes pueden explorarse posteriormente, pero el ETL no las ejecuta ni impone llaves foráneas.

### 7.3 Variables derivadas

| Variable | Cálculo | Interpretación y límite |
| --- | --- | --- |
| `adjudicado_si` | Texto recortado/en minúsculas dentro de `si`, `sí`, `true`, `1`, `yes`, `seleccionado`, `adjudicado` | Indicador de adjudicación reportada; valores no reconocidos no se consideran positivos |
| `tiene_contrato` | Coincidencia con una clave contractual no nula | Evidencia por portafolio, no atribución exclusiva a un proceso |
| `objetivo` | `adjudicado_si == 1 OR tiene_contrato == 1` | Resultado histórico del proceso; no afinidad empresa–oferta |
| `log_precio_base` | `log(max(precio_base, 0) + 1)` | Reduce asimetría; precios negativos se recortan solo en esta derivada |
| `dias_para_cerrar` | Días enteros entre recepción y publicación; negativos a nulo; máximo 365 | Plazo normalizado; se pierde detalle de valores superiores a un año y fracciones de día |
| `mes_publicacion` | Mes de la fecha de publicación | Estacionalidad; no representa una escala lineal continua entre diciembre y enero |
| `tiene_varios_lotes` | Lotes nulos asumidos como 1; comparación `> 1` | Indicador operativo; no informa que el dato original estuviera ausente |

El precio y las fechas originales permanecen en Gold. La columna puente `proceso_de_compra` también queda en el resultado del JOIN.

Gold conserva campos del resultado que pueden producir **fuga de información** si se usan para predecir desde la publicación: adjudicación, proveedor ganador, valor adjudicado, estados finales y evidencia contractual, entre otros. La disponibilidad temporal debe decidirse para cada modelo; que una variable esté almacenada no la convierte en predictor válido.

### 7.4 Dask y publicación de Gold

`build_gold()` crea un `LocalCluster` con workers, hilos y límite de memoria configurables. Dask prepara un grafo de operaciones; escribir Parquet o llamar a `compute()` materializa el trabajo.

La salida se construye en un directorio temporal. Tras completarla y, cuando corresponde, medir la auditoría, se publica como `data/gold/procesos_modelo`. Ante una excepción de construcción se conserva la versión anterior. Los renombrados de directorios tienen recuperación ante excepciones, pero no forman una transacción única resistente a cualquier corte del equipo.

## 8. PostgreSQL: carga, tablas e índices

### 8.1 Tablas resultantes

| Entrada | Tabla |
| --- | --- |
| Silver procesos | `secop.procesos` |
| Silver contratos | `secop.contratos` |
| Silver proveedores | `secop.proveedores` |
| Silver grupos | `secop.grupos` |
| Gold procesos_modelo | `gold.procesos_modelo` |

`load_all_to_postgres(replace=True)` espera disponibilidad del servidor y toma un bloqueo asesor transaccional para serializar las cargas completas que utilicen ese mismo mecanismo.

### 8.2 Preparación y publicación conjunta

1. Crea tablas temporales de preparación por nombre único dentro de los esquemas destino.
2. Obtiene tipos del esquema Arrow, sin leer una partición completa para inferirlos.
3. Lee lotes de hasta 50.000 filas de cada Parquet.
4. Los convierte en CSV en memoria y los envía con `COPY FROM STDIN`.
5. Solo cuando termina de preparar las cinco tablas, elimina las tablas públicas anteriores y renombra las preparadas.
6. Crea los índices y confirma la transacción.

Si falla la carga o la publicación dentro de esa transacción, se revierte el conjunto. Se necesita espacio para la versión anterior, la nueva y el registro transaccional. El reemplazo y la creación de índices pueden bloquear lectores. Una dependencia externa que impida `DROP TABLE` produce error; el código no usa `CASCADE` para eliminarla.

Las tablas de preparación son tablas normales con nombre temporal, no tablas PostgreSQL declaradas `TEMP`. No quedan publicadas si la transacción hace rollback.

`replace=False` está rechazado: agregar nuevamente todas las filas sin reconciliación por llave generaría duplicados. No hay una carga incremental de negocio.

### 8.3 Tipos y corrección de BIGINT

Las columnas declaradas numéricas en `TIPOS` se crean como `DOUBLE PRECISION`. Esto evita que una primera partición aparentemente entera produzca una columna `BIGINT` que luego rechace un valor textual como `0.0` durante COPY.

Para otras columnas, el mapeo utiliza booleanos, enteros, flotantes, fechas o texto según el tipo pandas del esquema. Es una solución de homogeneidad de carga; no sustituye la elección de `NUMERIC` si se requiere precisión decimal exacta.

### 8.4 Índices y ORM

Se crean índices sobre ID de proceso, portafolio de proceso, ID de contrato, puente contractual, código de proveedor e ID de Gold. Son índices de consulta, **no restricciones UNIQUE ni FOREIGN KEY**.

SQLAlchemy permite consultas SQL y acceso ORM. `models.py` mapea algunas columnas de `ProcesoAnalitico`, `Contrato` y `Proveedor`. El ORM marca atributos como llaves para identificar objetos, pero esa declaración no crea una restricción en las tablas cargadas mediante COPY. Si el dato real contiene IDs repetidos, el comportamiento de identidad del ORM requiere atención.

`orm.sample_processes()` devuelve una muestra con `LIMIT`; no tiene orden explícito ni realiza muestreo aleatorio. El ORM no se utiliza para insertar millones de registros.

## 9. Prefect: coordinación y fallos

`flows/secop_flow.py` define `secop_flow()` y las tareas de validación, extracción, transformación por base, calidad, Gold en dos etapas y PostgreSQL.

- La ejecución es secuencial a nivel de tareas; el paralelismo de Gold ocurre dentro de Dask.
- Validación API tiene hasta tres reintentos configurados en Prefect; extracción, dos. Las solicitudes HTTP tienen además sus propios reintentos.
- El modo local omite la validación API del flujo.
- `.pipeline.lock` impide ejecutar simultáneamente dos flujos en el mismo directorio.
- Las funciones invocadas directamente desde notebooks no adquieren necesariamente ese bloqueo; no deben escribir las mismas capas al mismo tiempo.
- Ambas etapas de Gold se ejecutan antes de PostgreSQL; si fallan, no se realiza la nueva carga.
- Los fallos capturables producen un informe final; un cierre abrupto puede dejar el último checkpoint en `EN_CURSO`.

No existe una transacción que abarque API, Bronze, Silver, Gold, PostgreSQL e informe. Un fallo tardío puede dejar capas locales nuevas y una base de datos anterior. El informe permite identificar qué etapas sí terminaron.

No hay una periodicidad automática definida en el código revisado. El flujo puede ejecutarse repetidamente o integrarse posteriormente con una programación; la existencia de `@flow` no crea por sí sola un horario diario.

## 10. Informe de transparencia

`audit.py` instrumenta el flujo y genera:

- `notebooks/07_informe_ejecucion_etl.ipynb`: último informe.
- `notebooks/informes_etl/<fecha-UTC-id>.ipynb`: histórico por ejecución.

El notebook contiene tablas Markdown ya visibles y una celda opcional con la evidencia incorporada para explorarla en memoria. No necesita ejecutar el ETL ni conectarse a PostgreSQL para mostrar lo registrado. No ejecuta el EDA.

Registra parámetros, versiones, hashes del código, fuentes, intervalo, tamaños, filas, cambios de archivos por hash, transformaciones, filtros, cruce, carga y recursos de ambas etapas. Para Gold mide las características del resultado escrito, además de los conteos de etapas previas. La auditoría añade lecturas y agregaciones; su costo forma parte del tiempo de ejecución.

Distinciones para interpretar el informe:

| Evidencia | Qué demuestra | Qué no demuestra |
| --- | --- | --- |
| Hash distinto de un CSV | Cambió el contenido del archivo | Cuántas filas de negocio cambiaron; reordenar también cambia el hash |
| Menos bytes en Silver | Diferencia física CSV/Parquet | Eliminación de filas |
| `invalidos_conversion` | Un texto no vacío ni centinela no pudo convertirse | Todos los errores semánticos de formato |
| Métricas de archivo `existing` | Se reutiliza una transformación anterior | Que esas conversiones se hicieron en esta ejecución |
| `portafolios_compartidos` | Portafolios compartidos dentro de Gold | Compartición con procesos excluidos de Gold |
| PostgreSQL completado | Terminó la publicación transaccional | Un recuento independiente posterior: la cifra viene de filas enviadas por COPY |
| Memoria configurada | Límite de los workers | Pico de RAM observado |
| `INVENTARIO_PREVIO` | Estado de archivos existentes | Ejecución nueva del ETL |

Los conteos por variable pueden superponerse y no deben sumarse como observaciones eliminadas. Los errores se guardan por tipo; el detalle operativo está en los logs. Los informes no incluyen credenciales ni filas individuales. Si falla la propia escritura del informe final, el flujo puede fallar después de completar etapas de datos.

## 11. Responsabilidad de cada archivo de código

| Archivo | Qué hace | Por qué se separa |
| --- | --- | --- |
| `secop_pipeline/__init__.py` | Identifica el paquete Python | Permite importar sus módulos desde scripts y notebooks |
| `config.py` | Rutas, fuentes, columnas, tipos, vacíos, filtros y configuración de conexión | Centraliza decisiones compartidas |
| `extract.py` | Cliente HTTP, validación, paginación, descargas, manifiesto y reutilización | Aísla el acceso a la fuente y la trazabilidad de la entrada |
| `transform.py` | Limpieza de texto, números y fechas; Parquet por bloques; huellas y métricas | Hace reproducible la preparación de Silver |
| `quality.py` | Inventario de Parquet y validación de columnas | Impide continuar con un esquema de columnas inesperado |
| `dask_jobs.py` | Filtros, cruce, variables y escritura de Gold en dos etapas | Concentra el procesamiento con Dask |
| `load.py` | Esquema SQL, COPY por lotes, preparación/publicación e índices | Separa la persistencia de las transformaciones |
| `db.py` | Motor SQLAlchemy, espera de disponibilidad y creación de esquemas | Reutiliza acceso y comprobación básica de conexión |
| `models.py` | Clases ORM para columnas de procesos Gold, contratos y proveedores | Ofrece objetos Python para consultas |
| `orm.py` | Consulta de muestra de procesos analíticos | Ejemplo de consumo ORM tras la carga |
| `storage.py` | Publicación de directorios y bloqueo local | Reduce interferencias entre ejecuciones y permite recuperación ante excepciones |
| `audit.py` | Inventario, registro de etapas, renderizado del notebook y conclusiones operativas | Explica el recorrido de datos con evidencia de cada ejecución |
| `flows/secop_flow.py` | Tareas Prefect, orden, opciones, reintentos y auditoría | Coordina el proceso completo |
| `scripts/run_pipeline.py` | Argumentos de terminal y llamada al flujo | Proporciona una entrada reproducible sin abrir notebooks |
| `scripts/db_status.py` | Espera PostgreSQL y lista tablas de `secop` y `gold` | Diagnóstico de disponibilidad y existencia; no verifica filas ni vigencia |
| `scripts/compare_manifests.py` | Compara filas y listas de hashes entre dos manifiestos; termina con código 0/1 | Detecta diferencias entre descargas; es sensible al orden de las listas |
| `sql/00_info.sql` | Consultas de base, usuario y tablas | Referencia para DBeaver; no crea ni carga tablas |

## 12. Rol de cada notebook

| Notebook | Qué hace | Dependencias / efectos |
| --- | --- | --- |
| `00_verificar_entorno.ipynb` | Muestra versiones, recursos, rutas y configuración; consulta API y disponibilidad de PostgreSQL | Es diagnóstico, pero sí realiza conexiones. Revisar la celda que imprime URL: su sustitución solo oculta la contraseña predeterminada, no cualquier contraseña personalizada |
| `01_extraccion_api.ipynb` | Valida la API y llama `extract_all()` | Con el código actual reutiliza Bronze por defecto. Para primera descarga se necesita `refresh=True`. Su validación separada sí consulta la API |
| `02_transformacion_silver.ipynb` | Ejecuta `transform_all(overwrite=False)` y reporte de calidad | Necesita Bronze. Puede escribir/regenerar Silver y retirar particiones obsoletas |
| `03_dask_gold.ipynb` | Construye Gold en dos etapas y calcula estadísticas descriptivas | Necesita Silver. Modifica Gold y muestra las dos configuraciones reales. La conversión a float64 de las columnas resumidas evita el problema observado con enteros anulables en percentiles |
| `04_postgres_orm.ipynb` | Espera servidor, crea esquemas, carga tablas y consulta una muestra ORM | Reemplaza el contenido publicado cuando se ejecuta la celda de carga; no es solo consulta |
| `05_prefect_pipeline.ipynb` | Ejecuta el flujo completo con dos etapas de Gold y PostgreSQL | Reutiliza Bronze por defecto y genera el informe 07 con el código actual |
| `06_eda_recomendacion_empresas.ipynb` | EDA avanzado, calidad, hipótesis, asociaciones, modelo auxiliar y ranking empresa–oportunidad | Lee PostgreSQL; mantiene tablas, figuras y resultados en el notebook. Se ejecuta por separado |
| `07_informe_ejecucion_etl.ipynb` | Explica la última ejecución o el inventario inicial | Lo genera `audit.py`; la próxima ejecución puede reemplazarlo. Para conservarlo se mantiene el histórico |

### EDA frente al ETL

El notebook 06 estudia tipos de variables, distribuciones, relaciones no lineales, asociaciones numéricas/categóricas, concentración contractual, territorio y grupos. Explora una etiqueta auxiliar de contrato observado a 240 días y un ranking a partir del historial de una empresa.

Su regresión auxiliar está implementada con NumPy; no depende de scikit-learn. Los experimentos usan ventanas y subconjuntos específicos, y las conclusiones no representan automáticamente toda la población. Los contratos observados son señales positivas implícitas; no observar un contrato de una empresa no demuestra que la oferta fuera irrelevante.

La muestra y las reglas del EDA no son las mismas que las de Gold. El notebook trata de manera conservadora portafolios ambiguos y disponibilidad temporal. Ejecutar Prefect no recalcula estos análisis ni actualiza sus conclusiones en Markdown.

### Orden de trabajo

Para estudiar las etapas: 00 → 01 → 02 → 03 → 04. Para ejecutar el proceso completo: 05 o el script de terminal. No hace falta ejecutar previamente todas las etapas manuales y luego repetirlas con 05. Abrir 07 permite revisar el informe; ejecutar 06 permite analizar los datos cargados.

Las celdas comparten estado dentro de su kernel. Si se ejecuta una celda que usa `gold_path` antes de definirlo, aparece `NameError`. Después de editar módulos Python, reiniciar el kernel evita conservar importaciones antiguas.

## 13. Cómo se usan las librerías

La tabla distingue uso real de dependencia instalada. Instalar una librería no significa que forme parte del camino ejecutado del ETL.

| Librería / herramienta | Uso concreto | Motivo |
| --- | --- | --- |
| pandas | Lee CSV por páginas/bloques, normaliza columnas, convierte tipos, produce DataFrames y CSV para COPY; también se usa en el EDA | Operaciones tabulares expresivas sobre bloques acotados |
| NumPy | Logaritmos en Gold y cálculos/optimización/bootstrap en el EDA | Cálculo vectorizado; no sustituye a Dask para distribuir tablas |
| requests | `Session.get()` al recurso CSV, parámetros, token opcional, timeout y validación HTTP | Acceso a la API |
| PyArrow | Esquemas explícitos, `ParquetWriter`, lectura de metadata e `iter_batches()` | Parquet homogéneo y lectura por bloques/columnas |
| Dask DataFrame | Lectura de particiones, filtros, deduplicación, JOIN, agrupaciones y escritura Gold | Ejecutar un grafo de operaciones sin cargar toda la tabla en un DataFrame pandas único |
| Dask Distributed | `LocalCluster`, `Client`, workers por procesos y límites de memoria | Controlar recursos del procesamiento local |
| Prefect | `@flow`, `@task`, reintentos y registros de ejecución | Orquestación y visibilidad de estados |
| psycopg | Conexiones PostgreSQL, transacciones, composición segura de identificadores SQL y COPY | Carga masiva y control de publicación |
| SQLAlchemy | `create_engine`, `text`, sesiones y modelos ORM | Consultas SQL y consumo de objetos desde Python |
| python-dotenv | `load_dotenv()` | Cargar configuración local de `.env` |
| psutil | RAM disponible/total y CPU; comprobación del presupuesto de los perfiles | Dimensionar recursos sin confundir límite con consumo real |
| matplotlib | Histogramas, barras, boxplots, scatterplots, hexbin, mapas de calor y curvas del EDA | Visualización dentro del notebook; no transforma Bronze ni publica tablas |
| JupyterLab | Interfaz para abrir y trabajar con notebooks | Ejecución interactiva y lectura de informes |
| ipykernel | Kernel Python del entorno | Ejecutar las celdas con las dependencias correctas |
| IPython.display | Mostrar tablas y Markdown en notebooks | Presentar resultados y explicaciones |
| DuckDB | Figura en `requirements.txt`; no se utiliza en el flujo ni en los notebooks actuales revisados | Dependencia disponible, sin atribuirle transformaciones que hoy no ejecuta |
| tqdm | Figura en `requirements.txt`; no se utiliza en el flujo ni en los notebooks actuales revisados | Disponible para barras de progreso, pero no instrumenta el flujo actual |
| Docker Compose | Servicio `postgres:16`, healthcheck y volumen `secop_pgdata` | Reproducir la infraestructura local de base de datos |
| DBeaver | Cliente externo opcional para consultar PostgreSQL | Inspección SQL; no participa en la carga |

Biblioteca estándar de Python:

- `pathlib` y `os`: rutas, variables de entorno y bloqueo de archivo.
- `hashlib`: huellas SHA-256; el EDA también usa hashes para selección reproducible.
- `json` y `csv`: manifiestos, metadatos, formato notebook y lectura lógica de registros CSV.
- `io.StringIO`: buffers para respuestas y COPY.
- `time`: duraciones, espera y reintentos.
- `datetime`: marcas UTC y fechas.
- `tempfile`, `shutil`, `uuid`: directorios temporales, sustituciones y nombres únicos.
- `contextlib`: gestión de recursos y transacciones.
- `argparse`, `sys`: parámetros de CLI e importación desde la raíz del proyecto.
- `collections.Counter`: acumulación de métricas.
- `importlib.metadata`, `platform`: versiones registradas en auditoría.
- `re`, `unicodedata`, `warnings`: tratamiento de texto y contexto analítico en el EDA. Algunos módulos conservan imports no utilizados; un import aislado no prueba una operación.

### Dos etapas Dask con memoria conservadora y adaptable

| Etapa | Operaciones | Workers | Hilos por worker | Memoria por worker con RAM suficiente |
| --- | --- | --- | --- | --- |
| A | Filtrar y deduplicar procesos y claves contractuales; escribir intermedios | 2 | 1 | 2 GiB |
| B | Leer intermedios, JOIN, derivadas, escritura Gold y auditoría | 1 | 1 | 4 GiB |

El informe fallido mostró que el rechazo ocurrió antes de arrancar Dask: la reserva
fija de 8 GiB excedía el 70% de unos 11 GiB disponibles. No era evidencia de workers
bloqueados ni de agotamiento de memoria durante un JOIN. Ahora se redujo concurrencia
y presupuesto: se elige el mínimo entre 4 GiB y el 50% de RAM disponible, dividido entre
los workers y redondeado hacia abajo en bloques de 64 MiB. Se recalcula el techo antes
de cada etapa y se registran valores planificados y efectivos. Mínimo: 512 MiB/worker.

Para modo automático: DASK_PREP_WORKERS=2, DASK_JOIN_WORKERS=1, ambas variables
DASK_PREP_MEMORY y DASK_JOIN_MEMORY vacías/ausentes y no pasar límite manual de memoria.
CPU se registra, pero el número de workers no se adapta automáticamente a los núcleos.
Si se especifican límites manuales mediante entorno, argumentos o `configs`, se respeta
la selección y se valida contra el 70% de RAM disponible. No se elimina toda protección.

Los clústeres son secuenciales. A escribe intermedios; B los consume sin repetir la
preparación. Gold y PostgreSQL se publican una sola vez. Un worker en B sigue siendo
Dask y procesa las particiones secuencialmente, con menor concurrencia y más margen
de memoria individual. Lecturas adaptativas de row groups de 128 MiB, al menos ocho
particiones intermedias y JOIN sin broadcast mantienen acotada la carga de trabajo.

El informe registra tiempos, recursos y conteos. No son dos tareas comparables para
calcular aceleración. No se mide pico de RAM ni se afirma que sea una configuración
óptima. Se verificó Gold aislado con publish=False; los resultados están al final de esta guía. No se volvió a cargar PostgreSQL. El notebook 07 del fallo se
conserva: la siguiente ejecución lo actualizará y mantendrá el histórico.

Tras modificar código, reiniciar el kernel de Jupyter. No es necesario repetir
extracción y transformación manualmente: el flujo verifica y reutiliza Silver cuando
sus huellas coinciden. `transform.py` no cambió en este ajuste.

## 14. Configuración y ejecución

### Preparar el entorno

Los siguientes comandos son instrucciones de uso; no se ejecutan por leer este documento. Ejecutarlos desde la raíz del proyecto.

```powershell
conda env create -f environment.yml
conda activate secop
python -m ipykernel install --user --name secop --display-name "Python (secop)"
```

El entorno declara Python 3.12.6 y dependencias desde `requirements.txt`. Si ya existe el entorno, no volver a crearlo innecesariamente. Los rangos de versiones admiten actualizaciones: no constituyen un lockfile exacto.

Crear `.env` a partir de `.env.example` únicamente si todavía no existe. No sobrescribir la configuración local ni publicar contraseñas.

| Variable | Uso |
| --- | --- |
| `DATA_GOV_APP_TOKEN` | Token opcional de la API |
| `START_DATE` | Inicio para extracción nueva; predeterminado 2017-01-01 |
| `CUTOFF_DATE` | Corte para actualización explícita; vacío usa el día actual |
| `SOCRATA_BATCH_SIZE` | Filas por página de API; 50.000 por defecto |
| `TRANSFORM_CHUNK_ROWS` | Filas por bloque de transformación; 50.000 por defecto |
| `DASK_PREP_WORKERS` / `DASK_JOIN_WORKERS` | Workers de preparación/cruce; 2 y 1 por defecto |
| `DASK_PREP_MEMORY` / `DASK_JOIN_MEMORY` | Vacías: selección automática; con RAM suficiente, 2GiB y 4GiB por worker |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | Conexión a PostgreSQL |
| `LOAD_POSTGRES` | Default de carga cuando el argumento es `None`; `1` habilita |

El CLI envía booleanos explícitos mediante sus flags; por eso sus opciones prevalecen sobre ese default. El notebook 05 también habilita la carga explícitamente.

### Iniciar PostgreSQL local

```powershell
docker compose up -d
```

El volumen `secop_pgdata` conserva la base fuera del ciclo de vida del contenedor. Cambiar credenciales de inicialización en `.env` no implica que una base ya inicializada cambie automáticamente sus usuarios o contraseñas.

### Reprocesar los datos actuales

```powershell
python scripts/run_pipeline.py
```

Este es el comportamiento predeterminado: verifica Bronze local, transforma cuando corresponde, construye Gold, carga PostgreSQL, genera el informe de los dos perfiles. No descarga novedades.

Variantes:

```powershell
python scripts/run_pipeline.py
python scripts/run_pipeline.py --no-postgres
python scripts/run_pipeline.py --reuse-bronze --dask-workers 2
```

`--no-postgres` omite la carga, pero sigue pudiendo modificar capas locales. `--reuse-bronze` expresa explícitamente el default; no puede combinarse con `--refresh-bronze`.

### Descargar datos nuevos, solo cuando se decida hacerlo

```powershell
python scripts/run_pipeline.py --refresh-bronze --start 2017-01-01 --cutoff 2026-10-07
```

La fecha es un ejemplo explícito, no una recomendación de cambiar el corte actual. Esta opción realiza una extracción completa. Para una primera instalación sin Bronze ni manifiesto es necesario solicitar esa descarga o disponer de un snapshot válido.

### Desde Python o notebook 05

```python
from flows.secop_flow import secop_flow

# Reutiliza Bronze. Sí procesa y carga las capas posteriores.
secop_flow(refresh=False, load_postgres=True)
```

### Utilidades

```powershell
python scripts/db_status.py
python scripts/compare_manifests.py ruta\manifest_a.json ruta\manifest_b.json
jupyter lab
```

Para consultar las cinco tablas puede utilizarse DBeaver con la conexión configurada. El script `db_status.py` solo enumera tablas: su salida no demuestra que la última carga esté completa ni que coincida con el último corte.

## 15. Límites, fallos habituales y operación

### Escalabilidad real

La API se pagina; Silver se transforma por bloques; Parquet admite lectura selectiva; Gold usa Dask; PostgreSQL recibe COPY por lotes. Estos mecanismos reducen presión de memoria, pero no hacen ilimitado el procesamiento.

La actualización remota y la carga SQL siguen siendo completas. Los JOIN y deduplicaciones de Dask pueden requerir redistribución y disco temporal. Todos los workers de `LocalCluster` comparten el mismo equipo; no se ha desplegado un clúster de varias máquinas. El histórico de notebooks crece con las ejecuciones y no tiene política automática de retención.

### Diagnóstico

| Situación | Explicación / acción |
| --- | --- |
| Falta manifiesto local | El modo predeterminado no inicia una descarga. Proporcionar un snapshot válido o solicitar explícitamente actualización |
| Hash o archivos no coinciden | Bronze difiere del manifiesto. Revisar la procedencia; no editar hashes para ocultar la diferencia |
| Corte distinto en modo local | Las fechas pedidas no corresponden al snapshot; omitir fechas para usar las registradas o actualizar explícitamente |
| `.pipeline.lock` ya existe | Puede haber otro flujo o un cierre abrupto. Confirmar que no sigue activo antes de retirarlo |
| Error de columnas Silver | Revisar `reporte_calidad.csv`, fuente y configuración antes de continuar |
| `gold_path` no definido | Ejecutar primero la celda que construye/asigna la ruta o definirla desde `GOLD_DIR` |
| Error `Int64Dtype` al describir | El notebook 03 convierte a float64 las columnas elegidas antes de `describe().compute()` |
| COPY rechaza `0.0` en BIGINT | La versión actual fuerza las columnas numéricas declaradas a DOUBLE PRECISION; revisar que el kernel haya recargado el módulo actualizado |
| PostgreSQL no disponible | Revisar servicio, puerto, credenciales y tiempo de espera |
| Flujo fallido después de cargar | Revisar etapas del informe: pudo fallar la escritura del informe después del commit |
| Informe en EN_CURSO | Es un checkpoint; no confirma éxito y puede corresponder a interrupción abrupta |

### Límites analíticos que permanecen

- La fuente actual no garantiza atributos tal como estaban disponibles al publicar.
- Gold contiene resultados posteriores y excluye procesos abiertos.
- El enlace por portafolio puede ser ambiguo.
- La limpieza de centinelas y separadores requiere validación semántica por campo.
- Los índices SQL no imponen integridad ni unicidad.
- No hay una política completa de reconciliación temporal, deduplicación de todas las bases o actualización incremental.
- Añadir otra fuente «similar» exige adaptar `BASES`, `COLUMNAS`, `TIPOS`, extracción, carga, auditoría y relaciones según su esquema. El flujo actual no es un importador universal de cualquier CSV.

## 16. Anexo: contrato de columnas y tipos

Las listas siguientes corresponden a `TIPOS` del código revisado. «Llave» expresa uso previsto como identificador; no significa que exista una restricción SQL. «Etiqueta» significa texto: también incluye códigos nominales. La lista de extracción completa se mantiene en `COLUMNAS`.

### Procesos — 36 columnas solicitadas

#### Identificadores (texto)

- `id_del_proceso`
- `id_del_portafolio`
- `nit_entidad`
- `codigo_entidad`
- `nit_del_proveedor_adjudicado`
- `codigoproveedor`

#### Etiquetas y códigos nominales (texto)

- `entidad`
- `departamento_entidad`
- `ciudad_entidad`
- `ordenentidad`
- `modalidad_de_contratacion`
- `tipo_de_contrato`
- `subtipo_de_contrato`
- `codigo_principal_de_categoria`
- `fase`
- `unidad_de_duracion`
- `adjudicado`
- `estado_del_procedimiento`
- `estado_resumen`
- `departamento_proveedor`
- `ciudad_proveedor`

#### Campos numéricos (float64 en Silver)

- `duracion`
- `precio_base`
- `numero_de_lotes`
- `proveedores_invitados`
- `proveedores_con_invitacion`
- `visualizaciones_del`
- `proveedores_que_manifestaron`
- `respuestas_al_procedimiento`
- `respuestas_externas`
- `conteo_de_respuestas_a_ofertas`
- `proveedores_unicos_con`
- `valor_total_adjudicacion`

#### Fechas (timestamp en Silver)

- `fecha_de_publicacion_del`
- `fecha_de_recepcion_de`
- `fecha_adjudicacion`

### Contratos — 38 columnas solicitadas

#### Identificadores (texto)

- `id_contrato`
- `proceso_de_compra`
- `nit_entidad`
- `codigo_entidad`
- `documento_proveedor`
- `codigo_proveedor`
- `duraci_n_del_contrato`

#### Etiquetas y códigos nominales (texto)

- `nombre_entidad`
- `departamento`
- `ciudad`
- `orden`
- `sector`
- `rama`
- `entidad_centralizada`
- `es_grupo`
- `es_pyme`
- `estado_contrato`
- `codigo_de_categoria_principal`
- `tipo_de_contrato`
- `modalidad_de_contratacion`
- `origen_de_los_recursos`
- `destino_gasto`
- `habilita_pago_adelantado`
- `liquidaci_n`
- `espostconflicto`

#### Campos numéricos (float64 en Silver)

- `dias_adicionados`
- `valor_del_contrato`
- `valor_facturado`
- `valor_pagado`
- `valor_pendiente_de_pago`
- `valor_amortizado`
- `valor_pendiente_de_ejecucion`
- `saldo_cdp`
- `saldo_vigencia`

#### Fechas (timestamp en Silver)

- `fecha_de_firma`
- `fecha_de_inicio_del_contrato`
- `fecha_de_fin_del_contrato`
- `ultima_actualizacion`

### Proveedores — 15 columnas solicitadas

#### Identificadores (texto)

- `codigo`
- `nit`

#### Etiquetas y códigos nominales (texto)

- `es_entidad`
- `es_grupo`
- `esta_activa`
- `codigo_categoria_principal`
- `descripcion_categoria_principal`
- `pais`
- `departamento`
- `municipio`
- `tipo_empresa`
- `espyme`
- `ubicacion`
- `tama_o_entidad`

#### Campos numéricos (float64 en Silver)

No hay columnas declaradas en este grupo.

#### Fechas (timestamp en Silver)

- `fecha_creacion`

### Grupos — 20 columnas solicitadas

#### Identificadores (texto)

- `codigo_grupo`
- `nit_grupo`
- `codigo_participante`
- `nit_participante`

#### Etiquetas y códigos nominales (texto)

- `es_entidad`
- `es_proveedor`
- `esta_activo`
- `codigo_categor_a_principal_grupo`
- `pais_grupo`
- `departamento_grupo`
- `minucipio`
- `tipo_empresa_grupo`
- `es_mipyme`
- `ubicaci_n_grupo`
- `tipo_empresa_participante`
- `ubicaci_n_participante`
- `es_lider_del_grupo`

#### Campos numéricos (float64 en Silver)

- `participacion`

#### Fechas (timestamp en Silver)

- `fecha_creaci_n_grupo`
- `fecha_creaci_n_participante`

### Valores considerados vacíos

La comparación se hace con el texto recortado y en minúsculas. La cadena vacía también se convierte en nulo.

- Cadena vacía (`""`).
- `-`
- `.`
- `0001-01-01t00:00:00.000`
- `n/a`
- `na`
- `no adjudicado`
- `no aplica`
- `no aplica/no pertenece`
- `no definida`
- `no definido`
- `no informado`
- `no provisto`
- `none`
- `null`
- `sin dato`

## 17. Cómo mantener esta documentación

Cuando cambien columnas, filtros, tipos, cruces o parámetros, actualizar esta guía y conservar el informe correspondiente a la ejecución. Las métricas históricas deben consultarse en su notebook de auditoría; no se presentan como valores permanentes en el README. Una actualización del código no recalcula las salidas antiguas de los notebooks ni valida automáticamente el proceso de extremo a extremo.


## Verificación real del cruce — 7 de octubre de 2026

El registro de Prefect mostró `KeyboardInterrupt` durante B, después de que A terminara.
No se pudo atribuir la interrupción a un fallo automático de workers. Se probaron tres
arquitecturas fuera del notebook, sobre los intermedios del Silver existente:

| Arquitectura del cruce | Tiempo total de prueba | Resultado |
| --- | --- | --- |
| P2P, 1 worker × 4 GiB | 15.46 s | Completado |
| Tareas, 2 workers × 3 GiB | 9.15 s | Completado |
| Tareas, 1 worker × 4 GiB | 13.49 s | Completado |

Las tres salidas tienen **626.821 filas e IDs distintos**, **525.835 coincidencias
contractuales** y **539.919 objetivos positivos**. La comparación bidireccional
`EXCEPT ALL` de todas las columnas produjo **0 filas diferentes**. Una referencia
SQL independiente del JOIN y objetivo también produjo **0 diferencias**.

Se eligió shuffle `tasks`, preparación con 2 workers × 2 GiB y cruce con 1 worker × 4 GiB.
El cruce con dos workers fue más rápido en esta medición, pero exigió 6 GiB en vez de
4 GiB; se priorizó el presupuesto conservador. Una sola medición no prueba optimalidad.

Después se ejecutó `build_gold(audit=..., publish=False)` con ambas etapas y memoria
automática. Terminó en **32.17 segundos**: preparación
**17.27 s**, cruce/auditoría
**11.96 s**; el total incluye arranque/cierre.
Se comprobó mediante SHA-256 que los Parquet Gold publicados permanecieron intactos.
No se descargaron datos ni se cargó PostgreSQL. Esto valida Gold aislado; no es una
nueva ejecución completa de Prefect ni garantiza que una interrupción externa no se repita.

**Para ejecutar en Jupyter:** reiniciar el kernel y ejecutar las celdas iniciales del
notebook 05 para cargar el módulo nuevo. Ahora Gold imprime avances de lectura,
preparación, JOIN, escritura y auditoría. Los mensajes de shuffle marcados como WARNING
no significan por sí solos que el flujo haya fallado.

Verificación opcional sin publicar, si se necesita repetir la comprobación:

```python
from secop_pipeline.dask_jobs import build_gold
evidencia = {}
build_gold(audit=evidencia, publish=False)
```

`publish=False` construye temporalmente ambas etapas, valida conteos y descarta la
salida. Devuelve `None`. La ejecución normal usa `publish=True` y devuelve la ruta Gold.
