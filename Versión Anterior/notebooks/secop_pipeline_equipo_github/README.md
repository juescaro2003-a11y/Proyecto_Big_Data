# SECOP II — ETL de control contractual

El objetivo activo es **priorizar contratos de contratación directa para revisión**. Gold tiene una fila por contrato, identificado por `id_contrato`. El modelo vigente post-Gold es `alertas-cd-2.0`: D1 concentración monetaria, D2 valor atípico y D3 acumulación en diciembre; ALERTA requiere al menos dos señales. Los componentes IRCD de Gold pertenecen a una etapa distinta. Una señal estadística no demuestra irregularidades ni estima una probabilidad de corrupción.


**Documentación funcional actualizada (09/10/2026):** [Funcionamiento completo del ETL y las alertas](docs/PRESENTACION_ETL_SECOP_II.md). La sección 19 describe el flujo post-Gold, notebooks 09–11, memoria, caché y resultados. `V2.ipynb` es diagnóstico monetario; la exclusión propuesta y el modelo 2.1 siguen pendientes. Las secciones siguientes describen principalmente la construcción de Gold.

## Estado de la migración

El código, flujo, cargador y notebooks están adaptados. Cambiar el código no reconstruye datos ni actualiza PostgreSQL. Los resultados previos y `gold.procesos_modelo` son históricos. Las pruebas usan datos sintéticos, no constituyen calibración real.

La extracción antigua carece de causal, tipo documental, objeto y URL. Se pueden reprocesar esos archivos: los campos faltantes quedan nulos y los indicadores afectados quedan incompletos. Una descarga explícita incorpora las nuevas columnas.

## 1. Recorrido

```text
API / Bronze local
 → extract.py: descarga explícita o verificación de hashes
 → Bronze: cuatro bases CSV + manifiesto
 → transform.py: limpieza, tipado y copias raw__ en contratos
 → Silver: cuatro bases Parquet, sin filtros de filas de negocio
 → quality.py: columnas esperadas
 → Dask A: normalización por particiones
 → Dask B: DuckDB con memoria acotada y spill
     versiones → contratación directa → filtro personas → C/R/A → IRCD
 → Gold: contratos_control/contratos.parquet
         exclusiones/exclusiones.parquet + auditoria_ircd.json
 → load.py: COPY y publicación transaccional
 → PostgreSQL: secop.* + gold.contratos_control
 → notebook 06: cobertura, bandeja y evidencia
```

Sigue siendo ETL: transforma antes de cargar. Prefect coordina y registra tareas. Gold utiliza contratos; procesos, proveedores y grupos se conservan como bases auxiliares, sin cruces para construir estos indicadores.

## 2. Fuentes

| Base | ID Socrata | Selección temporal |
| --- | --- | --- |
| Contratos electrónicos, principal | jbjy-vk9h | Mensual por fecha de firma |
| Procesos, auxiliar | p6dx-8zbt | Mensual por publicación |
| Proveedores, auxiliar | qmzu-gj57 | Catálogo completo |
| Grupos, auxiliar | ceth-n4bn | Catálogo completo |

Nombres verificados el 7 de octubre de 2026 en [metadatos oficiales](https://www.datos.gov.co/api/views/jbjy-vk9h.json):

| Campo nuevo | Uso |
| --- | --- |
| tipodocproveedor | Aproximación a persona natural |
| justificacion_modalidad_de | Causal y segmento |
| proveedor_adjudicado | Contexto, nunca clave |
| descripcion_del_proceso / objeto_del_contrato | Contexto para revisión; no cuarta alerta |
| urlproceso / referencia_del_contrato | Localizar expediente |

`valor_del_contrato` se describe como **valor total**, no se acredita como inicial. Se utiliza consistentemente la última versión inequívoca del total reportado. `definicion_valor` registra esa limitación. No se suman versiones. No se extraen cuentas bancarias ni documentos de supervisores.

## 3. Limpieza y exclusiones

### Bronze

`refresh=False` verifica manifiesto y hashes sin descargar. Fechas fuera del intervalo y fechas nulas no satisfacen las consultas mensuales. Las columnas no seleccionadas nunca entran. La API paginada no garantiza fotografía transaccional entre páginas.

`refresh=True` descarga las cuatro bases y conserva la versión anterior en `data/bronze.previous-<uuid>`, con su manifiesto. Estos respaldos consumen disco y no se eliminan automáticamente.

### Silver

Se procesan bloques de 50.000 filas; Parquet usa Snappy. Se recortan espacios, se reconocen centinelas y se tipan números/fechas. Los números Socrata se interpretan primero con punto decimal; existe conversión secundaria de formatos locales. Identificadores permanecen como texto.

Las fechas se interpretan en UTC y guardan sin zona. La fuente puede incluir fechas locales sin zona; revisar antes de análisis horarios. `no aplica` y `no adjudicado` conservan significado; ya no son centinelas globales. Los valores no convertibles quedan nulos y se contabilizan.

Contratos conserva columnas textuales `raw__...` para contrastar cada normalización. Bronze permanece como referencia original. No se filtran ni deduplican filas en Silver. La caché depende de CSV, código y reglas: esta migración puede reconstruir todas las particiones. Silver retira particiones que no pertenecen al Bronze activo.

### Identidades

Se normalizan acentos y espacios en categorías. Se preservan originales. Entidad se identifica por NIT normalizado. Proveedor prioriza código SECOP; si falta, tipo documental + documento. Sin ambos queda sin identidad fiable. Nunca se agrupa por nombre.

Códigos cambiantes o intermitentes pueden fragmentar una identidad; no se inventa un cruce para reconciliarlos. No se elimina un supuesto dígito verificador: documentos con y sin DV requieren revisión. CC, CE y pasaporte reconocidos se clasifican como `natural`; NIT y tipos desconocidos quedan `indeterminado`.

`person_filter`: `todos`, `natural` o `indeterminado`. Por defecto `todos` permite medir cobertura del histórico; seleccionar `natural` restringe todos los componentes y denominadores al mismo universo.

### Gold

1. Elegir versión contractual antes de filtrar modalidad.
2. Última actualización inequívoca; colapsar repeticiones idénticas de campos retenidos.
3. Versiones anteriores: exclusión explícita.
4. Variantes contradictorias en la última actualización, o variantes sin fecha que impiden ordenarlas: excluir ID como `version_ambigua`.
5. Sin ID: `sin_id_contrato`.
6. Conservar modalidad normalizada `contratacion directa`.
7. Excluir firmas posteriores al corte y aplicar filtro de personas.
8. Sin fecha/valor necesario: conservar contrato con componentes nulos, nunca imputar cero.

Cada fila termina en salida o exclusiones. Se verifica `entrada = contratos_gold + exclusiones`. Exclusiones guarda ID, huella, archivo y motivo; el detalle permanece en Bronze/Silver. Gold verifica ID único y que los cruces de agregados no multipliquen contratos.

## 4. Tres componentes

Parámetros en `ircd_config.json`, validados por `control_config.py`. SQL mantenible en `control_analytics.py`. Cada contrato registra versión, hash de parámetros, corte, fecha de cálculo, denominadores, grupo y umbrales reales.

### C — Concentración

Participación = suma del proveedor / suma de la entidad, dentro de entidad–año–causal–tipo y filtro de personas. Mínimo 30 contratos del segmento por entidad. Si hay valores inválidos o proveedores sin identificar en ese grupo, no se puntúa C: no se oculta la parte no calculable del denominador.

Referencia: participaciones entidad–proveedor–año de años completos de la línea base. Una observación por par y año. Segmentos por causal/tipo y volumen: hasta 99, 100–999, 1.000 o más contratos; grupos con menos de 30 no reciben C. Se exige mínimo de referencia configurable.

Debajo P90: 0; desde P90 hasta antes de P95: 1; desde P95: 2.

### R — Recurrencia

Contratos distintos del mismo par entidad–proveedor y segmento en `[firma−365 días, firma]`, ambos extremos inclusivos. Se agregan por día para incluir todos los contratos firmados el mismo día. No se suman entidades distintas ni firmas futuras. Se exige cobertura de la ventana completa.

Referencia: **máximo conteo móvil de cada par/año** en la línea base, por causal/tipo y volumen. Evita que los pares con más contratos aporten una observación por cada fila. Esta definición requiere calibración con expedientes y no equivale al percentil de todas las filas contractuales.

Con referencia suficiente: menos de 2 contratos → 0; después se aplican P90/P95. Si P90=P95, el score queda nulo por defecto. Se informa cuántas alertas habría al aceptar el empate. `accept_recurrence_ties` registra aceptación explícita tras calibrar.

### A — Valor atípico

Duración = fin − inicio + 1. Valor mensual = total × 30,44 / duración. Exige prestación de servicios, importe positivo finito, fechas válidas, duración entre 1 y `max_duration_days`, causal, categoría y departamento.

**No se presume remuneración mensual.** `monthly_validated_categories` está vacía por defecto: A permanece nulo hasta revisar comparabilidad de servicios, perfiles y remuneración. Compartir código UNSPSC no garantiza homogeneidad. Incluir un código supone documentar esa revisión y la comparabilidad económica entre años.

Referencia: último año de la línea base (2024), por causal/tipo, categoría y departamento de la entidad; mínimo 30 contratos. Se guarda el año de referencia. No se mezclan importes nominales de los tres años. Aplicarlo a 2025/2026 exige revisar inflación/composición; no se afirma que sea comparación del mismo año o región de ejecución.

Debajo P90: 0; desde P90: 1; desde P95 **y ratio frente a mediana ≥ 1,5**: 2. Ratio 1,2 no activa 2. Es una regla inicial, no un criterio legal. El objeto se conserva como evidencia, sin generar otra alerta.

### Composición y tiempo

`IRCD = score_C + score_R + score_A` con pesos iguales, solo si existen los tres. 0–1 seguimiento ordinario; 2–3 revisión preliminar; 4–6 revisión prioritaria. Faltantes: `ircd=null`, `estado_indice=incompleto`, componentes disponibles conservados.

Línea base 2022–2024; validación 2025; seguimiento 2026, configurables. La línea base no recibe puntajes con sus propios umbrales. Solo se aplican a años posteriores.

**Interpretación: foto anual al corte**, usando contratos conocidos del año hasta ese corte. C no representa una alerta disponible al firmar. R respeta la ventana hasta la firma. Últimas versiones actuales de contratos antiguos no reconstruyen atributos históricos: no usar como backtest as-of.

## 5. Dask y memoria

- A: 2 workers, hasta 3 GiB por worker y un hilo. Cada tarea lee únicamente las columnas necesarias de un archivo Silver en lotes de 5.000 filas, normaliza y escribe inmediatamente. Solo se envían dos tareas a la vez; al coordinador regresan contadores, no DataFrames. `DASK_PREP_BATCH_ROWS` permite entre 1 y 10.000 filas por lote. Esto acota filas en memoria, no garantiza un máximo de bytes si hay textos excepcionalmente grandes.
- B: 1 worker, hasta 6 GiB, un hilo: tarea Dask con SQL DuckDB para versiones, ventanas, referencias y escritura. DuckDB tiene 50% del límite del worker y spill a disco.
- Presupuesto automático por etapa: menor de 6 GiB y 50% de RAM disponible. Clústeres secuenciales; no se suman presupuestos. Los límites se redondean hacia abajo a múltiplos de 64 MiB y se reducen si baja la RAM disponible; son máximos, no memoria reservada. Se registran límites efectivos.
- Se conservan `DASK_PREP_*` y `DASK_JOIN_*`; JOIN configura ahora la etapa analítica. En manual revisar ambos límites.
- Las pruebas sintéticas prueban funcionamiento, no rendimiento sobre millones de contratos. Se necesita disco para intermedios y spill. No se afirma que la configuración sea óptima en cualquier equipo.

- Protecciones de Dask explícitas: objetivo 60%, spill 70%, pausa 80%, terminación 95%. No se desactiva la protección del Nanny. El spill de Dask no controla los temporales internos de pandas: por eso se limita también el lote de lectura.
- El informe conserva tamaño de lote, concurrencia, recursos efectivos y, si falla, mensaje de error y logs disponibles del Nanny. La consola muestra archivos y filas preparados tras cada tanda.
- Ajuste del 8 de octubre: motivado por `KilledWorker` en A con 2 × 2 GiB. La causa exacta del cierre no quedó registrada; la presión de memoria es una hipótesis. La nueva configuración todavía requiere una ejecución real para confirmar estabilidad. Las pruebas sintéticas descritas más adelante corresponden a la versión anterior.
- Para reintentar tras ese fallo, reutilizar Bronze con `refresh=False` (omitir `--refresh-bronze` en CLI). Silver se reutiliza cuando su caché sigue vigente. Gold se reconstruye y PostgreSQL solo se publica después de completarlo.

## 6. Archivos y librerías

| Archivo | Función y librerías |
| --- | --- |
| config.py | Fuentes, columnas, tipos, rutas; dotenv para entorno |
| extract.py | requests a Socrata; pandas CSV; manifiesto y hashes |
| transform.py | pandas/NumPy para tipos; PyArrow Parquet, caché y métricas |
| quality.py | PyArrow verifica columnas, no integridad semántica |
| control.py | Normalización, identidad, persona y funciones simples de scores |
| control_config.py | Configuración validada, versión y huella |
| control_analytics.py | DuckDB: ventanas, agregados, referencias, exclusiones e IRCD |
| dask_jobs.py | Dask/distributed, dos clústeres; psutil ajusta memoria |
| load.py | psycopg COPY, staging e índices; transacción PostgreSQL |
| models.py / orm.py | SQLAlchemy ContratoControl y sample_contracts |
| storage.py | Bloqueo de ejecuciones y publicación de directorios |
| audit.py | Informe autocontenido en notebook y métricas |
| flows/secop_flow.py | Prefect SECOP-II-CONTROL-ETL |
| scripts/run_pipeline.py | CLI |
| tests/ | unittest; no requiere añadir framework |

Clases ORM antiguas se conservan como legado. `sample_processes` pide explícitamente usar `sample_contracts`, evitando confundir la salida antigua con control.

## 7. Notebooks

| Notebook | Rol |
| --- | --- |
| 00 | Entorno y dependencias |
| 01 | Extracción o reutilización |
| 02 | Silver |
| 03 | Gold contractual y recursos Dask |
| 04 | Carga y consulta contractual |
| 05 | Flujo completo; reiniciar kernel tras migrar |
| 06_control_contratacion_directa | Cobertura, gráficos, bandeja y ficha |
| 07_informe_ejecucion_etl | Informe de la ejecución; comprobar fecha/enfoque |
| informes_etl/ | Históricos; los anteriores siguen siendo del diseño anterior |
| historico_enfoque_proveedor/ | Notebooks previos con sus resultados |

06 consulta Parquet con DuckDB, presenta scores ordinales con barras, scatter de pares y correlación de rangos C/R. Filtra contratos y permite examinar evidencia individual. No muestra documentos personales por defecto ni exporta resultados. Conclusiones empíricas requieren ejecutar el nuevo Gold y revisar expedientes. El informe 07 existente no se reescribe fingiendo una ejecución nueva.

## 8. Cómo ejecutar

Desde raíz y entorno secop; reiniciar kernels después de actualizar código:

```powershell
conda activate secop
python scripts/run_pipeline.py --reuse-bronze --no-postgres
```

Produce diagnóstico con cobertura local. Para descargar columnas nuevas y cargar PostgreSQL:

```powershell
python scripts/run_pipeline.py --refresh-bronze
```

Revisar primero `ircd_config.json`. No se cambian credenciales. `IRCD_CONFIG` permite otra ruta de parámetros. `cutoff` analítico puede fijarse allí; por defecto hereda manifiesto y no puede superar su cobertura. `--cutoff` del CLI controla extracción; en reutilización debe coincidir con manifiesto.

La carga publica `gold.contratos_control` y cuatro tablas Silver en una transacción. Índice único por ID contractual. Lee solo Parquet de nivel superior, sin incluir exclusiones. Reemplaza snapshots; no usa append ni UPSERT. `gold.procesos_modelo` se conserva como legado sin actualizar. No se configura una periodicidad nueva automáticamente.

## 9. Pruebas y límites pendientes

```powershell
python -m unittest discover -s tests -v
```

Pruebas sintéticas: filtros, versiones, ambigüedad, duplicados, ventana inclusiva, entidades, corte, NIT indeterminado, denominador con filtro, umbrales, ratios, fechas faltantes, columnas antiguas, universo vacío e índice incompleto. Integración real de los dos clústeres con publicación en directorio sintético temporal.

Antes de uso operativo: incorporar campos faltantes, revisar identidades, comparar cobertura, validar mensualización, empates R, proporción de alertas y correlación C/R. Examinar expedientes con/sin señales. Las pruebas de software no sustituyen calibración ni revisión de contratos.

El README anterior se conserva en `docs/README_enfoque_proveedor_historico.md`.

### Verificación de esta migración (7 de octubre de 2026)

Se ejecutó `python -m unittest discover -s tests -v`: **18 pruebas aprobadas**. Incluye dos clústeres Dask reales (2 × 2 GiB y 1 × 4 GiB en este equipo), publicación sintética y ejecución de todas las celdas de consulta del notebook 06 contra esa salida sintética. También se verificó sintaxis de módulos/notebooks y generación del informe. No se descargaron bases, no se reconstruyó Gold real y no se cargó PostgreSQL durante esta validación. Rendimiento y calibración con la nueva extracción quedan pendientes.

## EDA inspirado en Fedesarrollo (continuación)

`notebooks/08_eda_fedesarrollo_control.ipynb` replica las preguntas descriptivas del documento FEDESARROLLO_CPBD.pdf con Silver local: embudo de depuración, evolución, modalidades, tipos, nivel administrativo, participación de directa y especial, enero/estacionalidad, HHI/CR4, cuatro proveedores principales, deciles de gasto, recurrencia, valores extremos y sensibilidad. Incluye un mapa a tablas/figuras y explica diferencias de fuentes, períodos, unidades y filtros. Mantiene servicios relevantes para el caso actual. No reconstruye el índice IMCO ni añade componentes al IRCD. Las tablas, figuras y conclusiones se conservan exclusivamente en el notebook; no actualiza ETL, datos o PostgreSQL.

La réplica fue ejecutada con los datos locales: 19 celdas de código, 15 figuras y sin errores de ejecución. Incluye 5.642.414 contratos únicos (295 repeticiones excluidas solo de la vista analítica). La sección 2b documenta importes extremos presentes en Bronze y muestra su influencia: no se deben interpretar sumas monetarias como gasto verificado. Todas las conclusiones y tablas permanecen en el notebook 08.

## Alertas posteriores a Gold (nuevo flujo independiente)

La nueva etapa calcula concentración móvil, valor atípico robusto y diversificación UNSPSC, sin reconstruir capas anteriores. Ver [instrucciones de ejecución](docs/EJECUTAR_ALERTAS_POST_GOLD.md) y `notebooks/09_alertas_post_gold.ipynb`.

```powershell
python scripts/run_alertas.py --local
python scripts/run_alertas.py --check-aws
python scripts/run_alertas.py --solo-carga
```

AWS requiere `SECOP_RDS_PASSWORD`; la publicación usa clave contrato/corte/versión y UPSERT, sin reemplazar históricos. Esta etapa no usa los scores IRCD anteriores.


### D2 con Dask, cache recuperable y dashboard

La arquitectura por bloques se documenta en [ARQUITECTURA_DASK_ALERTAS.md](docs/ARQUITECTURA_DASK_ALERTAS.md). Monitor visual: `notebooks/11_monitor_alertas.ipynb` en otro kernel. Desde otra terminal: `python scripts/monitor_alertas.py --watch --open-dashboard`. Reiniciar el kernel de la ejecucion anterior antes de utilizar el codigo nuevo.


### D3 vigente: acumulación de diciembre (modelo 2.0)

La diversificación fue reemplazada por acumulación temporal por entidad-año. Definición, umbrales y limitaciones: [D3_CIERRE_ANUAL.md](docs/D3_CIERRE_ANUAL.md). D1/D2 se reutilizan si sus firmas son compatibles.
