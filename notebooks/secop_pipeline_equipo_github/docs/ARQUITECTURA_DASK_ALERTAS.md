# D2 con Dask, checkpoints y monitor

## Ejecución

Interrumpir la ejecución anterior, esperar que finalice y reiniciar el kernel del notebook 09. Luego ejecutar con `MODO = "local"`. El proceso activo anterior no puede cambiar de algoritmo en caliente.

Desde otra terminal en la raíz del proyecto:

```powershell
conda activate secop
python scripts/monitor_alertas.py --watch --open-dashboard
```

También está disponible `notebooks/11_monitor_alertas.ipynb`, con barra de progreso y enlace al dashboard. Usar un kernel diferente al del notebook 09.

## Cálculo exacto y distribución

El coordinador conserva la única conexión a `analitica.duckdb` y limita DuckDB a 512 MB durante D2, con temporales en disco. Exporta columnas pequeñas para D2 y agrupa por grupo, fecha y valor, conservando la cantidad de contratos como peso. Los workers únicamente leen Parquet y publican bloques independientes.

Cada grupo conserva tipo, causal y prefijo UNSPSC. Los grandes (más de 100.000 contratos) se dividen en bloques de 31 fechas distintas a evaluar. Los pequeños se agrupan en 32 particiones para reducir tareas diminutas. Los grupos se asignan completos a una partición; no se recorta su historia al dividir fechas de evaluación.

Para cada fecha, la ventana exacta incluye `fecha - 24 meses` y excluye la fecha actual. Los pesos de los valores se actualizan al entrar y salir fechas. Se calculan la mediana y P95 ponderados por contratos y la mediana ponderada de las desviaciones absolutas respecto a la mediana de esa misma ventana. No se promedian medianas ni se aproximan percentiles. El contrato actual, sus pares del mismo día y los futuros quedan fuera de la referencia.

Primero se calcula UNSPSC8. Solo las fechas que no alcanzan el mínimo de comparables generan trabajo para UNSPSC4 (y UNSPSC2 si está configurado). La referencia ampliada sí incluye todos los contratos elegibles del grupo amplio. Se mantienen la regla de MAD cero y el criterio de historia suficiente del flujo original.

El ID de grupo utiliza SHA256 de una lista JSON para evitar ambigüedades al concatenar categorías. El vínculo final se hace por ID de grupo y fecha; cada contrato mantiene su ID original.

## Clúster

`alertas_dask_config.json` permite fijar workers/memoria o utilizar 0 para automático:

| RAM disponible al iniciar | Workers máximos | Memoria por worker | Hilos |
|---|---:|---:|---:|
| ≥12 GiB | 4 | 2048 MiB | 1 |
| ≥6 GiB | 2 | 1536 MiB | 1 |
| Menor | 1 | 1536 MiB | 1 |

Se limita además por núcleos físicos. Se exige al menos 2 GiB de reserva aparte del presupuesto de workers; si no alcanza, se informa el problema antes de crear el clúster. La memoria del worker es supervisada por Dask; NumPy/Pandas consumen memoria no derramable durante una tarea. Los reinicios no convierten una tarea que no cabe en una tarea viable: si se repite falta de memoria, reducir el bloque o aumentar el presupuesto después de revisar RAM disponible.

Como máximo hay dos tareas enviadas por worker. Dask puede reintentar dos veces una tarea fallida. Los umbrales de memoria son 60% objetivo, 70% derrame, 85% pausa y 95% reinicio. El dashboard Bokeh solo escucha en localhost y usa puerto automático. El clúster se cierra al terminar cada nivel; mensajes de retiro/cierre de workers pueden ser normales, consultar el estado de la etapa y del bloque.

## Caché y recuperación

- Cada etapa del flujo tiene una firma de sus funciones, parámetros relevantes y dependencias. Cambiar memoria/hilos no invalida cálculos. Cambiar D2 no obliga a repetir preparación o D1.
- La ejecución antigua conocida se adopta únicamente si coinciden hash de Gold, código anterior reconocido y parámetros analíticos. Se comprueban conteos de source, universe y D1, incluida unicidad. La adopción ocurre dentro del lock global, después de detener la ejecución anterior.
- Los bloques D2 dependen de una huella del Parquet estrecho de entrada, algoritmo, ventana, mínimo de comparables, jerarquía y particionamiento. No dependen del número de workers, memoria ni umbral de clasificación Z.
- Los archivos se escriben temporalmente y se publican mediante reemplazo atómico. Un bloque es válido cuando coinciden clave, esquema, filas esperadas y SHA256. Un resultado incompleto/corrupto se recalcula.
- La tabla final y los metadatos de la etapa se reconstruyen tras recuperar los bloques. Las etapas superiores verifican que exista su tabla; un cierre antes de publicar la etapa provoca su repetición, aprovechando los bloques ya confirmados.
- El snapshot estrecho se vuelve a exportar y verificar al entrar a D2; la recuperación conserva los cálculos estadísticos costosos. Los directorios incompletos de preparación se ignoran. Pueden ocupar disco hasta una limpieza posterior, que debe hacerse sin ejecución activa.
- Cambiar datos o reglas puede invalidar trabajo. No se promete reutilización entre snapshots distintos. Las versiones de modelo en AWS siguen sujetas a la protección contra cambios de definición.

## Progreso y estimación

`salida/alertas/<ejecucion>/progreso_d2.json` contiene nivel, bloques totales/completados/recuperados, fechas procesadas, workers, RAM/CPU, dashboard, heartbeat y tiempo restante estimado. Se actualiza al completar tareas y aproximadamente cada 10 segundos mientras espera. Cada worker registra fechas procesadas aproximadamente cada 5 segundos.

El porcentaje corresponde a fechas del nivel actual, no al tiempo total del ETL. La estimación se habilita tras dos bloques nuevos y usa trabajo ponderado por tamaño de entrada compacta y fechas objetivo; grupos desiguales y caché del sistema pueden cambiar la velocidad. Los niveles de fallback se conocen después de terminar el anterior: se indican como pendientes, sin inventar un tiempo global.

Un bloque con 100% de fechas puede estar todavía publicándose. Solo los bloques confirmados son recuperables. Un heartbeat viejo se señala, y el monitor oculta el enlace de un clúster cerrado/desactualizado. No abrir una segunda ejecución para consultar progreso.

## Verificación y límites

Se valida equivalencia frente a SQL original, incluidos días repetidos, pesos, año bisiesto, límites de ventana, MAD cero y jerarquía. También se verifica caché por etapas y un clúster real de dos procesos con recuperación de bloques y detección de corrupción. Las pruebas son sintéticas; la duración sobre los 4,35 millones de contratos debe medirse al ejecutar la nueva versión.

Fuentes: [Dask Futures](https://docs.dask.org/en/stable/futures.html), [memoria de workers](https://distributed.dask.org/en/stable/worker-memory.html), [ventanas DuckDB](https://duckdb.org/docs/stable/sql/functions/window_functions.html).


## Hallazgos globales por indicador

El flujo registra D1/D2/D3 al completar cada etapa: alertas, sin alerta, evaluados, no evaluables, cobertura, porcentaje de alertas entre evaluados y porcentaje en todo el universo. Son conteos de contratos; D3 no representa número de proveedores. Un indicador pendiente no se presenta como cero.

La clasificación final publica contratos únicos con alguna señal, suma de señales, clasificación conjunta, distribución de puntajes, coincidencias por pares/triple y las 27 posibles combinaciones conservando nulos. Las coincidencias por pares incluyen la triple; no deben sumarse entre sí. Cero evaluados produce porcentaje nulo, no 0%.

El resumen final agrupa las seis columnas necesarias en una sola consulta y deriva el resto de métricas de esa tabla pequeña. Se almacena en `ejecucion.json`; el monitor solo lee ese JSON al refrescar, por lo que no vuelve a escanear los contratos. Las estadísticas parciales se guardan por firma de indicador y versión del resumen, reutilizándose al retomar. Si cambia solo la implementación externa del resumen, el flujo actualiza las estadísticas sin recalcular indicadores.

El notebook 11 incluye tabla, barras de alertas y combinaciones. Los resultados y gráficos se conservan en el notebook al guardarlo; no se exportan archivos de EDA.
