# D3: acumulación de contratos al cierre del año

Modelo vigente: `alertas-cd-2.0`. Sustituye por completo el D3 de diversidad de categorías. D1 y D2 conservan sus reglas.

## Definición y alcance

Para cada entidad y año se calcula:

`participacion_diciembre = contratos firmados en diciembre / contratos firmados en todo el año`

Se cuenta cada ID una vez: el flujo exige IDs únicos antes de analizar. El universo es contratación directa de Gold; no toda la contratación pública. La fecha utilizada es la firma, no publicación ni pago. No se utiliza valor monetario: se observa acumulación temporal del número de contratos.

Se compara la proporción del año con la distribución de proporciones de años completos **anteriores de la misma entidad**. Se excluyen el propio año, los futuros, años con menos contratos que el mínimo y años parcialmente cubiertos por las fechas declaradas de inicio/corte.

## Parámetros iniciales (exploratorios, no límites legales)

| Parámetro | Valor | Propósito |
|---|---:|---|
| `d3_min_contracts` | 30 | Mínimo anual en año evaluado y años de referencia |
| `d3_min_years` | 3 | Mínimo de años anteriores elegibles por entidad |
| `d3_min_share` | 0.30 | Piso de la proporción de diciembre para reducir señales pequeñas |
| `d3_iqr_factor` | 1.5 | Factor del rango intercuartílico histórico |

`umbral_d3 = max(d3_min_share, Q3_historico + d3_iqr_factor * (Q3_historico - Q1_historico))`

La proporción debe superar estrictamente el umbral. No se recorta el umbral a 1: si supera 100%, el historial es demasiado disperso para generar una alerta con esa regla. Con IQR cero se conserva el piso y la comparación estricta; no hay división entre cero.

## Asignación a contratos y nulos

La unidad del indicador es entidad-año. Dentro de un año alertado, solo los contratos firmados en diciembre reciben `alerta_d3=1`; los demás reciben 0 si la entidad-año es evaluable. Identidad no fiable, firma ausente, año incompleto, menos de 30 contratos o historia insuficiente producen NULL. No se interpreta una ausencia de historia como ausencia de riesgo.

Con corte **2026-09-21**, todos los contratos de 2026 tienen D3 no evaluable. Puede evaluarse 2025 si la entidad cuenta con suficiente historia anterior. Es un análisis retrospectivo, conocido al terminar el año; no un predictor disponible en la fecha de firma.

Ejemplo: referencia anual 10%, 12%, 15%; umbral resultante con estos parámetros 30%. Si el año evaluado tiene 45% de contratos en diciembre, sus contratos de diciembre activan D3. Que ocurra puede deberse a razones presupuestales/administrativas legítimas y requiere revisión; no demuestra irregularidad.

## Arquitectura y caché

Una agregación reduce contratos a entidad-año. El join histórico trabaja sobre esa tabla pequeña y agrupa referencias por entidad-año. Un LEFT JOIN asigna el resultado a los contratos. No usa ventanas móviles por contrato ni comparaciones entre pares de contratos. La tabla resultante debe conservar el número y unicidad de IDs.

La función es `calculate_year_end_accumulation`. La caché invalida D3, score, clasificación y resumen cuando cambia esta definición. Preparación, D1 y bloques D2 compatibles permanecen reutilizables; no se debe borrar `salida/alertas`.

Campos principales: `d3_anio`, `n_contratos_entidad_anio`, `n_contratos_diciembre`, `participacion_diciembre`, `anio_completo_d3`, `n_anios_referencia_d3`, `primer_anio_referencia_d3`, `ultimo_anio_referencia_d3`, `q1_diciembre`, `q3_diciembre`, `iqr_diciembre`, `umbral_d3`, `motivo_d3`, `alerta_d3`.

`reference_cutoff` se conserva como parámetro de compatibilidad del esquema de configuración anterior; el nuevo D3 no lo utiliza. La referencia se determina por el año de cada contrato y los años anteriores completos.

## Ejecución y AWS

Interrumpir/terminar la ejecución anterior antes de reiniciar su kernel. Ejecutar notebook 09 con `MODO = "local"`; supervisar notebook 11 o `python scripts/monitor_alertas.py --watch --open-dashboard`. No se cambian los datos Gold.

El modelo 2.0 mantiene separada la historia de la versión 1.0. La carga AWS incorpora columnas faltantes de forma aditiva dentro de la transacción, conservando columnas y filas antiguas; los reportes deben filtrar por versión y corte. El cambio de esquema no se aplica hasta ejecutar una carga AWS. La configuración y pruebas locales no implican conexión ni validación remota.

Los conteos de D3 por contratos pueden ser altos en una entidad-año alertada porque reflejan todos sus contratos de diciembre. El resumen incluye además `d3_entidades_anio` para informar cuántas unidades anuales se evaluaron y alertaron, evitando confundir ambas cifras.


## EDA y lecturas optimizadas

D3 publica `d3_entidad_anio.parquet` como caché interna de la ejecución, con una fila por entidad-año y escritura atómica. El notebook 10 reutiliza esa tabla para explorar distribución, cobertura y umbrales, evitando reconstruirlos desde millones de contratos. Si falta, el flujo invalida la etapa D3 para regenerar el archivo; el EDA puede leer columnas calculadas del resultado final como alternativa.

El EDA añade un mapa mensual de años completos, scatter de proporción/umbral por entidad-año y asociaciones entre D1/D2/D3 con denominadores evaluables. Los gráficos de valor reutilizan una tabla temporal estrecha en vez de releer el Parquet en cada consulta. No se cambió el código de D2: sus bloques compatibles mantienen su identidad.
