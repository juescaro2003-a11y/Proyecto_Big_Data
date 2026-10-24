# Resultados del análisis — indicador v3

[Inicio](../README.md) · [Perspectiva de negocio](PERSPECTIVA_NEGOCIO.md) · [Metodología](INDICADOR_V3.md)

Documento elaborado el **23 de octubre de 2026** mediante lectura del último lote v3 completo disponible. Es una fotografía de ese lote; no se actualiza automáticamente al ejecutar nuevos análisis. No se recalculó el indicador ni se publicaron datos en AWS/Grafana para elaborar este informe.

## 1. Fuente y trazabilidad

- Modelo: **alertas-cd-3.0**; estado del manifiesto: **COMPLETADA**.
- Lote: `f8045bc1b518250ab57fc739`.
- Ruta relativa local: `salida/alertas/indicador_v3/f8045bc1b518250ab57fc739/resultados.parquet`.
- Inicio de cobertura declarado: **2017-01-01**; corte declarado: **2026-09-21**.
- Fechas de firma observadas: **2017-09-12 a 2026-09-21**. No sustituyen la declaración de cobertura.
- Contratos e IDs distintos: **4.353.720 / 4.353.720**.
- SHA-256 del resultado, comprobado contra el manifiesto: `916d3b2d7acd6cd853f45de1547e5246a016cb7b60c615e243d05158a315b810`.

El análisis corresponde al universo contractual Gold, no al universo separado del tablero de procesos. El Parquet no se incorpora a GitHub; este documento sí contiene resultados agregados. No se consultó AWS ni se certifica aquí su sincronización.

## 2. Resumen ejecutivo

- **42.587 contratos (0,98 %)** quedaron priorizados para revisión.
- **228.671 (5,25 %)** tienen las siete señales evaluables.
- **4.125.049 (94,75 %)** no tienen puntaje final. La limitación dominante es cobertura de C3/P2.
- La etiqueta ALERTA depende del puntaje y del cupo trimestral; no mide prevalencia de corrupción.
- La mayoría de contratos con primera aparición del proveedor (P1) queda fuera del puntaje completo por falta de otras señales.

## 3. Clasificación final

| Clasificación | Contratos | % del universo |
|---|---:|---:|
| ALERTA | 42.587 | 0,98 % |
| NO_ALERTA | 186.084 | 4,27 % |
| NO_EVALUABLE | 4.125.049 | 94,75 % |

Entre los contratos evaluables, la proporción priorizada es **18,62 %**. Este denominador es diferente del universo completo.

**Conclusión:** NO_EVALUABLE no significa NO_ALERTA. La regla de exigir siete señales limita fuertemente el alcance de la bandeja. No debe concluirse que el resto de contratos carece de señales o de problemas.

## 4. Resultados por señal

Se diferencia la cobertura (evaluados/universo) de la frecuencia de señal (señal/evaluados). Los conteos corresponden a contratos, no a entidades o proveedores distintos.

| Señal | Evaluados | Cobertura | Señal = 1 | % señal entre evaluados | No evaluables |
|---|---:|---:|---:|---:|---:|
| C1 | 4.330.105 | 99,46 % | 206 | 0,00 % | 23.615 |
| C2 | 4.343.765 | 99,77 % | 369 | 0,01 % | 9.955 |
| C3 | 262.261 | 6,02 % | 69.082 | 26,34 % | 4.091.459 |
| V1 | 4.246.400 | 97,53 % | 13.340 | 0,31 % | 107.320 |
| V2 | 4.332.972 | 99,52 % | 13.057 | 0,30 % | 20.748 |
| P1 | 4.349.673 | 99,91 % | 1.024.545 | 23,55 % | 4.047 |
| P2 | 248.202 | 5,70 % | 57.264 | 23,07 % | 4.105.518 |

### Interpretación

- **C1/C2:** pocas activaciones bajo estos umbrales y este universo. Esto no demuestra competencia suficiente ni ausencia de favoritismo; se trabaja por conteos de contratos.
- **C3:** la dependencia se interpreta solo para proveedores que cumplen el mínimo y la cobertura; no describe a todos los proveedores.
- **V1:** identifica importes altos respecto de referencias históricas comparables, después de excluir valores no utilizables. No acredita sobrecostos.
- **V2:** repetición de contratos similares en una ventana corta; requiere contexto documental antes de hablar de fraccionamiento.
- **P1:** primera aparición observada; no es fecha de creación de la empresa.
- **P2:** diversidad de segmentos; su frecuencia entre evaluados no puede extrapolarse a los contratos sin evaluación.

No sumar las activaciones de señales para contar contratos en ALERTA: un contrato puede activar varias y la clasificación incorpora cobertura y cupo.

## 5. Por qué faltan evaluaciones en C3 y P2

| Señal | Motivo registrado | Contratos | % del universo |
|---|---|---:|---:|
| C3 | `pocos_contratos_proveedor` | 4.087.412 | 93,88 % |
| C3 | `historia_insuficiente` | 4.047 | 0,09 % |
| P2 | `pocos_contratos_proveedor` | 4.059.583 | 93,24 % |
| P2 | `categorias_incompletas_historia` | 21.140 | 0,49 % |
| P2 | `sin_categoria` | 20.748 | 0,48 % |
| P2 | `historia_insuficiente` | 4.047 | 0,09 % |

Cada contrato recibe un motivo por señal según el orden de las reglas; puede tener otras limitaciones simultáneas que no sean el primer motivo registrado. No sumar motivos entre C3 y P2 como filas únicas.

**Conclusión:** la falta de cinco contratos del proveedor en la ventana es el principal motivo registrado. Cambiar ese mínimo podría ampliar cobertura, pero también reducir estabilidad del indicador. No se modifica aquí: requiere una decisión metodológica y comparación de sensibilidad.

### Primera aparición: señal que casi no llega al puntaje completo

| Clasificación de contratos con P1 = 1 | Contratos |
|---|---:|
| NO_EVALUABLE | 1.024.490 |
| ALERTA | 55 |

**Conclusión:** P1 se calcula, pero su utilidad dentro de la bandeja final está limitada por la exigencia de las otras seis señales. Conviene revisar esas primeras apariciones en un análisis complementario; no convertirlas automáticamente en ALERTA.

## 6. Calidad monetaria

| Motivo de valor inválido | Contratos | % del universo |
|---|---:|---:|
| `no_positivo` | 35.664 | 0,82 % |
| `supera_factor_mediana` | 411 | 0,01 % |
| `supera_tope_absoluto` | 23 | 0,00 % |

Total: **36.098 valores marcados (0,83 %)**. El motivo es exclusivo según prioridad: nulo, no finito, no positivo, tope absoluto y factor de mediana. La ausencia de otros motivos en esta tabla indica conteo cero en el resumen comprobado.

Se aplicaron tope de 10 billones COP y factor 1.000 sobre mediana de tipo/año. Los contratos permanecen; V1 queda NULL. Estas marcas no permiten adivinar el importe real ni demostrar un error de digitación.

**Conclusión:** no reportar como valor económico confiable una suma que incluya estos importes. Este documento no presenta una suma monetaria global ni afirma que todos los valores restantes sean correctos.

## 7. Distribución temporal

| Año de firma | Contratos | Evaluables | Cobertura | ALERTA |
|---|---:|---:|---:|---:|
| 2017 | 4.047 | 0 | 0,00 % | 0 |
| 2018 | 92.173 | 228 | 0,25 % | 127 |
| 2019 | 85.664 | 327 | 0,38 % | 160 |
| 2020 | 273.892 | 1.618 | 0,59 % | 1.022 |
| 2021 | 451.912 | 12.419 | 2,75 % | 6.184 |
| 2022 | 566.007 | 22.815 | 4,03 % | 6.307 |
| 2023 | 651.894 | 30.545 | 4,69 % | 8.000 |
| 2024 | 734.821 | 34.311 | 4,67 % | 8.000 |
| 2025 | 802.117 | 67.781 | 8,45 % | 8.000 |
| 2026 | 691.193 | 58.627 | 8,48 % | 4.787 |

**Interpretación:** los 8.000 priorizados por año en 2023–2025 coinciden con el máximo de cuatro cupos trimestrales de 2.000. Esa cifra refleja capacidad de selección, no estabilidad demostrada del riesgo. 2026 es parcial al 21 de septiembre y no debe compararse directamente con un año completo. El inicio de la serie dispone de menos historia y referencias.

## 8. Qué hacer con estos hallazgos

1. Revisar el detalle y los motivos de los 42.587 priorizados, con el notebook 10 o el tablero contractual.
2. Presentar siempre cobertura junto al conteo de alertas: el puntaje completo representa solo una fracción del universo.
3. Evaluar metodológicamente la cobertura de C3/P2 y el efecto sobre P1 antes de ampliar el uso operativo.
4. Mantener calidad monetaria como diagnóstico separado del riesgo; revisar fuentes/documentos cuando un valor sea implausible.
5. Comparar periodos solo con versión, corte, cobertura y cupo explícitos.
6. Obtener revisión experta y evidencia externa antes de atribuir precisión, falsos positivos o capacidad predictiva al indicador.

## 9. Alcance y actualización

Se leyeron el manifiesto y el Parquet; se verificaron huella, IDs distintos, clasificaciones y conteos de las siete señales. Se consultaron motivos, años y clasificación de P1 con DuckDB limitado a 1 GB y un hilo. No se entrenaron modelos, no se alteraron reglas y no se modificaron bases.

Los resultados no constituyen evidencia de irregularidad. Son retrospectivos y dependen del universo disponible, umbrales, mínimos y política de evaluación completa. La red complementaria no entra al puntaje; sus métricas no se interpretan como sanciones o vínculos ilícitos.

Después de una nueva ejecución, actualizar este documento con el nuevo lote y mantener identificada la fotografía anterior si se necesita comparación. No cambiar solo la fecha del documento: recalcular sus tablas desde el resultado correspondiente.
