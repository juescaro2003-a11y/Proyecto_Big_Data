# Indicador redefinido: alertas-cd-3.0

## Alcance aprobado

Se calculan C1, C2, C3, V1, V2, P1 y P2 desde el Gold existente. La red entidad–proveedor es análisis complementario y no modifica el puntaje. C4, T1, T2, T3 y P3 no se inventan, no se rellenan con cero y no entran a la fórmula. No se descarga información adicional ni se entrena con sanciones.

Es un indicador **descriptivo retrospectivo de priorización**, no una probabilidad de corrupción ni una predicción disponible al firmar. C2 utiliza la actividad del año observado hasta el corte y la validación monetaria usa mediana del tipo/año. No debe evaluarse como predictor temporal sin cambiar esas referencias.

## Ejecución

1. Activar el entorno `secop`. Las dependencias se gestionan con `environment.yml`; no hay instalaciones dentro del código.
2. Revisar `indicador_v3_config.json`. Corte e inicio de cobertura se toman de `auditoria_ircd.json` si la configuración no los especifica. No se infieren de la primera fecha observada.
3. Reiniciar el kernel y ejecutar **09_dimensiones_indicador.ipynb**.
4. Usar **11_monitor_alertas.ipynb** desde otro kernel mientras corre. El porcentaje es de etapas, no estimación de tiempo.
5. Al terminar, ejecutar **10_agregados_indicadores.ipynb** para explorar cobertura, señal, puntaje, motivos y red.
6. Abrir Docker Desktop y ejecutar **12_dashboard_grafana.ipynb** para publicar localmente y abrir los dos tableros v3. No hay que repetir Compose en terminal si las banderas de inicio están activadas.
7. Ejecutar **13_publicar_alertas_aws.ipynb** para publicar el resultado compacto y sus agregados. Su última celda demuestra la carga con una conexión nueva de solo lectura.

Alternativas desde la raíz:

```powershell
python scripts/run_indicador_v3.py
python scripts/publicar_alertas_aws.py
```

La segunda orden publica el último lote v3 completo; no recalcula. Los notebooks 10/12/13 no cambian silenciosamente al último lote v2 si falta v3.

## Señales y parámetros iniciales

| Señal | Definición | Regla inicial |
|---|---|---|
| C1 | Participación del proveedor en el número de contratos de la entidad, ventana de 365 días incluyendo el día observado | >0,40; entidad con >=10 contratos |
| C2 | HHI anual: suma de cuadrados de participaciones por proveedor, sobre número de contratos | >0,25; entidad-año con >=10 contratos |
| C3 | Participación de la entidad en los contratos del proveedor, 365 días | >0,80; proveedor con >=5 contratos |
| V1 | Valor alto frente a contratos del mismo tipo, causal y prefijo UNSPSC de 4 dígitos | Z robusto >3,5; >=30 referencias; percentil 95 si MAD=0 |
| V2 | Repetición de entidad–proveedor–tipo–prefijo UNSPSC de 4 dígitos en 30 días | >=3 contratos |
| P1 | Primera fecha observada del proveedor en Gold | Solo con 365 días de cobertura previa; todas sus firmas del primer día comparten la señal |
| P2 | Diversidad de segmentos UNSPSC de 2 dígitos del proveedor, 365 días | >=3 segmentos y >=5 contratos |

Estas reglas son hipótesis operativas configurables, no umbrales con validación externa demostrada. V2 no acredita fraccionamiento; P1 no demuestra creación reciente; P2 no demuestra actividades incompatibles. C3 mide dependencia en el universo observado, no en toda la actividad económica.

C1/C3 no excluyen importes inválidos porque usan conteos. Los denominadores conservan los contratos con contraparte no identificada; la fila sin identificadores no recibe señal. C2 no se evalúa cuando hay proveedores no identificados en entidad-año. P2 queda no evaluable si faltan categorías dentro de la ventana del proveedor. El año en curso puede ser parcial (`anio_completo=false`); no se presenta como un año cerrado. C2 requiere cobertura desde el inicio del año.

## Valores inválidos y V1

Cada contrato conserva el importe original y un `motivo_valor_invalido`, con esta prioridad exclusiva:

1. Nulo.
2. No finito.
3. No positivo.
4. Superior a `absolute_value_cap` (por defecto 10 billones COP).
5. Superior a `median_value_factor` (1.000) por la mediana del tipo/año.

La mediana del tipo/año usa valores finitos, positivos y que no superan el tope absoluto. Se calculan una vez los motivos; no se adivinan importes reales ni se elimina la fila. Los inválidos se excluyen de todas las referencias V1 y reciben V1=NULL.

**Cambio explícito respecto de D2:** V1 utiliza los **dos años calendario completos anteriores** al año del contrato, no los 24 meses móviles de v2. Se comparten medianas/MAD por grupo y año objetivo para reducir costo. No se emplean referencias del año objetivo, ni años cuya cobertura inicial/final sea incompleta. La regla de validación tipo/año sigue siendo retrospectiva.

Se calcula sobre logaritmos: `z = 0,67448975 × (log(valor) − mediana_log) / MAD_log`. La señal es unilateral para valores altos. Con MAD cero se compara con p95 del logaritmo, documentado en `metodo_v1`. Sin referencias suficientes, no se emite cero: V1 queda NULL con motivo.

## Fórmula y cobertura

```text
C = (C1 + C2 + C3) / 3
V = (V1 + V2) / 2
P = (P1 + P2) / 2
puntaje = (C + V + P) / 3
```

Las tres familias pesan igual. C1/C2/C3 pesan 1/9 cada una; V1/V2/P1/P2 pesan 1/6 cada una. No se mantiene artificialmente una cuarta familia temporal vacía.

La primera versión exige las siete señales evaluables. Si falta alguna, `puntaje=NULL`, `clasificacion=NO_EVALUABLE` y `alerta_binaria=NULL`. Se conservan cobertura y motivos. `puntaje_min` y `puntaje_max` acotan el valor posible sustituyendo desconocidos por cero/uno, sin interpretarlos como señales observadas. No se redistribuyen pesos según disponibilidad.

**Efecto importante:** proveedores nuevos o con pocos contratos pueden activar P1 pero no cumplir mínimos de C3/P2; por ello pueden quedar sin puntaje completo. Revisar cobertura antes de extraer conclusiones. La regla es conservadora, no se garantiza que mejore el porcentaje evaluable de v2. Cambiar esta política requiere una decisión metodológica explícita.

## Clasificación

Por trimestre de firma, se ordenan los puntajes evaluables y superiores a `minimum_priority_score` (0 por defecto). Se priorizan hasta `review_top_k_per_quarter` (2.000) contratos. Empates resueltos por ID para hacer la selección reproducible y respetar capacidad, no por evidencia adicional de riesgo.

- ALERTA / 1: seleccionado para revisión.
- NO_ALERTA / 0: evaluable, fuera del cupo o sin puntaje positivo; no significa inocuidad demostrada.
- NO_EVALUABLE / NULL: información insuficiente para el puntaje completo.

No es la regla de dos de tres. El umbral de selección puede cambiar por trimestre. Un promedio de señales binarias tiene valores discretos y empates, aunque esté acotado entre 0 y 1.

## Arquitectura, archivos y recuperación

| Archivo | Responsabilidad |
|---|---|
| `secop_pipeline/indicador_v3.py` | Configuración validada, preparación, ventanas, referencias robustas, red básica, puntaje y resumen |
| `flows/indicador_v3_flow.py` | Orquestación Prefect, locks compartidos, checkpoints, manifiesto y publicación local del resultado |
| `secop_pipeline/indicador_v3_publicacion.py` | Validación del lote, COPY compacto AWS, agregados y prueba de lectura |
| `grafana_secop/publicacion_v3.py` | Publicación local incremental, adopción de Gold existente, vistas y agregados para Grafana |
| `grafana_secop/generar_dashboards_v3.py` | JSON de tableros separados de contratos/procesos y regla de priorización v3 |
| `indicador_v3_config.json` | Umbrales, mínimos, ventanas, memoria y cupo |

DuckDB usa por defecto 2.048 MB y un hilo, con spill por lote. Las medianas de V1 se comparten por grupo/año. Las referencias se expanden a dos años objetivo, no se realizan cruces entre cada contrato y todos los demás. La red agrupa parejas antes de resumir grado y actividad. Dask permanece en el ETL original; este indicador no arranca workers ni nannies.

Cada etapa usa una transacción DuckDB. Solo se registra COMPLETADA después del commit. Al reejecutar se conservan etapas confirmadas para los mismos datos, código y parámetros; un cambio crea otro lote. Los resultados están bajo `salida/alertas/indicador_v3/<huella>/`, junto con red, manifiesto y base de trabajo. No se borran checkpoints automáticamente.

El monitor utiliza `ejecucion_actual_v3.json`; los consumidores usan `ultima_ejecucion_v3.json`, actualizado solo al completar el lote. Se conservan todos los contratos del Gold: inválidos monetarios y no evaluables no se borran.

## Publicación y preservación de v2

- **Grafana local:** esquema `secop_v3`, tableros `secop-contratos-v3` y `secop-procesos-v3`. Usa los Gold existentes; conserva todo el universo, con detalle paginado por SQL. Puede reutilizar/adoptar Gold publicado si coincide exactamente. Los procesos no reciben señales contractuales ni se suman con contratos.
- **AWS:** esquema `analytics_v3`; tabla `indicadores` con **11 columnas**: ID, siete señales, puntaje, clasificación, alerta. No se suben todos los campos del contrato. Parámetros/corte/huella se guardan una vez en `publicacion`; agregados globales en `clasificaciones` y `dimensiones`.
- Las tablas y los resultados **v2 se conservan**. Las nuevas publicaciones no alteran el esquema `analytics` ni el puntero `ultima_ejecucion.json`.
- Los tableros anteriores se identifican como **Histórico v2** y su regla de alerta queda pausada al cargar la configuración de Grafana. La regla v3 queda habilitada; antes de publicar datos v3 puede mostrar `NoData`.
- Dentro de AWS v3 se mantiene una fotografía vigente; una nueva carga sustituye esa fotografía en una transacción. El historial se mantiene en los lotes locales.
- Una carga repetida con la misma huella/conteo omite COPY. No editar manualmente las tablas publicadas. Las pruebas de conexión no se interpretan como pruebas de efectividad del indicador.

## Estado de entrega

Se prepararon código, configuración, notebooks y JSON de Grafana; se revisó la sintaxis Python y el formato de celdas/JSON. No se ejecutó el cálculo completo, no se inició Docker, no se publicó v3 en AWS ni se ejecutaron pruebas de integración en esta entrega. La salida histórica guardada de una prueba v2 no se presenta como evidencia de v3. La primera ejecución y sus conteos deben obtenerse con los notebooks en el orden indicado.
