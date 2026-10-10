# Alertas posteriores a Gold: ejecución

## Qué se implementó

Flujo separado `SECOP-II-ALERTAS-POST-GOLD`, definido en `flows/alertas_flow.py`.
Lee `data/gold/contratos_control/contratos.parquet` y escribe únicamente en
`salida/alertas/` y, si se solicita, en `analytics` de AWS RDS. No llama al ETL
anterior ni reconstruye Bronze, Silver o Gold.

Las reglas nuevas no reutilizan los antiguos scores IRCD:

- **D1:** participación del proveedor en el valor de su entidad, ventana inclusiva
  `[firma−365 días, firma]`. Incluye el contrato y los contratos de ese día porque
  no existe un orden intradiario fiable. Exige 10 contratos, 3 proveedores y 95%
  de cobertura del valor. Valores inválidos en el denominador impiden evaluar.
- **D2:** logaritmo del valor, mediana y MAD exactos de los 24 meses anteriores,
  excluyendo todo el día de firma para evitar incluir el propio contrato o pares
  cuyo orden no se conoce. Primero categoría UNSPSC de 8 dígitos + tipo + causal;
  después familia de 4 dígitos manteniendo tipo y causal. Mínimo 30 antecedentes.
  La ampliación es una aproximación configurable de comparabilidad, no una
  validación económica. Use `d2_prefixes: [8]` si no acepta ampliar a familia.
  MAD cero usa comparación estricta contra P95 de log(valor), método identificado
  como `P95_LOG_MAD_CERO`; no inventa un Z. Mediana/MAD se almacenan **en escala log**.
- **D3 (modelo 2.0):** proporción de contratos firmados en diciembre por entidad-año.
  Compara con años completos anteriores de la misma entidad (al menos 3), cada uno con
  al menos 30 contratos. Umbral exploratorio: máximo entre 30% y Q3 + 1,5 IQR histórico.
  Exige superar estrictamente el umbral. Solo los contratos de diciembre reciben la
  señal del año alertado; los de otros meses reciben 0 si el año es evaluable.
  Un año incompleto, identidad inválida o historia insuficiente producen NULL.
  Con corte 2026-09-21, 2026 es NO_EVALUABLE para D3. Consultar `D3_CIERRE_ANUAL.md`.
- **Score:** suma observada de binarias, con faltantes explícitos. ALERTA si
  score_min ≥2; NO_ALERTA si score_max <2; NO_EVALUABLE en otro caso.
  Un nivel incompleto lleva la etiqueta `provisional`.

### Límites de interpretación que se conservan

Gold es un snapshot actual, con contratos ya seleccionados por el ETL anterior.
No permite recuperar variantes excluidas ni reconstruir los atributos conocidos
históricamente en cada firma. D1/D2 excluyen firmas futuras, pero eso no acredita
una reconstrucción histórica as-of. D3 es retrospectivo: utiliza el total del año evaluado y solo se conoce al cierre de ese año.
La referencia excluye ese mismo año y todos los posteriores. No debe usarse como predictor disponible en la fecha de firma. El total reportado no está acreditado como inicial.

UNSPSC admite exactamente ocho dígitos o el prefijo conocido `v1.` seguido de ocho
dígitos. Se retira solo ese prefijo, se conservan originales y se rechazan códigos
ambiguos y ceros. La comprobación es sintáctica; sin catálogo oficial de códigos
no se acredita existencia semántica de cada código. No se rellena con ceros.

Se usan códigos de entidad/proveedor como claves, nunca nombres. Los conflictos
observados entre un código y varios documentos normalizados se marcan como
identidad no fiable; no se mezclan silenciosamente. Contratos sin firma siguen
en cobertura pero no aportan a referencias ni se puntúan. Los duplicados de ID
provocan error para revisar su significado; no se suman versiones.

## 1. Preparar el entorno

En una terminal situada en la raíz del proyecto:

```powershell
conda activate secop
python -m pip install -r requirements.txt
```

Se reutilizan DuckDB, PyArrow, pandas, Prefect, SQLAlchemy 2.x y psycopg ya declarados.
No hace falta otro clúster Dask para esta etapa: DuckDB ejecuta ventanas SQL con
memoria limitada y temporales en disco. Configuración inicial: 3.072 MB y un hilo.
Se requiere espacio para la base analítica de trabajo, referencias y Parquet final.

## 2. Revisar parámetros

Editar `alertas_config.json`. `cutoff: null` y `coverage_start: null` heredan los
valores declarados en la auditoría del Gold existente. No usa la fecha actual
como si la fuente estuviera actualizada. `reference_cutoff: null` usa el día
anterior al corte. Un corte mayor que el snapshot Gold se rechaza.

Umbrales iniciales: D1 0,40; D2 3,5; D3 1,5 IQR; score 2. Puede ajustar ventanas,
mínimos y cobertura. Al cambiar reglas para publicar, cambie también `version`
(por ejemplo `alertas-cd-1.1`): AWS rechaza reutilizar una versión registrada con
otra definición. Las fechas de corte y parámetros exactos se guardan por fila.

## 3. Primera ejecución local, sin AWS

```powershell
python scripts/run_alertas.py --local
```

O usar `notebooks/09_dimensiones_indicador.ipynb`, modo `local`. Esto calcula las
dimensiones y valida resultados. No modifica el PostgreSQL local ni AWS.

Para 27 escenarios de umbrales, agregue `--escenarios`. D2 con MAD cero conserva
su método P95 al variar el umbral Z. Los escenarios muestran coberturas, alertas,
coincidencias y clasificación; no calculan precisión, recall o F1 sin etiquetas.

```powershell
python scripts/run_alertas.py --local --escenarios
```

## 4. Configurar AWS sin exponer la contraseña

Añada los campos de `.env.example` a su `.env` local y complete
`SECOP_RDS_PASSWORD`. Alternativamente puede definirla en el entorno de la sesión.
No pegar contraseñas en chats, celdas, comandos guardados ni archivos versionados.

El destino predeterminado es `secop.ccw10v99js6c.us-east-1.rds.amazonaws.com`,
base `postgres`, usuario `Secop`, puerto 5432. Compruebe que la red/security group
permita su conexión y que el usuario pueda crear objetos en `analytics`.

Se exige SSL. `require` cifra la conexión; para comprobar también la identidad
del servidor configure `SECOP_RDS_SSLMODE=verify-full` y
`SECOP_RDS_SSLROOTCERT` con la ruta del certificado CA de RDS.

```powershell
python scripts/run_alertas.py --check-aws
```

Este comando ejecuta SELECT 1 y comprueba SSL en la sesión. No escribe resultados.
Si falta la contraseña o falla red/autenticación, muestra el error real. El flujo
que solicita AWS comprueba esto antes de empezar cálculos largos.

## 5. Publicar resultados ya calculados

```powershell
python scripts/run_alertas.py --solo-carga
```

Busca resultados validados de la misma fuente, código y parámetros. No recalcula
dimensiones. Si no existen, pide primero `--local`.

Para calcular/reutilizar y publicar en una sola llamada:

```powershell
python scripts/run_alertas.py --aws
```

La conexión y la transacción se administran con SQLAlchemy. Dentro de esa misma
transacción se usa el controlador psycopg para COPY por lotes de 10.000 filas a
staging. Después se publica con INSERT ON CONFLICT DO UPDATE. No hay replace ni
borrado de históricos. Clave única: contrato + corte + versión. La creación de
tabla se obtiene del esquema tipado del resultado; `sql/01_alertas_schema.sql`
documenta ese esquema. No es necesario ejecutar el SQL manualmente.

Se comparan todas las columnas entre staging y destino antes de COMMIT. Si el
corte/versión existente tiene filas adicionales ausentes del nuevo resultado,
se rechaza la operación; no se borra historia para forzar coincidencia. Use una
nueva versión cuando el universo cambie de esa forma. Un reintento sobre el mismo
resultado actualiza sus claves sin añadir duplicados. Reintentos automáticos solo
ante errores operativos transitorios; otros errores se propagan a Prefect.

## 6. Resultados y seguimiento

```text
salida/alertas/
  ultima_ejecucion.json
  <huella_de_fuente_codigo_y_parametros>/
    analitica.duckdb
    resultados.parquet
    ejecucion.json
    spill/                       temporales cuando se necesiten
```

La caché usa firmas por etapa y dependencias. El cambio de D3 conserva D1/D2 compatibles y recalcula D3, score y clasificación. La huella de Gold se vuelve a comprobar
antes de publicar. El flujo tiene bloqueo propio para evitar ejecuciones analíticas
simultáneas; no se debe modificar Gold mientras lo lee.

`ejecucion.json` conserva tiempos, estados, conteos, calidad, referencia D3 y
acumulación de contratos al cierre del año. Los resultados preservan
campos originales seleccionados y columnas normalizadas separadas, motivos y
estados de no evaluación, referencia temporal, parámetros y procedencia.

Al terminar con AWS se ejecutan tres consultas reales, filtradas por corte **y
versión actual**: primeras diez filas, clasificación y alertas por dimensión.
Se imprimen como tablas y se devuelven como DataFrames en `resultado['consultas_aws']`.
No confundir primeras filas locales con resultados remotos. `aws=NO_SOLICITADA`
en modo local significa que no se ha verificado persistencia remota.

No usar el notebook 05 para lanzar esta etapa: el 05 corresponde al ETL anterior.
Reiniciar el kernel después de cambiar módulos de Python.

## 7. Pruebas

```powershell
python -m unittest discover -s tests -p test_alertas.py -v
```

Prueban fórmulas, nulos, todas las 27 combinaciones de clasificación, exclusión de
fechas futuras, MAD de la ventana correcta, alternativa P95, jerarquía, códigos
UNSPSC, referencias por proveedor, duplicados, conservación e inmutabilidad de
Gold sintético. Son pruebas de lógica; no sustituyen validación experta.

## 8. Programación futura

El flujo está disponible como `flows/alertas_flow.py:alertas_flow`. Se puede
registrar como deployment/serve en Prefect con parámetros y entorno del proyecto.
No se ha creado un calendario. El proceso que ejecute el deployment necesita
acceso a Gold, disco temporal, credenciales por entorno y conectividad con RDS.

## 9. Archivos añadidos

- `secop_pipeline/alertas.py`: cálculos y validación local.
- `secop_pipeline/alertas_aws.py`: conexión, tabla, COPY, UPSERT y consultas.
- `flows/alertas_flow.py`: tareas, caché y trazabilidad.
- `scripts/run_alertas.py`: ejecución desde consola.
- `alertas_config.json`: parámetros.
- `notebooks/09_dimensiones_indicador.ipynb`: ejecución interactiva.
- `sql/01_alertas_schema.sql` y `sql/02_validar_alertas.sql`: esquema y consultas.
- `tests/test_alertas.py`: pruebas sintéticas.

## Referencias de implementación

- [Ventanas analíticas de DuckDB](https://duckdb.org/docs/stable/sql/functions/window_functions.html).
- [Guía de señales de Open Contracting Partnership](https://www.open-contracting.org/resources/red-flags-in-public-procurement-a-guide-to-using-data-to-detect-and-mitigate-risks/).

Las reglas D1/D3 son adaptaciones de las indicadas por el usuario, no una
certificación de conformidad exacta con R040/R048. Las señales requieren revisión.

## Verificación de la implementación — 8 de octubre de 2026

- 17 pruebas automatizadas aprobadas, incluidas las 27 combinaciones de binarias
  y nulos, referencias históricas, MAD, fallback P95, conteos D3 y caché.
- Flujo Prefect real completado con 9 contratos sintéticos, en servidor local de
  pruebas aislado; ninguna llamada al ETL anterior ni publicación AWS.
- Sintaxis de módulos y celdas del notebook comprobada.
- `--check-aws` se detuvo con el error real `Falta SECOP_RDS_PASSWORD`. No se
  llegó a abrir conexión, ejecutar SELECT 1 ni publicar registros en RDS.
- No se ejecutó el nuevo análisis sobre los 4,35 millones de contratos. El
  rendimiento, las distribuciones reales y la persistencia remota quedan para
  la ejecución del usuario; no se presentan resultados sintéticos como reales.

Si una ejecución reutiliza la caché, `prefect_flow_run_id` de las filas identifica
la ejecución que calculó el resultado. El manifiesto identifica además la nueva
ejecución que lo reutilizó/publicó. Son dos eventos distintos.


## Reinicio desde Gold con recuperación de fallos

El notebook 09 queda preparado con `MODO = "local"` y
`LOTE = "desde_gold_v2_20261009"`, pasado al flujo como `cache_namespace`.
La primera ejecución de ese lote recalcula todo desde el Parquet Gold existente,
incluido D3 de diciembre, sin adoptar los resultados antiguos. Los resultados y
checkpoints quedan bajo `salida/alertas/lote_desde_gold_v2_20261009/`.
El monitor 11 sigue usando `salida/alertas/ultima_ejecucion.json`.

Reinicie el kernel del notebook 09 tras detener cualquier ejecución anterior y
ejecute sus celdas en orden. Si falla, conserve el nombre del lote para recuperar
sus checkpoints. Cambiarlo inicia otro cálculo completo y requiere espacio adicional.
Para publicar posteriormente, use `MODO = "solo_carga"` con el mismo lote.
Sin `cache_namespace`, otros consumidores conservan el comportamiento anterior.
El lock sigue siendo global: no se permiten cálculos simultáneos entre lotes.
