> **Versión activa del indicador: alertas-cd-3.0.** Ejecuta notebooks 09 → 10 → 12 → 13; 11 monitorea. La guía vigente es `docs/INDICADOR_V3.md`: siete señales C1/C2/C3/V1/V2/P1/P2, puntaje por tres familias, top K trimestral, AWS compacto de 11 columnas en `analytics_v3`. Los apartados anteriores sobre D1/D2/D3 y siete columnas describen v2, conservada para comparación. No se debe presentar una prueba histórica v2 como evidencia de carga v3.

# Ejecutar Grafana local y publicar alertas en AWS

## 1. Entorno

Desde una terminal Anaconda en la raíz del proyecto:

```powershell
conda env update -n secop -f environment.yml
conda activate secop
jupyter lab
```

La instalación se hace exclusivamente en esta preparación del entorno. No hay `pip install` dentro de los notebooks ni los scripts. La publicación AWS usa psycopg2; la publicación local mantiene su controlador psycopg 3.

## 2. Grafana local — notebook 12

1. Abre Docker Desktop y espera a que el motor esté disponible.
2. Abre `notebooks/12_dashboard_grafana.ipynb`, selecciona el entorno secop y reinicia el kernel.
3. Revisa las fuentes en `grafana_secop/publicacion.json`: Gold de contratos, Gold de procesos y versión del modelo permitida. El notebook permite seleccionar `RESULTADOS` de un lote terminado.
4. Ejecuta las celdas en orden. `PUBLICAR=True` prepara/publica los datos; `INICIAR_POSTGRES_ETL=True` y `INICIAR_GRAFANA=True` ejecutan Compose desde el notebook. Si administras PostgreSQL fuera de Docker, desactiva solamente el primero.
5. Abre http://localhost:3001. Usuario administrador: `admin`; contraseña: la configurada en `grafana_secop/.env`. No se imprime ni se cambia una contraseña existente.
6. Revisa los dashboards separados de contratos y procesos, el panel de trazabilidad y los conteos finales del notebook.

El notebook trabaja desde las versiones Gold existentes. No ejecuta notebooks de extracción, Silver o cálculo del indicador. Se conservan todas las filas; la consulta de detalle usa páginas de 200 para evitar enviar millones de filas al navegador. El filtro de año afecta a los gráficos; clasificación e ID afectan al detalle. Las fechas de procesos se muestran separadas del corte de alertas.

La primera publicación puede tardar por la validación y transferencia. Las siguientes omiten productos cuyo contenido no cambió. Para abrir el dashboard anterior sin publicación usa `PUBLICAR=False`. No se debe publicar mientras otra ejecución mantiene los locks del pipeline.

## 3. AWS — notebook 13

La conexión está definida en `.env.aws`, archivo local excluido de Git. Destino: `proyectobd.c10pdetlifr0.us-east-1.rds.amazonaws.com:5432`, base `basedatosproyectobd`. Las credenciales no forman parte del notebook. Se exige SSL.

1. Abre `notebooks/13_publicar_alertas_aws.ipynb` con el entorno secop.
2. Selecciona el `resultados.parquet` terminado. Por defecto se toma el último lote registrado.
3. La primera celda verifica conexión, estado COMPLETADA, claves únicas, corte, versión y clasificaciones frente al manifiesto.
4. Con `PUBLICAR=True`, ejecuta la publicación y espera al resultado COMMIT. COPY terminado todavía no significa publicación terminada: siguen integración, validación y agregados.
5. Consulta las tablas globales de la última celda. Conservan NO_EVALUABLE por separado de ausencia de señal.

Alternativa desde la raíz, sin recalcular el modelo:

```powershell
python scripts/publicar_alertas_aws.py --solo-conexion
python scripts/publicar_alertas_aws.py
```

Puedes indicar otro lote con `--resultados "ruta\resultados.parquet"`.

## 4. Publicación AWS compacta (actualizada)

Solo se transfieren estas siete columnas a `analytics.alertas_resumen`:

| Columna | Significado |
|---|---|
| id_contrato | Identificador para conectar con Gold |
| d1_concentracion | Señal D1: 1, 0 o NULL |
| d2_valor_atipico | Señal D2: 1, 0 o NULL |
| alerta_d3 | Señal D3: 1, 0 o NULL |
| score_total | Número de señales presentes |
| clasificacion | ALERTA, NO_ALERTA o NO_EVALUABLE |
| alerta_binaria | 1=sí, 0=no, NULL=no evaluable |

No se suben nombres, objetos, importes, fechas individuales, datos de entidades/proveedores ni parámetros repetidos por contrato. El código proyecta únicamente estas siete columnas desde Parquet. Gold y Grafana local conservan su información completa.

`analytics.publicacion_resumen` guarda una sola fila con corte, versión, huella del archivo, reglas, cantidad y fecha de publicación. `analytics.alertas_resumen` contiene una única fotografía vigente: una nueva publicación sustituye atómicamente la anterior. El historial completo sigue disponible en los archivos locales, no se acumula en esta tabla compacta.

Los agregados globales se publican en:

- `analytics.resumen_clasificaciones`: contratos y porcentaje por clasificación.
- `analytics.resumen_dimensiones`: total, evaluados, señales, sin señal, no evaluables, porcentaje de señales entre evaluados y cobertura.

No se generan agregados por año o entidad en este modo, puesto que esos campos no se suben. NULL se conserva como no evaluable, nunca como ausencia de riesgo. Denominador cero produce NULL.

La carga usa psycopg2 con COPY por lotes de 50.000 filas. Se validan conteos, unicidad, coherencia del puntaje y clasificación antes de confirmar la transacción. Un fallo antes del COMMIT revierte la publicación. Repetir exactamente el mismo archivo con huella/conteo coincidentes omite COPY y actualiza agregados; no se deben editar manualmente las tablas publicadas.

Grafana local permanece conectado al PostgreSQL local. Publicar en AWS no cambia esa fuente ni recalcula D1/D2/D3.
