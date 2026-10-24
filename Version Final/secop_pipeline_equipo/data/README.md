# Datos necesarios (no incluidos en GitHub)

[Inicio](../README.md) · [Entrega GitHub](../docs/ENTREGA_GITHUB.md)

Esta carpeta contiene datos locales pesados. Git solo incluye este documento y `.gitkeep` si existe; el código crea o requiere sus subdirectorios según la operación. No se entregan Bronze, Silver, Gold ni bases originales dentro del repositorio.

## Paquete mínimo para el recorrido desde Gold

Solicitar al responsable, por un medio de transferencia independiente:

1. `data/gold/contratos_control/contratos.parquet`.
2. `data/gold/contratos_control/auditoria_ircd.json` del mismo snapshot, con `parametros.cutoff` y `parametros.coverage_start`. Alternativamente esos valores deben declararse explícitamente en la configuración v3 con respaldo de cobertura.
3. Para el notebook 12 completo, los Parquet de una sola generación en `data/gold/procesos_modelo/`.

Conservar nombres y estructura. No mezclar particiones de generaciones distintas. Solicitar fecha de corte, cobertura, procedencia, número de filas, tamaños y SHA-256 del paquete; las fechas observadas en un Parquet no sustituyen la declaración de cobertura.

## Campos del Gold de contratos

Para el motor v3 se requieren: `id_contrato`, `entidad_id`, `proveedor_id`, `fecha_de_firma`, `tipo_de_contrato`, `causal`, `codigo_de_categoria_principal`, `valor_del_contrato`.

Para publicar el detalle en Grafana también se utilizan: `nombre_entidad`, `departamento`, `proveedor_adjudicado`, `objeto_del_contrato`, `urlproceso` y `fecha_corte`. Entregar el Gold completo compatible; no reducirlo solo a estas columnas antes de compararlo con un lote calculado: su huella de contenido cambiaría.

`id_contrato` debe ser único y no vacío. Fechas/valores deben tener tipos compatibles con el productor actual. No reemplazar fechas desconocidas por fechas inventadas ni ausencias por ceros. Las columnas pueden contener nulos según reglas del indicador, pero deben existir.

## Campos del Gold de procesos

`id_del_proceso`, `id_del_portafolio`, `fecha_de_publicacion_del`, `entidad`, `departamento_entidad`, `tipo_de_contrato`, `estado_del_procedimiento`, `precio_base`, `proveedores_invitados`, `respuestas_al_procedimiento`.

La publicación agrega su identificador interno de filas; no se exige que el ID de proceso sea único en el snapshot. No confundir ese universo con los contratos directos.

## Si no se recibe Gold

El ETL original puede descargar Bronze, construir Silver y Gold contractual mediante el notebook 05, configurando fechas y acceso a API. Esa ruta no asegura reproducir el snapshot histórico exacto, porque los registros de origen pueden actualizarse. Tampoco reconstruye automáticamente el Gold de procesos utilizado por el tablero actual. Para ese tablero es necesario recibir un snapshot compatible o desarrollar explícitamente su productor.

No es necesario recibir resultados v3: se generan ejecutando 09. Si se transfiere un lote ya calculado, conservar su `ejecucion.json`, sus Parquet y el Gold exacto utilizado; los punteros locales pueden contener rutas absolutas de otro computador. El recorrido recomendado para incorporarse es recalcular 09 con los Gold entregados.
