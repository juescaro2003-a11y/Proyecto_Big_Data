# Resultados locales de ejecución

GitHub no incluye resultados, manifiestos de ejecución, checkpoints, bases DuckDB ni spill. Esta carpeta se llena al ejecutar el proyecto; no es una dependencia binaria que deba subirse.

- `alertas/indicador_v3/<huella>/`: lotes con resultados, red y manifiesto.
- `alertas/ejecucion_actual_v3.json`: referencia para monitor.
- `alertas/ultima_ejecucion_v3.json`: último lote completo para consumidores.
- Otros archivos/carpetas: auditoría o ejecuciones anteriores.

Los punteros se generan localmente. No distribuir un puntero de otra máquina como si fuera portable. No borrar un lock, `.wal`, checkpoint o spill sin confirmar que su ejecución está cerrada.

Ver [guía de operación](../docs/GUIA_OPERACION.md) y [datos requeridos](../data/README.md).
