# Arquitectura de alertas con memoria acotada

## Recursos por fase

| Fase | Procesos/hilos | Presupuesto |
|---|---|---|
| Bloques D2 | 2 workers, 1 hilo cada uno | 2 GiB por worker; 4 GiB combinados |
| Coordinador durante referencias | DuckDB, 1 hilo | 512 MB |
| Integración después de cerrar workers | DuckDB, nueva conexión, 1 hilo | 3072 MB |

La configuración Dask exige además una reserva de 2 GiB disponibles al iniciar.
El presupuesto no limita toda la memoria de Python, NumPy o bibliotecas nativas.
Dos tareas por worker pueden estar enviadas; cada worker tiene un hilo de ejecución.
Se mantienen los umbrales de memoria: target 60%, spill 70%, pausa 85%, reinicio 95%.
Los arrays temporales de una tarea activa no se vuelcan automáticamente a disco por Dask.

## Integración D2

Las tablas de referencia se materializan en analitica.duckdb. Al terminar su
construcción se cierra la conexión: sus tablas temporales no pasan a integración.
Una conexión nueva restablece memoria, hilos, preserve_insertion_order=false y spill.
Se filtran las referencias que cumplen el mínimo de comparables; se calcula
min(prioridad) por id_contrato usando únicamente esas dos columnas. Después se
unen las claves con la fila de referencia completa. Se comprueba unicidad de ID.
Así se evita la ventana row_number sobre todas las columnas, conservando la
preferencia de referencias y la regla estadística. Los contratos sin referencia
siguen recuperándose por LEFT JOIN desde universe y quedan no evaluables.

## Recuperación

No se modificó alertas_dask.py ni el tamaño de los bloques: las claves y el hash
del motor siguen siendo iguales. Se conservan los checkpoints que superen la
validación habitual. La integración de D2 sí se recalcula por cambio de código.
Se reconstruyen las referencias desde los bloques recuperados cuando corresponda.
Mantener LOTE y reiniciar el kernel antes de reanudar notebook 09.

## Límites

Esta configuración prioriza margen de memoria; no garantiza mayor velocidad.
No se ejecutó el flujo ni una prueba de carga al aplicar este cambio. Si la RAM
disponible es menor a 6 GiB, la configuración actual rechaza iniciar los workers.
Los temporales siguen en la carpeta spill del lote; necesitan espacio libre.

## Fuentes

- https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads
- https://duckdb.org/docs/current/sql/functions/window_functions
- https://distributed.dask.org/en/stable/worker-memory.html
