# Validación de la refactorización

Se ejecutaron **9 pruebas**, todas correctas, en PostgreSQL 16 temporal, puerto 55439, sin volúmenes de producción. La última ejecución de la suite duró **3,818 segundos**. Las pruebas usan cinco contratos y un caso de paginación de 451; no son mediciones de rendimiento con millones de filas.

| Componente | Estado anterior | Ajuste realizado | Resultado de validación |
|---|---|---|---|
| Gold | Procesos de legado tratados como actuales | Contratos desde Gold; procesos deshabilitados o históricos explícitos | Huella distinta rechazada; histórico conserva sus tres filas de prueba |
| Integración de alertas | JOIN por 22 columnas | ID único, procedencia por SHA y validación | Cinco contratos conservados, uno SIN_RESULTADO; dimensiones y clasificaciones coinciden |
| PostgreSQL | Instancia independiente obligatoria | Existente preferida; modo aislado opcional | Adopción de Gold existente transfiere solo las cuatro alertas; reemplazo de tabla compatible |
| Carga | Recarga y CSV completo | COPY por lotes; registro de contenido | Segunda ejecución transfiere cero filas; fallo simulado mantiene corte anterior |
| Agregados | Resúmenes recalculados indiscriminadamente | Dos vistas materializadas por dependencia | Cambio solo de procesos transfiere tres filas; conserva agregado contractual |
| Grafana | Detalle sin límites | SQL paginada, búsqueda y filtros escapados | Todas las consultas SQL ejecutadas; 451 IDs recorridos sin pérdida ni duplicación |

Pruebas adicionales dentro de la suite: lock ajeno conservado; ejecución incompleta e IDs duplicados rechazados; cambio de bytes detectado aunque se conserve mtime; permisos de lectura sin DELETE; timeout de advisory lock; filtros Año/ID/página y texto con comillas; plan de detalle sin Function Scan.

`validacion_consultas.json` contiene tiempos individuales y EXPLAIN ANALYZE/BUFFERS de la fixture. La suite prueba las consultas, no sustituye una validación visual de los paneles en un navegador.

## Pendientes explícitos

- Validación visual y regla de alertas en una instancia Grafana activa.
- Medición con millones de registros: no ejecutada por la restricción de no cargar bases reales sin autorización específica.
- Primera adopción del PostgreSQL real: durante la inspección sus conexiones locales no respondieron y no había contenedores en ejecución. No se afirma que sus tablas estén actualizadas.
- La selección nueva `gold_snapshot` utiliza los archivos existentes; no requiere ni agrega un productor de procesos.

No se modificaron cálculos de dimensiones, archivos del modelo, AWS, credenciales ni volúmenes existentes. Una propuesta de modificación de `load.py` fue rechazada por revisión automática; se descartó y se resolvió la compatibilidad en la capa de publicación.

## Ajuste 3.1: Gold existente

El notebook 12 selecciona contratos/procesos existentes y el lote de alertas correspondiente. Se agrega `seleccion_gold.py`, se habilita `gold_snapshot` y se conserva el detalle completo de procesos. Los conteos inspeccionados fueron 4.353.720 contratos y 626.821 procesos (8 archivos).

Las nueve pruebas anteriores corresponden a la refactorización previa, no certifican este nuevo modo. Para este ajuste se revisaron sintaxis Python, celdas y JSON; no se ejecutaron cargas reales, servicios ni pruebas de integración. La publicación y comprobación visual quedan para la ejecución del usuario desde el notebook 12.
