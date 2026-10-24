# SECOP II — Empieza aquí

Este proyecto transforma datos de SECOP II y prioriza contratos de contratación directa para revisión. El indicador vigente es **alertas-cd-3.0**: C1, C2, C3, V1, V2, P1 y P2. Las señales son descriptivas; no demuestran corrupción ni constituyen una probabilidad validada.

## ¿Qué necesitas hacer?

| Necesidad | Abre |
|---|---|
| Consultar los hallazgos del último análisis documentado | [Resultados del análisis](docs/RESULTADOS_ANALISIS.md) |
| Entender el propósito y las decisiones de negocio | [Perspectiva de negocio](docs/PERSPECTIVA_NEGOCIO.md) |
| Preparar una copia descargada de GitHub | [Entrega y exclusiones](docs/ENTREGA_GITHUB.md) |
| Saber qué datos recibir por separado | [Datos requeridos](data/README.md) |
| Entender el recorrido completo y preparar el computador | [Guía de operación](docs/GUIA_OPERACION.md) |
| Saber para qué sirve cada carpeta, script y notebook | [Mapa completo del proyecto](docs/MAPA_PROYECTO.md) |
| Encontrar usuarios, puertos y dónde consultar las contraseñas | [Accesos y configuración](docs/ACCESOS_Y_CONFIGURACION.md) |
| Entender fórmulas, exclusiones y clasificación | [Especificación del indicador v3](docs/INDICADOR_V3.md) |
| Calcular el indicador con el Gold existente | [Notebook 09](notebooks/09_dimensiones_indicador.ipynb) |
| Ver conteos, cobertura, gráficos y red | [Notebook 10](notebooks/10_agregados_indicadores.ipynb) |
| Consultar avance o errores | [Notebook 11](notebooks/11_monitor_alertas.ipynb) |
| Iniciar Grafana y publicar localmente | [Notebook 12](notebooks/12_dashboard_grafana.ipynb) |
| Publicar resultados compactos en AWS y verificar la carga | [Notebook 13](notebooks/13_publicar_alertas_aws.ipynb) |
| Recuperar documentación o notebooks anteriores | Archivo local `versiones anteriores/README.md` (no incluido en GitHub) |

## Recorrido habitual: ya existe Gold

1. Preparar el entorno `secop` y abrir Jupyter desde la raíz del proyecto.
2. Ejecutar **09** hasta `COMPLETADA`. **11** permite consultar el estado desde otro kernel.
3. Ejecutar **10** para analizar los resultados.
4. Ejecutar **12** con Docker Desktop abierto para PostgreSQL/Grafana local.
5. Ejecutar **13** si se desea publicar en AWS. Es independiente de Grafana local.

No se necesita volver a descargar Bronze ni reconstruir Silver para este recorrido. Los notebooks no instalan paquetes. AWS y Grafana no se actualizan solo por ejecutar 09.

## Dos procesos diferentes

```text
API SECOP II → Bronze CSV → Silver Parquet → Gold contractual
                                                ↓
                          notebook 09: indicador v3 + red complementaria
                              ├─ 10: análisis y agregados
                              ├─ 11: consulta de avance
                              ├─ 12: PostgreSQL local → Grafana
                              └─ 13: PostgreSQL AWS (11 columnas + agregados)
```

El tablero de procesos lee otro Gold existente: `data/gold/procesos_modelo/`. No recibe automáticamente las señales contractuales ni comparte necesariamente su cobertura.

## Reglas de trabajo

- Usa la versión y el corte del manifiesto del lote, no solo la fecha del archivo.
- Una señal ausente es NULL, no cero. El puntaje final exige las siete señales evaluables.
- Mantén los datos y manifiestos juntos. No elimines locks, checkpoints o archivos DuckDB de una ejecución activa.
- Las credenciales están en `.env`, `.env.aws` y `grafana_secop/.env`; consulta la guía de accesos.
- El código antiguo que permanece puede ser dependencia del nuevo. No borrarlo por nombre.
- Los archivos de `versiones anteriores/` son referencias/restauraciones locales, excluidos de GitHub; no son dependencias operativas.

Documentación organizada el **23 de octubre de 2026**, a partir del código y configuración locales. Esta documentación no acredita disponibilidad de servicios ni pruebas de integración; la evidencia de ejecución está en cada manifiesto y en las verificaciones de publicación.
