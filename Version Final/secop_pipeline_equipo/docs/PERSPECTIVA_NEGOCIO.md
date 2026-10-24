# SECOP II: perspectiva de negocio

[Inicio](../README.md) · [Guía de operación](GUIA_OPERACION.md) · [Especificación técnica](INDICADOR_V3.md)

## 1. Qué problema resolvemos

La cantidad de contratos públicos supera la capacidad de revisión manual. El proyecto organiza información de contratación directa de SECOP II y ayuda a seleccionar contratos que merecen una revisión más detallada. Su producto de negocio es una **bandeja trazable de priorización**, acompañada de explicaciones y mediciones de cobertura.

El objetivo actual es apoyar control y análisis contractual. La recomendación de oportunidades para empresas pertenece a una etapa anterior. No se presenta este sistema como una herramienta para certificar corrupción, sancionar automáticamente, predecir adjudicaciones o recomendar proveedores.

## 2. Usuarios y decisiones

| Usuario | Pregunta que puede responder | Producto |
|---|---|---|
| Analista de contratación/control | ¿Qué contratos revisar primero y por qué? | Detalle contractual, señales, motivos y clasificación |
| Coordinador de revisión | ¿Cómo distribuir la capacidad de revisión por trimestre? | Cupo configurable, ranking y agregados |
| Responsable de datos | ¿Qué proporción puede evaluarse y dónde hay problemas de calidad? | Cobertura, nulos, valores inválidos y motivos de no evaluación |
| Equipo técnico | ¿Qué datos, reglas y versión produjeron estos resultados? | Manifiesto, huellas y checkpoints |
| Consultor del tablero | ¿Cómo se distribuyen contratos y procesos en el universo disponible? | Tableros separados y filtros |

El sistema produce evidencia para la decisión humana. La revisión documental, el contexto de la entidad y cualquier conclusión jurídica quedan fuera de su cálculo.

## 3. Unidad y alcance

La unidad principal es un contrato de contratación directa, identificado por `id_contrato`. Se agrupa por entidad y proveedor para construir señales. Proveedor y entidad pueden aparecer en muchos contratos; no son identificadores únicos de una fila.

El indicador se calcula sobre el Gold recibido y su cobertura declarada. No describe necesariamente toda la contratación colombiana ni toda la actividad económica de un proveedor. El tablero de procesos procede de otro Gold y puede tener modalidades/cobertura distintas: **no sumar sus filas con contratos como si fueran negocios independientes**, ni interpretar un proceso como un contrato equivalente.

## 4. Cómo se transforma información en una decisión

1. **Recibir y conservar:** los datos originales permiten rastrear de dónde vino la información.
2. **Normalizar:** se hacen comparables textos, fechas, identificadores y números.
3. **Definir el universo:** se resuelven versiones/duplicados y se selecciona contratación directa según el corte y filtros. La auditoría explica las exclusiones.
4. **Evaluar calidad:** un importe implausible se marca, no se inventa su valor correcto. Su contrato permanece, pero no alimenta V1.
5. **Calcular señales:** se observan concentración, valores y características del proveedor.
6. **Priorizar:** se combinan familias y se selecciona hasta el cupo trimestral de contratos evaluables con puntaje positivo.
7. **Revisar y documentar:** el analista consulta el contrato y contrasta con evidencia externa. El sistema actual no incluye un módulo completo de gestión de casos o registro de sanciones.

## 5. Qué significa cada señal

| Señal | Pregunta de negocio | Regla inicial | Qué no permite concluir |
|---|---|---|---|
| C1 | ¿Una parte alta de los contratos de una entidad va al mismo proveedor? | Participación por conteo >40% en 365 días; mínimo 10 contratos de entidad | No prueba favoritismo; especialización y oferta disponible pueden influir |
| C2 | ¿La contratación de la entidad está concentrada en pocos proveedores? | HHI anual observado >0,25; mínimo 10 contratos | No demuestra falta de competencia por sí sola |
| C3 | ¿El proveedor depende principalmente de esta entidad en el universo observado? | Más de 80% de sus contratos en 365 días; mínimo 5 contratos de proveedor | No mide sus ingresos privados ni contratos ausentes del universo |
| V1 | ¿El valor es alto frente a comparables históricos? | Logaritmo, mediana/MAD, z robusto >3,5; al menos 30 referencias; p95 si MAD=0 | No demuestra sobrecosto: cantidad, alcance y condiciones pueden variar |
| V2 | ¿Se repiten contratos similares con la misma pareja entidad–proveedor en poco tiempo? | Al menos 3 en 30 días, mismo tipo y categoría de 4 dígitos | No acredita fraccionamiento contractual |
| P1 | ¿Es la primera aparición observada del proveedor? | Primera fecha en Gold, con cobertura previa suficiente | No equivale a empresa recién constituida |
| P2 | ¿El proveedor aparece en varios segmentos de productos/servicios? | Al menos 3 segmentos UNSPSC en 365 días y 5 contratos | No demuestra actividades incompatibles ni falta de capacidad |

Los umbrales son hipótesis operativas configurables, pendientes de validación externa. Las señales no son independientes: varias describen aspectos relacionados de concentración. La red entidad–proveedor se presenta como contexto adicional; no agrega puntos al indicador.

## 6. Cómo se obtiene el puntaje y la bandeja

Se promedian las señales dentro de tres familias y luego las tres familias con igual peso:

```text
C = (C1 + C2 + C3) / 3
V = (V1 + V2) / 2
P = (P1 + P2) / 2
Puntaje = (C + V + P) / 3
```

Cada señal vale 1, 0 o NULL (no evaluable). El puntaje definitivo exige las siete señales evaluables. Un puntaje 0,6 no significa 60% de probabilidad de corrupción. Los límites `puntaje_min`/`puntaje_max` expresan qué valores serían posibles al completar señales faltantes; no son intervalos estadísticos de confianza.

La configuración inicial prioriza hasta **2.000 contratos por trimestre de firma**, entre los evaluables con puntaje >0. Si hay menos elegibles, selecciona menos. Los empates se resuelven por ID para reproducibilidad; ese desempate no aporta evidencia de mayor riesgo. El cupo es un parámetro de capacidad operativa que debe acordarse con quienes revisan, no una tasa de irregularidad esperada.

| Clasificación | Interpretación correcta | Acción sugerida |
|---|---|---|
| ALERTA | Contrato seleccionado para revisión dentro del cupo | Revisar señales, calidad, objeto, documentos y contexto |
| NO_ALERTA | Contrato evaluable fuera del cupo o sin puntaje positivo | Mantener seguimiento; no asumir ausencia de irregularidad |
| NO_EVALUABLE | Falta información/referencia para completar las siete señales | Revisar motivos; no tratarlo como riesgo cero |

Ejemplo ilustrativo: dos contratos empatados en puntaje pueden quedar a ambos lados del límite del cupo. Esa diferencia de etiqueta responde a capacidad/desempate, no a que uno esté demostrado como más riesgoso.

## 7. Calidad y exclusiones monetarias

Un valor se marca inválido si es nulo, no finito, no positivo, supera 10 billones COP o supera 1.000 veces la mediana de tipo/año. Tope y factor son configurables. La mediana usa importes finitos, positivos y dentro del tope absoluto. La marca indica que el valor no es apto para ese análisis; no permite determinar automáticamente el importe verdadero.

El contrato no se elimina del resultado v3: V1 queda no evaluable y, bajo la política de cobertura completa, también el puntaje final. Si se necesita una cola especial para estos registros, debe consultarse el motivo de calidad, no reinterpretarlos como ALERTA del indicador.

La política completa puede dejar sin puntaje a proveedores nuevos/con pocos contratos, aun con P1 activada. Esto exige observar cobertura junto al número de alertas, para no confundir ausencia de evaluación con ausencia de señales.

## 8. Qué entregar a un responsable

- Número de contratos del universo y periodo/corte declarado.
- Porcentaje evaluable total y por cada señal, con sus motivos de exclusión.
- Contratos priorizados por trimestre y cupo aplicado.
- Número y porcentaje de señales entre evaluados; no sumar señales como contratos únicos.
- Valores inválidos por motivo; evitar sumar importes inválidos como valor contractual confiable.
- Versión del modelo y huella del lote para reproducibilidad.
- Evidencia de carga cuando se distribuyen resultados en AWS.

Grafana sirve para explorar; el notebook 10 permite análisis más detallado; AWS distribuye una tabla compacta y agregados. Los motivos completos permanecen localmente: la tabla compacta no sustituye al expediente analítico.

## 9. Límites temporales y evaluación del éxito

El análisis es retrospectivo. C2 usa el año observado hasta el corte y la validación monetaria usa tipo/año. No debe presentarse como predicción disponible al firmar. Un año parcial debe identificarse y no compararse automáticamente con un año completo.

Más alertas no significa mejor calidad. Indicadores de gestión razonables son cobertura evaluable, tiempo de ejecución, proporción de revisiones efectivamente realizadas y utilidad de las señales confirmada por analistas. Precisión, falsos positivos o capacidad predictiva requieren casos revisados/etiquetas externas; actualmente no hay evidencia para atribuir esos resultados al modelo.

## 10. Responsabilidades recomendadas

El responsable de datos confirma procedencia y cobertura; el dueño de negocio acuerda reglas/cupo; el analista interpreta evidencias; el operador ejecuta y publica; el administrador gestiona accesos. Antes de comparar periodos o cambiar reglas, documentar versión y diferencias del universo. Los cambios de política requieren decisión explícita, no ajustar umbrales solo para obtener más o menos alertas.
