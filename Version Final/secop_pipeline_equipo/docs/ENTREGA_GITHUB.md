# Entrega del proyecto por GitHub

[Inicio](../README.md) · [Negocio](PERSPECTIVA_NEGOCIO.md) · [Datos requeridos](../data/README.md) · [Accesos](ACCESOS_Y_CONFIGURACION.md)

## Qué recibe una persona externa

El repositorio contiene código, notebooks, parámetros, dependencias, Docker, diseño de Grafana, pruebas y documentación. **No incluye bases iniciales, Parquet Gold, resultados calculados ni contraseñas reales.** Un clon permite preparar el entorno, pero no ejecutar análisis sin conseguir datos compatibles.

Al revisar esta carpeta no existía `.git`: no se ha creado un repositorio aquí, no se ha hecho commit ni se ha publicado en GitHub. La revisión de exclusiones usa Git con metadatos temporales separados del proyecto.

## Qué se versiona y qué queda local

| Se incluye | Motivo |
|---|---|
| `secop_pipeline/`, `flows/`, `scripts/` | Código actual y dependencias compartidas/compatibilidad |
| Notebooks principales 00–13 y EDA complementario | Interfaces operativas y documentación ejecutable |
| `tests/` y prueba de publicación de Grafana | Pruebas existentes; no equivalen a cobertura completa v3 |
| `environment.yml`, requirements raíz y de Grafana | Preparación del entorno |
| JSON de parámetros y publicación | Necesarios para configuración y validación |
| Compose, JSON de tableros, YAML de provisioning, init.sh | Arranque de servicios y diseño de Grafana |
| `.env.example`, `.env.aws.example`, `grafana_secop/.env.example` | Plantillas sin las contraseñas locales |
| README, documentos, `data/README.md`, `salida/README.md` | Incorporación, explicación del negocio y contrato de datos |
| `sql/00_info.sql` | Referencia de consultas del ETL original |

| Se excluye | Cómo se obtiene / conserva |
|---|---|
| `.env` y variantes reales, certificados/claves privados | Configuración privada en cada máquina |
| Contenido de `data/` salvo README/.gitkeep | Transferencia externa de snapshots o reconstrucción compatible |
| Contenido de `salida/` salvo README/.gitkeep | Generado localmente por los flujos |
| DuckDB, WAL, bases locales, logs, locks, spill y cachés | Artefactos operativos locales; no son código fuente |
| `versiones anteriores/` | Archivo local, con respaldos y ZIP; no es dependencia operativa ni se entrega en este clon |
| `notebooks/informes_etl/` | Evidencias históricas locales generadas por ejecuciones |
| `.vscode/`, `.idea/`, entornos Python | Preferencias/entornos de cada equipo |

No se usan exclusiones genéricas `*.json`, `*.sql`, `*.ipynb` o `*.yml`: ocultarían parámetros, código o configuración indispensable. Excluir datos de Git no implica borrarlos del disco.

## Incorporación desde cero

1. Clonar/descargar el repositorio y abrir la raíz.
2. Leer [perspectiva de negocio](PERSPECTIVA_NEGOCIO.md), luego [operación](GUIA_OPERACION.md).
3. Crear el entorno Conda con `environment.yml` y seleccionar su kernel.
4. Crear los tres `.env` desde sus plantillas y completar valores privados. No dejar `REEMPLAZAR_...` como una contraseña operativa.
5. Recibir los Gold indicados en [data/README.md](../data/README.md). No se entrega una muestra sintética como sustituto silencioso de las bases reales.
6. Ejecutar 09 y confirmar lote completo; consultar 10 y 11.
7. Abrir Docker Desktop y ejecutar 12 para publicación local. El Gold de procesos también es necesario para ese notebook completo.
8. Ejecutar 13 solo si se desea publicar en RDS y existe acceso autorizado de red/credenciales.

Los enlaces de la guía de accesos a `.env` son deliberadamente locales y estarán ausentes en GitHub hasta configurar la copia. Los enlaces al archivo histórico se marcan como locales: no forman parte del clon.

## Reconstrucción y reproducibilidad

Si no se reciben snapshots, se puede reconstruir Gold contractual desde la API mediante el ETL. Esto no asegura reproducir exactamente un corte histórico; los registros de la API cambian. El Gold de procesos del tablero no tiene un productor actualizado dentro del flujo contractual actual: debe entregarse aparte o desarrollarse explícitamente esa producción.

Para entregar datos por otro medio, adjuntar inventario de archivos, SHA-256, conteos, fechas declaradas y auditoría. No enviar credenciales en ese paquete. Para v3, código + parámetros + Gold exacto determinan la identidad del lote. Los punteros guardan rutas locales y no son portables tal cual.

`requirements.txt` fija rangos y `environment.yml` fija Python, pero no hay un lock completo de versiones transitivas: una instalación futura puede resolver versiones diferentes. La compatibilidad real se confirma en el entorno donde se ejecute; esta revisión no certifica que un clon limpio haya ejecutado el pipeline completo.

## Qué se comprobó para esta entrega

- Reglas `.gitignore` evaluadas con el motor real de Git, sin inicializar el proyecto ni tocar el índice de un repositorio del usuario.
- Código, notebooks principales, configuraciones, Docker, plantillas y documentación deben quedar fuera de las exclusiones.
- Casos representativos de Bronze/Silver/Gold, resultados anidados, secretos y locks deben quedar excluidos.
- `.vscode/settings.json` es una preferencia local excluida intencionadamente, no una dependencia del proceso.
- Búsqueda de coincidencias con las contraseñas locales conocidas en los archivos candidatos, sin imprimir sus valores. No equivale a un detector exhaustivo de todos los secretos posibles.
- Revisión de tamaños de candidatos, sintaxis de cambios puntuales y enlaces de la documentación nueva. No se ejecutan cálculos, Docker ni cargas AWS.

Antes de un primer commit, revisar `git status --short` y `git diff --cached --stat` desde el repositorio que se vaya a publicar. `.gitignore` no retira secretos ni datos que ya estén rastreados en otra copia. Revisar también salidas de notebooks: pueden incluir datos o rutas locales aunque el archivo sea necesario.

## Contraseñas y código

La conexión local ya no utiliza una contraseña predeterminada incrustada. Docker exige `POSTGRES_PASSWORD` en `.env`; el archivo ejemplo contiene un marcador. El notebook 00 oculta la contraseña de la URL independientemente de su valor. Estos cambios no rotan ni cambian usuarios de bases existentes.

### Resultado de la revisión del 23 de octubre de 2026

Se comprobaron **99 archivos necesarios**: ninguno quedó ignorado. Los **13 casos de exclusión** de datos, secretos y artefactos quedaron excluidos. No se encontraron coincidencias con los valores de secretos locales conocidos en los candidatos revisados ni candidatos mayores de 10 MiB. Los enlaces nuevos existen y la sintaxis de los cambios puntuales es válida. Esto no verifica disponibilidad de datos en otro equipo ni sustituye una ejecución del flujo completo.
