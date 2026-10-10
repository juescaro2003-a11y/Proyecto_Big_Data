# Copia ligera para GitHub

Esta copia conserva código, configuraciones de reglas, documentación y notebooks sin salidas. El proyecto original no se modificó.

## Elementos excluidos

- `data/`: Bronze, Silver y Gold.
- `salida/`: resultados, bases DuckDB, checkpoints, locks e informes.
- `.dask-spill/`, cachés Python, configuración del editor y reportes de `notebooks/informes_etl/`.
- `.env` y credenciales locales. `.env.example` conserva las claves y deja vacíos los secretos.
- Resultados, imágenes adjuntas y metadatos de ejecución incrustados en notebooks.

Los datos deben descargarse o prepararse de nuevo antes de ejecutar los notebooks analíticos. Copia `.env.example` a `.env` y configura tus credenciales localmente. No publiques `.env`.

## Publicar con GitHub Desktop

1. Crea un repositorio local usando esta carpeta existente.
2. Revisa la lista de archivos y crea el primer commit.
3. Pulsa **Publish repository** y elige la visibilidad.

## Publicar con Git

Desde esta carpeta, crea primero un repositorio vacío en GitHub (sin README adicional) y ejecuta:

```powershell
git init
git add .
git status
git commit -m "Agregar pipeline SECOP II sin datos ni resultados"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/TU_REPOSITORIO.git
git push -u origin main
```

`.gitignore` excluye nuevos datos y resultados, pero no elimina archivos ya registrados en Git. Antes de cada commit, limpia las salidas de los notebooks en Jupyter o VS Code y revisa `git diff --cached`.

No se creó ni publicó un repositorio remoto desde esta copia.
