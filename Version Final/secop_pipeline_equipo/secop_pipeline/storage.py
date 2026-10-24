"""Publicación local recuperable y exclusión entre ejecuciones del ETL."""
from contextlib import contextmanager
from pathlib import Path
import os
import shutil
import uuid


def publish_directory(stage: Path, destination: Path, *, keep_previous: bool = False) -> None:
    from .config import ROOT
    workspace = ROOT.resolve()
    stage, destination = stage.resolve(), destination.resolve()
    if (stage == destination or stage == workspace or destination == workspace
            or workspace not in stage.parents or workspace not in destination.parents
            or stage.parent != destination.parent):
        raise ValueError('La publicación exige directorios hermanos dentro del proyecto')
    # Dos renombrados no son una transacción de filesystem. Recuperamos ante
    # excepciones; una caída del equipo puede requerir restaurar .previous-*.
    backup = destination.with_name(destination.name + '.previous-' + uuid.uuid4().hex)
    existed = destination.exists()
    if existed:
        destination.rename(backup)
    try:
        stage.rename(destination)
    except BaseException:
        if existed:
            backup.rename(destination)
        raise
    if existed and not keep_previous:
        shutil.rmtree(backup)


@contextmanager
def pipeline_lock(root: Path):
    path = root / '.pipeline.lock'
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError(f'Existe {path}; hay otra ejecución o un cierre abrupto. '
                           'Retírelo únicamente después de confirmar que no sigue activa.') from None
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(str(os.getpid()))
        yield
    finally:
        path.unlink(missing_ok=True)
