"""Selección explícita de productos Gold existentes. No lee ni construye Silver."""
from pathlib import Path
import pyarrow.parquet as pq


def gold_path(root, configured):
    base=(Path(root)/'data/gold').resolve()
    path=(Path(root)/configured).resolve()
    if not path.is_relative_to(base):raise ValueError('La fuente debe estar dentro de data/gold.')
    if not path.exists():raise FileNotFoundError(path)
    return path


def process_files(root,config):
    path=gold_path(root,config.get('process_source','data/gold/procesos_modelo'))
    files=[path] if path.is_file() else sorted(path.glob('*.parquet'))
    if not files or any(p.suffix!='.parquet' for p in files):
        raise ValueError('La versión Gold seleccionada no tiene archivos Parquet. Seleccione una carpeta concreta, sin mezclar generaciones.')
    return files


def describe_processes(root,config):
    files=process_files(root,config)
    count=0;dates=[];complete=True
    for path in files:
        with pq.ParquetFile(path) as pf:
            count+=pf.metadata.num_rows
            idx=pf.schema_arrow.get_field_index('fecha_de_publicacion_del')
            if idx<0:raise ValueError('Gold de procesos no contiene fecha_de_publicacion_del.')
            for i in range(pf.num_row_groups):
                stats=pf.metadata.row_group(i).column(idx).statistics
                if stats and stats.has_min_max:
                    dates.extend([str(stats.min),str(stats.max)])
                elif not stats or stats.num_values!=0:complete=False
    return {'fuente':str(gold_path(root,config.get('process_source','data/gold/procesos_modelo'))),
            'archivos':len(files),'filas':count,'bytes':sum(p.stat().st_size for p in files),
            'fecha_min_observada_footer':min(dates) if dates else None,
            'fecha_max_observada_footer':max(dates) if dates else None,
            'estadisticas_fecha_completas':complete,
            'corte_declarado':config.get('process_cutoff'),
            'nota':'Las fechas observadas no acreditan por sí mismas el corte de extracción.'}
