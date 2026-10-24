from __future__ import annotations

import io
import uuid
from contextlib import nullcontext
from pathlib import Path

import pandas as pd
import pyarrow as pa
import psycopg
from psycopg import sql
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
)

from .config import DB, GOLD_DIR, SILVER_DIR, TIPOS
from .db import create_schemas, wait_for_postgres

def parquet_files(path: Path) -> list[Path]:
    return sorted(p for p in path.glob("*.parquet") if p.is_file())

def postgres_type(series: pd.Series) -> str:
    if is_bool_dtype(series.dtype):
        return "BOOLEAN"
    if is_integer_dtype(series.dtype):
        return "BIGINT"
    if is_float_dtype(series.dtype):
        return "DOUBLE PRECISION"
    if is_datetime64_any_dtype(series.dtype):
        return "TIMESTAMP"
    return "TEXT"

def _connect():
    return psycopg.connect(
        host=DB["host"],
        port=DB["port"],
        dbname=DB["database"],
        user=DB["user"],
        password=DB["password"],
    )

def create_table_from_sample(
    schema: str,
    table: str,
    sample: pd.DataFrame,
    replace: bool = True,
    conn=None,
) -> None:
    owned = conn is None
    with (_connect() if owned else nullcontext(conn)) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            if replace:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )

            cols = []
            numeric_columns = {
                column
                for spec in TIPOS.values()
                for column in spec["numero"]
            }
            for name in sample.columns:
                typ = (
                    "DOUBLE PRECISION"
                    if name in numeric_columns
                    else postgres_type(sample[name])
                )
                cols.append(
                    sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(typ))
                )

            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(cols),
                )
            )
        if owned:
            conn.commit()

def copy_dataframe(conn, schema: str, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

    # COPY en CSV evita insertar fila por fila con ORM.
    buf = io.StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        na_rep="\\N",
        date_format="%Y-%m-%d %H:%M:%S",
    )
    buf.seek(0)

    statement = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, df.columns)),
    )

    with conn.cursor().copy(statement) as copy:
        while chunk := buf.read(1024 * 1024):
            copy.write(chunk)

def _nullable_arrow_type(arrow_type):
    # Mantener enteros exactos al pasar de Arrow a pandas, incluso con NULL.
    # El valor 153 debe serializarse como "153", nunca como "153.0" para BIGINT.
    # También evita pérdida de precisión por un paso intermedio por float64.
    if pa.types.is_signed_integer(arrow_type):
        return pd.Int64Dtype()
    if pa.types.is_unsigned_integer(arrow_type):
        return pd.UInt64Dtype()
    if pa.types.is_boolean(arrow_type):
        return pd.BooleanDtype()
    return None


def _stage_dataset(conn, source: Path, schema: str, table: str) -> dict:
    import pyarrow.parquet as pq
    files = parquet_files(source)
    if not files:
        raise FileNotFoundError(f"No hay parquet en {source}")
    stage = "_etl_" + uuid.uuid4().hex
    # Arrow vacío conserva tipos sin leer una partición completa para inferirlos.
    sample = pq.ParquetFile(files[0]).schema_arrow.empty_table().to_pandas(types_mapper=_nullable_arrow_type)
    create_table_from_sample(schema, stage, sample, replace=False, conn=conn)
    total = 0
    for i, path in enumerate(files, 1):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=50000):
            df = batch.to_pandas(types_mapper=_nullable_arrow_type)
            copy_dataframe(conn, schema, stage, df)
            total += len(df)
        print(f"{schema}.{table}: {i}/{len(files)} archivos; {total:,} filas preparadas")
    return {"schema": schema, "table": table, "stage": stage, "rows": total, "files": len(files)}


def _publish(conn, item):
    schema, table, stage = (item[k] for k in ("schema", "table", "stage"))
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
    conn.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
        sql.Identifier(schema), sql.Identifier(stage), sql.Identifier(table)))


def load_dataset(source: Path, schema: str, table: str, replace: bool = True) -> dict:
    # Append sin reconciliación por clave duplicaría registros en la siguiente ejecución.
    if not replace:
        raise ValueError("Carga append deshabilitada: use replace=True para publicar un snapshot completo")
    with _connect() as conn:
        item = _stage_dataset(conn, source, schema, table)
        _publish(conn, item)
    return {k: v for k, v in item.items() if k != "stage"}


def create_indexes(conn=None) -> None:
    statements = [
        "CREATE INDEX IF NOT EXISTS ix_procesos_id ON secop.procesos (id_del_proceso)",
        "CREATE INDEX IF NOT EXISTS ix_procesos_portafolio ON secop.procesos (id_del_portafolio)",
        "CREATE INDEX IF NOT EXISTS ix_contratos_id ON secop.contratos (id_contrato)",
        "CREATE INDEX IF NOT EXISTS ix_contratos_proceso ON secop.contratos (proceso_de_compra)",
        "CREATE INDEX IF NOT EXISTS ix_proveedores_codigo ON secop.proveedores (codigo)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_control_contrato ON gold.contratos_control (id_contrato)",
        "CREATE INDEX IF NOT EXISTS ix_control_entidad ON gold.contratos_control (entidad_id, anio)",
        "CREATE INDEX IF NOT EXISTS ix_control_prioridad ON gold.contratos_control (estado_indice, ircd)",
    ]
    with (_connect() if conn is None else nullcontext(conn)) as active:
        for statement in statements:
            active.execute(statement)


def load_all_to_postgres(replace: bool = True) -> list[dict]:
    if not replace:
        raise ValueError("Use replace=True; append no garantiza una recarga sin duplicados")
    wait_for_postgres()
    sources = [(SILVER_DIR / base, "secop", base)
               for base in ("procesos", "contratos", "proveedores", "grupos")]
    sources.append((GOLD_DIR / "contratos_control", "gold", "contratos_control"))
    # Ninguna tabla pública cambia hasta que todas las cargas estén preparadas.
    # Si COPY, publicación o índices fallan, la transacción revierte todo.
    with _connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(73192026)")
        results = [_stage_dataset(conn, *source) for source in sources]
        for item in results:
            _publish(conn, item)
        create_indexes(conn)
    return [{k: v for k, v in item.items() if k != "stage"} for item in results]
