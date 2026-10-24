from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from .config import COLUMNAS, OUTPUT_DIR, SILVER_DIR

UNIQUE_KEYS = {
    "procesos": "id_del_proceso",
    "contratos": "id_contrato",
    "proveedores": "codigo",
    # grupos requiere llave compuesta y se valida aparte
}

def parquet_metadata(base: str) -> pd.DataFrame:
    rows = []
    for p in sorted((SILVER_DIR / base).glob("*.parquet")):
        meta = pq.ParquetFile(p).metadata
        rows.append({
            "base": base,
            "archivo": p.name,
            "filas": meta.num_rows,
            "columnas": meta.num_columns,
            "mb": p.stat().st_size / 1024**2,
        })
    return pd.DataFrame(rows)

def schema_check(base: str) -> dict:
    files = sorted((SILVER_DIR / base).glob("*.parquet"))
    if not files:
        return {"base": base, "ok": False, "detalle": "sin archivos"}

    expected = set(COLUMNAS[base])
    schemas = [{c for c in pq.ParquetFile(p).schema_arrow.names if not c.startswith('raw__')} for p in files]
    missing = sorted(set().union(*(expected - cols for cols in schemas)))
    extra = sorted(set().union(*(cols - expected for cols in schemas)))
    return {
        "base": base,
        "ok": not missing and not extra,
        "faltan": ",".join(missing),
        "sobran": ",".join(extra),
    }

def build_quality_report() -> pd.DataFrame:
    records = []
    for base in COLUMNAS:
        md = parquet_metadata(base)
        check = schema_check(base)
        records.append({
            "base": base,
            "archivos": len(md),
            "filas": int(md["filas"].sum()) if len(md) else 0,
            "gb_parquet": float(md["mb"].sum() / 1024) if len(md) else 0.0,
            "schema_ok": check["ok"],
            "faltan": check.get("faltan", ""),
            "sobran": check.get("sobran", ""),
        })

    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_DIR / "reporte_calidad.csv", index=False, encoding="utf-8-sig")
    if not df["schema_ok"].all():
        raise ValueError("Falló la validación de columnas Silver; revise reporte_calidad.csv")
    return df
