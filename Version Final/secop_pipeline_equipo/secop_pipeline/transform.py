from __future__ import annotations

from pathlib import Path
import re
import json
import hashlib
import os
import pyarrow as pa
import pyarrow.parquet as pq
from .extract import sha256_file

import numpy as np
import pandas as pd

from .config import BRONZE_DIR, SILVER_DIR, TIPOS, VACIOS, CONTROL_OPTIONAL_COLUMNS

def clean_text(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    mask = x.str.lower().isin(VACIOS)
    return x.mask(mask, pd.NA)

def to_number(s: pd.Series) -> pd.Series:
    """Convierte números comunes en SECOP, incluyendo formato colombiano.

    Ejemplos:
    1.234.567,89 -> 1234567.89
    15.693.000   -> 15693000
    1234,5       -> 1234.5
    """
    x = clean_text(s)
    x = (
        x.str.replace("\u00a0", "", regex=False)
         .str.replace("$", "", regex=False)
         .str.replace(" ", "", regex=False)
    )

    # 1.234.567,89
    both = x.str.contains(r"\.", na=False) & x.str.contains(",", na=False)
    x.loc[both] = (
        x.loc[both]
         .str.replace(".", "", regex=False)
         .str.replace(",", ".", regex=False)
    )

    # 15.693.000 -> miles
    dot_thousands = x.str.match(r"^-?\d{1,3}(\.\d{3})+$", na=False)
    x.loc[dot_thousands] = x.loc[dot_thousands].str.replace(".", "", regex=False)

    # 15,693,000 -> miles
    comma_thousands = x.str.match(r"^-?\d{1,3}(,\d{3})+$", na=False)
    x.loc[comma_thousands] = x.loc[comma_thousands].str.replace(",", "", regex=False)

    # 1234,5 -> decimal
    comma_decimal = x.str.contains(",", na=False)
    x.loc[comma_decimal] = x.loc[comma_decimal].str.replace(",", ".", regex=False)

    return pd.to_numeric(x, errors="coerce")

def to_date(s: pd.Series) -> pd.Series:
    x = clean_text(s)
    return pd.to_datetime(x, errors="coerce", format="mixed", utc=True).dt.tz_convert(None)

def transform_dataframe(df: pd.DataFrame, base: str) -> pd.DataFrame:
    spec = TIPOS[base]
    out = df.copy()
    if base == 'contratos':
        for col in CONTROL_OPTIONAL_COLUMNS:
            if col not in out:
                out[col] = pd.Series(pd.NA, index=out.index, dtype='string')
        for col in list(out.columns):
            out['raw__' + col] = out[col].astype('string')

    for c in spec["llave"]:
        if c in out:
            out[c] = clean_text(out[c])

    # Se guardan como strings, no category, para que todos los Parquet tengan
    # un esquema homogéneo entre particiones.
    for c in spec["etiqueta"]:
        if c in out:
            out[c] = clean_text(out[c])

    for c in spec["numero"]:
        if c in out:
            # Socrata number usa punto decimal; priorizar esa representación.
            raw = clean_text(out[c])
            parsed = pd.to_numeric(raw, errors='coerce')
            out[c] = parsed.fillna(to_number(raw)).astype("float64")

    for c in spec["fecha"]:
        if c in out:
            out[c] = to_date(out[c])

    return out

def transform_file(path: Path, base: str, overwrite: bool = False) -> dict:
    dest_dir = SILVER_DIR / base
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{path.stem}.parquet"

    fingerprint = hashlib.sha256((sha256_file(path) + sha256_file(Path(__file__))
        + json.dumps(TIPOS[base], sort_keys=True) + repr(sorted(VACIOS))).encode()).hexdigest()
    marker = dest.with_suffix(".parquet.meta.json")
    if dest.exists() and marker.exists() and not overwrite:
        cached = json.loads(marker.read_text(encoding="utf-8"))
        if cached.get("fingerprint") == fingerprint:
            return {"base": base, "status": "existing", "path": str(dest),
                    "rows": cached.get('rows'), "metrics": cached.get('metrics', {}),
                    "metrics_origin": "transformacion_previa"}
    temp = dest.with_suffix(".parquet.partial")
    writer = None
    rows = 0
    metrics = {}
    try:
        for df in pd.read_csv(path, dtype=str, keep_default_na=False,
                              chunksize=int(os.getenv("TRANSFORM_CHUNK_ROWS", "50000"))):
            out = transform_dataframe(df, base)
            for col in out.columns:
                record = metrics.setdefault(col, {'nulos_entrada': 0, 'nulos_salida': 0,
                                                   'invalidos_conversion': 0, 'texto_normalizado': 0})
                raw = df.get(col, df.get(col.removeprefix('raw__'), pd.Series(pd.NA, index=df.index, dtype='string'))).astype('string')
                record['nulos_entrada'] += int((raw.isna() | raw.eq('').fillna(False)).sum())
                record['nulos_salida'] += int(out[col].isna().sum())
                if col in TIPOS[base]['numero'] or col in TIPOS[base]['fecha']:
                    record['invalidos_conversion'] += int((clean_text(raw).notna() & out[col].isna()).sum())
                else:
                    cleaned = out[col].astype('string')
                    changed = (raw.notna() != cleaned.notna()) | (raw.ne(cleaned).fillna(False))
                    record['texto_normalizado'] += int(changed.sum())
            # Esquema explícito incluso en columnas totalmente vacías.
            fields = [pa.field(c, pa.float64() if c in TIPOS[base]["numero"] else
                      pa.timestamp("ns") if c in TIPOS[base]["fecha"] else pa.string())
                      for c in out.columns]
            table = pa.Table.from_pandas(out, schema=pa.schema(fields), preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(temp, table.schema, compression="snappy")
            writer.write_table(table)
            rows += len(out)
        if writer is None:
            raise ValueError(f"CSV sin cabecera válida: {path}")
        writer.close()
        writer = None
        temp.replace(dest)
        marker_tmp = marker.with_suffix(".partial")
        marker_tmp.write_text(json.dumps({"fingerprint": fingerprint, "rows": rows, "metrics": metrics}), encoding="utf-8")
        marker_tmp.replace(marker)
    finally:
        if writer is not None:
            writer.close()
        temp.unlink(missing_ok=True)
    return {"base": base, "status": "written", "path": str(dest), "rows": rows,
            "metrics": metrics, "metrics_origin": "esta_ejecucion"}


def transform_base(base: str, overwrite: bool = False) -> list[dict]:
    src = BRONZE_DIR / base
    files = sorted(src.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No hay CSV en {src}. Ejecute primero la extracción.")
    results = [transform_file(p, base, overwrite=overwrite) for p in files]
    # Retirar particiones que ya no pertenecen al snapshot Bronze activo.
    expected = {p.stem for p in files}
    for old in (SILVER_DIR / base).glob("*.parquet"):
        if old.stem not in expected:
            removed = {'base': base, 'status': 'removed', 'path': str(old),
                       'rows': pq.ParquetFile(old).metadata.num_rows, 'bytes': old.stat().st_size}
            old.unlink()
            old.with_suffix(".parquet.meta.json").unlink(missing_ok=True)
            results.append(removed)
    return results

def transform_all(overwrite: bool = False) -> dict[str, list[dict]]:
    return {base: transform_base(base, overwrite=overwrite) for base in TIPOS}

if __name__ == "__main__":
    transform_all()
