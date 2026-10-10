from __future__ import annotations

import hashlib
import io
import json
import time
import tempfile
import shutil
import csv
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from .config import (
    APP_TOKEN, BASES, BATCH_SIZE, BRONZE_DIR, COLUMNAS,
    CUTOFF_DATE, OUTPUT_DIR, PORTAL, START_DATE,
)

def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(block_size):
            h.update(chunk)
    return h.hexdigest()

def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)

def month_periods(start=START_DATE, cutoff=CUTOFF_DATE):
    for p in pd.period_range(start, cutoff, freq="M"):
        yield p.year, p.month

class SocrataClient:
    def __init__(self, app_token: str | None = APP_TOKEN, timeout: int = 300):
        self.session = requests.Session()
        self.timeout = timeout
        if app_token:
            self.session.headers.update({"X-App-Token": app_token})

    def request_csv(
        self,
        code: str,
        columns: list[str],
        where: str | None = None,
        limit: int = BATCH_SIZE,
        offset: int = 0,
        retries: int = 4,
    ) -> pd.DataFrame:
        url = f"{PORTAL}/{code}.csv"
        params = {
            "$select": ",".join(columns),
            "$limit": limit,
            "$offset": offset,
            "$order": ":id",
        }
        if where:
            params["$where"] = where

        last = None
        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                return pd.read_csv(
                    io.StringIO(r.text),
                    dtype=str,
                    keep_default_na=False,
                    na_values=[""],
                )
            except Exception as exc:
                last = exc
                if attempt == retries:
                    raise
                time.sleep(5 * attempt)
        raise last

def validate_api(client: SocrataClient | None = None) -> dict[str, bool]:
    client = client or SocrataClient()
    result = {}
    for base, info in BASES.items():
        df = client.request_csv(info["codigo"], COLUMNAS[base], limit=2)
        missing = sorted(set(COLUMNAS[base]) - set(df.columns))
        if missing:
            raise RuntimeError(f"{base}: columnas inexistentes en API: {missing}")
        result[base] = True
    return result

def _download_query_to_file(
    client: SocrataClient,
    code: str,
    columns: list[str],
    destination: Path,
    where: str | None,
    batch_size: int = BATCH_SIZE,
) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return {
            "status": "existing",
            "rows": count_csv_rows(destination),
            "bytes": destination.stat().st_size,
            "sha256": sha256_file(destination),
        }

    temp = destination.with_suffix(destination.suffix + ".partial")
    if temp.exists():
        temp.unlink()

    offset = 0
    total = 0
    wrote_header = False

    while True:
        batch = client.request_csv(
            code, columns, where=where, limit=batch_size, offset=offset
        )
        if batch.empty:
            if not wrote_header:
                pd.DataFrame(columns=columns).to_csv(temp, index=False, encoding="utf-8")
            break

        batch.to_csv(
            temp,
            mode="a",
            header=not wrote_header,
            index=False,
            encoding="utf-8",
        )
        wrote_header = True
        total += len(batch)

        if len(batch) < batch_size:
            break
        offset += batch_size

    temp.replace(destination)
    return {
        "status": "downloaded",
        "rows": total,
        "bytes": destination.stat().st_size,
        "sha256": sha256_file(destination),
    }

def download_month(
    base: str,
    year: int,
    month: int,
    client: SocrataClient | None = None,
    *, bronze_dir: Path = BRONZE_DIR, cutoff=None, lower_bound=None,
) -> dict:
    cutoff = pd.Timestamp(cutoff if cutoff is not None else CUTOFF_DATE)
    if BASES[base]["particion"] != "mensual":
        raise ValueError(f"{base} no es una base mensual")

    client = client or SocrataClient()
    start = pd.Timestamp(year, month, 1)
    if start > cutoff:
        return {"status": "outside_cutoff", "rows": 0}

    end = min(start + pd.offsets.MonthBegin(1), cutoff + pd.Timedelta(days=1))
    start = max(start, pd.Timestamp(lower_bound)) if lower_bound is not None else start
    field = BASES[base]["fecha"]
    where = f"{field} >= '{start:%Y-%m-%d}' AND {field} < '{end:%Y-%m-%d}'"

    dest = bronze_dir / base / f"{base}_{year}-{month:02d}.csv"
    result = _download_query_to_file(
        client, BASES[base]["codigo"], COLUMNAS[base], dest, where
    )
    result.update({"base": base, "partition": f"{year}-{month:02d}", "path": str(dest)})
    return result

def download_catalog(
    base: str,
    client: SocrataClient | None = None,
    batch_size: int = BATCH_SIZE,
    *, bronze_dir: Path = BRONZE_DIR,
) -> list[dict]:
    if BASES[base]["particion"] != "catalogo":
        raise ValueError(f"{base} no es catálogo")

    client = client or SocrataClient()
    out = []
    offset = 0
    part = 0
    folder = bronze_dir / base
    folder.mkdir(parents=True, exist_ok=True)

    while True:
        dest = folder / f"{base}_parte_{part:04d}.csv"

        if dest.exists():
            rows = count_csv_rows(dest)
            out.append({
                "base": base,
                "partition": f"parte_{part:04d}",
                "path": str(dest),
                "status": "existing",
                "rows": rows,
                "bytes": dest.stat().st_size,
                "sha256": sha256_file(dest),
            })
            # Una partición menor al límite es la última.
            if rows < batch_size:
                break
            offset += batch_size
            part += 1
            continue

        batch = client.request_csv(
            BASES[base]["codigo"],
            COLUMNAS[base],
            limit=batch_size,
            offset=offset,
        )
        if batch.empty:
            break

        temp = dest.with_suffix(".csv.partial")
        batch.to_csv(temp, index=False, encoding="utf-8")
        temp.replace(dest)

        out.append({
            "base": base,
            "partition": f"parte_{part:04d}",
            "path": str(dest),
            "status": "downloaded",
            "rows": len(batch),
            "bytes": dest.stat().st_size,
            "sha256": sha256_file(dest),
        })

        if len(batch) < batch_size:
            break
        offset += batch_size
        part += 1

    return out

def extract_base(base: str, client: SocrataClient | None = None, *,
                 bronze_dir: Path = BRONZE_DIR, start=None, cutoff=None) -> list[dict]:
    client = client or SocrataClient()
    start = pd.Timestamp(start if start is not None else START_DATE)
    cutoff = pd.Timestamp(cutoff if cutoff is not None else CUTOFF_DATE)
    if BASES[base]["particion"] == "mensual":
        return [download_month(base, y, m, client=client, bronze_dir=bronze_dir,
                               cutoff=cutoff, lower_bound=start) for y, m in month_periods(start, cutoff)]
    return download_catalog(base, client=client, bronze_dir=bronze_dir)


def extract_all(*, refresh: bool = False, start=None, cutoff=None) -> dict:
    """Reutiliza el snapshot local por defecto; refresh=True descarga uno nuevo.

    La API no ofrece aquí una transacción entre páginas: el manifiesto documenta
    la descarga, pero no garantiza una fotografía atómica del proveedor externo.
    """
    manifest_path = OUTPUT_DIR / "manifest_extraccion.json"
    if not refresh:
        if not manifest_path.exists():
            raise FileNotFoundError("No hay manifiesto local. Para una primera descarga, "
                                    "use refresh=True o --refresh-bronze explícitamente.")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        # Reprocesar conserva el intervalo del snapshot, aunque cambie el día o .env.
        # Fechas explícitas incompatibles fallan sin descargar automáticamente.
        start = pd.Timestamp(start if start is not None else manifest["start_date"]).normalize()
        cutoff = pd.Timestamp(cutoff if cutoff is not None else manifest["cutoff_date"]).normalize()
        if manifest["start_date"] != str(start.date()) or manifest["cutoff_date"] != str(cutoff.date()):
            raise ValueError("El snapshot local tiene otro intervalo; use refresh=True")
        if set(manifest['bases']) != set(BASES):
            raise ValueError('Las bases configuradas cambiaron; refresque la extracción')
        for base, info in manifest["bases"].items():
            if info['source_id'] != BASES[base]['codigo']:
                raise ValueError('Cambió la fuente configurada; refresque la extracción')
            expected_paths = {Path(part['path']).resolve() for part in info['files']}
            actual_paths = {p.resolve() for p in (BRONZE_DIR / base).glob('*.csv')}
            if expected_paths != actual_paths:
                raise ValueError('Los archivos Bronze no coinciden con el manifiesto')
            for part in info["files"]:
                if sha256_file(Path(part["path"])) != part["sha256"]:
                    raise ValueError("Bronze cambió respecto al manifiesto; refresque la extracción")
        return manifest
    start = pd.Timestamp(start if start is not None else START_DATE).normalize()
    cutoff = pd.Timestamp(cutoff if cutoff is not None else
                          (os.getenv("CUTOFF_DATE") or pd.Timestamp.today())).normalize()
    if pd.isna(start) or pd.isna(cutoff) or start > cutoff:
        raise ValueError("El intervalo de extracción debe contener fechas válidas y ordenadas")
    client = SocrataClient()
    validate_api(client)
    manifest = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "start_date": str(start.date()), "cutoff_date": str(cutoff.date()),
                "batch_size": BATCH_SIZE, "bases": {}}
    from .storage import publish_directory
    stage = Path(tempfile.mkdtemp(prefix=".bronze-", dir=BRONZE_DIR.parent))
    try:
        for base in BASES:
            parts = extract_base(base, client, bronze_dir=stage, start=start, cutoff=cutoff)
            (stage / base).mkdir(exist_ok=True)
            for part in parts:
                part["path"] = str(BRONZE_DIR / base / Path(part["path"]).name)
            manifest["bases"][base] = {"source_id": BASES[base]["codigo"],
                "files": parts, "columns": COLUMNAS[base], "rows": int(sum(x.get("rows", 0) for x in parts))}
        # Una copia viaja con los datos; ayuda a recuperar una interrupción al publicar.
        payload = json.dumps(manifest, ensure_ascii=False, indent=2)
        (stage / "manifest.json").write_text(payload, encoding="utf-8")
        publish_directory(stage, BRONZE_DIR, keep_previous=True)
        temp = manifest_path.with_suffix(".json.partial")
        temp.write_text(payload, encoding="utf-8")
        temp.replace(manifest_path)
        return manifest
    finally:
        client.session.close()
        if stage.exists():
            shutil.rmtree(stage)


if __name__ == "__main__":
    extract_all()
