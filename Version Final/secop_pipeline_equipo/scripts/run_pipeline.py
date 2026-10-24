from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flows.secop_flow import secop_flow

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-postgres", action="store_true")
    parser.add_argument("--no-benchmark", action="store_true", help="Obsoleto: no hay benchmark separado; Gold usa dos etapas secuenciales")
    extraction = parser.add_mutually_exclusive_group()
    extraction.add_argument("--refresh-bronze", action="store_true", help="Descarga explícitamente un snapshot nuevo desde la API")
    extraction.add_argument("--reuse-bronze", action="store_true", help="Reutiliza Bronze local (opción predeterminada)")
    parser.add_argument("--start", help="Fecha inicial YYYY-MM-DD")
    parser.add_argument("--cutoff", help="Fecha de corte YYYY-MM-DD")
    parser.add_argument("--dask-workers", type=int, help="Workers de preparación; analítica se configura con DASK_JOIN_WORKERS")
    parser.add_argument("--dask-memory", help="Memoria por worker de preparación, por ejemplo 2GiB")
    args = parser.parse_args()

    if args.no_benchmark:
        print('--no-benchmark es obsoleto; se mantienen las etapas de normalización y analítica contractual.')

    secop_flow(
        load_postgres=not args.no_postgres,
        refresh=args.refresh_bronze, start=args.start, cutoff=args.cutoff,
        dask_workers=args.dask_workers, dask_memory=args.dask_memory,
    )

if __name__ == "__main__":
    main()
