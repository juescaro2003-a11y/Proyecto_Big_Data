from __future__ import annotations

import argparse
import json
from pathlib import Path

def summarize(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        base: {
            "rows": info["rows"],
            "files": len(info["files"]),
            "hashes": [f.get("sha256") for f in info["files"]],
        }
        for base, info in data["bases"].items()
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("manifest_a")
    ap.add_argument("manifest_b")
    args = ap.parse_args()

    a = summarize(Path(args.manifest_a))
    b = summarize(Path(args.manifest_b))

    all_ok = True
    for base in sorted(set(a) | set(b)):
        same_rows = a.get(base, {}).get("rows") == b.get(base, {}).get("rows")
        same_hashes = a.get(base, {}).get("hashes") == b.get(base, {}).get("hashes")
        ok = same_rows and same_hashes
        all_ok &= ok
        print(
            f"{base:12} filas={'OK' if same_rows else 'DIF'} "
            f"archivos={'OK' if same_hashes else 'DIF'}"
        )

    raise SystemExit(0 if all_ok else 1)

if __name__ == "__main__":
    main()
