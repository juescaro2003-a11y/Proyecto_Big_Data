from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text
from secop_pipeline.db import get_engine, wait_for_postgres

wait_for_postgres()
engine = get_engine()

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('secop', 'gold')
        ORDER BY table_schema, table_name
    """)).all()

for schema, table in rows:
    print(f"{schema}.{table}")
