from __future__ import annotations

import time

import psycopg
from sqlalchemy import create_engine, text

from .config import DB, postgres_url

def get_engine():
    return create_engine(postgres_url(), pool_pre_ping=True)

def wait_for_postgres(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    last = None
    while time.time() < deadline:
        try:
            with psycopg.connect(
                host=DB["host"],
                port=DB["port"],
                dbname=DB["database"],
                user=DB["user"],
                password=DB["password"],
                connect_timeout=5,
            ) as conn:
                conn.execute("SELECT 1")
                return
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError(f"PostgreSQL no respondió en {timeout_seconds}s: {last}")

def create_schemas() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS secop"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
