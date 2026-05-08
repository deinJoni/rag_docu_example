"""Shared psycopg3 connection pool for pipeline workers.

The pool is constructed lazily on first use so that import-time side effects
stay minimal. We use a small pool (default 8) to match the article-download
worker count in the bronze loader.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import psycopg
from psycopg_pool import ConnectionPool

from .env import get_settings

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


@lru_cache(maxsize=1)
def get_pool(min_size: int = 1, max_size: int = 8) -> ConnectionPool:
    """Return a process-wide psycopg3 connection pool to the Supabase database.

    Uses ``DATABASE_URL`` from settings. Callers acquire connections via
    ``with get_pool().connection() as conn:``.
    """
    settings = get_settings()
    pool = ConnectionPool(
        conninfo=settings.database_url,
        min_size=min_size,
        max_size=max_size,
        open=True,
        kwargs={"autocommit": False},
    )
    return pool


def close_pool() -> None:
    """Close the cached pool, if any. Safe to call multiple times."""
    if get_pool.cache_info().currsize:
        get_pool().close()
        get_pool.cache_clear()


def apply_migrations(conn: psycopg.Connection, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply *.sql files in lexical order. DDL is expected to be idempotent."""
    applied: list[str] = []
    for sql_path in sorted(migrations_dir.glob("*.sql")):
        with conn.cursor() as cur:
            cur.execute(sql_path.read_text())
        applied.append(sql_path.name)
    conn.commit()
    return applied
