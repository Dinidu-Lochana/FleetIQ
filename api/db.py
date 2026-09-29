"""Thin Postgres read layer for the serving API. Uses a small connection
pool since FastAPI/uvicorn handles requests concurrently."""
import sys
from contextlib import contextmanager

from psycopg2 import pool
from psycopg2.extras import RealDictCursor

sys.path.insert(0, "/app/common")
from pg import get_dsn  # noqa: E402

_pool = pool.SimpleConnectionPool(1, 10, get_dsn())


@contextmanager
def get_cursor():
    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    finally:
        _pool.putconn(conn)


def query(sql: str, params: tuple = ()) -> list:
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_one(sql: str, params: tuple = ()):
    rows = query(sql, params)
    return rows[0] if rows else None
