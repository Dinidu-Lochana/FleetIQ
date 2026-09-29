"""Postgres connection-string helper shared by every component."""
import os


def get_dsn(dbname_env: str = "POSTGRES_DB", default_db: str = "fleetiq") -> str:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "fleetiq")
    password = os.environ.get("POSTGRES_PASSWORD", "fleetiq")
    dbname = os.environ.get(dbname_env, default_db)
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"
