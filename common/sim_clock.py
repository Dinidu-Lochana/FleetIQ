"""Shared simulated-clock helper.

One "sim day" = SIM_DAY_SECONDS wall-clock seconds. The pipeline's start
time is written once to Postgres (pipeline_meta.start_time) by whichever
component boots first, and every other component reads it back so they all
agree on the current sim_day. This avoids depending on wall-clock start
order between docker-compose services.
"""
import os
import time

import psycopg2


def get_or_init_start_time(dsn: str, retries: int = 30, delay: float = 2.0) -> float:
    """Return the pipeline's epoch start time, creating it on first call."""
    last_err = None
    for _ in range(retries):
        try:
            with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT value FROM pipeline_meta WHERE key = 'start_time'")
                    row = cur.fetchone()
                    if row:
                        return float(row[0])
                    now = time.time()
                    cur.execute(
                        "INSERT INTO pipeline_meta (key, value) VALUES ('start_time', %s) "
                        "ON CONFLICT (key) DO NOTHING",
                        (str(now),),
                    )
                    conn.commit()
                    cur.execute("SELECT value FROM pipeline_meta WHERE key = 'start_time'")
                    return float(cur.fetchone()[0])
        except psycopg2.OperationalError as exc:
            last_err = exc
            time.sleep(delay)
    raise RuntimeError(f"Could not reach Postgres to establish sim clock: {last_err}")


def sim_day_for(start_time: float, sim_day_seconds: int, at: float | None = None) -> int:
    at = at if at is not None else time.time()
    return int((at - start_time) // sim_day_seconds)


def sim_day_seconds_from_env() -> int:
    return int(os.environ.get("SIM_DAY_SECONDS", "300"))
