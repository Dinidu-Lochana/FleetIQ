"""Observability watchdog: polls Postgres every WATCHDOG_INTERVAL_SECONDS
and enforces two health rules, per the assignment's minimum observability
requirement:

  1. Pipeline staleness - no telemetry event ingested in the last
     STALENESS_THRESHOLD_SECONDS -> critical alert (the speed layer looks
     dead).
  2. Vehicle idle too long - a vehicle stuck in `idle` status for more
     than IDLE_THRESHOLD_SECONDS -> warning alert (fleet utilization
     issue).

Alerts are deduplicated (one unresolved row per alert_type+entity_id) and
auto-resolved once the underlying condition clears, so /alerts always
reflects current state rather than an ever-growing log.
"""
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/app/common")
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402

import psycopg2  # noqa: E402

log = get_logger("watchdog")

WATCHDOG_INTERVAL_SECONDS = int(os.environ.get("WATCHDOG_INTERVAL_SECONDS", "30"))
STALENESS_THRESHOLD_SECONDS = int(os.environ.get("STALENESS_THRESHOLD_SECONDS", "60"))
IDLE_THRESHOLD_SECONDS = int(os.environ.get("IDLE_THRESHOLD_SECONDS", "120"))


def get_conn():
    for attempt in range(30):
        try:
            return psycopg2.connect(get_dsn())
        except psycopg2.OperationalError as exc:
            log.warning(f"Postgres not ready (attempt {attempt}): {exc}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Postgres")


def raise_alert(cur, alert_type: str, severity: str, entity_id: str, message: str):
    cur.execute(
        "SELECT id FROM alerts WHERE alert_type = %s AND entity_id = %s AND NOT resolved",
        (alert_type, entity_id),
    )
    if cur.fetchone():
        return
    cur.execute(
        "INSERT INTO alerts (alert_type, severity, entity_id, message) VALUES (%s, %s, %s, %s)",
        (alert_type, severity, entity_id, message),
    )
    log.warning(message, extra={"alert_type": alert_type})


def resolve_alert(cur, alert_type: str, entity_id: str):
    cur.execute(
        "UPDATE alerts SET resolved = true WHERE alert_type = %s AND entity_id = %s AND NOT resolved",
        (alert_type, entity_id),
    )


def check_staleness(cur):
    cur.execute("SELECT max(event_ts) FROM trip_events_raw")
    (last_ts,) = cur.fetchone()
    if last_ts is None:
        raise_alert(cur, "pipeline_staleness", "critical", "telemetry", "No telemetry events ingested yet")
        return
    age = (datetime.now(last_ts.tzinfo) - last_ts).total_seconds()
    if age > STALENESS_THRESHOLD_SECONDS:
        raise_alert(
            cur, "pipeline_staleness", "critical", "telemetry",
            f"No telemetry ingested in {age:.0f}s (threshold {STALENESS_THRESHOLD_SECONDS}s)",
        )
    else:
        resolve_alert(cur, "pipeline_staleness", "telemetry")


def check_idle_vehicles(cur):
    cur.execute(
        "SELECT vehicle_id, status_since FROM vehicle_status_latest WHERE status = 'idle'"
    )
    idle_now = set()
    for vehicle_id, status_since in cur.fetchall():
        idle_seconds = (datetime.now(status_since.tzinfo) - status_since).total_seconds()
        if idle_seconds > IDLE_THRESHOLD_SECONDS:
            idle_now.add(vehicle_id)
            raise_alert(
                cur, "vehicle_idle", "warning", vehicle_id,
                f"{vehicle_id} has been idle for {idle_seconds:.0f}s (threshold {IDLE_THRESHOLD_SECONDS}s)",
            )

    cur.execute("SELECT DISTINCT entity_id FROM alerts WHERE alert_type = 'vehicle_idle' AND NOT resolved")
    for (entity_id,) in cur.fetchall():
        if entity_id not in idle_now:
            resolve_alert(cur, "vehicle_idle", entity_id)


def heartbeat(cur):
    cur.execute(
        """INSERT INTO pipeline_health (component, last_event_at, status, updated_at)
           VALUES ('watchdog', now(), 'healthy', now())
           ON CONFLICT (component) DO UPDATE SET
               last_event_at = now(), status = 'healthy', updated_at = now()"""
    )


def main():
    log.info(
        f"Watchdog started: staleness_threshold={STALENESS_THRESHOLD_SECONDS}s, "
        f"idle_threshold={IDLE_THRESHOLD_SECONDS}s, interval={WATCHDOG_INTERVAL_SECONDS}s"
    )
    conn = get_conn()
    conn.autocommit = False
    while True:
        try:
            with conn.cursor() as cur:
                check_staleness(cur)
                check_idle_vehicles(cur)
                heartbeat(cur)
            conn.commit()
        except psycopg2.Error as exc:
            log.error(f"Watchdog check failed: {exc}")
            conn.rollback()
        time.sleep(WATCHDOG_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
