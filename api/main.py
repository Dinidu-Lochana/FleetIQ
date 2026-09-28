"""FleetIQ serving layer: read-only FastAPI app over the Postgres store
that both the speed layer (Spark Structured Streaming) and the batch
layer (Airflow + Spark) write into. Also exposes Prometheus-format
metrics and serves the static dashboard.
"""
import os
import sys
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

sys.path.insert(0, "/app/common")
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402
from sim_clock import get_or_init_start_time, sim_day_for, sim_day_seconds_from_env  # noqa: E402

import db  # noqa: E402

log = get_logger("api")
app = FastAPI(title="FleetIQ API")

SIM_DAY_SECONDS = sim_day_seconds_from_env()
START_TIME = get_or_init_start_time(get_dsn())
STALENESS_THRESHOLD_SECONDS = int(os.environ.get("STALENESS_THRESHOLD_SECONDS", "60"))

REQUEST_COUNTER = Counter("fleetiq_api_requests_total", "API requests", ["path"])
TELEMETRY_LAG_GAUGE = Gauge("fleetiq_telemetry_lag_seconds", "Seconds since the last telemetry event was ingested")
UNRESOLVED_ALERTS_GAUGE = Gauge("fleetiq_unresolved_alerts", "Current unresolved alert count")


def current_sim_day() -> int:
    return sim_day_for(START_TIME, SIM_DAY_SECONDS)


@app.get("/health")
def health():
    REQUEST_COUNTER.labels(path="/health").inc()
    components = db.query("SELECT component, last_event_at, status, updated_at FROM pipeline_health")
    last_event = db.query_one("SELECT max(event_ts) AS ts FROM trip_events_raw")
    lag_seconds = None
    if last_event and last_event["ts"]:
        lag_seconds = (datetime.now(last_event["ts"].tzinfo) - last_event["ts"]).total_seconds()
        TELEMETRY_LAG_GAUGE.set(lag_seconds)
    overall = "healthy"
    if lag_seconds is None or lag_seconds > STALENESS_THRESHOLD_SECONDS:
        overall = "degraded"
    return {
        "status": overall,
        "sim_day": current_sim_day(),
        "telemetry_lag_seconds": lag_seconds,
        "staleness_threshold_seconds": STALENESS_THRESHOLD_SECONDS,
        "components": components,
    }


@app.get("/metrics/realtime")
def realtime_metrics():
    REQUEST_COUNTER.labels(path="/metrics/realtime").inc()
    day = current_sim_day()

    # "Live" state is intentionally recency-based (last few minutes) rather
    # than an exact sim_day match: with a short SIM_DAY_SECONDS the sim
    # clock can roll over faster than a zone's next windowed aggregate is
    # computed, which would otherwise make this endpoint go briefly empty
    # right at every day boundary.
    status_counts = db.query(
        "SELECT status, count(*) AS n FROM vehicle_status_latest "
        "WHERE updated_at > now() - interval '5 minutes' GROUP BY status"
    )
    counts = {row["status"]: row["n"] for row in status_counts}
    active_vehicles = sum(counts.values())
    idle_ratio = (counts.get("idle", 0) / active_vehicles) if active_vehicles else 0.0

    latest_windows = db.query(
        """
        SELECT DISTINCT ON (zone) zone, window_start, window_end, active_vehicles,
               idle_count, enroute_count, on_trip_count, trip_count, total_earnings, avg_speed, sim_day
        FROM zone_metrics_windowed
        WHERE window_end > now() - interval '5 minutes'
        ORDER BY zone, window_start DESC
        """
    )

    trips_last_hour = db.query_one(
        "SELECT count(DISTINCT trip_id) AS n FROM trip_events_raw "
        "WHERE event_ts > now() - interval '1 hour' AND trip_id != '-'"
    )

    return {
        "sim_day": day,
        "active_vehicles": active_vehicles,
        "idle_ratio": round(idle_ratio, 3),
        "status_breakdown": counts,
        "trips_last_hour": trips_last_hour["n"] if trips_last_hour else 0,
        "zones": latest_windows,
    }


@app.get("/reports/daily/{sim_day}")
def daily_report(sim_day: int):
    REQUEST_COUNTER.labels(path="/reports/daily").inc()
    rows = db.query(
        """SELECT vehicle_id, total_fare, fuel_cost, maintenance_cost, net_profit,
                  distance_covered, cost_per_km, profitable, generated_at
           FROM daily_profitability_report WHERE sim_day = %s ORDER BY net_profit ASC""",
        (sim_day,),
    )
    unprofitable = [r for r in rows if not r["profitable"]]
    return {
        "sim_day": sim_day,
        "vehicle_count": len(rows),
        "unprofitable_count": len(unprofitable),
        "vehicles": rows,
    }


@app.get("/alerts")
def alerts(include_resolved: bool = False):
    REQUEST_COUNTER.labels(path="/alerts").inc()
    if include_resolved:
        rows = db.query("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 200")
    else:
        rows = db.query("SELECT * FROM alerts WHERE NOT resolved ORDER BY created_at DESC LIMIT 200")
    UNRESOLVED_ALERTS_GAUGE.set(sum(1 for r in rows if not r["resolved"]))
    return {"count": len(rows), "alerts": rows}


@app.get("/metrics/prometheus")
def prometheus_metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.mount("/static", StaticFiles(directory="/app/static"), name="static")


@app.get("/")
def dashboard_root():
    return FileResponse("/app/static/index.html")
