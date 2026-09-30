-- FleetIQ application schema (database: fleetiq)
-- Loaded once by postgres's docker-entrypoint-initdb.d mechanism.

-- Shared clock so every component (producer, streaming job, batch job, Airflow, API)
-- agrees on when the simulated pipeline started and which "sim day" it is now in.
CREATE TABLE IF NOT EXISTS pipeline_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Raw cleaned/enriched telemetry events (speed layer append log).
CREATE TABLE IF NOT EXISTS trip_events_raw (
    id         BIGSERIAL PRIMARY KEY,
    trip_id    TEXT NOT NULL,
    driver_id  TEXT NOT NULL,
    vehicle_id TEXT NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,
    lon        DOUBLE PRECISION NOT NULL,
    speed      DOUBLE PRECISION NOT NULL,
    status     TEXT NOT NULL,
    fare       DOUBLE PRECISION NOT NULL DEFAULT 0,
    event_ts   TIMESTAMPTZ NOT NULL,
    sim_day    INTEGER NOT NULL,
    zone       TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trip_events_raw_event_ts ON trip_events_raw (event_ts);
CREATE INDEX IF NOT EXISTS idx_trip_events_raw_vehicle ON trip_events_raw (vehicle_id);

-- Latest known status per vehicle (upserted every micro-batch) - drives live map/idle alerts.
CREATE TABLE IF NOT EXISTS vehicle_status_latest (
    vehicle_id TEXT PRIMARY KEY,
    driver_id  TEXT NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,
    lon        DOUBLE PRECISION NOT NULL,
    speed      DOUBLE PRECISION NOT NULL,
    status     TEXT NOT NULL,
    zone       TEXT NOT NULL,
    last_fare  DOUBLE PRECISION NOT NULL DEFAULT 0,
    sim_day    INTEGER NOT NULL,
    status_since TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Windowed per-zone utilization/earnings aggregates (append-only, 1-minute tumbling windows).
CREATE TABLE IF NOT EXISTS zone_metrics_windowed (
    zone            TEXT NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    active_vehicles INTEGER NOT NULL,
    idle_count      INTEGER NOT NULL,
    enroute_count   INTEGER NOT NULL,
    on_trip_count   INTEGER NOT NULL,
    trip_count      INTEGER NOT NULL,
    total_earnings  DOUBLE PRECISION NOT NULL,
    avg_speed       DOUBLE PRECISION NOT NULL,
    sim_day         INTEGER NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (zone, window_start)
);
CREATE INDEX IF NOT EXISTS idx_zone_metrics_window_end ON zone_metrics_windowed (window_end);

-- Authoritative streaming-side per-vehicle daily earnings, feeds the batch reconciliation.
CREATE TABLE IF NOT EXISTS vehicle_daily_earnings (
    vehicle_id   TEXT NOT NULL,
    sim_day      INTEGER NOT NULL,
    total_fare   DOUBLE PRECISION NOT NULL,
    trip_count   INTEGER NOT NULL,
    idle_seconds INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_day)
);

-- Daily-batch expense feed, loaded by Airflow from the dropped CSV file.
CREATE TABLE IF NOT EXISTS vehicle_expenses (
    vehicle_id       TEXT NOT NULL,
    sim_day          INTEGER NOT NULL,
    fuel_cost        DOUBLE PRECISION NOT NULL,
    maintenance_cost DOUBLE PRECISION NOT NULL,
    distance_covered DOUBLE PRECISION NOT NULL,
    service_flag     BOOLEAN NOT NULL DEFAULT false,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_day)
);

-- Batch-layer output: daily profitability reconciliation (also mirrored to Parquet).
CREATE TABLE IF NOT EXISTS daily_profitability_report (
    vehicle_id       TEXT NOT NULL,
    sim_day          INTEGER NOT NULL,
    total_fare       DOUBLE PRECISION NOT NULL,
    fuel_cost        DOUBLE PRECISION NOT NULL,
    maintenance_cost DOUBLE PRECISION NOT NULL,
    net_profit       DOUBLE PRECISION NOT NULL,
    distance_covered DOUBLE PRECISION NOT NULL,
    cost_per_km      DOUBLE PRECISION,
    profitable       BOOLEAN NOT NULL,
    generated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_day)
);

-- Observability: alerts raised by the watchdog / Airflow health-check task.
CREATE TABLE IF NOT EXISTS alerts (
    id         BIGSERIAL PRIMARY KEY,
    alert_type TEXT NOT NULL,
    severity   TEXT NOT NULL,
    entity_id  TEXT,
    message    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved   BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON alerts (alert_type, entity_id) WHERE NOT resolved;

-- Observability: per-component heartbeat, backs the /health endpoint.
CREATE TABLE IF NOT EXISTS pipeline_health (
    component    TEXT PRIMARY KEY,
    last_event_at TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'unknown',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
