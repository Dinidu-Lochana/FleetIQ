# FleetIQ

An end-to-end **Lambda architecture** data pipeline for the **Ride-Hailing Fleet
Operations** use case: live fleet utilization and earnings from streaming GPS
telemetry, reconciled once a day against per-vehicle fuel/maintenance costs
from garages and fuel partners.

> Built for the *Applied Big Data Engineering* mini-project assessment.

## Business question

*What is fleet utilization and earnings by zone right now, and which vehicles
are becoming unprofitable once yesterday's fuel/maintenance costs are factored
in?*

## Architecture

```
                         SPEED LAYER (real-time)
 ┌────────────────────┐    ┌───────┐    ┌──────────────────────────┐
 │ telemetry-producer  │──▶│ Kafka │──▶│ Spark Structured Streaming │
 │ (GPS/status events, │    │ topic │    │  - clean                  │
 │  every few seconds) │    │ fleet.│    │  - enrich (zone, sim_day) │
 └────────────────────┘    │telem. │    │  - 1-min windowed agg      │
                            └───────┘    │  - per-vehicle daily agg   │
                                         └─────────────┬─────────────┘
                                                        │ upsert
                                                        ▼
                                         ┌──────────────────────────┐
                                         │        PostgreSQL         │
                                         │ vehicle_status_latest     │
                                         │ zone_metrics_windowed     │
                                         │ vehicle_daily_earnings    │◀─┐
                                         │ trip_events_raw           │  │
                                         │ daily_profitability_report│  │join
                                         │ vehicle_expenses          │◀─┤
                                         │ alerts / pipeline_health  │  │
                                         └─────────────┬─────────────┘  │
                                                        │ read           │
                         ┌──────────────────────────────┘                │
                         ▼                                               │
              ┌────────────────────┐                     ┌──────────────┴───────────┐
              │   FastAPI serving   │                     │  Spark batch job          │
              │  + dashboard (HTML) │                     │  (join earnings x expenses,│
              │  + /metrics/prometheus                    │   compute net profit)     │
              └────────────────────┘                     └──────────────┬───────────┘
                                                                          │ writes
                                          ┌──────────────────────────────┴──────────┐
                                          │  Postgres (report) + Parquet snapshot    │
                                          │  under data/warehouse/                   │
                                          └───────────────────────────────────────────┘
                                                        ▲
                                                        │ triggers, once per sim day
                         BATCH LAYER          ┌──────────────────┐
 ┌─────────────────┐    (daily, orchestrated) │  Apache Airflow  │
 │  batch-source     │──▶ drops CSV ──────────▶│ wait file -> load │
 │  (fuel/maint CSV,  │   into data/landing/   │ -> reconcile ->   │
 │   once per sim day)│                        │ health check       │
 └─────────────────┘                          └──────────────────┘

              health_watchdog polls Postgres every 30s -> alerts table
              (pipeline staleness, vehicles idle too long)
```

### Why Lambda, not Kappa

The two sources have genuinely different correctness and latency needs:

- **Telemetry** must drive a low-latency *operational* view (active vehicles,
  idle ratio, live earnings by zone). It's append-only and best-effort - a
  dropped or late GPS ping doesn't need to be "fixed", it's just noise in a
  continuous stream.
- **Vehicle expenses** must drive an *authoritative, correctable* daily view.
  If a garage submits a corrected invoice for yesterday, the fix is: re-run
  yesterday's reconciliation batch. That must **not** require replaying or
  reprocessing the live telemetry stream.

A Kappa architecture would force this once-a-day, correctable reconciliation
logic through the same continuous stream-processing engine as the real-time
telemetry path, which only complicates the exact case where reprocessing
matters most (a corrected daily feed) without buying anything, since the
"batch" source here is not itself a replay of history - it's a genuinely
periodic external feed. Lambda's separation - a disposable, best-effort speed
layer and an idempotent, rerunnable batch layer that write into the same
serving store - maps directly onto that distinction.

### Tech stack justification

| Layer | Choice | Why |
|---|---|---|
| Ingestion | **Apache Kafka** (KRaft, single broker, 3 partitions on `fleet.telemetry`, keyed by `vehicle_id`) | Decouples the producer from the stream processor, gives per-vehicle event ordering via partition keying, and is the de-facto standard for exactly this "many small continuous events" shape. |
| Stream processing | **Spark Structured Streaming** (local mode) | Native Kafka source, a proper watermark + tumbling-window API for the "utilization per zone per minute" aggregation, and one engine that can express cleaning, enrichment, and windowed aggregation in the same DataFrame pipeline. |
| Orchestration | **Apache Airflow** | The daily reconciliation is a small DAG of dependent steps (wait for file → load → join/compute → health-check) with retries and a schedule - exactly Airflow's job, and it gives an operator-facing UI to see batch runs succeed/fail. |
| Storage/serving | **PostgreSQL** (primary) **+ Parquet on the file system** (batch snapshot) | Postgres gives the API cheap indexed point/range reads for a live dashboard, which a stream of Parquet files does not. The daily batch report is *also* mirrored to Parquet under `data/warehouse/`, satisfying the "file system with Parquet" storage option and giving a durable, replayable historical record independent of the operational database. |

### Simulated clock

Real time is compressed: **1 simulated day = `SIM_DAY_SECONDS` wall-clock
seconds** (default **300s / 5 minutes**, configurable in `.env`). The
`telemetry-producer` writes `pipeline_meta.start_time` to Postgres once on
boot; every other component (batch source, streaming job, Airflow DAG, API)
reads that same value back and computes `sim_day = floor((now - start_time) /
SIM_DAY_SECONDS)`, so the whole pipeline agrees on "what day it is" without
depending on container start order. The Airflow DAG runs on a
`timedelta(seconds=SIM_DAY_SECONDS)` schedule and always reconciles the sim
day that just closed.

## Repository layout

```
sql/                    Postgres schema (init scripts, run automatically on first boot)
common/                 Shared code: JSON logging, zone bucketing, sim-clock, DSN helper
simulators/
  telemetry_producer.py  Streaming source: per-vehicle GPS/status events -> Kafka
  batch_expense_source.py Daily-batch source: drops a CSV into data/landing/
streaming/
  stream_processor.py     Spark Structured Streaming speed-layer job
batch/
  batch_reconciliation.py Spark batch job: join earnings x expenses -> profitability
airflow/dags/
  daily_reconciliation_dag.py  Batch-layer orchestration
api/
  main.py, db.py           FastAPI serving layer
  static/                  Dependency-free HTML/CSS/JS dashboard
observability/
  health_watchdog.py       Staleness + idle-vehicle alerting loop
data/                    Bind-mounted: landing/ (batch drop zone), warehouse/ (Parquet)
```

## Running it

Requires Docker Desktop (or Docker Engine + Compose v2).

```bash
cp .env.example .env        # adjust SIM_DAY_SECONDS / fleet size if you want
docker compose up -d --build
```

First boot builds five custom images (simulators, streaming, batch/airflow,
api, watchdog) and starts Postgres, Kafka, the two simulators, the Spark
streaming job, Airflow (webserver + scheduler), the API, and the watchdog.
Give it 1-2 minutes for Kafka and Airflow's DB migration to finish; `docker
compose ps` shows health status.

**Note on resources:** this stack runs Kafka + two Spark JVMs (streaming job
+ Airflow's batch job) + Airflow's webserver/scheduler + Postgres
simultaneously. It's a lot for a laptop but is intentionally split into
separate containers per architectural layer, matching how each of these
would actually be deployed. If resources are tight, `SIM_DAY_SECONDS=60` lets
you see a full day cycle (including the Airflow reconciliation) in about a
minute instead of five.

### Where to look

- **Dashboard:** http://localhost:8000 - live zone utilization/earnings,
  alerts, and the daily profitability report (pick a day with the input at
  the top of that panel).
- **API directly:**
  - `GET /metrics/realtime` - active vehicles, idle ratio, trips/hour, earnings by zone
  - `GET /reports/daily/{sim_day}` - per-vehicle profitability reconciliation for that day
  - `GET /alerts` - current unresolved alerts
  - `GET /health` - component heartbeats + telemetry staleness
  - `GET /metrics/prometheus` - Prometheus-format metrics
- **Airflow UI:** http://localhost:8081 (login from `.env`, default
  `admin`/`admin`) - watch `daily_reconciliation_dag` run once per sim day.
- **Postgres directly:**
  ```bash
  docker compose exec postgres psql -U fleetiq -d fleetiq \
    -c "select zone, active_vehicles, total_earnings from zone_metrics_windowed order by window_start desc limit 10;"
  docker compose exec postgres psql -U fleetiq -d fleetiq \
    -c "select * from daily_profitability_report order by net_profit asc limit 10;"
  ```
- **Parquet snapshots:** `data/warehouse/daily_profitability/sim_day=<N>/*.parquet`
  on the host, written by the batch job after each sim day closes.

Stop everything with `docker compose down` (add `-v` to also drop the named
volumes / reset all data).

## Data flow in detail

1. `telemetry-producer` simulates 24 vehicles (`FLEET_SIZE`) cycling through
   `idle -> enroute -> on_trip -> idle`, emitting one event per vehicle every
   `TICK_SECONDS` (default 3s) to Kafka topic `fleet.telemetry`, keyed by
   `vehicle_id`. A configurable fraction of vehicles are kept "chronically
   idle" so the idle-vehicle alert has something to fire on during a demo.
2. `stream_processor.py` reads the topic, **cleans** (drops malformed
   coordinates/speeds/status values), **enriches** (buckets lat/lon into one
   of 9 named zones, derives `sim_day` from event time), then runs two
   streaming queries against the same parsed source:
   - a per-event sink (`trip_events_raw` append + `vehicle_status_latest`
     upsert), and
   - a **windowed aggregation** (1-minute tumbling windows x zone -> active/idle/
     enroute/on_trip counts, trip count, total earnings, avg speed) plus a
     **per-vehicle daily earnings** aggregate that the batch layer later joins
     against.
3. Once a sim day closes, `batch_expense_source.py` drops
   `expenses_day{N}.csv` into `data/landing/`, with each vehicle's
   distance/fuel figures loosely correlated to that vehicle's actual trip
   count for the day (read from `vehicle_daily_earnings`) so the
   reconciliation numbers are meaningful rather than pure noise.
4. `daily_reconciliation_dag` (Airflow) senses that file, loads it into
   `vehicle_expenses`, runs `batch_reconciliation.py` (a small Spark batch
   job that **joins** `vehicle_daily_earnings` against `vehicle_expenses`,
   computes `net_profit` and `cost_per_km`, and writes the result to both
   Postgres and Parquet), then runs a health check that raises an alert if
   the report is empty or more than half the fleet is unprofitable that day.
5. The FastAPI layer and dashboard read all of the above straight out of
   Postgres.

## Observability

- **Structured logging:** every component (`common/logging_setup.py`) emits
  single-line JSON logs (`timestamp`, `level`, `component`, `message`, plus
  contextual fields like `sim_day`/`vehicle_id` where relevant) to stdout, so
  `docker compose logs -f <service>` gives greppable, machine-parseable logs
  per pipeline stage.
- **Metrics:** the API exposes Prometheus-format counters/gauges at
  `/metrics/prometheus` (request counts, telemetry lag, unresolved alert
  count) - point a Prometheus server at it if you want to graph the pipeline
  over time; out of scope here to keep the stack's footprint down for a
  laptop demo.
- **Health/alerting rules** (`observability/health_watchdog.py`, polling
  Postgres every `WATCHDOG_INTERVAL_SECONDS`):
  1. **Pipeline staleness** - no telemetry event ingested in
     `STALENESS_THRESHOLD_SECONDS` (default 60s) -> critical alert.
  2. **Vehicle idle too long** - a vehicle stuck in `idle` for more than
     `IDLE_THRESHOLD_SECONDS` (default 120s) -> warning alert.
  Alerts are deduplicated (one open row per alert type + entity) and
  auto-resolved once the condition clears.
  A third rule runs on the batch side (`health_check_and_alert` task in the
  Airflow DAG): an empty reconciliation report, or more than half the fleet
  showing unprofitable, raises an alert too.
- `/health` reports overall pipeline status plus per-component heartbeats
  (`pipeline_health` table, written by both the streaming job and the
  watchdog).

## Known trade-offs (and why)

- **Spark runs in local mode**, not a standalone cluster with separate
  master/worker services. At this project's data volume (a few dozen
  simulated vehicles) a cluster adds operational complexity with no benefit;
  swapping to `--master spark://...` for horizontal scale is a config change,
  not a redesign.
- **Streaming sinks use `foreachBatch` + `psycopg2` upserts**, not the Spark
  JDBC writer. Every destination table here needs an upsert (latest status,
  windowed aggregates, running daily totals) which plain Spark JDBC doesn't
  support; collecting each small aggregated micro-batch to the driver and
  upserting is simple and reliable at this scale. At production scale this
  would move to a proper upsert-capable sink (Kafka Connect JDBC sink with a
  merge mode, or Debezium-style CDC).
- **`vehicle_daily_earnings` is a plain (non-windowed) streaming aggregation**,
  so Spark never evicts its state - acceptable for a bounded demo run, but a
  long-lived production version would key on a true time window instead of
  the custom `sim_day` column so state gets cleaned up automatically.

## Testing

`docker compose config` validates the compose file. End-to-end verification
was done by bringing the full stack up, confirming Postgres tables populate
from live telemetry within seconds, watching `daily_reconciliation_dag`
complete a full run (file sensed -> loaded -> reconciled -> health-checked)
after one simulated day, confirming a Parquet file lands under
`data/warehouse/`, and forcing a staleness/idle condition to confirm the
watchdog raises and later auto-resolves an alert.
