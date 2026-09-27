import os
import sys
import time

from pyspark.sql import SparkSession, functions as F, types as T

sys.path.insert(0, "/app/common")
from geo import zone_for  # noqa: E402
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402
from sim_clock import get_or_init_start_time, sim_day_seconds_from_env  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

log = get_logger("stream-processor")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC = os.environ.get("TELEMETRY_TOPIC", "fleet.telemetry")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/app/checkpoints")
SIM_DAY_SECONDS = sim_day_seconds_from_env()
VALID_STATUSES = ("idle", "enroute", "on_trip")

START_TIME = get_or_init_start_time(get_dsn())

EVENT_SCHEMA = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("driver_id", T.StringType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("speed", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("fare", T.DoubleType()),
    T.StructField("event_ts", T.StringType()),
])


@F.udf(T.StringType())
def zone_udf(lat, lon):
    if lat is None or lon is None:
        return None
    return zone_for(lat, lon)


@F.udf(T.IntegerType())
def sim_day_udf(ts):
    if ts is None:
        return None
    return int((ts.timestamp() - START_TIME) // SIM_DAY_SECONDS)


def get_pg_conn():
    for attempt in range(10):
        try:
            return psycopg2.connect(get_dsn())
        except psycopg2.OperationalError as exc:
            log.warning(f"Postgres not ready (attempt {attempt}): {exc}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Postgres from streaming sink")


def build_source_df(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # --- cleaning: drop structurally invalid / out-of-range events ---
    cleaned = (
        parsed.withColumn("event_ts", F.to_timestamp("event_ts"))
        .filter(F.col("vehicle_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("status").isin(*VALID_STATUSES))
        .filter((F.col("lat").between(-90, 90)) & (F.col("lon").between(-180, 180)))
        .filter((F.col("speed") >= 0) & (F.col("speed") <= 200))
        .filter((F.col("fare") >= 0) & (F.col("fare") <= 1000))
    )

    # --- enrichment: zone bucketing + simulated-day derivation ---
    enriched = cleaned.withColumn("zone", zone_udf("lat", "lon")).withColumn(
        "sim_day", sim_day_udf("event_ts")
    )
    return enriched


def sink_events_and_status(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df = batch_df.cache()
    rows = batch_df.collect()
    log.info(f"[events] batch {batch_id}: {len(rows)} cleaned events")

    raw_rows = [
        (r.trip_id, r.driver_id, r.vehicle_id, r.lat, r.lon, r.speed, r.status, r.fare, r.event_ts, r.sim_day, r.zone)
        for r in rows
    ]

    latest_by_vehicle = {}
    for r in rows:
        prev = latest_by_vehicle.get(r.vehicle_id)
        if prev is None or r.event_ts > prev.event_ts:
            latest_by_vehicle[r.vehicle_id] = r
    status_rows = [
        (r.vehicle_id, r.driver_id, r.lat, r.lon, r.speed, r.status, r.zone, r.fare, r.sim_day, r.event_ts)
        for r in latest_by_vehicle.values()
    ]

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO trip_events_raw
                    (trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, event_ts, sim_day, zone)
                   VALUES %s""",
                raw_rows,
            )
            execute_values(
                cur,
                """INSERT INTO vehicle_status_latest
                    (vehicle_id, driver_id, lat, lon, speed, status, zone, last_fare, sim_day, status_since)
                   VALUES %s
                   ON CONFLICT (vehicle_id) DO UPDATE SET
                       driver_id = EXCLUDED.driver_id,
                       lat = EXCLUDED.lat,
                       lon = EXCLUDED.lon,
                       speed = EXCLUDED.speed,
                       status = EXCLUDED.status,
                       zone = EXCLUDED.zone,
                       last_fare = EXCLUDED.last_fare,
                       sim_day = EXCLUDED.sim_day,
                       status_since = CASE WHEN vehicle_status_latest.status = EXCLUDED.status
                                            THEN vehicle_status_latest.status_since
                                            ELSE EXCLUDED.status_since END,
                       updated_at = now()""",
                status_rows,
            )
        conn.commit()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO pipeline_health (component, last_event_at, status, updated_at)
               VALUES ('stream-processor', now(), 'healthy', now())
               ON CONFLICT (component) DO UPDATE SET
                   last_event_at = now(), status = 'healthy', updated_at = now()"""
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    batch_df.unpersist()


def sink_zone_metrics(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    rows = batch_df.collect()
    log.info(f"[zone-metrics] batch {batch_id}: {len(rows)} window rows")
    values = [
        (
            r.zone, r.window.start, r.window.end, r.active_vehicles, r.idle_count,
            r.enroute_count, r.on_trip_count, r.trip_count, r.total_earnings, r.avg_speed, r.sim_day,
        )
        for r in rows
    ]
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO zone_metrics_windowed
                    (zone, window_start, window_end, active_vehicles, idle_count, enroute_count,
                     on_trip_count, trip_count, total_earnings, avg_speed, sim_day)
                   VALUES %s
                   ON CONFLICT (zone, window_start) DO UPDATE SET
                       window_end = EXCLUDED.window_end,
                       active_vehicles = EXCLUDED.active_vehicles,
                       idle_count = EXCLUDED.idle_count,
                       enroute_count = EXCLUDED.enroute_count,
                       on_trip_count = EXCLUDED.on_trip_count,
                       trip_count = EXCLUDED.trip_count,
                       total_earnings = EXCLUDED.total_earnings,
                       avg_speed = EXCLUDED.avg_speed,
                       sim_day = EXCLUDED.sim_day,
                       computed_at = now()""",
                values,
            )
        conn.commit()
    finally:
        conn.close()


def sink_daily_earnings(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    rows = batch_df.collect()
    log.info(f"[daily-earnings] batch {batch_id}: {len(rows)} vehicle-day rows")
    values = [(r.vehicle_id, r.sim_day, r.total_fare, r.trip_count) for r in rows]
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO vehicle_daily_earnings (vehicle_id, sim_day, total_fare, trip_count)
                   VALUES %s
                   ON CONFLICT (vehicle_id, sim_day) DO UPDATE SET
                       total_fare = EXCLUDED.total_fare,
                       trip_count = EXCLUDED.trip_count,
                       updated_at = now()""",
                values,
            )
        conn.commit()
    finally:
        conn.close()


def main():
    spark = (
        SparkSession.builder.appName("FleetIQStreamProcessor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Stream processor starting, sim_day_seconds={SIM_DAY_SECONDS}, start_time={START_TIME}")

    source = build_source_df(spark)

    events_query = (
        source.writeStream.foreachBatch(sink_events_and_status)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/events")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Structured Streaming does not support countDistinct() on streaming
    # DataFrames at all (even windowed); collect_set(...).size gives the
    # same distinct-count semantics (nulls are skipped by collect_set)
    # without hitting that restriction.
    zone_agg = (
        source.withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"), F.col("zone"))
        .agg(
            F.size(F.collect_set("vehicle_id")).alias("active_vehicles"),
            F.size(F.collect_set(F.when(F.col("status") == "idle", F.col("vehicle_id")))).alias("idle_count"),
            F.size(F.collect_set(F.when(F.col("status") == "enroute", F.col("vehicle_id")))).alias("enroute_count"),
            F.size(F.collect_set(F.when(F.col("status") == "on_trip", F.col("vehicle_id")))).alias("on_trip_count"),
            F.size(F.collect_set(F.when(F.col("trip_id") != "-", F.col("trip_id")))).alias("trip_count"),
            F.sum("fare").alias("total_earnings"),
            F.avg("speed").alias("avg_speed"),
        )
        .withColumn("sim_day", sim_day_udf(F.col("window.start")))
    )
    zone_query = (
        zone_agg.writeStream.outputMode("update")
        .foreachBatch(sink_zone_metrics)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/zone_metrics")
        .trigger(processingTime="15 seconds")
        .start()
    )

    earnings_agg = source.groupBy("vehicle_id", "sim_day").agg(
        F.sum("fare").alias("total_fare"),
        F.size(F.collect_set(F.when(F.col("trip_id") != "-", F.col("trip_id")))).alias("trip_count"),
    )
    earnings_query = (
        earnings_agg.writeStream.outputMode("update")
        .foreachBatch(sink_daily_earnings)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/daily_earnings")
        .trigger(processingTime="15 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
