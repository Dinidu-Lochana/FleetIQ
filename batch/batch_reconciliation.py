"""Batch-layer Spark job: daily profitability reconciliation.

Invoked once per simulated day by the Airflow DAG (daily_reconciliation_dag)
after that day's expense CSV has been loaded into vehicle_expenses. Joins
the streaming layer's authoritative vehicle_daily_earnings for that day
against the batch expense feed, computes per-vehicle net profit, and
writes the result both to Postgres (daily_profitability_report, for the
API/dashboard) and as a Parquet snapshot under data/warehouse/ (the
file-system-backed historical record for the batch view, independent of
the operational database).

Runs Spark in local mode - the data volume for one simulated day (one row
per vehicle) is tiny, so this favours simplicity/reliability over cluster
submission, matching the same trade-off made in the streaming job.
"""
import argparse
import sys

sys.path.insert(0, "/opt/airflow/common")
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402
from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

log = get_logger("batch-reconciliation")

WAREHOUSE_DIR = "/opt/airflow/data/warehouse/daily_profitability"

EARNINGS_SCHEMA = T.StructType([
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("total_fare", T.DoubleType()),
    T.StructField("trip_count", T.IntegerType()),
])
EXPENSES_SCHEMA = T.StructType([
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("fuel_cost", T.DoubleType()),
    T.StructField("maintenance_cost", T.DoubleType()),
    T.StructField("distance_covered", T.DoubleType()),
    T.StructField("service_flag", T.BooleanType()),
])


def fetch_rows(sql: str, params: tuple) -> list:
    with psycopg2.connect(get_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def upsert_report(rows: list):
    if not rows:
        return
    with psycopg2.connect(get_dsn()) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO daily_profitability_report
                    (vehicle_id, sim_day, total_fare, fuel_cost, maintenance_cost,
                     net_profit, distance_covered, cost_per_km, profitable)
                   VALUES %s
                   ON CONFLICT (vehicle_id, sim_day) DO UPDATE SET
                       total_fare = EXCLUDED.total_fare,
                       fuel_cost = EXCLUDED.fuel_cost,
                       maintenance_cost = EXCLUDED.maintenance_cost,
                       net_profit = EXCLUDED.net_profit,
                       distance_covered = EXCLUDED.distance_covered,
                       cost_per_km = EXCLUDED.cost_per_km,
                       profitable = EXCLUDED.profitable,
                       generated_at = now()""",
                rows,
            )
        conn.commit()


def run(sim_day: int):
    log.info(f"Starting profitability reconciliation for sim_day={sim_day}")

    earnings_rows = fetch_rows(
        "SELECT vehicle_id, total_fare, trip_count FROM vehicle_daily_earnings WHERE sim_day = %s",
        (sim_day,),
    )
    expense_rows = fetch_rows(
        "SELECT vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag "
        "FROM vehicle_expenses WHERE sim_day = %s",
        (sim_day,),
    )

    if not expense_rows:
        log.warning(f"No expense records found for sim_day={sim_day}; nothing to reconcile yet")
        return 0

    spark = SparkSession.builder.master("local[*]").appName("FleetIQBatchReconciliation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    earnings_df = spark.createDataFrame(earnings_rows, schema=EARNINGS_SCHEMA)
    expenses_df = spark.createDataFrame(expense_rows, schema=EXPENSES_SCHEMA)

    joined = expenses_df.join(earnings_df, on="vehicle_id", how="left").select(
        "vehicle_id",
        F.coalesce(F.col("total_fare"), F.lit(0.0)).alias("total_fare"),
        F.col("fuel_cost"),
        F.col("maintenance_cost"),
        F.col("distance_covered"),
    )

    report = joined.withColumn(
        "net_profit",
        F.col("total_fare") - F.col("fuel_cost") - F.col("maintenance_cost"),
    ).withColumn(
        "cost_per_km",
        F.when(F.col("distance_covered") > 0, (F.col("fuel_cost") + F.col("maintenance_cost")) / F.col("distance_covered")),
    ).withColumn("profitable", F.col("net_profit") > 0)

    rows = report.collect()

    warehouse_path = f"{WAREHOUSE_DIR}/sim_day={sim_day}"
    report.write.mode("overwrite").parquet(warehouse_path)
    log.info(f"Wrote {len(rows)} rows to Parquet at {warehouse_path}")

    db_rows = [
        (r.vehicle_id, sim_day, r.total_fare, r.fuel_cost, r.maintenance_cost,
         r.net_profit, r.distance_covered, r.cost_per_km, r.profitable)
        for r in rows
    ]
    upsert_report(db_rows)
    log.info(f"Upserted {len(db_rows)} rows into daily_profitability_report for sim_day={sim_day}")

    unprofitable = sum(1 for r in rows if not r.profitable)
    log.info(f"sim_day={sim_day}: {unprofitable}/{len(rows)} vehicles unprofitable")

    spark.stop()
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim-day", type=int, required=True)
    args = parser.parse_args()
    run(args.sim_day)
