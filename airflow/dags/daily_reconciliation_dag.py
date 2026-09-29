"""Batch-layer orchestration: once per simulated day, wait for that day's
expense CSV, load it, run the Spark profitability reconciliation job, and
run a health check over the result.

The DAG is scheduled on the same SIM_DAY_SECONDS cadence as the rest of
the pipeline (see common/sim_clock.py) rather than a real calendar
schedule, so "sim_day" - not Airflow's own execution_date - is the unit of
work threaded through every task via XCom.
"""
import csv
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/common")
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402
from sim_clock import get_or_init_start_time, sim_day_for, sim_day_seconds_from_env  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from airflow import DAG  # noqa: E402
from airflow.exceptions import AirflowSkipException  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.sensors.python import PythonSensor  # noqa: E402

log = get_logger("airflow-dag")

SIM_DAY_SECONDS = sim_day_seconds_from_env()
LANDING_DIR = os.environ.get("LANDING_DIR", "/opt/airflow/data/landing")


def _compute_target_day(**context):
    start_time = get_or_init_start_time(get_dsn())
    current_day = sim_day_for(start_time, SIM_DAY_SECONDS)
    target_day = current_day - 1
    if target_day < 0:
        raise AirflowSkipException("No simulated day has closed yet")
    log.info(f"Reconciling sim_day={target_day} (current_day={current_day})")
    return target_day


def _expense_file_path(target_day: int) -> str:
    return os.path.join(LANDING_DIR, f"expenses_day{target_day}.csv")


def _wait_for_file(**context):
    target_day = context["ti"].xcom_pull(task_ids="compute_sim_day")
    return os.path.exists(_expense_file_path(target_day))


def _load_expenses(**context):
    target_day = context["ti"].xcom_pull(task_ids="compute_sim_day")
    path = _expense_file_path(target_day)
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append(
                (
                    r["vehicle_id"],
                    target_day,
                    float(r["fuel_cost"]),
                    float(r["maintenance_cost"]),
                    float(r["distance_covered"]),
                    r["service_flag"].strip().lower() == "true",
                )
            )
    with psycopg2.connect(get_dsn()) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO vehicle_expenses
                    (vehicle_id, sim_day, fuel_cost, maintenance_cost, distance_covered, service_flag)
                   VALUES %s
                   ON CONFLICT (vehicle_id, sim_day) DO UPDATE SET
                       fuel_cost = EXCLUDED.fuel_cost,
                       maintenance_cost = EXCLUDED.maintenance_cost,
                       distance_covered = EXCLUDED.distance_covered,
                       service_flag = EXCLUDED.service_flag,
                       loaded_at = now()""",
                rows,
            )
        conn.commit()
    log.info(f"Loaded {len(rows)} expense rows for sim_day={target_day}")


def _health_check(**context):
    target_day = context["ti"].xcom_pull(task_ids="compute_sim_day")
    with psycopg2.connect(get_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), sum(CASE WHEN NOT profitable THEN 1 ELSE 0 END) "
                "FROM daily_profitability_report WHERE sim_day = %s",
                (target_day,),
            )
            total, unprofitable = cur.fetchone()
            total = total or 0
            unprofitable = unprofitable or 0

            if total == 0:
                cur.execute(
                    """INSERT INTO alerts (alert_type, severity, entity_id, message)
                       VALUES ('reconciliation_missing', 'critical', %s, %s)""",
                    (str(target_day), f"No profitability report rows produced for sim_day={target_day}"),
                )
            elif unprofitable / total > 0.5:
                cur.execute(
                    """INSERT INTO alerts (alert_type, severity, entity_id, message)
                       VALUES ('fleet_profitability', 'warning', %s, %s)""",
                    (
                        str(target_day),
                        f"{unprofitable}/{total} vehicles unprofitable on sim_day={target_day}",
                    ),
                )
            cur.execute(
                """INSERT INTO pipeline_health (component, last_event_at, status, updated_at)
                   VALUES ('airflow-reconciliation', now(), 'healthy', now())
                   ON CONFLICT (component) DO UPDATE SET
                       last_event_at = now(), status = 'healthy', updated_at = now()"""
            )
        conn.commit()
    log.info(f"Health check for sim_day={target_day}: {unprofitable}/{total} unprofitable")


with DAG(
    dag_id="daily_reconciliation_dag",
    description="Batch-layer: load daily vehicle expenses and reconcile fleet profitability",
    schedule_interval=timedelta(seconds=SIM_DAY_SECONDS),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["fleetiq", "batch-layer"],
) as dag:

    compute_sim_day = PythonOperator(
        task_id="compute_sim_day",
        python_callable=_compute_target_day,
    )

    wait_for_batch_file = PythonSensor(
        task_id="wait_for_batch_file",
        python_callable=_wait_for_file,
        poke_interval=10,
        timeout=SIM_DAY_SECONDS * 3,
        mode="reschedule",
    )

    load_expenses_to_postgres = PythonOperator(
        task_id="load_expenses_to_postgres",
        python_callable=_load_expenses,
    )

    run_profitability_reconciliation = BashOperator(
        task_id="run_profitability_reconciliation",
        bash_command=(
            "python /opt/airflow/batch/batch_reconciliation.py "
            "--sim-day {{ ti.xcom_pull(task_ids='compute_sim_day') }}"
        ),
    )

    health_check_and_alert = PythonOperator(
        task_id="health_check_and_alert",
        python_callable=_health_check,
    )

    compute_sim_day >> wait_for_batch_file >> load_expenses_to_postgres
    load_expenses_to_postgres >> run_profitability_reconciliation >> health_check_and_alert
