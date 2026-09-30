import csv
import math
import os
import random
import sys
import time

import psycopg2

sys.path.insert(0, "/app/common")
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402
from sim_clock import get_or_init_start_time, sim_day_for, sim_day_seconds_from_env  # noqa: E402

log = get_logger("batch-expense-source")

FLEET_SIZE = int(os.environ.get("FLEET_SIZE", "24"))
LANDING_DIR = os.environ.get("LANDING_DIR", "/app/data/landing")
POLL_SECONDS = 5

FUEL_RATE_PER_KM = 0.35
MAINT_BASE = 1.0
SERVICE_PROBABILITY = 0.05
KM_PER_DEGREE = 111.0  # same approximation used by the telemetry simulator's movement model
MOVING_STATUSES = ("enroute", "on_trip")


def fetch_vehicle_day_stats(day: int) -> dict:
    """vehicle_id -> {trip_count, distance_km}, derived from that day's own
    speed-layer output (vehicle_daily_earnings, trip_events_raw)."""
    stats = {}
    try:
        with psycopg2.connect(get_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT vehicle_id, trip_count FROM vehicle_daily_earnings WHERE sim_day = %s",
                    (day,),
                )
                for vehicle_id, trip_count in cur.fetchall():
                    stats[vehicle_id] = {"trip_count": trip_count, "distance_km": 0.0}

                cur.execute(
                    "SELECT vehicle_id, lat, lon, status FROM trip_events_raw "
                    "WHERE sim_day = %s ORDER BY vehicle_id, event_ts",
                    (day,),
                )
                prev_vehicle = prev_lat = prev_lon = None
                for vehicle_id, lat, lon, status in cur.fetchall():
                    if vehicle_id == prev_vehicle and status in MOVING_STATUSES:
                        d_deg = math.hypot(lat - prev_lat, lon - prev_lon)
                        stats.setdefault(vehicle_id, {"trip_count": 0, "distance_km": 0.0})
                        stats[vehicle_id]["distance_km"] += d_deg * KM_PER_DEGREE
                    prev_vehicle, prev_lat, prev_lon = vehicle_id, lat, lon
    except Exception as exc:  # noqa: BLE001
        log.warning(f"Could not read vehicle stats for day {day}: {exc}")
    return stats


def write_expense_file(day: int):
    stats = fetch_vehicle_day_stats(day)
    path = os.path.join(LANDING_DIR, f"expenses_day{day}.csv")
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"])
        for i in range(1, FLEET_SIZE + 1):
            vehicle_id = f"V{i:03d}"
            distance = stats.get(vehicle_id, {}).get("distance_km", 0.0)
            # small odometer/measurement noise so the figure isn't a bit-exact echo of telemetry
            distance *= random.uniform(0.95, 1.05)
            service_flag = random.random() < SERVICE_PROBABILITY
            fuel_cost = round(distance * FUEL_RATE_PER_KM * random.uniform(0.9, 1.2), 2)
            maintenance_cost = round(MAINT_BASE + (random.uniform(15, 60) if service_flag else random.uniform(0, 2)), 2)
            writer.writerow([vehicle_id, fuel_cost, maintenance_cost, round(distance, 2), service_flag])
    os.replace(tmp_path, path)
    log.info(f"Wrote daily expense file for sim_day={day}", extra={"sim_day": day})


def main():
    os.makedirs(LANDING_DIR, exist_ok=True)
    sim_day_seconds = sim_day_seconds_from_env()
    start_time = get_or_init_start_time(get_dsn())
    last_written_day = -1

    log.info(f"Batch expense source started, sim_day_seconds={sim_day_seconds}")
    while True:
        current_day = sim_day_for(start_time, sim_day_seconds)
        # Once a new sim_day begins, the previous day is "closed" - drop its file.
        day_to_write = current_day - 1
        if day_to_write > last_written_day and day_to_write >= 0:
            write_expense_file(day_to_write)
            last_written_day = day_to_write
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
