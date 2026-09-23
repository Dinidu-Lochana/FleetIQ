import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

sys.path.insert(0, "/app/common")
from geo import CITY_BOUNDS, zone_for  # noqa: E402
from logging_setup import get_logger  # noqa: E402
from pg import get_dsn  # noqa: E402
from sim_clock import get_or_init_start_time, sim_day_for, sim_day_seconds_from_env  # noqa: E402

log = get_logger("telemetry-producer")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC = os.environ.get("TELEMETRY_TOPIC", "fleet.telemetry")
FLEET_SIZE = int(os.environ.get("FLEET_SIZE", "24"))
TICK_SECONDS = float(os.environ.get("TICK_SECONDS", "3"))
CHRONICALLY_IDLE_FRACTION = float(os.environ.get("CHRONICALLY_IDLE_FRACTION", "0.15"))

BASE_FARE = 1.5
PER_KM_RATE = 0.9
PER_MIN_RATE = 0.25
TRIP_SPEED_KMH = (25, 55)
ENROUTE_TICKS_RANGE = (2, 5)
ON_TRIP_TICKS_RANGE = (4, 12)
DISPATCH_PROBABILITY = 0.18


def rand_point():
    lat = random.uniform(CITY_BOUNDS["min_lat"], CITY_BOUNDS["max_lat"])
    lon = random.uniform(CITY_BOUNDS["min_lon"], CITY_BOUNDS["max_lon"])
    return lat, lon


def move_towards(lat, lon, target_lat, target_lon, km_this_tick):
    d_lat = target_lat - lat
    d_lon = target_lon - lon
    dist_deg = math.hypot(d_lat, d_lon)
    deg_per_km = 1 / 111.0
    step_deg = km_this_tick * deg_per_km
    if dist_deg <= step_deg or dist_deg == 0:
        return target_lat, target_lon, True
    frac = step_deg / dist_deg
    return lat + d_lat * frac, lon + d_lon * frac, False


class Vehicle:
    def __init__(self, idx: int):
        self.vehicle_id = f"V{idx:03d}"
        self.driver_id = f"D{idx:03d}"
        self.lat, self.lon = rand_point()
        self.status = "idle"
        self.status_since = time.time()
        self.trip_id = None
        self.target_lat = None
        self.target_lon = None
        self.ticks_remaining = 0
        self.chronically_idle = random.random() < CHRONICALLY_IDLE_FRACTION

    def maybe_dispatch(self):
        if self.chronically_idle:
            return random.random() < DISPATCH_PROBABILITY * 0.1
        return random.random() < DISPATCH_PROBABILITY

    def tick(self):
        fare = 0.0
        speed_kmh = 0.0
        just_completed_trip_id = None

        if self.status == "idle":
            self.lat += random.uniform(-0.0003, 0.0003)
            self.lon += random.uniform(-0.0003, 0.0003)
            if self.maybe_dispatch():
                self.trip_id = f"T{uuid.uuid4().hex[:10]}"
                self.target_lat, self.target_lon = rand_point()
                self.ticks_remaining = random.randint(*ENROUTE_TICKS_RANGE)
                self._set_status("enroute")
            speed_kmh = 0.0

        elif self.status == "enroute":
            speed_kmh = random.uniform(*TRIP_SPEED_KMH)
            km_this_tick = speed_kmh * (TICK_SECONDS / 3600.0)
            self.lat, self.lon, arrived = move_towards(
                self.lat, self.lon, self.target_lat, self.target_lon, km_this_tick
            )
            self.ticks_remaining -= 1
            if arrived or self.ticks_remaining <= 0:
                self.target_lat, self.target_lon = rand_point()
                self.ticks_remaining = random.randint(*ON_TRIP_TICKS_RANGE)
                self._set_status("on_trip")

        elif self.status == "on_trip":
            speed_kmh = random.uniform(*TRIP_SPEED_KMH)
            km_this_tick = speed_kmh * (TICK_SECONDS / 3600.0)
            self.lat, self.lon, arrived = move_towards(
                self.lat, self.lon, self.target_lat, self.target_lon, km_this_tick
            )
            fare = round(PER_KM_RATE * km_this_tick + PER_MIN_RATE * (TICK_SECONDS / 60.0), 3)
            self.ticks_remaining -= 1
            if arrived or self.ticks_remaining <= 0:
                fare += BASE_FARE
                just_completed_trip_id = self.trip_id
                self._set_status("idle")
                self.trip_id = None

        self.lat = min(max(self.lat, CITY_BOUNDS["min_lat"] - 0.01), CITY_BOUNDS["max_lat"] + 0.01)
        self.lon = min(max(self.lon, CITY_BOUNDS["min_lon"] - 0.01), CITY_BOUNDS["max_lon"] + 0.01)

        trip_id_for_event = just_completed_trip_id or self.trip_id

        return {
            "trip_id": trip_id_for_event or "-",
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": round(speed_kmh, 2),
            "status": self.status,
            "fare": fare,
            "event_ts": datetime.now(timezone.utc).isoformat(),
        }

    def _set_status(self, new_status):
        self.status = new_status
        self.status_since = time.time()


def build_producer():
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Kafka not ready yet (attempt %s): %s", attempt, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def main():
    sim_day_seconds = sim_day_seconds_from_env()
    start_time = get_or_init_start_time(get_dsn())
    log.info("Sim clock established", extra={"sim_day": sim_day_for(start_time, sim_day_seconds)})

    producer = build_producer()
    fleet = [Vehicle(i) for i in range(1, FLEET_SIZE + 1)]
    log.info(f"Telemetry producer started for {FLEET_SIZE} vehicles, tick={TICK_SECONDS}s")

    sent = 0
    while True:
        tick_start = time.time()
        for vehicle in fleet:
            event = vehicle.tick()
            producer.send(TOPIC, key=vehicle.vehicle_id, value=event)
            sent += 1
        producer.flush()
        if sent % (FLEET_SIZE * 20) < FLEET_SIZE:
            log.info(f"Emitted {sent} telemetry events so far")
        elapsed = time.time() - tick_start
        time.sleep(max(0.0, TICK_SECONDS - elapsed))


if __name__ == "__main__":
    main()
