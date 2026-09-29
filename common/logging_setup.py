"""Structured JSON logging shared by every FleetIQ component.

Every component (simulators, streaming job, batch job, API, watchdog) calls
get_logger(component_name) so all log lines share one shape and can be
correlated across the pipeline by "component" and "sim_day".
"""
import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def __init__(self, component: str):
        super().__init__()
        self.component = component

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "component": self.component,
            "message": record.getMessage(),
        }
        for key in ("sim_day", "vehicle_id", "trip_id", "zone", "alert_type"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(component))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
