FROM python:3.11-slim

WORKDIR /app

COPY observability/requirements.txt /app/observability/requirements.txt
RUN pip install --no-cache-dir -r /app/observability/requirements.txt

COPY common /app/common
COPY observability /app/observability

CMD ["python", "/app/observability/health_watchdog.py"]
