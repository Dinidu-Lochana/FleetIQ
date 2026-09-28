FROM python:3.11-slim

WORKDIR /app

COPY simulators/requirements.txt /app/simulators/requirements.txt
RUN pip install --no-cache-dir -r /app/simulators/requirements.txt

COPY common /app/common
COPY simulators /app/simulators

CMD ["python", "/app/simulators/telemetry_producer.py"]
