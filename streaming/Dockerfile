FROM apache/spark-py:v3.4.0

USER root
WORKDIR /app

COPY streaming/requirements.txt /app/streaming/requirements.txt
RUN pip install --no-cache-dir -r /app/streaming/requirements.txt

COPY common /app/common
COPY streaming /app/streaming

# Pre-fetch the Kafka connector + transitive jars at build time so `docker
# compose up` doesn't need to hit Maven Central on every fresh container.
RUN /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --conf spark.jars.ivy=/opt/spark/.ivy2 \
    /app/streaming/warm_cache.py || true

ENV SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0
# UDFs (zone_udf, sim_day_udf) run in separate PySpark worker subprocesses
# that don't inherit the driver's sys.path.insert() - they need /app/common
ENV PYTHONPATH=/app/common

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", \
     "--conf", "spark.jars.ivy=/opt/spark/.ivy2", \
     "--conf", "spark.executorEnv.PYTHONPATH=/app/common", \
     "/app/streaming/stream_processor.py"]
