FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyspark==3.4.0 psycopg2-binary==2.9.9

COPY --chown=airflow:root common /opt/airflow/common
COPY --chown=airflow:root batch /opt/airflow/batch
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
