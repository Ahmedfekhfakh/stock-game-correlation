FROM python:3.12-slim

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl procps ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/java \
    && curl -fsSL "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz" \
       | tar xz --strip-components=1 -C /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY dags/ /app/dags/
COPY credentials/ /app/credentials/
COPY export.ndjson /app/export.ndjson

ENV PYTHONPATH="/app" \
    AIRFLOW_HOME="/app/airflow_home" \
    AIRFLOW__CORE__LOAD_EXAMPLES="False" \
    AIRFLOW__CORE__DAGS_FOLDER="/app/dags" \
    AIRFLOW__CORE__EXECUTOR="LocalExecutor" \
    PYSPARK_PYTHON="python3"

RUN mkdir -p /app/airflow_home/logs
