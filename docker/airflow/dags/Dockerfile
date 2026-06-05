FROM apache/airflow:2.10.4-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir iso8601
