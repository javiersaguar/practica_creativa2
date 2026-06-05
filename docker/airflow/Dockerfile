FROM apache/airflow:2.10.4-python3.10
ARG TARGETARCH=amd64
ARG KUBECTL_VERSION=v1.36.1
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io curl ca-certificates \
    && curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" -o /usr/local/bin/kubectl \
    && chmod 0755 /usr/local/bin/kubectl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir iso8601==2.1.0
