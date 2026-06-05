#!/bin/bash
set -e

KAFKA_HOME=/opt/kafka
CLUSTER_ID="dGhpcy1pcy1teS1rYWZrYS1pZA"
SERVER_PROPERTIES="$KAFKA_HOME/config/server.properties"
KAFKA_EXTERNAL_HOST="${KAFKA_ADVERTISED_HOST:-localhost}"
KAFKA_EXTERNAL_PORT="${KAFKA_EXTERNAL_PORT:-9092}"
KAFKA_INTERNAL_PORT="${KAFKA_INTERNAL_PORT:-29092}"
KAFKA_INTERNAL_BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:${KAFKA_INTERNAL_PORT}}"

set_kafka_property() {
    local key="$1"
    local value="$2"
    local key_pattern="${key//./\\.}"

    if grep -qE "^#?${key_pattern}=" "$SERVER_PROPERTIES"; then
        sed -i "s|^#\?${key_pattern}=.*|${key}=${value}|" "$SERVER_PROPERTIES"
    else
        printf '\n%s=%s\n' "$key" "$value" >> "$SERVER_PROPERTIES"
    fi
}

# Configurar listeners
set_kafka_property "listeners" "PLAINTEXT://0.0.0.0:${KAFKA_EXTERNAL_PORT},INTERNAL://0.0.0.0:${KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:9093"
set_kafka_property "advertised.listeners" "PLAINTEXT://${KAFKA_EXTERNAL_HOST}:${KAFKA_EXTERNAL_PORT},INTERNAL://localhost:${KAFKA_INTERNAL_PORT}"
set_kafka_property "listener.security.protocol.map" "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
set_kafka_property "inter.broker.listener.name" "PLAINTEXT"

$KAFKA_HOME/bin/kafka-storage.sh format \
    --standalone \
    -t $CLUSTER_ID \
    -c "$SERVER_PROPERTIES" \
    --ignore-formatted

$KAFKA_HOME/bin/kafka-server-start.sh "$SERVER_PROPERTIES" &
KAFKA_PID=$!

wait_for_kafka() {
    local timeout=120
    local interval=3
    local elapsed=0

    echo "Esperando a que Kafka acepte conexiones en ${KAFKA_INTERNAL_BOOTSTRAP}..."
    while ! $KAFKA_HOME/bin/kafka-broker-api-versions.sh \
        --bootstrap-server "$KAFKA_INTERNAL_BOOTSTRAP" >/dev/null 2>&1; do
        if ! kill -0 "$KAFKA_PID" 2>/dev/null; then
            echo "El proceso Kafka termino antes de estar listo"
            return 1
        fi

        if [ "$elapsed" -ge "$timeout" ]; then
            echo "Kafka no respondio tras ${timeout}s; el broker seguira ejecutandose"
            return 1
        fi

        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    echo "Kafka listo"
    return 0
}

create_topic_with_retry() {
    local topic="$1"
    local attempts=20
    local interval=3
    local i

    for i in $(seq 1 "$attempts"); do
        if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$KAFKA_INTERNAL_BOOTSTRAP" \
            --create --if-not-exists \
            --topic "$topic" \
            --partitions 1 --replication-factor 1; then
            echo "Topic disponible: $topic"
            return 0
        fi

        echo "No se pudo crear $topic (intento $i/$attempts); reintentando..."
        sleep "$interval"
    done

    echo "No se pudo crear $topic; el broker seguira ejecutandose"
    return 1
}

bootstrap_topics() {
    if wait_for_kafka; then
        create_topic_with_retry flight-delay-ml-request || true
        create_topic_with_retry flight-delay-ml-response || true
        echo "Bootstrap de topics finalizado"
    else
        echo "Bootstrap de topics omitido; Kafka continua en ejecucion"
    fi
}

bootstrap_topics &

wait "$KAFKA_PID"
