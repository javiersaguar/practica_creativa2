#!/bin/bash
set -e

KAFKA_HOME=/opt/kafka
CLUSTER_ID="dGhpcy1pcy1teS1rYWZrYS1pZA"

# Configurar listeners
sed -i "s|^#listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093|" \
    $KAFKA_HOME/config/server.properties
sed -i "s|^#advertised.listeners=.*|advertised.listeners=PLAINTEXT://${KAFKA_ADVERTISED_HOST:-localhost}:9092|" \
    $KAFKA_HOME/config/server.properties
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${KAFKA_ADVERTISED_HOST:-localhost}:9092|" \
    $KAFKA_HOME/config/server.properties

$KAFKA_HOME/bin/kafka-storage.sh format \
    --standalone \
    -t $CLUSTER_ID \
    -c $KAFKA_HOME/config/server.properties \
    --ignore-formatted

$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
KAFKA_PID=$!

wait_for_kafka() {
    local timeout=120
    local interval=3
    local elapsed=0

    echo "Esperando a que Kafka acepte conexiones en localhost:9092..."
    while ! $KAFKA_HOME/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; do
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
        if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
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
