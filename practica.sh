#!/bin/bash
# ============================================================
#   PRACTICA BIGDATA — SCRIPT UNIFICADO
#   ETSIT UPM 2026
# ============================================================

set -o pipefail

ZONE="${ZONE:-europe-southwest1-a}"
CLUSTER="${CLUSTER:-practica-k8s}"
PROJECT_HOME="${PROJECT_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
MANIFESTS="${MANIFESTS:-$PROJECT_HOME/k8s-gke}"
SPARK_HOME=~/spark-4.1.1
PRACTICA_NONINTERACTIVE="${PRACTICA_NONINTERACTIVE:-0}"
PRACTICA_ASSUME_YES="${PRACTICA_ASSUME_YES:-0}"
IMAGE_TAG="${IMAGE_TAG:-$(git -C "$PROJECT_HOME" rev-parse --short=12 HEAD 2>/dev/null || echo latest)}"
export PROJECT_HOME

# PATH para spark-submit
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# ============================================================
#   FUNCIONES AUXILIARES
# ============================================================

header() {
  echo ""
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo -e "${BOLD}${BLUE}  $1${NC}"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
}

subheader() {
  echo ""
  echo -e "${BOLD}${CYAN}  -- $1 --${NC}"
  echo ""
}

ok()   { echo -e "  ${GREEN}OK${NC} $1"; }
info() { echo -e "  ${CYAN}->${NC} $1"; }
warn() { echo -e "  ${YELLOW}!!${NC} $1"; }
err()  { echo -e "  ${RED}XX${NC} $1"; }
pass_check() { echo -e "  ${GREEN}✓${NC} $1"; }
fail_check() { echo -e "  ${RED}✗${NC} $1"; }

pause() {
  if [ "$PRACTICA_NONINTERACTIVE" = "1" ] || [ ! -t 0 ]; then
    return 0
  fi
  echo ""
  echo -n "  Pulsa ENTER para continuar..."
  read || true
}

wait_until() {
  local timeout_seconds="$1" interval="$2" description="$3"
  shift 3
  local deadline=$((SECONDS + timeout_seconds))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$interval"
  done
  err "Timeout esperando: $description"
  return 1
}

require_docker() {
  command -v docker >/dev/null 2>&1 || {
    err "Docker no esta instalado. Ejecuta ./install.sh --all"
    return 1
  }
  docker info >/dev/null 2>&1 || {
    err "La sesion actual no puede usar Docker sin sudo."
    echo "  Cierra y vuelve a abrir SSH, o ejecuta: newgrp docker"
    return 1
  }
  docker compose version >/dev/null 2>&1 || {
    err "Docker Compose plugin no esta disponible"
    return 1
  }
}

require_gcloud() {
  command -v gcloud >/dev/null 2>&1 || {
    err "gcloud no esta instalado. Ejecuta ./install.sh --all"
    return 1
  }
  gcloud version >/dev/null 2>&1 || {
    err "gcloud esta instalado, pero no funciona en esta sesion"
    return 1
  }
}

ensure_repo_resources() {
  local required
  bash "$PROJECT_HOME/resources/download_data.sh" || return 1

  for required in \
    data/origin_dest_distances.jsonl \
    data/simple_flight_delay_features.jsonl.bz2 \
    docker/spark/Dockerfile \
    docker/spark/download_jars.sh \
    docker/spark/iceberg-spark-runtime.jar \
    docker/spark/flight_prediction_2.13-0.1.jar \
    docker/kafka/Dockerfile \
    docker/kafka/start-kafka.sh \
    shared-jars/flight_prediction_2.13-0.1.jar; do
    if [ ! -s "$PROJECT_HOME/$required" ]; then
      err "Recurso obligatorio ausente: $required"
      return 1
    fi
  done
  cmp -s \
    "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" \
    "$PROJECT_HOME/docker/spark/flight_prediction_2.13-0.1.jar" ||
    { err "Las dos copias del JAR flight_prediction no coinciden"; return 1; }
}

wait_for_airflow() {
  if wait_until 180 5 "base de datos Airflow" docker exec airflow airflow db check &&
     wait_until 180 5 "webserver Airflow" curl -fsS http://localhost:8081/health; then
    return 0
  fi
  warn "Airflow no quedo listo; reiniciando una vez"
  docker compose restart airflow >/dev/null || return 1
  wait_until 180 5 "base de datos Airflow tras restart" docker exec airflow airflow db check &&
    wait_until 180 5 "webserver Airflow tras restart" curl -fsS http://localhost:8081/health
}

active_predictor_driver_id() {
  curl -fsS http://localhost:8080/json/ 2>/dev/null | python3 -c '
import json, sys
data = json.load(sys.stdin)
drivers = [
    d for d in data.get("activedrivers", [])
    if "MakePrediction" in d.get("mainclass", "")
    and d.get("state") == "RUNNING"
    and d.get("worker")
]
print(drivers[0].get("id", "") if drivers else "")
' 2>/dev/null
}

predictor_streams_ready() {
  local driver_id worker count
  driver_id="$(active_predictor_driver_id)"
  [ -n "$driver_id" ] || return 1
  for worker in spark-worker-1 spark-worker-2; do
    count="$(docker exec "$worker" sh -c "grep -c 'Stream started from' /opt/spark/work/$driver_id/stderr 2>/dev/null || true" 2>/dev/null | tail -1)"
    [ "${count:-0}" -ge 4 ] 2>/dev/null && return 0
  done
  return 1
}

kill_predictor_drivers_docker() {
  docker exec -i spark-master python3 - <<'PY' >/dev/null 2>&1
import json
import urllib.request

try:
    with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
        drivers = json.load(response).get("activedrivers", [])
except Exception:
    raise SystemExit(0)

for driver in drivers:
    if "MakePrediction" not in driver.get("mainclass", ""):
        continue
    request = urllib.request.Request(
        "http://spark-master:6066/v1/submissions/kill/" + driver["id"],
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except Exception:
        pass
PY
  local deadline=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ -z "$(active_predictor_driver_id)" ] && return 0
    sleep 3
  done
  return 1
}

kill_practica_drivers_docker() {
  docker exec -i spark-master python3 - <<'PY' >/dev/null 2>&1
import json
import urllib.request

try:
    with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
        drivers = json.load(response).get("activedrivers", [])
except Exception:
    raise SystemExit(0)

for driver in drivers:
    if not any(name in driver.get("mainclass", "") for name in ("MakePrediction", "TrainModel")):
        continue
    request = urllib.request.Request(
        "http://spark-master:6066/v1/submissions/kill/" + driver["id"],
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except Exception:
        pass
PY
  local deadline=$((SECONDS + 120))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS http://localhost:8080/json/ 2>/dev/null | python3 -c '
import json, sys
drivers = json.load(sys.stdin).get("activedrivers", [])
raise SystemExit(1 if any(any(name in d.get("mainclass", "") for name in ("MakePrediction", "TrainModel")) for d in drivers) else 0)
' 2>/dev/null; then
      return 0
    fi
    sleep 3
  done
  return 1
}

run_e2e_docker_check() {
  local uuid result status
  uuid="$(curl -fsS -X POST http://localhost:5001/flights/delays/predict/classify_realtime \
    -d "DepDelay=15&Carrier=AA&FlightDate=2016-12-25&Origin=ATL&Dest=SFO&FlightNum=1234" |
    python3 -c 'import sys,json; print(json.load(sys.stdin).get("id",""))' 2>/dev/null)"
  [ -n "$uuid" ] || return 1
  for _ in $(seq 1 40); do
    result="$(curl -fsS "http://localhost:5001/flights/delays/predict/classify_realtime/response/$uuid" 2>/dev/null || true)"
    status="$(printf "%s" "$result" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status",""))' 2>/dev/null)"
    if [ "$status" = "OK" ]; then
      echo "$result"
      return 0
    fi
    sleep 3
  done
  return 1
}

render_k8s_manifests() {
  local registry="$1" tag="$2" source target
  RENDERED_MANIFESTS="$(mktemp -d /tmp/practica-k8s-rendered.XXXXXX)" || return 1
  for source in "$MANIFESTS"/*.yaml; do
    target="$RENDERED_MANIFESTS/$(basename "$source")"
    sed \
      -e "s#PRACTICA_REGISTRY#$registry#g" \
      -e "s#PRACTICA_TAG#$tag#g" \
      "$source" > "$target" || return 1
  done
  if grep -R "PRACTICA_REGISTRY\|PRACTICA_TAG" "$RENDERED_MANIFESTS" >/dev/null 2>&1; then
    err "Quedaron placeholders sin renderizar en los manifests K8s"
    return 1
  fi
  export RENDERED_MANIFESTS
}

configure_gke_nodeport_firewall() {
  local project_id="$1"
  local rule="${K8S_FIREWALL_RULE:-practica-k8s-nodeports}"
  local source_ranges="${K8S_FIREWALL_SOURCE_RANGES:-0.0.0.0/0}"
  local ports="tcp:30001,tcp:30300,tcp:30502,tcp:30808,tcp:30880,tcp:30901,tcp:30909"
  local node_name tags node_tag="" network vm_public_ip vm_internal_ip vm_ip

  if [ "$source_ranges" != "0.0.0.0/0" ]; then
    vm_public_ip="$(curl -fsS --max-time 5 \
      -H 'Metadata-Flavor: Google' \
      http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip \
      2>/dev/null || curl -fsS --max-time 5 https://ifconfig.me 2>/dev/null || true)"
    vm_internal_ip="$(curl -fsS --max-time 5 \
      -H 'Metadata-Flavor: Google' \
      http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip \
      2>/dev/null || true)"
    for vm_ip in "$vm_public_ip" "$vm_internal_ip"; do
      if [[ "$vm_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] &&
         [[ ",$source_ranges," != *",$vm_ip/32,"* ]]; then
        source_ranges="$source_ranges,$vm_ip/32"
        info "Anadida IP de la VM al firewall para validar NodePorts: $vm_ip/32"
      fi
    done
  fi

  for node_name in $(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}' 2>/dev/null); do
    tags="$(gcloud compute instances describe "$node_name" \
      --zone "$ZONE" --project "$project_id" --format='value(tags.items)' 2>/dev/null || true)"
    node_tag="$(printf '%s\n' "$tags" | tr ';, ' '\n' |
      awk -v prefix="gke-${CLUSTER}-" 'index($0, prefix) == 1 && $0 ~ /-node$/ { print; exit }')"
    [ -n "$node_tag" ] && break
  done

  if [ -z "$node_tag" ]; then
    err "No se pudo detectar el target tag real de los nodos GKE"
    echo "  Diagnostica cada nodo con:"
    echo "  gcloud compute instances describe NODO --zone $ZONE --project $project_id --format='value(tags.items)'"
    return 1
  fi

  network="$(gcloud container clusters describe "$CLUSTER" \
    --zone "$ZONE" --project "$project_id" --format='value(network)' 2>/dev/null || true)"
  network="${network##*/}"
  [ -n "$network" ] || { err "No se pudo detectar la red del cluster GKE"; return 1; }

  info "Configurando firewall NodePort con target tag real: $node_tag"
  if gcloud compute firewall-rules describe "$rule" --project="$project_id" >/dev/null 2>&1; then
    gcloud compute firewall-rules update "$rule" \
      --allow="$ports" \
      --source-ranges="$source_ranges" \
      --target-tags="$node_tag" \
      --project="$project_id" --quiet >/dev/null || {
        err "No se pudo actualizar la regla $rule"
        return 1
      }
  else
    gcloud compute firewall-rules create "$rule" \
      --network="$network" \
      --direction=INGRESS \
      --allow="$ports" \
      --source-ranges="$source_ranges" \
      --target-tags="$node_tag" \
      --project="$project_id" --quiet >/dev/null || {
        err "No se pudo crear la regla $rule"
        return 1
      }
  fi
  ok "Firewall NodePort listo: $rule ($source_ranges -> $node_tag)"
}

verify_k8s_nodeports() {
  local node_ip name url check
  node_ip="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)"
  [ -n "$node_ip" ] || { err "Los nodos GKE no tienen IP externa para verificar NodePorts"; return 1; }

  local checks=(
    "Flask|http://$node_ip:30001/metrics"
    "Spark UI|http://$node_ip:30880/json/"
    "Grafana|http://$node_ip:30300/api/health"
    "MLflow|http://$node_ip:30502/health"
    "Airflow|http://$node_ip:30808/health"
    "MinIO Console|http://$node_ip:30901/"
    "Prometheus|http://$node_ip:30909/-/healthy"
  )
  for check in "${checks[@]}"; do
    name="${check%%|*}"
    url="${check#*|}"
    wait_until 180 5 "NodePort $name ($url)" curl -fsS "$url" || return 1
    ok "NodePort accesible: $name"
  done
}

run_websocket_k8s_check() {
  kubectl exec -i deployment/flask -- python3 - <<'PY'
import threading
import time
import requests
import socketio

received = []
prediction_id = {"value": None}
event = threading.Event()
sio = socketio.Client(reconnection=False, logger=False, engineio_logger=False)

@sio.on("prediction_response")
def on_prediction(data):
    received.append(data)
    event.set()

sio.connect("http://localhost:5001", transports=["websocket"], wait_timeout=20)
if sio.transport() != "websocket":
    raise RuntimeError("Socket.IO no negocio transporte WebSocket")

response = requests.post(
    "http://localhost:5001/flights/delays/predict/classify_realtime",
    data={
        "DepDelay": "15",
        "Carrier": "AA",
        "FlightDate": "2016-12-25",
        "Origin": "ATL",
        "Dest": "SFO",
        "FlightNum": "1234",
    },
    timeout=20,
)
response.raise_for_status()
prediction_id["value"] = response.json()["id"]
deadline = time.monotonic() + 90
matched = None
while time.monotonic() < deadline:
    matched = next((item for item in received if item.get("UUID") == prediction_id["value"]), None)
    if matched:
        break
    event.clear()
    event.wait(min(3, max(0, deadline - time.monotonic())))
if not matched:
    raise RuntimeError("No se recibio prediction_response por WebSocket para el UUID solicitado")
print("WebSocket prediction_response OK", prediction_id["value"], matched.get("Prediction"))
sio.disconnect()
PY
}

confirm_action() {
  local prompt="$1" answer
  if [ "$PRACTICA_ASSUME_YES" = "1" ]; then
    return 0
  fi
  if [ "$PRACTICA_NONINTERACTIVE" = "1" ] || [ ! -t 0 ]; then
    err "La operacion requiere confirmacion. Usa PRACTICA_ASSUME_YES=1."
    return 1
  fi
  read -r -p "  $prompt (s/N): " answer || return 1
  [[ "$answer" =~ ^[sS]$ ]]
}

latest_docker_dag_state() {
  docker exec airflow airflow dags list-runs -d retrain_flight_delay_model -o json 2>/dev/null |
    python3 -c '
import json, sys
runs = json.load(sys.stdin)
runs.sort(key=lambda run: run.get("execution_date", ""), reverse=True)
print(runs[0].get("state", "") if runs else "")
' 2>/dev/null
}

wait_for_docker_dag_success() {
  local deadline=$((SECONDS + 2700)) state
  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(latest_docker_dag_state)"
    case "$state" in
      success) return 0 ;;
      failed) err "El DAG de reentrenamiento termino en failed"; return 1 ;;
      running|queued|"") info "DAG Airflow: ${state:-esperando registro}" ;;
      *) info "DAG Airflow: $state" ;;
    esac
    sleep 10
  done
  err "Timeout esperando el DAG de reentrenamiento Docker"
  return 1
}

k8s_predictor_streams_ready() {
  local spark_pod driver_id worker_pod count
  spark_pod="$(kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [ -n "$spark_pod" ] || return 1
  driver_id="$(kubectl exec "$spark_pod" -- python3 -c '
import json, urllib.request
data = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=5))
drivers = [
    d for d in data.get("activedrivers", [])
    if "MakePrediction" in d.get("mainclass", "")
    and d.get("state") == "RUNNING"
    and d.get("worker")
]
print(drivers[0].get("id", "") if drivers else "")
' 2>/dev/null)"
  [ -n "$driver_id" ] || return 1
  for worker_pod in $(kubectl get pod -l app=spark-worker --field-selector=status.phase=Running -o name 2>/dev/null); do
    count="$(kubectl exec "$worker_pod" -- sh -c "grep -c 'Stream started from' /opt/spark/work/$driver_id/stderr 2>/dev/null || true" 2>/dev/null | tail -1)"
    [ "${count:-0}" -ge 4 ] 2>/dev/null && return 0
  done
  return 1
}

k8s_trainmodel_completed_on_worker() {
  kubectl exec deployment/spark-master -- python3 -c '
import json
import urllib.request
d = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=10))
drivers = [
    x for x in d.get("completeddrivers", [])
    if "TrainModel" in x.get("mainclass", "")
    and x.get("state") == "FINISHED"
    and x.get("worker")
]
assert drivers, "TrainModel no aparece FINISHED con worker asignado"
print("TrainModel cluster:", drivers[0]["id"], drivers[0]["worker"])
'
}

run_e2e_k8s_check() {
  kubectl exec -i deployment/flask -- python3 - <<'PY'
import json
import sys
import time
import requests

base = "http://localhost:5001/flights/delays/predict/classify_realtime"
response = requests.post(base, data={
    "DepDelay": "15",
    "Carrier": "AA",
    "FlightDate": "2016-12-25",
    "Origin": "ATL",
    "Dest": "SFO",
    "FlightNum": "1234",
}, timeout=15)
response.raise_for_status()
prediction_id = response.json()["id"]
for _ in range(40):
    result = requests.get(f"{base}/response/{prediction_id}", timeout=15).json()
    if result.get("status") == "OK":
        print(json.dumps(result, sort_keys=True))
        raise SystemExit(0)
    time.sleep(3)
print("Timeout esperando prediccion", file=sys.stderr)
raise SystemExit(1)
PY
}

verify_k8s_prediction_sinks() {
  local uuid="$1" mongo_count cassandra_hit kafka_hit
  [ -n "$uuid" ] || { err "No se recibio UUID para verificar sinks K8s"; return 1; }

  mongo_count="$(kubectl exec deployment/mongo -- mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments({UUID: '$uuid'})" 2>/dev/null |
    tail -1 | tr -d '[:space:]')"
  [ "${mongo_count:-0}" -gt 0 ] 2>/dev/null ||
    { err "MongoDB K8s no contiene el UUID $uuid"; return 1; }

  cassandra_hit="$(kubectl exec deployment/cassandra -- cqlsh -e \
    "SELECT uuid FROM agile_data_science.flight_delay_classification_response WHERE uuid='$uuid';" \
    2>/dev/null | grep "$uuid" | head -1 || true)"
  [ -n "$cassandra_hit" ] ||
    { err "Cassandra K8s no contiene el UUID $uuid"; return 1; }

  kafka_hit="$(kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --timeout-ms 10000 --max-messages 1000 2>/dev/null |
    grep "$uuid" | tail -1 || true)"
  [ -n "$kafka_hit" ] ||
    { err "Kafka K8s no contiene el UUID $uuid en flight-delay-ml-response"; return 1; }

  ok "Sinks K8s verificados para $uuid: Kafka, Cassandra y MongoDB"
}

latest_k8s_dag_state() {
  local airflow_pod="$1"
  kubectl exec "$airflow_pod" -- airflow dags list-runs -d retrain_flight_delay_model -o json 2>/dev/null |
    python3 -c '
import json, sys
runs = json.load(sys.stdin)
runs.sort(key=lambda run: run.get("execution_date", ""), reverse=True)
print(runs[0].get("state", "") if runs else "")
' 2>/dev/null
}

wait_for_k8s_dag_success() {
  local airflow_pod="$1" deadline=$((SECONDS + 2700)) state
  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(latest_k8s_dag_state "$airflow_pod")"
    case "$state" in
      success) return 0 ;;
      failed) err "El DAG K8s termino en failed"; return 1 ;;
      *) info "DAG Airflow K8s: ${state:-esperando registro}" ;;
    esac
    sleep 10
  done
  err "Timeout esperando el DAG K8s"
  return 1
}

show_urls_docker() {
  IP=$(curl -s ifconfig.me 2>/dev/null || echo "IP_DESCONOCIDA")
  header "URLS DOCKER COMPOSE"
  echo -e "  ${BOLD}Flask (prediccion):${NC}"
  echo -e "    http://$IP:5001/flights/delays/predict_kafka"
  echo ""
  echo -e "  ${BOLD}Spark UI:${NC}           http://$IP:8080"
  echo -e "  ${BOLD}Grafana:${NC}            http://$IP:3000  (admin/admin)"
  echo -e "  ${BOLD}Prometheus:${NC}         http://$IP:9090"
  echo -e "  ${BOLD}MinIO consola:${NC}      http://$IP:9001  (minioadmin/minioadmin)"
  echo -e "  ${BOLD}MLflow:${NC}             http://$IP:5002"
  echo -e "  ${BOLD}Airflow:${NC}            http://$IP:8081  (admin/admin)"
  echo ""
  pause
}

show_urls_k8s() {
  command -v kubectl >/dev/null 2>&1 || { err "kubectl no esta disponible"; return 1; }
  kubectl cluster-info >/dev/null 2>&1 || { err "No hay un cluster Kubernetes accesible"; return 1; }
  NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)
  FLASK_IP=$(kubectl get service flask -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
  [ -n "$NODE_IP" ] || { err "El cluster no tiene una IP externa de nodo disponible"; return 1; }
  header "URLS KUBERNETES (GKE)"
  if [ -n "$FLASK_IP" ]; then
    echo -e "  ${BOLD}Flask (LoadBalancer):${NC}"
    echo -e "    http://$FLASK_IP:5001/flights/delays/predict_kafka"
  else
    echo -e "  ${BOLD}Flask (NodePort):${NC}   http://$NODE_IP:30001/flights/delays/predict_kafka"
  fi
  echo -e "  ${BOLD}Grafana:${NC}            http://$NODE_IP:30300  (admin/admin)"
  echo -e "  ${BOLD}Prometheus:${NC}         http://$NODE_IP:30909"
  echo -e "  ${BOLD}MinIO consola:${NC}      http://$NODE_IP:30901  (minioadmin/minioadmin)"
  echo -e "  ${BOLD}MLflow:${NC}             http://$NODE_IP:30502"
  echo -e "  ${BOLD}Airflow:${NC}            http://$NODE_IP:30808  (admin/admin)"
  echo ""
  pause
}

# ============================================================
#   ARRANQUE DOCKER COMPOSE
# ============================================================

arrancar_docker() {
  header "ARRANCANDO STACK DOCKER COMPOSE"
  cd "$PROJECT_HOME" || return 1
  require_docker || return 1
  ensure_repo_resources || return 1

  info "Compilando JAR Scala si no existe o es antiguo..."
  if [ ! -f "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" ] ||
     [ "$PROJECT_HOME/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/MakePrediction.scala" -nt "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" ] ||
     [ "$PROJECT_HOME/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/TrainModel.scala" -nt "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" ]; then
    info "Recompilando JAR con sbt..."
    docker run --rm \
      -v "$PROJECT_HOME/flight_prediction":/app \
      -w /app \
      sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.10_2.13.18 \
      sbt assembly || return 1
    local built_jar="$PROJECT_HOME/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
    [ -s "$built_jar" ] || { err "sbt no genero $built_jar"; return 1; }
    cp "$built_jar" "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" || return 1
    cp "$built_jar" "$PROJECT_HOME/docker/spark/flight_prediction_2.13-0.1.jar" || return 1
    ok "JAR compilado"
    info "Reconstruyendo imagenes Spark con nuevo JAR..."
    docker compose build spark-master spark-worker-1 spark-worker-2 spark-predictor || return 1
    ok "Imagenes reconstruidas"
  else
    ok "JAR ya existe y esta actualizado"
  fi

  info "Levantando contenedores..."
  docker compose up -d --build || { err "docker compose up --build fallo"; return 1; }

  info "Esperando a que Cassandra este lista..."
  wait_until 300 5 "Cassandra" docker exec cassandra cqlsh -e "describe keyspaces" || return 1
  ok "Cassandra lista"

  info "Esperando Kafka, MinIO, Spark, Flask y Airflow..."
  wait_until 240 5 "Kafka" docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || return 1
  wait_until 180 5 "MinIO" docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc ready local" || return 1
  wait_until 180 5 "Spark UI" curl -fsS http://localhost:8080/json/ || return 1
  wait_until 180 5 "Flask" curl -fsS http://localhost:5001/metrics || return 1
  wait_for_airflow || return 1
  ok "Servicios base listos"

  info "Configurando Airflow..."
  docker exec airflow airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname Admin \
    --role Admin --email admin@example.com 2>/dev/null || true
  docker exec airflow airflow dags unpause retrain_flight_delay_model >/dev/null 2>&1 ||
    { err "No se pudo habilitar el DAG Airflow manual"; return 1; }

  info "Configurando MinIO bucket..."
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/flight-data 2>/dev/null || true" 2>/dev/null

  info "Creando keyspace y tablas en Cassandra..."
  docker exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin TEXT, dest TEXT, distance DOUBLE, PRIMARY KEY (origin, dest));
DROP TABLE IF EXISTS agile_data_science.flight_delay_classification_response;
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_classification_response (
  uuid TEXT PRIMARY KEY,
  origin TEXT,
  dayofweek INT,
  dayofyear INT,
  dayofmonth INT,
  dest TEXT,
  depdelay DOUBLE,
  timestamp TIMESTAMP,
  flightdate DATE,
  carrier TEXT,
  distance DOUBLE,
  route TEXT,
  prediction DOUBLE);" 2>/dev/null || { err "Error creando tablas Cassandra"; return 1; }
  ok "Keyspace creado"

  info "Importando distancias en Cassandra..."
  python3 -c "
import json
lines_cql = []
with open('$PROJECT_HOME/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        lines_cql.append(\"INSERT INTO agile_data_science.origin_dest_distances (origin, dest, distance) VALUES ('{}', '{}', {});\".format(r['Origin'], r['Dest'], float(r['Distance'])))
with open('/tmp/distances.cql', 'w') as f:
    f.write(chr(10).join(lines_cql))
" || { err "Error generando /tmp/distances.cql"; return 1; }
  docker cp /tmp/distances.cql cassandra:/tmp/distances.cql >/dev/null || return 1
  docker exec cassandra cqlsh -f /tmp/distances.cql >/dev/null 2>&1 || { err "Error importando distancias"; return 1; }
  local distance_count
  distance_count="$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/{gsub(/[[:space:]]/,""); print; exit}')"
  [ "$distance_count" = "4696" ] || { err "Cassandra contiene ${distance_count:-0} distancias; esperado 4696"; return 1; }
  ok "Distancias importadas: $distance_count"

  info "Subiendo datos de entrenamiento a MinIO..."
  docker cp "$PROJECT_HOME/data/simple_flight_delay_features.jsonl.bz2" minio:/tmp/ >/dev/null || return 1
  docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc cp /tmp/simple_flight_delay_features.jsonl.bz2 local/flight-data/data/simple_flight_delay_features.jsonl.bz2 >/dev/null" \
    || { err "Error subiendo datos"; return 1; }
  ok "Datos subidos a MinIO"

  info "Deteniendo predictor anterior y limpiando checkpoints..."
  docker compose --profile predictor stop spark-predictor >/dev/null 2>&1 || true
  kill_practica_drivers_docker || { err "No se pudieron detener los drivers Spark anteriores"; return 1; }
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc rm --recursive --force local/flight-data/checkpoints/predictor >/dev/null 2>&1 || true"

  info "Creando tabla Iceberg (2-3 min)..."
  cat > /tmp/load_iceberg.py << 'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("load-iceberg") \
  .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.minio","org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.minio.type","hadoop") \
  .config("spark.sql.catalog.minio.warehouse","s3a://flight-data/warehouse") \
  .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000") \
  .config("spark.hadoop.fs.s3a.access.key","minioadmin") \
  .config("spark.hadoop.fs.s3a.secret.key","minioadmin") \
  .config("spark.hadoop.fs.s3a.path.style.access","true") \
  .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false") \
  .getOrCreate()
df = spark.read.json("s3a://flight-data/data/simple_flight_delay_features.jsonl.bz2")
spark.sql("CREATE NAMESPACE IF NOT EXISTS minio.flights")
spark.sql("DROP TABLE IF EXISTS minio.flights.training_data")
df.writeTo("minio.flights.training_data").create()
print("Registros Iceberg:", spark.table("minio.flights.training_data").count())
spark.stop()
PY
  docker cp /tmp/load_iceberg.py spark-master:/tmp/load_iceberg.py >/dev/null || return 1
  if ! timeout 900 docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.cores.max=2 \
    --conf spark.files.io.connectionTimeout=600s \
    /tmp/load_iceberg.py 2>&1 | tee /tmp/practica_iceberg.log; then
    err "Error creando tabla Iceberg"
    tail -40 /tmp/practica_iceberg.log
    return 1
  fi
  grep -q "Registros Iceberg: 457013" /tmp/practica_iceberg.log ||
    { err "La tabla Iceberg no contiene los 457013 registros esperados"; return 1; }
  ok "Tabla Iceberg creada: 457013 registros"

  info "Entrenando modelo TrainModel en deploy-mode cluster (3-4 min)..."
  if ! timeout 1800 docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --class es.upm.dit.ging.predictor.TrainModel \
    --conf spark.standalone.submit.waitAppCompletion=true \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.cores.max=2 \
    --conf spark.jars.ivy=/home/spark/.ivy2 \
    --conf spark.driverEnv.MLFLOW_TRACKING_URI=http://mlflow:5000 \
    --conf spark.driverEnv.MODEL_BASE_PATH=s3a://flight-data/models \
    --conf spark.driverEnv.TRAINING_TABLE=minio.flights.training_data \
    --conf spark.driverEnv.S3_ENDPOINT=http://minio:9000 \
    --conf spark.driverEnv.AWS_ACCESS_KEY_ID=minioadmin \
    --conf spark.driverEnv.AWS_SECRET_ACCESS_KEY=minioadmin \
    --conf 'spark.driver.extraJavaOptions=-DMLFLOW_TRACKING_URI=http://mlflow:5000 -DTRAINING_TABLE=minio.flights.training_data -DMODEL_BASE_PATH=s3a://flight-data/models -DS3_ENDPOINT=http://minio:9000 -DAWS_ACCESS_KEY_ID=minioadmin -DAWS_SECRET_ACCESS_KEY=minioadmin' \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.minio.type=hadoop \
    --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
    file:///shared-jars/flight_prediction_2.13-0.1.jar 2>&1 | tee /tmp/practica_trainmodel.log; then
    err "Error entrenando el modelo"
    tail -60 /tmp/practica_trainmodel.log
    return 1
  fi
  grep -q "State of driver .* is FINISHED" /tmp/practica_trainmodel.log ||
    { err "Spark no confirmo que TrainModel finalizara"; return 1; }
  ok "Modelo entrenado"

  info "Arrancando Spark predictor en modo cluster..."
  docker compose --profile predictor up -d spark-predictor || return 1
  wait_until 300 5 "los cuatro streams del predictor Spark" predictor_streams_ready || {
    for worker in spark-worker-1 spark-worker-2; do
      docker exec "$worker" sh -c 'D=$(ls -td /opt/spark/work/driver-* 2>/dev/null | head -1); tail -80 "$D/stderr" 2>/dev/null' || true
    done
    return 1
  }
  ok "Predictor listo con cuatro streams activos"

  info "DAG Airflow disponible para reentrenamiento manual"

  info "Ejecutando prediccion end-to-end de validacion..."
  local e2e_result
  e2e_result="$(run_e2e_docker_check)" || { err "La prediccion end-to-end fallo"; return 1; }
  ok "Prediccion end-to-end correcta"
  echo "  $e2e_result"

  ok "Stack Docker listo"
  show_urls_docker
}

# ============================================================
#   ARRANQUE KUBERNETES (GKE)
# ============================================================

arrancar_k8s() {
  header "ARRANCANDO KUBERNETES GKE"

  cd "$PROJECT_HOME" || return 1
  ensure_repo_resources || return 1
  require_gcloud || return 1
  export USE_GKE_GCLOUD_AUTH_PLUGIN=True
  export PATH="/usr/local/bin:/tmp:$PATH"

  info "Asegurando kubectl al inicio..."
  if ! command -v kubectl >/dev/null 2>&1; then
    KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt 2>/dev/null || echo "v1.35.0")
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" -o /tmp/kubectl || {
      err "No se pudo descargar kubectl"
      return 1
    }
    chmod +x /tmp/kubectl
    sudo install -m 755 /tmp/kubectl /usr/local/bin/kubectl 2>/dev/null || export PATH="/tmp:$PATH"
  fi
  ok "kubectl: $(command -v kubectl)"

  info "Asegurando gke-gcloud-auth-plugin..."
  if ! command -v gke-gcloud-auth-plugin >/dev/null 2>&1; then
    cat > /tmp/gke-gcloud-auth-plugin << 'SHIM'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  echo "gke-gcloud-auth-plugin shim 1.0.0"
  exit 0
fi
GCLOUD_BIN="${GCLOUD_BIN:-gcloud}"
TOKEN="$("$GCLOUD_BIN" auth print-access-token)"
EXPIRATION="$("$GCLOUD_BIN" config config-helper --format='value(credential.token_expiry)' 2>/dev/null || true)"
if [[ -n "$EXPIRATION" ]]; then
  printf '{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1beta1","status":{"expirationTimestamp":"%s","token":"%s"}}\n' "$EXPIRATION" "$TOKEN"
else
  printf '{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1beta1","status":{"token":"%s"}}\n' "$TOKEN"
fi
SHIM
    chmod +x /tmp/gke-gcloud-auth-plugin
    sudo install -m 755 /tmp/gke-gcloud-auth-plugin /usr/local/bin/gke-gcloud-auth-plugin 2>/dev/null || export PATH="/tmp:$PATH"
  fi
  gke-gcloud-auth-plugin --version >/dev/null 2>&1 \
    && ok "gke-gcloud-auth-plugin: $(command -v gke-gcloud-auth-plugin)" \
    || { err "gke-gcloud-auth-plugin no funciona"; return 1; }

  PROJECT_ID=$(gcloud config get-value project 2>/dev/null || true)
  if [ -z "$PROJECT_ID" ]; then
    err "gcloud no tiene proyecto configurado"
    echo "  Ejecuta: gcloud config set project PROJECT_ID"
    return 1
  fi

  ACTIVE_ACCOUNT=$(gcloud config get-value account 2>/dev/null || true)
  [ -n "$ACTIVE_ACCOUNT" ] || {
    err "gcloud no tiene una cuenta activa"
    echo "  Ejecuta: gcloud auth login --no-launch-browser"
    return 1
  }
  ok "gcloud activo: $ACTIVE_ACCOUNT / $PROJECT_ID"

  if ! gcloud container clusters describe "$CLUSTER" --zone "$ZONE" --project "$PROJECT_ID" >/dev/null 2>&1; then
    err "La cuenta activa no puede describir el cluster $CLUSTER en $ZONE, o el cluster no existe"
    echo "  Verifica con: gcloud container clusters list --project=$PROJECT_ID"
    echo "  Si recibes PERMISSION_DENIED: gcloud auth login --no-launch-browser"
    echo "  Si no existe, crealo siguiendo la seccion GKE del README"
    return 1
  fi
  if ! gcloud compute firewall-rules list --limit=1 --project="$PROJECT_ID" >/dev/null 2>&1; then
    err "La cuenta activa no puede consultar/administrar reglas de firewall en $PROJECT_ID"
    echo "  Necesita roles/compute.viewer y roles/compute.securityAdmin, o una cuenta autorizada"
    return 1
  fi

  info "Autenticando con el cluster..."
  gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT_ID" || {
    err "No se pudo generar kubeconfig para $CLUSTER"
    return 1
  }
  kubectl cluster-info >/dev/null || {
    err "kubectl no puede conectar con el API server"
    return 1
  }
  ok "Cluster GKE accesible"

	  TARGET_NODES="${K8S_NODE_COUNT:-2}"
  NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | sed '/^$/d' | wc -l)
  if [ "$NODE_COUNT" -lt "$TARGET_NODES" ]; then
    info "Escalando cluster a $TARGET_NODES nodos..."
    if ! gcloud container clusters resize "$CLUSTER" --num-nodes="$TARGET_NODES" --zone "$ZONE" --project "$PROJECT_ID" --quiet; then
      if [ "$TARGET_NODES" -gt "1" ]; then
        warn "No hay cuota para $TARGET_NODES nodos; intentando fallback a 1 nodo"
        TARGET_NODES=1
        gcloud container clusters resize "$CLUSTER" --num-nodes="$TARGET_NODES" --zone "$ZONE" --project "$PROJECT_ID" --quiet || {
          err "No se pudo escalar el cluster ni siquiera a 1 nodo"
          echo "  Abre:"
          echo "  https://console.cloud.google.com/kubernetes/clusters/details/$ZONE/$CLUSTER/nodes?project=$PROJECT_ID"
          echo "  O libera/aumenta cuota regional SSD_TOTAL_GB:"
          echo "  https://console.cloud.google.com/iam-admin/quotas?usage=USED&project=$PROJECT_ID"
          echo "  Despues relanza: ./practica.sh -> opcion 2"
          return 1
        }
      else
        err "No se pudo escalar el cluster"
        echo "  Abre:"
        echo "  https://console.cloud.google.com/kubernetes/clusters/details/$ZONE/$CLUSTER/nodes?project=$PROJECT_ID"
        echo "  O libera/aumenta cuota regional SSD_TOTAL_GB:"
        echo "  https://console.cloud.google.com/iam-admin/quotas?usage=USED&project=$PROJECT_ID"
        echo "  Despues relanza: ./practica.sh -> opcion 2"
        return 1
      fi
    fi
    info "Esperando a que aparezcan $TARGET_NODES nodos..."
    for _ in {1..60}; do
      NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | sed '/^$/d' | wc -l)
      [ "$NODE_COUNT" -ge "$TARGET_NODES" ] && break
      sleep 10
    done
  fi
  kubectl wait --for=condition=Ready nodes --all --timeout=300s || return 1
  NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | sed '/^$/d' | wc -l)
  ok "Cluster con $NODE_COUNT nodos Ready"
  configure_gke_nodeport_firewall "$PROJECT_ID" || return 1

  AR_LOCATION="${ZONE%-*}"
  REGISTRY="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/practica"
  info "Verificando repositorio Artifact Registry..."
  gcloud artifacts repositories describe practica --location="$AR_LOCATION" --project="$PROJECT_ID" >/dev/null 2>&1 ||
    gcloud artifacts repositories create practica \
      --project="$PROJECT_ID" \
      --repository-format=docker \
      --location="$AR_LOCATION" \
      --description="Imagenes Docker de la practica Big Data" || {
        err "No se pudo crear/verificar Artifact Registry practica"
        return 1
      }

  require_docker || return 1
  if command -v docker >/dev/null 2>&1; then
    info "Publicando imagenes K8s en Artifact Registry..."
    gcloud auth configure-docker "${AR_LOCATION}-docker.pkg.dev" --quiet >/dev/null || return 1
    docker build \
      -t "$REGISTRY/spark-master:$IMAGE_TAG" \
      -t "$REGISTRY/spark-worker:$IMAGE_TAG" \
      -t "$REGISTRY/spark-predictor:$IMAGE_TAG" \
      "$PROJECT_HOME/docker/spark" \
      && docker push "$REGISTRY/spark-master:$IMAGE_TAG" \
      && docker push "$REGISTRY/spark-worker:$IMAGE_TAG" \
      && docker push "$REGISTRY/spark-predictor:$IMAGE_TAG" \
      && ok "Imagenes Spark publicadas con el JAR actual" \
      || { err "Error publicando imagenes Spark"; return 1; }

    docker build -t "$REGISTRY/kafka:$IMAGE_TAG" "$PROJECT_HOME/docker/kafka" \
      && docker push "$REGISTRY/kafka:$IMAGE_TAG" \
      && ok "Imagen Kafka publicada" \
      || { err "Error publicando imagen Kafka"; return 1; }

    docker build -f "$PROJECT_HOME/docker/flask/Dockerfile" -t "$REGISTRY/flask:$IMAGE_TAG" "$PROJECT_HOME" \
      && docker push "$REGISTRY/flask:$IMAGE_TAG" \
      && ok "Imagen Flask publicada" \
      || { err "Error publicando imagen Flask"; return 1; }

    docker build -t "$REGISTRY/airflow:$IMAGE_TAG" "$PROJECT_HOME/docker/airflow" \
      && docker push "$REGISTRY/airflow:$IMAGE_TAG" \
      && ok "Imagen Airflow con kubectl publicada" \
      || { err "Error publicando imagen Airflow"; return 1; }
  fi

  render_k8s_manifests "$REGISTRY" "$IMAGE_TAG" || return 1
  ok "Manifests renderizados para $REGISTRY con tag $IMAGE_TAG"

  info "Creando ConfigMap del DAG de Airflow..."
  kubectl create configmap airflow-dags \
    --from-file=retrain_model.py="$PROJECT_HOME/docker/airflow/dags/retrain_model.py" \
    --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null \
    && ok "ConfigMap airflow-dags creado" || { err "Error creando ConfigMap"; return 1; }

  info "Aplicando manifests base..."
  for manifest in mongo cassandra minio kafka spark flask prometheus grafana mlflow airflow; do
    kubectl apply -f "$RENDERED_MANIFESTS/$manifest.yaml" || return 1
  done

  info "Esperando deployments base..."
  for deployment in mongo minio kafka spark-master spark-worker mlflow flask airflow prometheus grafana; do
    kubectl rollout status "deployment/$deployment" --timeout=300s ||
      { err "Deployment $deployment no quedo listo"; kubectl get pods -o wide; return 1; }
  done
  if [ "$NODE_COUNT" -ge 2 ]; then
    SPARK_WORKER_NODE_COUNT="$(kubectl get pods -l app=spark-worker \
      -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null |
      sed '/^$/d' | sort -u | wc -l)"
    [ "$SPARK_WORKER_NODE_COUNT" -ge 2 ] ||
      { err "Los dos spark-worker no quedaron distribuidos entre nodos GKE"; kubectl get pods -l app=spark-worker -o wide; return 1; }
    ok "Spark workers distribuidos entre $SPARK_WORKER_NODE_COUNT nodos GKE"
  else
    warn "Cluster con un solo nodo: deploy-mode cluster funciona, pero no demuestra distribucion entre nodos GKE"
  fi

  info "Deteniendo predictor existente antes del bootstrap de datos..."
  kubectl scale deployment/spark-predictor --replicas=0 2>/dev/null || true

  info "Configurando MinIO y subiendo datos..."
  kubectl wait --for=condition=Ready pod -l app=minio --timeout=180s ||
    { err "MinIO no quedo Ready"; return 1; }
  MINIO_POD=$(kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  kubectl exec "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc mb -p local/flight-data 2>/dev/null || true"
  kubectl exec -i "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc pipe local/flight-data/data/simple_flight_delay_features.jsonl.bz2" \
    < "$PROJECT_HOME/data/simple_flight_delay_features.jsonl.bz2" ||
    { err "No se pudieron cargar los datos de entrenamiento en MinIO"; return 1; }
  if [ -d "$PROJECT_HOME/models" ]; then
    find "$PROJECT_HOME/models" -type f | while IFS= read -r f; do
      REL="${f#$PROJECT_HOME/models/}"
      kubectl exec -i "$MINIO_POD" -- sh -c \
        "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc pipe \"local/flight-data/models/$REL\"" \
        < "$f" >/dev/null 2>&1 || true
    done
  fi
  kubectl exec "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc rm --recursive --force local/flight-data/checkpoints/predictor 2>/dev/null || true"
  ok "MinIO listo con datos de entrenamiento"

  info "Esperando Cassandra lista..."
  kubectl wait --for=condition=Ready pod -l app=cassandra --timeout=240s ||
    { err "Cassandra no quedo Ready"; return 1; }
  sleep 10

  info "Creando keyspace y tablas en Cassandra..."
  kubectl exec deployment/cassandra -- cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS agile_data_science.origin_dest_distances (
  origin TEXT, dest TEXT, distance DOUBLE, PRIMARY KEY (origin, dest));
DROP TABLE IF EXISTS agile_data_science.flight_delay_classification_response;
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_classification_response (
  uuid TEXT PRIMARY KEY,
  origin TEXT,
  dayofweek INT,
  dayofyear INT,
  dayofmonth INT,
  dest TEXT,
  depdelay DOUBLE,
  timestamp TIMESTAMP,
  flightdate DATE,
  carrier TEXT,
  distance DOUBLE,
  route TEXT,
  prediction DOUBLE);" \
    && ok "Keyspace y tablas creados" || { err "Error creando tablas Cassandra"; return 1; }

  info "Importando distancias en Cassandra..."
  python3 << 'PYEOF'
import json, os
lines = []
with open(os.environ.get('PROJECT_HOME', '.') + '/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        origin = r['Origin']
        dest = r['Dest']
        distance = float(r['Distance'])
        lines.append(f"INSERT INTO agile_data_science.origin_dest_distances (origin, dest, distance) VALUES ('{origin}', '{dest}', {distance});")
with open('/tmp/distances_k8s.cql', 'w') as f:
    f.write('\n'.join(lines))
print(f'Generadas {len(lines)} sentencias')
PYEOF
  kubectl exec -i deployment/cassandra -- cqlsh < /tmp/distances_k8s.cql \
    && ok "Distancias importadas" || { err "Error importando distancias"; return 1; }
  K8S_DISTANCE_COUNT=$(kubectl exec deployment/cassandra -- cqlsh -e \
    "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null |
    awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ { gsub(/[[:space:]]/, ""); print; exit }')
  [ "$K8S_DISTANCE_COUNT" = "4696" ] ||
    { err "Cassandra K8s contiene ${K8S_DISTANCE_COUNT:-0} distancias; esperado 4696"; return 1; }
  ok "Distancias verificadas: 4696"

  info "Verificando JAR en Spark..."
  kubectl wait --for=condition=Ready pod -l app=spark-master --timeout=180s
  SPARK_POD=$(kubectl get pod -l app=spark-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  kubectl exec "$SPARK_POD" -- test -f /app/jars/flight_prediction_2.13-0.1.jar \
    && ok "JAR Scala presente en /app/jars" \
    || { err "No se encuentra el JAR en /app/jars"; return 1; }

  info "Deteniendo drivers Spark activos antes del bootstrap..."
  kubectl exec -i "$SPARK_POD" -- python3 <<'PYKILL'
import json, time, urllib.request

MASTER_JSON = "http://spark-master:8080/json/"
KILL_URL = "http://spark-master:6066/v1/submissions/kill/"

def get_master():
    with urllib.request.urlopen(MASTER_JSON, timeout=10) as r:
        return json.load(r)

def kill_driver(driver_id):
    req = urllib.request.Request(KILL_URL + driver_id, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("kill", driver_id, r.read().decode("utf-8", "ignore"))
    except Exception as exc:
        print("kill failed", driver_id, exc)

for d in get_master().get("activedrivers", []):
    mainclass = d.get("mainclass", "")
    if "MakePrediction" in mainclass or "TrainModel" in mainclass:
        print("Stopping", d["id"], mainclass, d.get("state"))
        kill_driver(d["id"])

for _ in range(30):
    time.sleep(2)
    busy = [
        d for d in get_master().get("activedrivers", [])
        if "MakePrediction" in d.get("mainclass", "") or "TrainModel" in d.get("mainclass", "")
    ]
    if not busy:
        print("Spark drivers cleared")
        break
    print("Waiting:", [(d.get("id"), d.get("state")) for d in busy])
else:
    raise RuntimeError("Spark drivers did not stop in time")
PYKILL

  info "Creando tabla Iceberg en MinIO..."
  cat > /tmp/LoadIcebergK8s.java << 'JAVA'
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadIcebergK8s {
  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession.builder()
      .appName("load-iceberg-k8s")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.minio.type", "hadoop")
      .config("spark.sql.catalog.minio.warehouse", "s3a://flight-data/warehouse")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate();

    Dataset<Row> df = spark.read().json("s3a://flight-data/data/simple_flight_delay_features.jsonl.bz2");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS minio.flights");
    spark.sql("DROP TABLE IF EXISTS minio.flights.training_data");
    df.writeTo("minio.flights.training_data").create();
    System.out.println("Registros Iceberg: " + spark.table("minio.flights.training_data").count());
    spark.stop();
  }
}
JAVA
  kubectl cp /tmp/LoadIcebergK8s.java "$SPARK_POD":/tmp/LoadIcebergK8s.java
  kubectl exec "$SPARK_POD" -- bash -lc \
    "rm -rf /tmp/load_iceberg_classes /tmp/load-iceberg-k8s.jar && mkdir -p /tmp/load_iceberg_classes && javac -cp '/opt/spark/jars/*' -d /tmp/load_iceberg_classes /tmp/LoadIcebergK8s.java && jar cf /tmp/load-iceberg-k8s.jar -C /tmp/load_iceberg_classes ." \
    || { err "Error compilando helper Iceberg"; return 1; }
  kubectl cp "$SPARK_POD":/tmp/load-iceberg-k8s.jar /tmp/load-iceberg-k8s.jar \
    || { err "Error copiando helper Iceberg desde spark-master"; return 1; }
  kubectl cp /tmp/load-iceberg-k8s.jar "$SPARK_POD":/tmp/load-iceberg-k8s.jar \
    || { err "Error copiando helper Iceberg a spark-master"; return 1; }
  for WORKER_POD in $(kubectl get pod -l app=spark-worker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}' 2>/dev/null); do
    kubectl cp /tmp/load-iceberg-k8s.jar "$WORKER_POD":/tmp/load-iceberg-k8s.jar
  done
	  if kubectl exec "$SPARK_POD" -- /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --conf spark.standalone.submit.waitAppCompletion=true \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.cores.max=2 \
    --conf spark.jars.ivy=/home/spark/.ivy2 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.minio.type=hadoop \
    --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
    --class LoadIcebergK8s \
    file:///tmp/load-iceberg-k8s.jar 2>&1 | tee /tmp/practica_k8s_iceberg.log; then
    ICEBERG_DRIVER_ID="$(awk '/Driver successfully submitted as/ { print $NF }' /tmp/practica_k8s_iceberg.log | tail -1)"
    ICEBERG_COUNT_LINE=""
    if [ -n "$ICEBERG_DRIVER_ID" ]; then
      for WORKER_POD in $(kubectl get pod -l app=spark-worker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}' 2>/dev/null); do
        ICEBERG_COUNT_LINE="$(kubectl exec "$WORKER_POD" -- bash -lc \
          "grep -h 'Registros Iceberg:' '/opt/spark/work/$ICEBERG_DRIVER_ID/stdout' '/opt/spark/work/$ICEBERG_DRIVER_ID/stderr' 2>/dev/null | tail -1" 2>/dev/null || true)"
        [ -n "$ICEBERG_COUNT_LINE" ] && break
      done
    fi
    if [ -z "$ICEBERG_COUNT_LINE" ]; then
      ICEBERG_COUNT_LINE="$(grep 'Registros Iceberg:' /tmp/practica_k8s_iceberg.log 2>/dev/null | tail -1 || true)"
    fi
    printf '%s\n' "$ICEBERG_COUNT_LINE" | grep -q "Registros Iceberg: 457013" ||
      { err "La tabla Iceberg K8s no contiene los 457013 registros esperados"; return 1; }
    ok "Tabla Iceberg K8s creada: 457013 registros"
  else
    err "Error creando tabla Iceberg"
    return 1
  fi

  info "Entrenando modelo TrainModel en K8s (deploy-mode cluster)..."
  kubectl exec "$SPARK_POD" -- bash -lc '
MLFLOW_TRACKING_URI=http://mlflow:5000 \
MODEL_BASE_PATH=s3a://flight-data/models \
TRAINING_TABLE=minio.flights.training_data \
S3_ENDPOINT=http://minio:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --class es.upm.dit.ging.predictor.TrainModel \
  --conf spark.standalone.submit.waitAppCompletion=true \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --conf spark.driverEnv.MLFLOW_TRACKING_URI=http://mlflow:5000 \
  --conf spark.driverEnv.MODEL_BASE_PATH=s3a://flight-data/models \
  --conf spark.driverEnv.TRAINING_TABLE=minio.flights.training_data \
  --conf spark.driverEnv.S3_ENDPOINT=http://minio:9000 \
  --conf spark.driverEnv.AWS_ACCESS_KEY_ID=minioadmin \
  --conf spark.driverEnv.AWS_SECRET_ACCESS_KEY=minioadmin \
  --conf "spark.driver.extraJavaOptions=-DMLFLOW_TRACKING_URI=http://mlflow:5000 -DTRAINING_TABLE=minio.flights.training_data -DMODEL_BASE_PATH=s3a://flight-data/models -DS3_ENDPOINT=http://minio:9000 -DAWS_ACCESS_KEY_ID=minioadmin -DAWS_SECRET_ACCESS_KEY=minioadmin" \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
  file:///app/jars/flight_prediction_2.13-0.1.jar
' \
    && ok "Modelo entrenado y registrado en MLflow" || { err "Error entrenando modelo"; return 1; }
  k8s_trainmodel_completed_on_worker ||
    { err "No se pudo demostrar TrainModel K8s en deploy-mode cluster"; return 1; }
  ok "TrainModel K8s confirmado en worker"

  K8S_MODEL_COUNT=$(kubectl exec "$MINIO_POD" -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc ls local/flight-data/models/" 2>/dev/null | wc -l)
  [ "$K8S_MODEL_COUNT" -ge 7 ] ||
    { err "MinIO K8s solo contiene ${K8S_MODEL_COUNT:-0} componentes de modelo; esperado al menos 7"; return 1; }
  ok "Modelos verificados en MinIO K8s: $K8S_MODEL_COUNT componentes"

  info "Arrancando Spark predictor en K8s..."
  kubectl scale deployment/spark-predictor --replicas=0 2>/dev/null || true
  kubectl exec "$SPARK_POD" -- python3 <<'PY'
import json
import time
import urllib.request

MASTER_JSON = "http://spark-master:8080/json/"
KILL_URL = "http://spark-master:6066/v1/submissions/kill/"

def get_master():
    with urllib.request.urlopen(MASTER_JSON, timeout=10) as r:
        return json.load(r)

def kill_driver(driver_id):
    req = urllib.request.Request(KILL_URL + driver_id, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        print(r.read().decode("utf-8", "ignore"))

for d in get_master().get("activedrivers", []):
    if "MakePrediction" in d.get("mainclass", ""):
        print("Stopping old predictor driver", d["id"], d.get("state"))
        kill_driver(d["id"])

for _ in range(30):
    time.sleep(2)
    active = [
        d for d in get_master().get("activedrivers", [])
        if "MakePrediction" in d.get("mainclass", "")
    ]
    if not active:
        print("Predictor drivers cleared")
        break
    print("Waiting:", [(d.get("id"), d.get("state")) for d in active])
else:
    raise RuntimeError("Predictor drivers did not stop in time")
PY
  kubectl apply -f "$RENDERED_MANIFESTS/spark-predictor-patch.yaml" || return 1
  kubectl scale deployment/spark-predictor --replicas=1 || return 1
  kubectl rollout status deployment/spark-predictor --timeout=300s || return 1
  wait_until 420 5 "los cuatro streams del predictor K8s" k8s_predictor_streams_ready || return 1
  PREDICTOR_POD=$(kubectl get pod -l app=spark-predictor --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -z "$PREDICTOR_POD" ]; then
    err "spark-predictor no esta Running"
    kubectl get pods -o wide
    return 1
  fi
  ASSERT_ERR=$(kubectl logs "$PREDICTOR_POD" --tail=200 2>/dev/null | grep -c "AssertionError\|Decision Tree load failed\|Exception" 2>/dev/null || true)
  if [ "$ASSERT_ERR" -gt "0" ]; then
    warn "El predictor tiene errores en logs recientes"
    kubectl logs "$PREDICTOR_POD" --tail=120
  else
    ok "Predictor K8s arrancado correctamente"
  fi

  info "Configurando Airflow (kubectl + DAG manual)..."
  AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -n "$AIRFLOW_POD" ]; then
    kubectl exec "$AIRFLOW_POD" -- test -x /usr/local/bin/kubectl ||
      { err "La imagen Airflow K8s no contiene kubectl"; return 1; }
    kubectl exec "$AIRFLOW_POD" -- airflow dags unpause retrain_flight_delay_model 2>/dev/null || true
    ok "Airflow configurado"
  else
    err "Airflow no esta Running"
    return 1
  fi

  info "Ejecutando prediccion end-to-end interna en K8s..."
  run_e2e_k8s_check >/tmp/practica_k8s_e2e.json ||
    { err "La prediccion end-to-end K8s fallo"; return 1; }
  ok "Prediccion end-to-end K8s correcta"
  K8S_E2E_UUID="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("id", ""))' \
    < /tmp/practica_k8s_e2e.json 2>/dev/null)"
  verify_k8s_prediction_sinks "$K8S_E2E_UUID" || return 1
  run_websocket_k8s_check ||
    { err "La prediccion K8s no llego por WebSocket real"; return 1; }
  ok "WebSocket K8s real verificado"
  verify_k8s_nodeports || return 1

  echo ""
  info "Estado de pods:"
  kubectl get pods -o wide
  echo ""

  ok "Stack Kubernetes listo"
  show_urls_k8s
}

# ============================================================
#   REENTRENAMIENTO
# ============================================================

reentrenar_docker() {
  header "REENTRENAMIENTO -- DOCKER"
  warn "El reentrenamiento en Docker ejecuta el DAG de Airflow"
  cd "$PROJECT_HOME" || return 1
  require_docker || return 1
  wait_for_airflow || return 1

  info "Disparando DAG retrain_flight_delay_model en Airflow..."
  docker exec airflow airflow dags trigger retrain_flight_delay_model >/dev/null 2>&1 ||
    { err "Error disparando DAG"; return 1; }
  ok "DAG disparado correctamente"

  info "Esperando finalizacion del DAG (max 45 min)..."
  wait_for_docker_dag_success || return 1
  wait_until 300 5 "predictor tras reentrenamiento" predictor_streams_ready || return 1
  run_e2e_docker_check >/tmp/practica_retrain_e2e.json ||
    { err "La validacion end-to-end tras reentrenar fallo"; return 1; }
  ok "Reentrenamiento Docker y prediccion posterior correctos"
}

reentrenar_k8s() {
  header "REENTRENAMIENTO -- KUBERNETES"
  warn "El reentrenamiento en K8s ejecuta el DAG de Airflow"

  info "Verificando conexion al cluster..."
  require_gcloud || return 1
  local project_id
  project_id="$(gcloud config get-value project 2>/dev/null || true)"
  [ -n "$project_id" ] || { err "gcloud no tiene proyecto configurado"; return 1; }
  gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$project_id" >/dev/null ||
    return 1
  kubectl cluster-info >/dev/null || return 1

  AIRFLOW_POD=$(kubectl get pod -l app=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  SPARK_POD=$(kubectl get pod -l app=spark-master --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

  if [ -z "$AIRFLOW_POD" ]; then
    err "Pod de Airflow no encontrado. Esta el cluster arrancado?"
    return 1
  fi

  info "Verificando kubectl dentro de Airflow..."
  kubectl exec "$AIRFLOW_POD" -- test -x /usr/local/bin/kubectl ||
    { err "Airflow K8s no tiene kubectl disponible"; return 1; }

  if [ -n "$SPARK_POD" ]; then
    kubectl exec $SPARK_POD -- test -f /app/jars/flight_prediction_2.13-0.1.jar 2>/dev/null \
      || warn "No se encuentra el JAR en /app/jars dentro de spark-master"
  fi

  info "Disparando DAG retrain_flight_delay_model..."
  kubectl exec "$AIRFLOW_POD" -- airflow dags trigger retrain_flight_delay_model >/dev/null ||
    { err "Error disparando DAG K8s"; return 1; }
  ok "DAG disparado correctamente"
  wait_for_k8s_dag_success "$AIRFLOW_POD" || return 1
  k8s_trainmodel_completed_on_worker ||
    { err "El DAG termino, pero TrainModel no quedo demostrado en un worker"; return 1; }
  wait_until 420 5 "predictor K8s tras reentrenamiento" k8s_predictor_streams_ready || return 1
  run_e2e_k8s_check >/tmp/practica_retrain_k8s_e2e.json ||
    { err "La prediccion K8s tras reentrenar fallo"; return 1; }
  K8S_RETRAIN_UUID="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("id", ""))' \
    < /tmp/practica_retrain_k8s_e2e.json 2>/dev/null)"
  verify_k8s_prediction_sinks "$K8S_RETRAIN_UUID" || return 1
  run_websocket_k8s_check ||
    { err "La prediccion posterior al DAG no llego por WebSocket"; return 1; }
  ok "Reentrenamiento K8s y prediccion posterior correctos"
}

apagar_k8s() {
  header "APAGANDO CLUSTER GKE"
  warn "Esto apaga los nodos del cluster (ahorra dinero)"
  if confirm_action "Confirmas"; then
    require_gcloud || return 1
    gcloud container clusters resize "$CLUSTER" --num-nodes=0 --zone "$ZONE" --quiet || return 1
    ok "Cluster apagado"
  else
    info "Operacion cancelada"
    return 1
  fi
}

# ============================================================
#   PARAR DOCKER
# ============================================================

parar_docker() {
  header "PARANDO DOCKER COMPOSE"
  cd "$PROJECT_HOME" || return 1
  require_docker || return 1
  docker compose stop || return 1
  ok "Docker Compose parado"
}

# ============================================================
#   DIAGNOSTICO
# ============================================================

diag_cassandra() {
  header "DIAGNOSTICO -- CASSANDRA"

  subheader "Keyspaces y tablas"
  docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null || err "Cassandra no disponible"

  subheader "Tablas en agile_data_science"
  docker exec cassandra cqlsh -e "DESCRIBE TABLES;" agile_data_science 2>/dev/null

  subheader "Distancias origin-dest (primeras 5)"
  docker exec cassandra cqlsh -e "SELECT origin, dest, distance FROM agile_data_science.origin_dest_distances LIMIT 5;" 2>/dev/null

  subheader "Total distancias almacenadas"
  docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null

  subheader "Ultimas 5 predicciones en Cassandra"
  docker exec cassandra cqlsh -e "SELECT uuid, origin, dest, prediction, timestamp FROM agile_data_science.flight_delay_classification_response LIMIT 5;" 2>/dev/null

  subheader "Total predicciones almacenadas"
  docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.flight_delay_classification_response;" 2>/dev/null

  pause
}

diag_kafka() {
  local messages
  header "DIAGNOSTICO -- KAFKA"

  subheader "Topics activos"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null || err "Kafka no disponible"

  subheader "Detalles del topic de requests"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic flight-delay-ml-request 2>/dev/null

  subheader "Detalles del topic de responses"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic flight-delay-ml-response 2>/dev/null

  subheader "Hasta 3 mensajes en flight-delay-ml-response"
  messages="$(docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --max-messages 3 \
    --timeout-ms 10000 2>/dev/null || true)"
  if [ -n "$messages" ]; then
    printf '%s\n' "$messages" | python3 -c '
import json, sys
for line in sys.stdin:
    try:
        print(json.dumps(json.loads(line), indent=2))
    except json.JSONDecodeError:
        print(line, end="")
'
  else
    warn "Sin mensajes en el topic"
  fi

  subheader "Consumer groups activos"
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null ||
    warn "No se pudieron listar consumer groups"

  pause
}

diag_minio() {
  header "DIAGNOSTICO -- MINIO (Data Lakehouse)"

  subheader "Buckets disponibles"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/" 2>/dev/null

  subheader "Modelos en MinIO (s3a://flight-data/models/)"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/models/" 2>/dev/null

  subheader "Tabla Iceberg -- Training Data (warehouse)"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/warehouse/flights/training_data/" 2>/dev/null || warn "Tabla Iceberg no encontrada"

  subheader "Metadatos Iceberg (snapshots)"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/warehouse/flights/training_data/metadata/" 2>/dev/null

  subheader "Tamano total del bucket flight-data"
  docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc du local/flight-data/" 2>/dev/null

  pause
}

diag_spark() {
  header "DIAGNOSTICO -- SPARK STREAMING"

  local driver_id worker
  driver_id="$(active_predictor_driver_id)"

  subheader "Launcher del Spark Predictor"
  docker logs spark-predictor 2>&1 | grep "Spark master:" | tail -3

  subheader "Driver y cuatro streams"
  if [ -n "$driver_id" ] && predictor_streams_ready; then
    ok "Driver $driver_id RUNNING con cuatro streams activos"
  else
    err "El predictor no tiene cuatro streams activos"
  fi

  subheader "Estado reciente del driver en workers"
  for worker in spark-worker-1 spark-worker-2; do
    echo "  $worker:"
    docker exec "$worker" sh -c \
      "test -n '$driver_id' && grep -E 'Stream started from|MicroBatch|committed|ERROR|Exception' /opt/spark/work/$driver_id/stderr 2>/dev/null | tail -8" ||
      true
  done

  subheader "Workers conectados al master"
  docker logs spark-master 2>&1 | grep -E "worker|Worker|registered|Registered" | tail -8

  pause
}

diag_mongodb() {
  header "DIAGNOSTICO -- MONGODB"

  subheader "Bases de datos disponibles"
  docker exec mongo mongosh --quiet --eval "db.adminCommand('listDatabases').databases.map(d => d.name)" 2>/dev/null

  subheader "Total predicciones en MongoDB"
  docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments()" 2>/dev/null

  subheader "Ultimas 3 predicciones en MongoDB"
  docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.find({},{UUID:1,Prediction:1,Origin:1,Dest:1,_id:0}).sort({Timestamp:-1}).limit(3).toArray()" 2>/dev/null

  subheader "Tamano de la coleccion"
  docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.stats().size" 2>/dev/null

  pause
}

diag_prometheus() {
  header "DIAGNOSTICO -- PROMETHEUS & GRAFANA"

  subheader "Targets de Prometheus (estado)"
  curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool 2>/dev/null | \
    grep -E '"job"|"health"|"lastError"' | head -20 || err "Prometheus no disponible"

  subheader "Metricas Flask -- Total requests a Kafka"
  curl -s "http://localhost:9090/api/v1/query?query=flight_requests_total" | \
    python3 -m json.tool 2>/dev/null | grep -E "value|result" | head -5

  subheader "Metricas Flask -- Predicciones por categoria"
  curl -s "http://localhost:9090/api/v1/query?query=flight_predictions_total" | \
    python3 -m json.tool 2>/dev/null | grep -E "category|value" | head -15

  subheader "Metricas disponibles en Flask /metrics"
  curl -s http://localhost:5001/metrics | grep -E "^flight_|^flask_http_request_total" | head -15

  subheader "Dashboard Grafana cargado"
  curl -s http://admin:admin@localhost:3000/api/search | \
    python3 -m json.tool 2>/dev/null | grep -E "title|uid" | head -5

  pause
}

diag_pipeline_completo() {
  header "DIAGNOSTICO -- PIPELINE COMPLETO"

  subheader "Estado de todos los contenedores"
  docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null

  subheader "Verificacion end-to-end"
  echo ""

  FLASK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/flights/delays/predict_kafka 2>/dev/null)
  [ "$FLASK_STATUS" = "200" ] && ok "Flask responde (HTTP $FLASK_STATUS)" || err "Flask no responde (HTTP $FLASK_STATUS)"

  TOPICS=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ' ')
  echo "$TOPICS" | grep -q "flight-delay-ml-request" && ok "Kafka topic request existe" || err "Topic request no encontrado"
  echo "$TOPICS" | grep -q "flight-delay-ml-response" && ok "Kafka topic response existe" || err "Topic response no encontrado"

  CASS_COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM agile_data_science.origin_dest_distances;" 2>/dev/null \
    | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ { gsub(/[[:space:]]/, ""); print; exit }')
  [ "$CASS_COUNT" = "4696" ] && ok "Cassandra: 4696 distancias cargadas" || warn "Cassandra: $CASS_COUNT distancias (esperado 4696)"

  MONGO_COUNT=$(docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments()" 2>/dev/null)
  ok "MongoDB: $MONGO_COUNT predicciones almacenadas"

  MODELS=$(docker exec minio sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null && mc ls local/flight-data/models/" 2>/dev/null | wc -l)
  [ "$MODELS" -gt "0" ] && ok "MinIO: modelos disponibles ($MODELS ficheros)" || err "MinIO: no hay modelos"

  PROM_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy 2>/dev/null)
  [ "$PROM_STATUS" = "200" ] && ok "Prometheus healthy" || err "Prometheus no responde"

  FLASK_METRICS=$(curl -s http://localhost:5001/metrics 2>/dev/null | grep -c "flight_requests_total")
  [ "$FLASK_METRICS" -gt "0" ] && ok "Flask /metrics activo (prometheus-flask-exporter)" || err "Flask /metrics no disponible"

  if predictor_streams_ready; then
    ok "Spark Streaming activo: cuatro streams confirmados en el driver"
  else
    err "Spark Streaming no tiene los cuatro streams activos"
  fi

  pause
}

diag_websockets() {
  header "DIAGNOSTICO -- WEBSOCKETS"

  subheader "Thread consumer Kafka en Flask"
  docker logs flask 2>&1 | grep -E "kafka|Kafka|consumer|Consumer|socket|Socket|prediction" | tail -10

  subheader "Eventos Socket.IO emitidos"
  docker logs flask 2>&1 | grep -E "emit|prediction_response|socketio" | tail -10

  subheader "Polling REST activo"
  docker logs flask 2>&1 | grep "classify_realtime" | tail -10

  subheader "Prueba end-to-end -- enviar prediccion y verificar respuesta Kafka"
  local result
  if result="$(run_e2e_docker_check)"; then
    ok "Prediccion recibida por el pipeline Flask/Kafka/Spark"
    echo "  $result"
    pause
    return 0
  fi
  err "No se recibio respuesta del pipeline en 120 segundos"
  pause
  return 1
}

diag_mlflow() {
  header "DIAGNOSTICO -- MLFLOW"

  local experiments experiment_ids payload runs
  subheader "Experimentos registrados"
  experiments="$(curl -fsS http://localhost:5002/api/2.0/mlflow/experiments/search \
    -H "Content-Type: application/json" -d '{"max_results": 100}' 2>/dev/null)" ||
    { err "MLflow no disponible"; pause; return 1; }
  printf "%s" "$experiments" | python3 -c '
import json, sys
for experiment in json.load(sys.stdin).get("experiments", []):
    print("  {}: {}".format(experiment.get("experiment_id"), experiment.get("name")))
' || { err "Respuesta de experimentos MLflow invalida"; pause; return 1; }

  subheader "Ultimas ejecuciones (runs)"
  experiment_ids="$(printf "%s" "$experiments" | python3 -c '
import json, sys
print(",".join(e["experiment_id"] for e in json.load(sys.stdin).get("experiments", [])))
')"
  [ -n "$experiment_ids" ] || { err "MLflow no tiene experimentos"; pause; return 1; }
  payload="$(EXPERIMENT_IDS="$experiment_ids" python3 -c '
import json, os
print(json.dumps({"experiment_ids": os.environ["EXPERIMENT_IDS"].split(","), "max_results": 5, "order_by": ["attributes.start_time DESC"]}))
')"
  runs="$(curl -fsS http://localhost:5002/api/2.0/mlflow/runs/search \
    -H "Content-Type: application/json" -d "$payload" 2>/dev/null)" ||
    { err "No se pudieron consultar runs MLflow"; pause; return 1; }
  printf "%s" "$runs" | python3 -c '
import json, sys
runs = json.load(sys.stdin).get("runs", [])
for run in runs:
    info = run.get("info", {})
    print("  {} status={} start={}".format(info.get("run_id"), info.get("status"), info.get("start_time")))
raise SystemExit(0 if runs else 1)
' || { err "MLflow no contiene runs"; pause; return 1; }

  pause
}

diag_airflow() {
  header "DIAGNOSTICO -- AIRFLOW"

  subheader "DAGs disponibles"
  docker exec airflow airflow dags list 2>/dev/null | grep -v "^$"

  subheader "Ultimas ejecuciones del DAG de reentrenamiento"
  docker exec airflow airflow dags list-runs -d retrain_flight_delay_model 2>/dev/null | head -10

  subheader "Estado del scheduler"
  docker logs airflow 2>&1 | grep -E "scheduler|Scheduler|DAG|dag" | tail -8

  pause
}

diag_test_e2e_docker() {
  header "TEST END-TO-END AUTOMATICO -- DOCKER"

  local endpoint="http://localhost:5001/flights/delays/predict/classify_realtime"
  local payload="DepDelay=15&Carrier=AA&FlightDate=2016-12-25&Origin=ATL&Dest=SFO&FlightNum=1234"
  local response body http_code uuid result result_body status prediction label failures=0

  info "Enviando prediccion a Flask Docker..."
  response=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" -X POST "$endpoint" -d "$payload" 2>/dev/null)
  http_code=$(printf "%s\n" "$response" | awk -F= '/^HTTP_CODE=/{print $2}' | tail -1)
  body=$(printf "%s" "$response" | sed '/^HTTP_CODE=/d')

  if [ "$http_code" = "200" ]; then
    pass_check "Flask acepto la peticion (HTTP 200)"
  else
    fail_check "Flask no respondio correctamente (HTTP ${http_code:-N/A})"
    echo "$body"
    pause
    return 1
  fi

  uuid=$(JSON_BODY="$body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("id",""))' 2>/dev/null)
  if [ -z "$uuid" ]; then
    fail_check "No se pudo extraer UUID de la respuesta"
    echo "$body"
    pause
    return 1
  fi
  pass_check "UUID generado: $uuid"

  info "Esperando respuesta de Spark/Kafka (max 60s)..."
  status="TIMEOUT"
  for _ in $(seq 1 20); do
    result=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" "$endpoint/response/$uuid" 2>/dev/null)
    result_body=$(printf "%s" "$result" | sed '/^HTTP_CODE=/d')
    status=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("status",""))' 2>/dev/null)
    [ "$status" = "OK" ] && break
    sleep 3
  done

  if [ "$status" = "OK" ]; then
    pass_check "Respuesta recibida por polling REST"
  else
    fail_check "No se recibio respuesta en 60s"
    failures=$((failures + 1))
  fi

  prediction=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("prediction",{}).get("Prediction",""))' 2>/dev/null)
  case "$prediction" in
    0|0.0) label="no delay" ;;
    1|1.0) label="small delay" ;;
    2|2.0) label="moderate delay" ;;
    3|3.0) label="severe delay" ;;
    *) label="desconocida" ;;
  esac
  echo ""
  echo -e "  ${BOLD}Prediction:${NC} ${prediction:-N/A} ($label)"

  subheader "Verificacion de sinks"
  local mongo_count cassandra_hit kafka_hit
  mongo_count=$(docker exec mongo mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments({UUID: '$uuid'})" 2>/dev/null | tail -1 | tr -d '[:space:]')
  if [ "${mongo_count:-0}" -gt 0 ] 2>/dev/null; then pass_check "MongoDB contiene UUID $uuid"; else fail_check "MongoDB no contiene UUID $uuid"; failures=$((failures + 1)); fi

  cassandra_hit=$(docker exec cassandra cqlsh -e \
    "SELECT uuid FROM agile_data_science.flight_delay_classification_response WHERE uuid='$uuid';" 2>/dev/null | grep "$uuid" | head -1)
  if [ -n "$cassandra_hit" ]; then pass_check "Cassandra contiene UUID $uuid"; else fail_check "Cassandra no contiene UUID $uuid"; failures=$((failures + 1)); fi

  kafka_hit=$(docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --timeout-ms 5000 --max-messages 1000 2>/dev/null | grep "$uuid" | tail -1)
  if [ -n "$kafka_hit" ]; then pass_check "Kafka response topic contiene UUID $uuid"; else fail_check "Kafka response topic no muestra UUID $uuid"; failures=$((failures + 1)); fi

  pause
  [ "$failures" -eq 0 ]
}

diag_test_e2e_k8s() {
  header "TEST END-TO-END AUTOMATICO -- K8S"

  if ! command -v kubectl >/dev/null 2>&1; then
    err "kubectl no esta disponible"
    pause
    return 1
  fi

  local node_ip endpoint payload response body http_code uuid result result_body status prediction label failures=0
  node_ip=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null)
  if [ -z "$node_ip" ]; then
    err "No se pudo obtener la IP externa del nodo GKE"
    pause
    return 1
  fi

  endpoint="http://$node_ip:30001/flights/delays/predict/classify_realtime"
  payload="DepDelay=10&Carrier=UA&FlightDate=2016-07-04&Origin=ORD&Dest=LAX&FlightNum=500"

  info "Enviando prediccion a Flask K8s ($endpoint)..."
  response=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" -X POST "$endpoint" -d "$payload" 2>/dev/null)
  http_code=$(printf "%s\n" "$response" | awk -F= '/^HTTP_CODE=/{print $2}' | tail -1)
  body=$(printf "%s" "$response" | sed '/^HTTP_CODE=/d')

  if [ "$http_code" = "200" ]; then
    pass_check "Flask K8s acepto la peticion (HTTP 200)"
  else
    fail_check "Flask K8s no respondio correctamente (HTTP ${http_code:-N/A})"
    echo "$body"
    pause
    return 1
  fi

  uuid=$(JSON_BODY="$body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("id",""))' 2>/dev/null)
  if [ -z "$uuid" ]; then
    fail_check "No se pudo extraer UUID de la respuesta"
    echo "$body"
    pause
    return 1
  fi
  pass_check "UUID generado: $uuid"

  info "Esperando respuesta de Spark/Kafka en K8s (max 60s)..."
  status="TIMEOUT"
  for _ in $(seq 1 20); do
    result=$(curl -s -w "\nHTTP_CODE=%{http_code}\n" "$endpoint/response/$uuid" 2>/dev/null)
    result_body=$(printf "%s" "$result" | sed '/^HTTP_CODE=/d')
    status=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("status",""))' 2>/dev/null)
    [ "$status" = "OK" ] && break
    sleep 3
  done

  if [ "$status" = "OK" ]; then
    pass_check "Respuesta recibida por polling REST"
  else
    fail_check "No se recibio respuesta en 60s"
    failures=$((failures + 1))
  fi

  prediction=$(JSON_BODY="$result_body" python3 -c 'import os,json; d=json.loads(os.environ.get("JSON_BODY","{}")); print(d.get("prediction",{}).get("Prediction",""))' 2>/dev/null)
  case "$prediction" in
    0|0.0) label="no delay" ;;
    1|1.0) label="small delay" ;;
    2|2.0) label="moderate delay" ;;
    3|3.0) label="severe delay" ;;
    *) label="desconocida" ;;
  esac
  echo ""
  echo -e "  ${BOLD}Prediction:${NC} ${prediction:-N/A} ($label)"

  subheader "Verificacion de sinks K8s"
  local mongo_count cassandra_hit kafka_hit
  mongo_count=$(kubectl exec deployment/mongo -- mongosh --quiet agile_data_science \
    --eval "db.flight_delay_ml_response.countDocuments({UUID: '$uuid'})" 2>/dev/null | tail -1 | tr -d '[:space:]')
  if [ "${mongo_count:-0}" -gt 0 ] 2>/dev/null; then pass_check "MongoDB K8s contiene UUID $uuid"; else fail_check "MongoDB K8s no contiene UUID $uuid"; failures=$((failures + 1)); fi

  cassandra_hit=$(kubectl exec deployment/cassandra -- cqlsh -e \
    "SELECT uuid FROM agile_data_science.flight_delay_classification_response WHERE uuid='$uuid';" 2>/dev/null | grep "$uuid" | head -1)
  if [ -n "$cassandra_hit" ]; then pass_check "Cassandra K8s contiene UUID $uuid"; else fail_check "Cassandra K8s no contiene UUID $uuid"; failures=$((failures + 1)); fi

  kafka_hit=$(kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic flight-delay-ml-response \
    --from-beginning --timeout-ms 5000 --max-messages 1000 2>/dev/null | grep "$uuid" | tail -1)
  if [ -n "$kafka_hit" ]; then pass_check "Kafka K8s response topic contiene UUID $uuid"; else fail_check "Kafka K8s response topic no muestra UUID $uuid"; failures=$((failures + 1)); fi

  if run_websocket_k8s_check; then
    pass_check "WebSocket K8s real entrego prediction_response"
  else
    fail_check "WebSocket K8s real no entrego prediction_response"
    failures=$((failures + 1))
  fi

  pause
  [ "$failures" -eq 0 ]
}

diag_deploy_mode_cluster() {
  header "DIAGNOSTICO -- DEPLOY-MODE CLUSTER"

  subheader "Docker Spark Standalone"
  python3 - <<'PY'
import json
import urllib.request

try:
    data = json.load(urllib.request.urlopen("http://localhost:8080/json/", timeout=10))
except Exception as exc:
    print(f"  ERROR leyendo Spark UI Docker: {exc}")
    raise SystemExit(0)

drivers = data.get("activedrivers", [])
completed = data.get("completeddrivers", [])[:5]
print(f"  Active drivers: {len(drivers)}")
for driver in drivers:
    worker = driver.get("worker")
    print(f"  Driver ID: {driver.get('id')}")
    print(f"    MainClass: {driver.get('mainclass')}")
    print(f"    State: {driver.get('state')}")
    print(f"    Worker: {worker or 'None'}")
    print("    ✓ CLUSTER MODE: driver en worker" if worker else "    ✗ CLIENT MODE: sin worker")
if completed:
    print("  Completed drivers recientes:")
    for driver in completed:
        print(f"    {driver.get('id')} {driver.get('mainclass')} state={driver.get('state')} worker={driver.get('worker')}")

workers = data.get("workers", [])
print(f"  Workers activos: {len(workers)}")
for worker in workers:
    print(f"    {worker.get('id')}: cores={worker.get('coresused')}/{worker.get('cores')} mem={worker.get('memoryused')}/{worker.get('memory')}MB")
PY

  subheader "Docker stderr del driver mas reciente en workers"
  echo "  spark-worker-1:"
  docker exec spark-worker-1 bash -c "cat \$(ls -td /opt/spark/work/driver-*/stderr 2>/dev/null | head -1) 2>/dev/null | grep -E 'MicroBatch|bootstrap.servers|ERROR' | tail -5" 2>/dev/null || warn "No hay stderr reciente en spark-worker-1"
  echo ""
  echo "  spark-worker-2:"
  docker exec spark-worker-2 bash -c "cat \$(ls -td /opt/spark/work/driver-*/stderr 2>/dev/null | head -1) 2>/dev/null | grep -E 'MicroBatch|bootstrap.servers|ERROR' | tail -5" 2>/dev/null || warn "No hay stderr reciente en spark-worker-2"

  subheader "K8s Spark Standalone"
  if command -v kubectl >/dev/null 2>&1; then
    kubectl exec -i deployment/spark-master -- python3 - <<'PY' 2>/dev/null || echo "  No se pudo consultar spark-master en K8s"
import json
import re
import urllib.request

try:
    data = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=10))
except Exception as exc:
    print(f"  ERROR leyendo Spark UI K8s: {exc}")
    raise SystemExit(0)

drivers = data.get("activedrivers", [])
print(f"  Active drivers: {len(drivers)}")
for driver in drivers:
    worker = driver.get("worker")
    print(f"  Driver ID: {driver.get('id')}")
    print(f"    MainClass: {driver.get('mainclass')}")
    print(f"    State: {driver.get('state')}")
    print(f"    Worker: {worker or 'None'}")
    try:
        status = json.load(urllib.request.urlopen(
            f"http://spark-master:6066/v1/submissions/status/{driver.get('id')}",
            timeout=10,
        ))
        host_port = status.get("workerHostPort", "")
        print(f"    REST State: {status.get('driverState')}")
        print(f"    workerHostPort: {host_port}")
        if re.match(r"^\d+\.\d+\.\d+\.\d+:", str(host_port)):
            print("    ✓ CLUSTER MODE: workerHostPort es IP de pod")
        else:
            print("    ✗ Revisar: workerHostPort no parece IP")
    except Exception as exc:
        print(f"    REST status no disponible: {exc}")

workers = data.get("workers", [])
print(f"  Workers activos: {len(workers)}")
for worker in workers:
    print(f"    {worker.get('id')}: cores={worker.get('coresused')}/{worker.get('cores')} mem={worker.get('memoryused')}/{worker.get('memory')}MB")
PY
    echo ""
    echo "  Pods spark-worker:"
    kubectl get pods -l app=spark-worker -o wide 2>/dev/null || true
  else
    warn "kubectl no esta disponible"
  fi

  pause
}

diag_versiones() {
  header "VERIFICACION DE VERSIONES DEL ENUNCIADO"

  local spark_out spark_real scala_real kafka_version kafka_real mongo_real cassandra_real airflow_real mlflow_real
  local python_flask_real python_spark_real python_airflow_real iceberg_real jar_classes failures=0
  local zookeeper_count kafka_proc

  spark_out=$(docker exec spark-master /opt/spark/bin/spark-submit --version 2>&1)
  spark_real=$(echo "$spark_out" | grep -oE 'version [0-9]+\.[0-9]+\.[0-9]+' | head -1 | awk '{print $2}')
  scala_real=$(echo "$spark_out" | sed -nE 's/.*Scala version ([0-9]+\.[0-9]+).*/\1/p' | head -1)
  kafka_version=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | head -1)
  zookeeper_count=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -ci zookeeper || true)
  kafka_proc=$(docker exec kafka ps aux 2>/dev/null | grep -v grep | grep kafka | grep -v zookeeper | head -1)
  if [ "$kafka_version" = "4.2.0" ] && [ "$zookeeper_count" = "0" ] && [ -n "$kafka_proc" ]; then
    kafka_real="kafka_2.13-$kafka_version (KRaft, sin Zookeeper)"
  else
    kafka_real="${kafka_version:-N/A}"
  fi
  mongo_real=$(docker exec mongo mongosh --quiet --eval 'db.version()' 2>/dev/null | head -1)
  cassandra_real=$(docker exec cassandra cqlsh -e "SELECT release_version FROM system.local;" 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  airflow_real=$(docker exec airflow airflow version 2>/dev/null | head -1)
  mlflow_real=$(docker exec mlflow mlflow --version 2>/dev/null | awk '{print $3}' | head -1)
  if [ -z "$mlflow_real" ]; then
    mlflow_real=$(curl -s http://localhost:5002/api/2.0/mlflow/experiments/search -H 'Content-Type: application/json' -d '{"max_results":1}' 2>/dev/null | python3 -c "import sys,json; json.load(sys.stdin); print('API OK')" 2>/dev/null)
  fi
  python_flask_real=$(docker exec flask python3 --version 2>/dev/null | awk '{print $2}')
  python_spark_real=$(docker exec spark-master python3 --version 2>/dev/null | awk '{print $2}')
  python_airflow_real=$(docker exec airflow python --version 2>/dev/null | awk '{print $2}')
  iceberg_real=$(docker exec spark-master sh -c \
    "ls /opt/spark/jars/*iceberg-spark-runtime*1.10.1*.jar" 2>/dev/null |
    sed -nE 's/.*-([0-9]+\.[0-9]+\.[0-9]+)\.jar/\1/p' | head -1)
  jar_classes=$(jar tf shared-jars/flight_prediction_2.13-0.1.jar 2>/dev/null | grep -c '\.class')

  printf "  %-14s | %-24s | %-36s | %s\n" "Componente" "Version esperada" "Version real" "Estado"
  printf "  %-14s-+-%-24s-+-%-36s-+-%s\n" "--------------" "------------------------" "------------------------------------" "------"
  version_row() {
    local component="$1" expected="$2" real="$3" condition="$4" state
    if eval "$condition"; then
      state="${GREEN}✓${NC}"
    else
      state="${RED}✗${NC}"
      failures=$((failures + 1))
    fi
    printf "  %-14s | %-24s | %-36s | %b\n" "$component" "$expected" "${real:-N/A}" "$state"
  }

  version_row "Spark" "4.1.1" "$spark_real" "[[ \"$spark_real\" == \"4.1.1\" ]]"
  version_row "Scala" "2.13" "$scala_real" "[[ \"$scala_real\" == \"2.13\" ]]"
  version_row "Kafka" "kafka_2.13-4.2.0 KRaft" "$kafka_real" "[[ \"$kafka_real\" == *\"4.2.0\"* && \"$kafka_real\" == *\"KRaft\"* ]]"
  version_row "MongoDB" "7.0.17" "$mongo_real" "[[ \"$mongo_real\" == \"7.0.17\" ]]"
  version_row "Cassandra" "4.1" "$cassandra_real" "[[ \"$cassandra_real\" == 4.1* ]]"
  version_row "Airflow" "2.10.4" "$airflow_real" "[[ \"$airflow_real\" == \"2.10.4\" ]]"
  version_row "MLflow" "2.19.0" "$mlflow_real" "[[ \"$mlflow_real\" == \"2.19.0\" || \"$mlflow_real\" == \"API OK\" ]]"
  version_row "Python Flask" "3.10.x" "$python_flask_real" "[[ \"$python_flask_real\" == 3.10.* ]]"
  version_row "Python Spark" "3.10.x" "$python_spark_real" "[[ \"$python_spark_real\" == 3.10.* ]]"
  version_row "Python Airflow" "3.10.x" "$python_airflow_real" "[[ \"$python_airflow_real\" == 3.10.* ]]"
  version_row "Iceberg" "1.10.1" "$iceberg_real" "[[ \"$iceberg_real\" == \"1.10.1\" ]]"

  echo ""
  echo "  JAR classes: ${jar_classes:-0}"
  if [ "$zookeeper_count" = "0" ] && [ -n "$kafka_proc" ]; then
    pass_check "KRaft mode confirmado (sin Zookeeper)"
  else
    warn "Verificar modo Kafka: no se pudo confirmar KRaft completamente"
    failures=$((failures + 1))
  fi

  pause
  [ "$failures" -eq 0 ]
}

diag_versiones_k8s() {
  header "VERIFICACION DE VERSIONES -- KUBERNETES"
  kubectl cluster-info >/dev/null 2>&1 ||
    { err "No hay un cluster Kubernetes accesible"; return 1; }

  local spark_out spark_real scala_real kafka_real kafka_proc mongo_real cassandra_real
  local airflow_real mlflow_real python_flask_real python_spark_real python_airflow_real iceberg_real failures=0

  spark_out="$(kubectl exec deployment/spark-master -- /opt/spark/bin/spark-submit --version 2>&1)"
  spark_real="$(printf '%s\n' "$spark_out" | grep -oE 'version [0-9]+\.[0-9]+\.[0-9]+' | head -1 | awk '{print $2}')"
  scala_real="$(printf '%s\n' "$spark_out" | sed -nE 's/.*Scala version ([0-9]+\.[0-9]+\.[0-9]+).*/\1/p' | head -1)"
  kafka_real="$(kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | head -1)"
  kafka_proc="$(kubectl exec deployment/kafka -- ps aux 2>/dev/null | grep -v grep | grep kafka | grep -v zookeeper | head -1 || true)"
  mongo_real="$(kubectl exec deployment/mongo -- mongosh --quiet --eval 'db.version()' 2>/dev/null | head -1)"
  cassandra_real="$(kubectl exec deployment/cassandra -- cqlsh -e \
    'SELECT release_version FROM system.local;' 2>/dev/null |
    grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
  airflow_real="$(kubectl exec deployment/airflow -- airflow version 2>/dev/null | head -1)"
  mlflow_real="$(kubectl exec deployment/mlflow -- mlflow --version 2>/dev/null | awk '{print $3}' | head -1)"
  python_flask_real="$(kubectl exec deployment/flask -- python3 --version 2>/dev/null | awk '{print $2}')"
  python_spark_real="$(kubectl exec deployment/spark-master -- python3 --version 2>/dev/null | awk '{print $2}')"
  python_airflow_real="$(kubectl exec deployment/airflow -- python --version 2>/dev/null | awk '{print $2}')"
  iceberg_real="$(kubectl exec deployment/spark-master -- sh -c \
    'ls /opt/spark/jars/*iceberg-spark-runtime*1.10.1*.jar' 2>/dev/null |
    sed -nE 's/.*-([0-9]+\.[0-9]+\.[0-9]+)\.jar/\1/p' | head -1)"

  printf "  %-14s | %-20s | %-24s | %s\n" "Componente" "Version esperada" "Version real" "Estado"
  printf "  %-14s-+-%-20s-+-%-24s-+-%s\n" "--------------" "--------------------" "------------------------" "------"
  k8s_version_row() {
    local component="$1" expected="$2" real="$3" matches="$4"
    if [ "$matches" = "0" ]; then
      printf "  %-14s | %-20s | %-24s | %b\n" "$component" "$expected" "${real:-N/A}" "${GREEN}✓${NC}"
    else
      printf "  %-14s | %-20s | %-24s | %b\n" "$component" "$expected" "${real:-N/A}" "${RED}✗${NC}"
      failures=$((failures + 1))
    fi
  }

  [[ "$spark_real" == "4.1.1" ]]; k8s_version_row "Spark" "4.1.1" "$spark_real" "$?"
  [[ "$scala_real" == 2.13.* ]]; k8s_version_row "Scala" "2.13.x" "$scala_real" "$?"
  [[ "$kafka_real" == "4.2.0" && -n "$kafka_proc" ]]; k8s_version_row "Kafka KRaft" "4.2.0" "$kafka_real" "$?"
  [[ "$mongo_real" == "7.0.17" ]]; k8s_version_row "MongoDB" "7.0.17" "$mongo_real" "$?"
  [[ "$cassandra_real" == 4.1.* ]]; k8s_version_row "Cassandra" "4.1.x" "$cassandra_real" "$?"
  [[ "$airflow_real" == "2.10.4" ]]; k8s_version_row "Airflow" "2.10.4" "$airflow_real" "$?"
  [[ "$mlflow_real" == "2.19.0" ]]; k8s_version_row "MLflow" "2.19.0" "$mlflow_real" "$?"
  [[ "$python_flask_real" == 3.10.* ]]; k8s_version_row "Python Flask" "3.10.x" "$python_flask_real" "$?"
  [[ "$python_spark_real" == 3.10.* ]]; k8s_version_row "Python Spark" "3.10.x" "$python_spark_real" "$?"
  [[ "$python_airflow_real" == 3.10.* ]]; k8s_version_row "Python Airflow" "3.10.x" "$python_airflow_real" "$?"
  [[ "$iceberg_real" == "1.10.1" ]]; k8s_version_row "Iceberg" "1.10.1" "$iceberg_real" "$?"

  pause
  [ "$failures" -eq 0 ]
}

dispatch_diagnostic_option() {
  case "${1:-}" in
    1) diag_pipeline_completo ;;
    2) diag_cassandra ;;
    3) diag_kafka ;;
    4) diag_websockets ;;
    5) diag_minio ;;
    6) diag_spark ;;
    7) diag_mongodb ;;
    8) diag_prometheus ;;
    9) diag_mlflow ;;
    a|A) diag_airflow ;;
    b|B) diag_test_e2e_docker ;;
    c|C) diag_test_e2e_k8s ;;
    d|D) diag_deploy_mode_cluster ;;
    e|E) diag_versiones ;;
    f|F) diag_versiones_k8s ;;
    *) err "Opcion de diagnostico no valida: ${1:-vacia}"; return 2 ;;
  esac
}

menu_diagnostico() {
  while true; do
    clear
    echo ""
    echo -e "${BOLD}${MAGENTA}============================================${NC}"
    echo -e "${BOLD}${MAGENTA}  DIAGNOSTICO -- PRACTICA BIGDATA${NC}"
    echo -e "${BOLD}${MAGENTA}============================================${NC}"
    echo ""
    echo -e "  ${BOLD}COMPONENTES${NC}"
    echo -e "  ${GREEN}1)${NC} Pipeline completo (resumen rapido)"
    echo -e "  ${GREEN}2)${NC} Cassandra (keyspace, tablas, datos)"
    echo -e "  ${GREEN}3)${NC} Kafka (topics, mensajes, consumer groups)"
    echo -e "  ${GREEN}4)${NC} WebSockets (prueba end-to-end automatica)"
    echo -e "  ${GREEN}5)${NC} MinIO -- Data Lakehouse (modelos, Iceberg)"
    echo -e "  ${GREEN}6)${NC} Spark Streaming (sinks, micro-batches)"
    echo -e "  ${GREEN}7)${NC} MongoDB (predicciones almacenadas)"
    echo -e "  ${GREEN}8)${NC} Prometheus & Grafana (metricas)"
    echo -e "  ${GREEN}9)${NC} MLflow (experimentos y runs)"
    echo -e "  ${GREEN}a)${NC} Airflow (DAGs y ejecuciones)"
    echo -e "  ${GREEN}b)${NC} Test end-to-end automatico Docker"
    echo -e "  ${GREEN}c)${NC} Test end-to-end automatico K8s"
    echo -e "  ${GREEN}d)${NC} Diagnostico deploy-mode cluster"
    echo -e "  ${GREEN}e)${NC} Verificar versiones del enunciado"
    echo -e "  ${GREEN}f)${NC} Verificar versiones en Kubernetes"
    echo ""
    echo -e "  ${GREEN}0)${NC} Volver al menu principal"
    echo -e "${BOLD}${MAGENTA}============================================${NC}"
    echo ""
    echo -n "  Selecciona una opcion: "
    if ! read subopcion; then
      break
    fi

    [ "$subopcion" = "0" ] && break
    dispatch_diagnostic_option "$subopcion" || warn "El diagnostico termino con errores"
  done
}

# ============================================================
#   LOGS
# ============================================================

logs_spark_worker_driver() {
  local worker="$1"
  header "LOGS -- ${worker^^} (DRIVERS)"
  docker exec "$worker" bash -lc '
    DRIVER=$(ls -td /opt/spark/work/driver-*/ 2>/dev/null | head -1)
    if [ -n "$DRIVER" ]; then
      echo "=== Driver mas reciente: $DRIVER ==="
      echo "--- stdout ---"
      cat "$DRIVER/stdout" 2>/dev/null | tail -20
      echo "--- stderr (ultimas 30 lineas) ---"
      cat "$DRIVER/stderr" 2>/dev/null | grep -v NativeCodeLoader | grep -v SLF4J | tail -30
    else
      echo "No hay drivers en este worker"
    fi
  ' 2>/dev/null || warn "No se pudo leer $worker"
  pause
}

logs_flask_metrics() {
  header "FLASK METRICS -- /metrics"
  curl -s http://localhost:5001/metrics 2>/dev/null | \
    grep -E "^flight_|^flask_http_request_total|^flask_http_request_duration" || warn "No se pudieron leer metricas Flask"
  pause
}

dispatch_log_option() {
  local logop="${1:-}" lines="${LOG_LINES:-50}" svc
  case "$logop" in
    1)
      header "LOGS -- FLASK"
      docker logs flask --tail="$lines" 2>&1
      pause ;;
    2)
      header "LOGS -- SPARK PREDICTOR"
      docker logs spark-predictor --tail="$lines" 2>&1
      pause ;;
    3)
      header "LOGS -- SPARK MASTER"
      docker logs spark-master --tail="$lines" 2>&1
      pause ;;
    4)
      header "LOGS -- KAFKA"
      docker logs kafka --tail="$lines" 2>&1
      pause ;;
    5)
      header "LOGS -- MONGODB"
      docker logs mongo --tail="$lines" 2>&1
      pause ;;
    6)
      header "LOGS -- CASSANDRA"
      docker logs cassandra --tail="$lines" 2>&1
      pause ;;
    7)
      header "LOGS -- MINIO"
      docker logs minio --tail="$lines" 2>&1
      pause ;;
    8)
      header "LOGS -- AIRFLOW"
      docker logs airflow --tail="$lines" 2>&1
      pause ;;
    9)
      header "LOGS -- MLFLOW"
      docker logs mlflow --tail="$lines" 2>&1
      pause ;;
    a|A)
      header "LOGS -- PROMETHEUS"
      docker logs prometheus --tail="$lines" 2>&1
      pause ;;
    b|B)
      header "LOGS -- GRAFANA"
      docker logs grafana --tail="$lines" 2>&1
      pause ;;
    c|C)
      header "LOGS -- TODOS LOS SERVICIOS"
      for svc in flask spark-predictor spark-master kafka mongo cassandra minio airflow mlflow prometheus grafana; do
        subheader "$svc"
        docker logs "$svc" --tail=5 2>&1
      done
      pause ;;
    d|D) logs_spark_worker_driver "spark-worker-1" ;;
    e|E) logs_spark_worker_driver "spark-worker-2" ;;
    f|F) logs_flask_metrics ;;
    *) err "Opcion de logs no valida: ${logop:-vacia}"; return 2 ;;
  esac
}

menu_logs() {
  while true; do
    clear
    echo ""
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo -e "${BOLD}${YELLOW}  LOGS -- PRACTICA BIGDATA${NC}"
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo ""
    echo -e "  ${GREEN}1)${NC}  Flask (API + WebSockets + Kafka consumer)"
    echo -e "  ${GREEN}2)${NC}  Spark Predictor (Streaming + 4 sinks)"
    echo -e "  ${GREEN}3)${NC}  Spark Master"
    echo -e "  ${GREEN}4)${NC}  Kafka (KRaft broker)"
    echo -e "  ${GREEN}5)${NC}  MongoDB"
    echo -e "  ${GREEN}6)${NC}  Cassandra"
    echo -e "  ${GREEN}7)${NC}  MinIO"
    echo -e "  ${GREEN}8)${NC}  Airflow (scheduler + webserver)"
    echo -e "  ${GREEN}9)${NC}  MLflow"
    echo -e "  ${GREEN}a)${NC}  Prometheus"
    echo -e "  ${GREEN}b)${NC}  Grafana"
    echo -e "  ${GREEN}c)${NC}  Todos los servicios (ultimas 5 lineas c/u)"
    echo -e "  ${GREEN}d)${NC}  Spark Worker-1 (logs de drivers)"
    echo -e "  ${GREEN}e)${NC}  Spark Worker-2 (logs de drivers)"
    echo -e "  ${GREEN}f)${NC}  Flask metricas Prometheus (/metrics)"
    echo ""
    echo -e "  ${GREEN}0)${NC}  Volver"
    echo -e "${BOLD}${YELLOW}============================================${NC}"
    echo ""
    echo -n "  Selecciona servicio: "
    if ! read logop; then
      break
    fi

    [ "$logop" = "0" ] && break
    dispatch_log_option "$logop" || warn "No se pudieron obtener los logs solicitados"
  done
}

limpiar_checkpoints_docker() {
  header "LIMPIAR CHECKPOINTS S3A -- DOCKER"
  info "Limpiando checkpoints S3A del predictor..."
  warn "Esto requiere reiniciar el predictor"
  cd "$PROJECT_HOME" || return 1
  require_docker || return 1

  if confirm_action "Confirmas"; then
    docker compose --profile predictor stop spark-predictor >/dev/null 2>&1 || true
    kill_predictor_drivers_docker || { err "No se pudo detener el driver MakePrediction"; return 1; }
    docker exec minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc rm --recursive --force local/flight-data/checkpoints/predictor/ >/dev/null 2>&1 || true" ||
      return 1
    docker compose --profile predictor up -d spark-predictor || return 1
    wait_until 300 5 "predictor tras limpiar checkpoints" predictor_streams_ready || return 1
    ok "Predictor reiniciado con checkpoints limpios"
  else
    info "Operacion cancelada"
    return 1
  fi
}

# ============================================================
#   CLI Y MENU PRINCIPAL
# ============================================================

print_usage() {
  cat <<'EOF'
Uso:
  ./practica.sh                    Menu interactivo
  ./practica.sh OPCION             Ejecuta una opcion principal sin pausas
  ./practica.sh 10 SUBOPCION       Ejecuta un diagnostico sin pausas
  ./practica.sh 11 SUBOPCION       Muestra logs sin pausas

Opciones principales: 1..9
Diagnosticos: 1..9, a..f
Logs: 1..9, a..f

Variables utiles:
  PROJECT_HOME, ZONE, CLUSTER, K8S_NODE_COUNT, IMAGE_TAG
  K8S_FIREWALL_RULE, K8S_FIREWALL_SOURCE_RANGES
  PRACTICA_ASSUME_YES=1 para confirmar opciones destructivas no interactivas
  LOG_LINES=100 para cambiar el numero de lineas de log
EOF
}

dispatch_main_option() {
  local option="${1:-}" suboption="${2:-}"
  case "$option" in
    1) arrancar_docker ;;
    2) arrancar_k8s ;;
    3) reentrenar_docker ;;
    4) reentrenar_k8s ;;
    5) show_urls_docker ;;
    6) show_urls_k8s ;;
    7) apagar_k8s ;;
    8) parar_docker ;;
    9) limpiar_checkpoints_docker ;;
    10)
      if [ -n "$suboption" ]; then dispatch_diagnostic_option "$suboption"; else menu_diagnostico; fi
      ;;
    11)
      if [ -n "$suboption" ]; then dispatch_log_option "$suboption"; else menu_logs; fi
      ;;
    0) info "Hasta luego!" ;;
    -h|--help|help) print_usage ;;
    *) err "Opcion principal no valida: ${option:-vacia}"; print_usage; return 2 ;;
  esac
}

if [ "$#" -gt 0 ]; then
  PRACTICA_NONINTERACTIVE=1
  if { [ "$1" = "10" ] || [ "$1" = "11" ]; } && [ -z "${2:-}" ]; then
    err "La opcion $1 requiere una subopcion en modo no interactivo"
    print_usage
    exit 2
  fi
  dispatch_main_option "$1" "${2:-}"
  exit $?
fi

while true; do
  clear
  echo ""
  echo -e "${BOLD}${CYAN}"
  echo "  ██████╗ ██╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗ "
  echo "  ██╔══██╗██║██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗"
  echo "  ██████╔╝██║██║  ███╗    ██║  ██║███████║   ██║   ███████║"
  echo "  ██╔══██╗██║██║   ██║    ██║  ██║██╔══██║   ██║   ██╔══██║"
  echo "  ██████╔╝██║╚██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║"
  echo "  ╚═════╝ ╚═╝ ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝"
  echo -e "${NC}"
  echo -e "  ${BOLD}Practica Creativa Big Data -- ETSIT UPM 2026${NC}"
  echo -e "  ${CYAN}Iria Lozano y Javier Saguar${NC}"
  echo ""
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo -e "  ${BOLD}ARRANQUE${NC}"
  echo -e "  ${GREEN}1)${NC} Arrancar con Docker Compose"
  echo -e "  ${GREEN}2)${NC} Arrancar con Kubernetes (GKE)"
  echo ""
  echo -e "  ${BOLD}REENTRENAMIENTO${NC}"
  echo -e "  ${GREEN}3)${NC} Reentrenar modelo -- Docker"
  echo -e "  ${GREEN}4)${NC} Reentrenar modelo -- Kubernetes (DAG Airflow)"
  echo ""
  echo -e "  ${BOLD}GESTION${NC}"
  echo -e "  ${GREEN}5)${NC} Ver URLs actuales -- Docker"
  echo -e "  ${GREEN}6)${NC} Ver URLs actuales -- Kubernetes"
  echo -e "  ${GREEN}7)${NC} Apagar cluster GKE (ahorra dinero)"
  echo -e "  ${GREEN}8)${NC} Parar Docker Compose"
  echo -e "  ${GREEN}9)${NC} Limpiar checkpoints S3A (Docker)"
  echo ""
  echo -e "  ${BOLD}DIAGNOSTICO & LOGS${NC}"
  echo -e "  ${GREEN}10)${NC} Diagnostico del sistema"
  echo -e "  ${GREEN}11)${NC} Ver logs por servicio"
  echo ""
  echo -e "  ${GREEN}0)${NC} Salir"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
  echo -n "  Selecciona una opcion: "
  if ! read opcion; then
    break
  fi
  if [ "$opcion" = "0" ]; then
    echo ""
    info "Hasta luego!"
    echo ""
    break
  fi
  dispatch_main_option "$opcion" || warn "La opcion termino con errores"
done
