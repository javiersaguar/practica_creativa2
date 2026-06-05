from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {"owner": "practica", "retries": 0}

bash_cmd = r'''
set -euo pipefail

if [ "${IN_K8S:-false}" = "true" ]; then
  KUBECTL="${KUBECTL:-/tmp/kubectl}"
  if [ ! -x "$KUBECTL" ]; then
    echo "kubectl no disponible en $KUBECTL"
    exit 1
  fi

  echo "=== Stop K8s predictor ==="
  "$KUBECTL" scale deployment/spark-predictor --replicas=0 || true

  cleanup() {
    echo "=== Restart K8s predictor ==="
    "$KUBECTL" scale deployment/spark-predictor --replicas=1 || true
  }
  trap cleanup EXIT

  SPARK_POD="$("$KUBECTL" get pod -l app=spark-master -o jsonpath='{.items[0].metadata.name}')"
  if [ -z "$SPARK_POD" ]; then
    echo "spark-master pod no encontrado"
    exit 1
  fi

  echo "=== Kill Spark drivers in K8s ==="
  "$KUBECTL" exec -i "$SPARK_POD" -- python3 <<'INNER'
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
INNER

  echo "=== TrainModel K8s cluster mode ==="
  "$KUBECTL" exec "$SPARK_POD" -- bash -lc '
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
'

  echo "=== K8s training finished ==="
  exit 0
fi

echo "=== Kill Spark drivers ==="
python3 <<'INNER'
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
INNER

docker stop spark-predictor || true

cleanup() {
  echo "=== Restart predictor ==="
  docker start spark-predictor || true
}
trap cleanup EXIT

echo "=== TrainModel cluster mode ==="
docker exec spark-master /opt/spark/bin/spark-submit \
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
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.minio=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.minio.type=hadoop \
  --conf spark.sql.catalog.minio.warehouse=s3a://flight-data/warehouse \
  file:///shared-jars/flight_prediction_2.13-0.1.jar

echo "=== Training finished ==="
'''

with DAG(
    "retrain_flight_delay_model",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["practica", "spark", "mlflow"],
) as dag:
    BashOperator(
        task_id="train_model_cluster_mode",
        bash_command=bash_cmd,
        execution_timeout=timedelta(minutes=45),
    )
