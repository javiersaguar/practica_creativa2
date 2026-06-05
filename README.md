# Práctica Big Data — Predicción de Retraso de Vuelos

![Spark](https://img.shields.io/badge/Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)
![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?logo=scala&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-4.2.0-231F20?logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-GKE-326CE5?logo=kubernetes&logoColor=white)

Sistema Big Data para entrenar y servir predicciones de retraso de vuelos con Spark, Kafka, Cassandra, MongoDB, MinIO/Iceberg, MLflow, Airflow, Prometheus y Grafana. El proyecto soporta despliegue con Docker Compose y despliegue cloud en Kubernetes sobre Google Kubernetes Engine.

**Autores**

- Iria Lozano Carrasco — <irialozanocarrasco26@gmail.com> — ETSIT UPM
- Javier Saguar — <javisaguarantona@gmail.com> — ETSIT UPM
- Asignatura: Ingeniería Big Data en la Nube (GISD)
- Curso: 2025-2026
- Repositorio reproducible: <https://github.com/javiersaguar/practica_creativa2>

## ✅ Requisitos Cumplidos

| Requisito | Puntos | Estado | Descripción de implementación |
|-----------|--------|--------|-------------------------------|
| Iceberg Data Lakehouse | 1 | ✅ | Datos en MinIO `s3a://flight-data/warehouse` mediante Apache Iceberg, tabla `minio.flights.training_data`. |
| Distancias en Cassandra | 1 | ✅ | Distancias en `agile_data_science.origin_dest_distances`; Flask las consulta desde Cassandra. |
| Kafka + Cassandra + WebSockets | 1 | ✅ | `MakePrediction` escribe en `flight-delay-ml-response` y Cassandra; Flask presenta la respuesta mediante WebSockets. |
| TrainModel lee/guarda Lakehouse | 1 | ✅ | `TrainModel.scala` lee `minio.flights.training_data` y guarda modelos en `s3a://flight-data/models`. |
| Docker Compose completo | 1 | ✅ | Servicios dockerizados, Spark Standalone y predictor en `deploy-mode cluster`. |
| K8s GKE completo | 3 | ✅ | Escenario completo desplegable en GKE y `deploy-mode cluster` verificable desde la API/UI de Spark. |
| Airflow + MLflow Docker | 1 | ✅ | Airflow orquesta reentrenamientos Spark y MLflow registra parámetros, métricas y estado. |
| GCloud | 1 | ✅ | VM de Compute Engine, GKE y Artifact Registry dentro del mismo proyecto de Google Cloud. |
| Observabilidad | 1 | ✅ | Prometheus, Grafana, métricas Flask y diagnósticos automáticos en `practica.sh`. |

## 📊 Arquitectura

El sistema implementa dos flujos principales: entrenamiento batch y predicción en tiempo real.

### Batch: entrenamiento

Los datos históricos de vuelos se incluyen en `data/simple_flight_delay_features.jsonl.bz2`. Durante el arranque se cargan en MinIO y se crea la tabla Iceberg `minio.flights.training_data` sobre `s3a://flight-data/warehouse`.

`TrainModel.scala` ejecuta Spark MLlib en `deploy-mode cluster`, lee la tabla Iceberg y guarda los componentes del modelo en `s3a://flight-data/models`:

- `arrival_bucketizer_2.0.bin`
- `string_indexer_model_Carrier.bin`
- `string_indexer_model_Origin.bin`
- `string_indexer_model_Dest.bin`
- `string_indexer_model_Route.bin`
- `numeric_vector_assembler.bin`
- `spark_random_forest_classifier.flight_delays.5.0.bin`

MLflow registra parámetros y métricas del entrenamiento, incluyendo `deploy_mode`, `training_records` y `accuracy`.

### Realtime: predicción

1. El usuario envía los datos del vuelo desde Flask.
2. Flask consulta la distancia origen-destino en Cassandra y genera un UUID.
3. Flask publica la petición en Kafka `flight-delay-ml-request`.
4. `MakePrediction.scala` consume la petición con Spark Structured Streaming, carga modelos desde MinIO y calcula la predicción.
5. Spark escribe el resultado en Kafka `flight-delay-ml-response`, Cassandra, MongoDB y consola.
6. Flask consume el topic de respuesta y emite `prediction_response` mediante Socket.IO/WebSocket.
7. El navegador muestra únicamente la respuesta cuyo UUID coincide con su petición.

El endpoint REST de respuesta se conserva como compatibilidad y diagnóstico, pero el flujo normal de la interfaz Kafka presenta la predicción mediante WebSocket.

### Servicios y puertos

| Servicio | Puerto Docker | Puerto K8s (NodePort) | Credenciales |
|----------|---------------|-----------------------|--------------|
| Flask UI | 5001 | 30001 | - |
| Spark UI | 8080 | 30880 | - |
| MLflow | 5002 | 30502 | - |
| Airflow | 8081 | 30808 | `admin/admin` |
| MinIO Console | 9001 | 30901 | `minioadmin/minioadmin` |
| Grafana | 3000 | 30300 | `admin/admin` |
| Prometheus | 9090 | 30909 | - |
| MongoDB | 27017 | - | - |
| Cassandra | 9042 | - | - |
| Kafka | 9092 | - | - |

### Deploy-Mode Cluster

`TrainModel` y `MakePrediction` se envían a Spark Standalone con `--deploy-mode cluster` tanto en Docker como en GKE. El driver se ejecuta dentro de un worker Spark, no dentro del contenedor o pod que lanza `spark-submit`.

Para evaluación:

- Docker Spark UI: `http://IP_VM:8080`
- K8s Spark UI: `http://IP_NODO_GKE:30880`
- `practica.sh` → `10` → `d` confirma que cada driver tiene un worker asignado.

## Entorno de despliegue (VM de Google Cloud)

El escenario completo está pensado para administrarse desde una VM de Compute Engine dentro del mismo proyecto de Google Cloud que GKE y Artifact Registry. Docker Compose se ejecuta en la VM; Kubernetes se despliega en el cluster GKE.

Especificaciones mínimas recomendadas para una VM nueva:

| Parámetro | Valor recomendado |
|-----------|-------------------|
| Sistema operativo | Debian 12 Bookworm o Ubuntu 22.04 LTS |
| Tipo de máquina | `e2-standard-4` — 4 vCPU, 16 GB RAM |
| Disco de arranque | 100 GB como mínimo |
| Scopes | Acceso total a las API Cloud o autenticación de usuario mediante `gcloud auth login` |
| Zona de la VM real | `europe-southwest1-b` |
| Zona del cluster GKE real | `europe-southwest1-a` |

La VM y el cluster pueden estar en zonas distintas de la misma región. Lo obligatorio para este repositorio es que la variable `ZONE` de `practica.sh` coincida con la zona del cluster GKE. El valor predeterminado del script es `europe-southwest1-a`; el ejemplo crea la VM en `europe-southwest1-b`.

Ejemplo recomendado con Debian 12, que es la imagen habitual de una VM nueva de GCloud:

```bash
export PROJECT_ID="TU_PROJECT_ID"
gcloud config set project "$PROJECT_ID"

gcloud compute instances create practica-bigdata-v2 \
  --zone=europe-southwest1-b \
  --machine-type=e2-standard-4 \
  --boot-disk-size=100GB \
  --boot-disk-type=pd-balanced \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --tags=practica-creativa \
  --scopes=https://www.googleapis.com/auth/cloud-platform
```

Si la política del proyecto no permite scopes amplios, inicia sesión en la VM con una cuenta de usuario que tenga permisos sobre Compute Engine, GKE y Artifact Registry:

```bash
gcloud auth login --no-launch-browser
gcloud config set project "$PROJECT_ID"
```

## Instalación de dependencias en la VM

Una VM limpia debe ejecutar primero `install.sh`. Detecta Debian frente a Ubuntu y configura el repositorio Docker correcto para cada distribución.

```bash
git clone https://github.com/javiersaguar/practica_creativa2.git
cd practica_creativa2
chmod +x install.sh practica.sh
./install.sh --all

# Después de añadir el usuario al grupo docker:
exit
# Volver a entrar por SSH.

docker info
./install.sh --verify
```

`install.sh` instala o verifica de forma idempotente:

- Docker Engine y Docker Compose plugin.
- Java 17.
- Google Cloud CLI.
- `kubectl` oficial en `/usr/local/bin`.
- Plugin oficial `gke-gcloud-auth-plugin` o el shim compatible usado por `practica.sh`.
- Dependencias Python de `requirements.txt` dentro de `.venv`.
- Versiones de herramientas y presencia de los recursos críticos del repositorio.

La autenticación GCloud se configura por separado, porque puede requerir interacción y permisos que dependen del proyecto:

```bash
gcloud auth login --no-launch-browser  # solo si la service account de la VM no tiene permisos suficientes
PROJECT_ID="TU_PROJECT_ID" ./install.sh --configure-gcloud
```

`install.sh` también mantiene un menú interactivo si se ejecuta sin argumentos.

## Firewall de la VM

Docker expone los servicios en la VM, pero GCP bloquea el acceso externo hasta crear una regla de firewall. Ejecuta los comandos en el proyecto que contiene la VM:

```bash
export PROJECT_ID="TU_PROJECT_ID"
export VM_NAME="practica-bigdata-v2"
export VM_ZONE="europe-southwest1-b"
export ADMIN_CIDR="TU_IP_PUBLICA/32"

gcloud compute instances add-tags "$VM_NAME" \
  --tags=practica-creativa \
  --zone="$VM_ZONE" \
  --project="$PROJECT_ID"

gcloud compute firewall-rules describe practica-creativa-web \
  --project="$PROJECT_ID" >/dev/null 2>&1 && \
gcloud compute firewall-rules update practica-creativa-web \
  --allow=tcp:3000,tcp:5001,tcp:5002,tcp:8080,tcp:8081,tcp:9001,tcp:9090 \
  --source-ranges="$ADMIN_CIDR" \
  --target-tags=practica-creativa \
  --project="$PROJECT_ID" || \
gcloud compute firewall-rules create practica-creativa-web \
  --direction=INGRESS \
  --action=ALLOW \
  --allow=tcp:3000,tcp:5001,tcp:5002,tcp:8080,tcp:8081,tcp:9001,tcp:9090 \
  --source-ranges="$ADMIN_CIDR" \
  --target-tags=practica-creativa \
  --project="$PROJECT_ID"
```

No uses `0.0.0.0/0` salvo para una evaluación temporal controlada. El firewall solo afecta al acceso desde fuera de la VM; los tests locales de `practica.sh` funcionan sin esta regla.

## Creación del cluster GKE desde cero

El cluster esperado se llama `practica-k8s` y, por defecto, está en `europe-southwest1-a`. `practica.sh` crea/verifica el repositorio Artifact Registry y publica imágenes con el Project ID activo; los manifests no contienen un Project ID fijo.

```bash
export PROJECT_ID="TU_PROJECT_ID"
export ZONE="europe-southwest1-a"
export CLUSTER="practica-k8s"
export USE_GKE_GCLOUD_AUTH_PLUGIN=True

gcloud auth login --no-launch-browser  # necesario si la service account de la VM no tiene permisos
gcloud config set project "$PROJECT_ID"
gcloud config set compute/zone "$ZONE"
gcloud services enable compute.googleapis.com container.googleapis.com artifactregistry.googleapis.com

# Deben funcionar antes de continuar:
gcloud container clusters list --project="$PROJECT_ID"
gcloud compute firewall-rules list --project="$PROJECT_ID"
```

Crear el cluster zonal:

```bash
gcloud container clusters create "$CLUSTER" \
  --zone="$ZONE" \
  --num-nodes=2 \
  --machine-type=e2-standard-2 \
  --disk-type=pd-balanced \
  --disk-size=100 \
  --enable-ip-alias \
  --project="$PROJECT_ID"

gcloud container clusters get-credentials "$CLUSTER" \
  --zone="$ZONE" \
  --project="$PROJECT_ID"

kubectl get nodes
```

Para ahorrar costes, el cluster puede dejarse con 0 nodos después de crearlo:

```bash
gcloud container clusters resize "$CLUSTER" \
  --num-nodes=0 \
  --zone="$ZONE" \
  --project="$PROJECT_ID" \
  --quiet
```

La opción `2` de `practica.sh` vuelve a escalar automáticamente el cluster al número de nodos indicado por `K8S_NODE_COUNT`, con valor predeterminado `2`. `e2-standard-2` usa 4 vCPU totales con dos nodos y suele caber en la cuota inicial; usa `e2-standard-4` solo si el proyecto tiene al menos 8 vCPU regionales disponibles.

Consulta las cuotas antes de crear el cluster:

```bash
REGION="${ZONE%-*}"
gcloud compute regions describe "$REGION" \
  --flatten="quotas[]" \
  --filter="quotas.metric=CPUS" \
  --format="table(quotas.metric,quotas.usage,quotas.limit)"
gcloud compute project-info describe \
  --flatten="quotas[]" \
  --filter="quotas.metric=CPUS_ALL_REGIONS" \
  --format="table(quotas.metric,quotas.usage,quotas.limit)"
```

### Firewall NodePort de GKE

`./practica.sh 2` detecta el tag real de los nodos, crea o actualiza la regla `practica-k8s-nodeports` y abre `30001,30300,30502,30808,30880,30901,30909`. El Project ID y la red también se detectan en tiempo de ejecución. Si se indica un CIDR restringido, el script añade automáticamente las IP interna y pública de la VM para poder ejecutar sus verificaciones externas de NodePort. Restringe el origen para uso normal:

```bash
export K8S_FIREWALL_SOURCE_RANGES="TU_IP_PUBLICA/32"
./practica.sh 2
```

Para una evaluación temporal desde ubicaciones cambiantes puede usarse `K8S_FIREWALL_SOURCE_RANGES=0.0.0.0/0`. Si la cuenta no puede administrar firewall, un administrador puede ejecutar el equivalente manual:

```bash
NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
NODE_ZONE="$(gcloud compute instances list --filter="name=$NODE" --format='value(zone.basename())' --project="$PROJECT_ID")"
NODE_TAGS="$(gcloud compute instances describe "$NODE" --zone="$NODE_ZONE" --project="$PROJECT_ID" --format='value(tags.items)')"
NODE_TAG="$(printf '%s\n' "$NODE_TAGS" | tr ';, ' '\n' | grep -E "^gke-${CLUSTER}-.*-node$" | head -1)"
NETWORK="$(gcloud container clusters describe "$CLUSTER" --zone="$ZONE" --project="$PROJECT_ID" --format='value(network)')"
NETWORK="${NETWORK##*/}"
PORTS="tcp:30001,tcp:30300,tcp:30502,tcp:30808,tcp:30880,tcp:30901,tcp:30909"
K8S_FIREWALL_SOURCE_RANGES="${K8S_FIREWALL_SOURCE_RANGES:-TU_IP_PUBLICA/32}"
VM_PUBLIC_IP="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip)"
VM_INTERNAL_IP="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)"
K8S_FIREWALL_SOURCE_RANGES="$K8S_FIREWALL_SOURCE_RANGES,$VM_PUBLIC_IP/32,$VM_INTERNAL_IP/32"

gcloud compute firewall-rules describe practica-k8s-nodeports --project="$PROJECT_ID" >/dev/null 2>&1 && \
gcloud compute firewall-rules update practica-k8s-nodeports \
  --allow="$PORTS" --source-ranges="$K8S_FIREWALL_SOURCE_RANGES" \
  --target-tags="$NODE_TAG" --project="$PROJECT_ID" || \
gcloud compute firewall-rules create practica-k8s-nodeports \
  --network="$NETWORK" --direction=INGRESS \
  --allow="$PORTS" --source-ranges="$K8S_FIREWALL_SOURCE_RANGES" \
  --target-tags="$NODE_TAG" --project="$PROJECT_ID"
```

## 🚀 Primer Uso

Flujo completo desde cero:

1. Crear la VM de Compute Engine.
2. Clonar este repositorio.
3. Ejecutar `./install.sh --all` y volver a entrar por SSH para activar el grupo `docker`.
4. Autenticar una cuenta autorizada y crear el cluster GKE o recuperar sus credenciales.
5. Ejecutar `./practica.sh 1` para Docker o `./practica.sh 2` para GKE.
6. Ejecutar los diagnósticos `./practica.sh 10 c`, `10 d` y `10 f` para certificar K8s.

Los datos necesarios ya se incluyen en Git:

```text
data/origin_dest_distances.jsonl
data/simple_flight_delay_features.jsonl.bz2
```

Los contextos de build necesarios también están incluidos:

```text
docker/kafka/
docker/spark/
```

Arranque:

```bash
git clone https://github.com/javiersaguar/practica_creativa2.git
cd practica_creativa2

chmod +x install.sh practica.sh
./install.sh --all
exit

# Volver a entrar por SSH:
cd practica_creativa2
docker info
./practica.sh 1
./practica.sh 10 b

# Flujo GKE, después de crear el cluster:
export PROJECT_ID="TU_PROJECT_ID"
export ZONE="europe-southwest1-a"
export CLUSTER="practica-k8s"
export K8S_FIREWALL_SOURCE_RANGES="TU_IP_PUBLICA/32"
gcloud auth login --no-launch-browser
gcloud config set project "$PROJECT_ID"
./practica.sh 2
./practica.sh 10 c
./practica.sh 10 d
./practica.sh 10 f
```

En `practica.sh`:

- Opción `1`: arranque completo con Docker Compose.
- Opción `2`: autenticación, firewall NodePort, publicación de imágenes y despliegue completo en GKE.

El JAR compilado ya se incluye en `shared-jars/flight_prediction_2.13-0.1.jar` y en el contexto Spark; `practica.sh` verifica que ambas copias coincidan. El runtime Iceberg 1.10.1 también está versionado en `docker/spark/iceberg-spark-runtime.jar` y el Dockerfile lo copia a `/opt/spark/jars/`, por lo que los drivers en `deploy-mode cluster` no dependen de una descarga en caliente. SHA-256 esperado:

```text
2192a0881ed0f5773b5a83a8820d2b0b2069beec203028643a1c338551007f09  docker/spark/iceberg-spark-runtime.jar
```

Las demás dependencias Kafka, Cassandra, MongoDB y S3A se descargan con versiones fijas durante el build mediante `docker/spark/download_jars.sh`. La recompilación del JAR propio solo ocurre si se modifica el código Scala:

```bash
docker run --rm \
  -v "$PWD/flight_prediction":/app \
  -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.10_2.13.18 \
  sbt clean assembly

cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar shared-jars/
cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar docker/spark/
```

## Uso de `practica.sh`

`practica.sh` es el punto de entrada operativo. Centraliza arranque, reentrenamiento, URLs, diagnósticos, logs y mantenimiento.

Puede usarse con menú o directamente desde CLI. El modo CLI no hace pausas y devuelve un código distinto de cero cuando una operación falla:

```bash
./practica.sh 1       # Arranque Docker completo + test end-to-end
./practica.sh 3       # Reentrenamiento Docker + test posterior
./practica.sh 10 b    # Test end-to-end Docker
./practica.sh 10 e    # Versiones
./practica.sh 10 c    # Test end-to-end K8s, incluidos sinks y WebSocket
./practica.sh 10 d    # Evidencia deploy-mode cluster Docker y K8s
./practica.sh 10 f    # Versiones reales dentro de los pods K8s
./practica.sh 11 d    # Logs del driver más reciente en spark-worker-1
PRACTICA_ASSUME_YES=1 ./practica.sh 9
```

Para GKE se pueden sobrescribir `ZONE`, `CLUSTER`, `K8S_NODE_COUNT` e `IMAGE_TAG` sin editar scripts ni manifests.

### Arranque

1. **Arrancar con Docker Compose**
   Levanta los servicios, configura MinIO y Cassandra, crea la tabla Iceberg, entrena el modelo y arranca el predictor Spark Streaming.
   En una VM limpia el primer arranque puede tardar 15-20 minutos por build de imágenes, arranque de Cassandra, creación Iceberg y entrenamiento `TrainModel`. Es normal que Cassandra tarde varios minutos en aceptar CQL.

2. **Arrancar con Kubernetes (GKE)**
   Autentica con GKE, escala el cluster, detecta el tag real de los nodos y configura el firewall. Después crea Artifact Registry, construye y publica Spark/Kafka/Flask/Airflow, renderiza los manifests con el Project ID activo, distribuye los dos Spark workers entre nodos GKE, carga los datos por `stdin`, crea Cassandra e Iceberg, entrena, arranca el predictor y valida polling, Kafka, Cassandra, MongoDB, WebSocket y NodePorts.

### Reentrenamiento

3. **Reentrenar modelo — Docker**
   Lanza manualmente el DAG de Airflow que ejecuta `TrainModel` en Spark `deploy-mode cluster`.

4. **Reentrenar modelo — Kubernetes**
   Lanza manualmente el DAG de Airflow K8s, que usa el `kubectl` incluido en la imagen Airflow y RBAC para enviar `TrainModel` al cluster Spark. Al terminar confirma el worker del driver, reinicia el predictor y repite sinks y WebSocket.

El DAG no tiene calendario automático (`schedule_interval=None`) y permanece habilitado. Así, únicamente las opciones `3` y `4` disparan reentrenamientos manuales y ninguna ejecución semanal pendiente puede interrumpir el predictor durante el arranque.

### Gestión

- `5`: ver URLs Docker.
- `6`: ver URLs Kubernetes.
- `7`: apagar nodos del cluster GKE.
- `8`: parar Docker Compose sin borrar volúmenes.
- `9`: limpiar checkpoints S3A del predictor Docker.

### Diagnóstico

La opción `10` abre comprobaciones de:

- Pipeline completo.
- Cassandra, Kafka, MinIO/Iceberg, MongoDB y Spark Streaming.
- WebSockets reales para Docker y K8s.
- Prometheus, Grafana, MLflow y Airflow.
- Tests end-to-end Docker/K8s.
- Drivers Spark en `deploy-mode cluster`.
- Versiones exigidas en Docker (`10 e`) y en pods Kubernetes (`10 f`).

La opción `11` muestra logs por servicio y logs de drivers dentro de los Spark workers.

## ⚠️ Errores Comunes y Soluciones

### Error 1: la predicción se queda en `Processing...`

**Causa:** predictor Spark detenido, error en el pipeline o checkpoints incompatibles. En VMs lentas, un arranque antiguo podía dejar Flask muerto si Cassandra tardaba demasiado en aceptar conexiones; el bootstrap quedaba a medias y faltaban distancias, modelos o tabla Iceberg.

**Solución:**

```text
practica.sh -> 1         Relanzar arranque Docker; es idempotente y completa lo que falte
practica.sh -> 10 -> 4   Diagnóstico WebSockets
practica.sh -> 10 -> d   Diagnóstico deploy-mode cluster
practica.sh -> 9         Limpiar checkpoints S3A Docker
practica.sh -> 11        Logs por servicio
```

La versión actual espera explícitamente a que Cassandra acepte CQL y a que Flask arranque conectado a Cassandra antes de continuar. Si un arranque fue interrumpido en una VM lenta, vuelve a ejecutar `./practica.sh` opción `1`; el script recrea tablas, recarga distancias, rehace Iceberg, reentrena y arranca el predictor sin intervención manual.

### Error 2: MLflow vacío o `TrainModel` no registra runs

Comprueba los modelos en MinIO y ejecuta el reentrenamiento:

```text
practica.sh -> 3   Reentrenamiento Docker
practica.sh -> 4   Reentrenamiento Kubernetes
```

### Error 3: `gcloud auth` muestra `insufficient authentication scopes`

La VM usa una service account con scopes insuficientes. Autentica una cuenta de usuario:

```bash
gcloud auth login --no-launch-browser
gcloud config set project TU_PROJECT_ID
```

### Error 4: `kubectl: command not found`

Ejecuta `install.sh` → opción `4`, o instala todo con la opción `0`.

### Error 5: `gke-gcloud-auth-plugin not found`

Ejecuta `install.sh` → opción `5`. Instala el plugin oficial cuando está disponible y usa un shim compatible como fallback.

### Error 6: `No resources found` en `kubectl get nodes`

El cluster puede estar escalado a 0 nodos:

```bash
./practica.sh
# Opción 2, o:
gcloud container clusters resize practica-k8s \
  --num-nodes=2 \
  --zone=europe-southwest1-a \
  --quiet
```

### Error 7: Airflow no carga en el puerto 8081 o queda `Init:0/1` en K8s

La inicialización de la base de datos y del usuario admin es idempotente. Docker y K8s eliminan el PID obsoleto del webserver antes de arrancar; además, `practica.sh` espera el healthcheck y realiza un único restart automático en Docker si el primer arranque no queda listo. En K8s se usa la imagen propia `airflow`, que contiene `kubectl v1.36.1`, un `emptyDir` escribible mediante `fsGroup` y RBAC para `pods/exec` y `deployments/scale`.

```bash
kubectl describe pod -l app=airflow
kubectl logs deployment/airflow -c airflow-init
kubectl auth can-i create pods/exec --as=system:serviceaccount:default:airflow-sa
kubectl auth can-i patch deployments/scale --as=system:serviceaccount:default:airflow-sa
```

### Error 8: el JAR es más antiguo que el código Scala

Recompila el JAR usando el comando indicado en Primer Uso. `practica.sh` también detecta si las fuentes Scala son más recientes que el JAR.

### Error 9: la IP externa cambia tras reiniciar la VM

Reserva una IP estática o consulta la IP actual:

```bash
gcloud compute instances describe practica-bigdata-v2 \
  --zone=europe-southwest1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

### Error 10: `docker compose down -v` elimina los modelos

Los modelos Docker están en el volumen de MinIO. No uses `down -v` salvo que quieras resetear el escenario y volver a entrenar.

### Error 11: `docker/spark` o `docker/kafka` no encontrado al hacer build

**Causa:** el repositorio se clonó desde una versión antigua que no incluía los contextos de build.

```bash
git pull
test -f docker/spark/Dockerfile
test -f docker/kafka/Dockerfile
```

### Error 12: `data/` vacío o falla la creación de Iceberg

**Causa:** el repositorio se clonó desde una versión antigua que no incluía los datos.

```bash
git pull
bash resources/download_data.sh
sha256sum data/simple_flight_delay_features.jsonl.bz2 data/origin_dest_distances.jsonl
```

### Error 13: GKE devuelve `PERMISSION_DENIED` o `insufficient authentication scopes`

La automatización no puede concederse permisos a sí misma. La cuenta activa necesita permisos para GKE, Artifact Registry, consulta de instancias y administración del firewall. Usa una cuenta de usuario autorizada con `gcloud auth login --no-launch-browser` o pide a un administrador del proyecto que conceda los roles necesarios a la service account de la VM:

```bash
export PROJECT_ID="TU_PROJECT_ID"
export VM_SERVICE_ACCOUNT="SERVICE_ACCOUNT_DE_LA_VM"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$VM_SERVICE_ACCOUNT" \
  --role="roles/container.admin"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$VM_SERVICE_ACCOUNT" \
  --role="roles/artifactregistry.admin"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$VM_SERVICE_ACCOUNT" \
  --role="roles/compute.viewer"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$VM_SERVICE_ACCOUNT" \
  --role="roles/compute.securityAdmin"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$VM_SERVICE_ACCOUNT" \
  --role="roles/serviceusage.serviceUsageAdmin"
```

En organizaciones que no conceden acceso de lectura de Artifact Registry a la cuenta de nodos GKE, un administrador también debe ejecutar:

```bash
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

Estos roles son amplios para una práctica reproducible; en producción deben sustituirse por roles personalizados de mínimo privilegio.

### Error 14: no hay cuota para crear dos nodos GKE

Crea el cluster con `e2-standard-2` y dos nodos: consume 4 vCPU frente a las 8 vCPU de `e2-standard-4` por dos nodos. Comprueba `CPUS` regional y `CPUS_ALL_REGIONS` con los comandos de la sección de creación del cluster. Si aún no cabe, solicita aumento de cuota en:

```text
https://console.cloud.google.com/iam-admin/quotas
```

Como último recurso puede usarse temporalmente `K8S_NODE_COUNT=1 ./practica.sh 2`, pero la evidencia distribuida debe realizarse con dos workers.

### Error 15: `ImagePullBackOff` en imágenes propias

Comprueba que `./practica.sh 2` construyó y publicó Spark, Kafka, Flask y Airflow usando el Project ID activo:

```bash
gcloud artifacts docker images list "${ZONE%-*}-docker.pkg.dev/$PROJECT_ID/practica"
kubectl describe pod NOMBRE_DEL_POD
```

No edites los manifests con un Project ID fijo. `practica.sh` sustituye `PRACTICA_REGISTRY` y `PRACTICA_TAG` en copias temporales antes de aplicar los YAML.

## Versiones Instaladas

| Componente | Versión | Notas |
|------------|---------|-------|
| Apache Spark | 4.1.1 | Scala 2.13, deploy-mode cluster |
| Scala | 2.13.17 | Runtime usado por Spark |
| Apache Kafka | 4.2.0 | KRaft mode, sin Zookeeper |
| MongoDB | 7.0.17 | Persistencia compatible y diagnóstico |
| Apache Cassandra | 4.1.11 | Distancias y predicciones evaluables |
| Apache Airflow | 2.10.4 | Orquestación de entrenamiento |
| MLflow | 2.19.0 | Tracking de entrenamientos |
| Python | 3.10.x | Flask y utilidades |
| MinIO | RELEASE.2025-09-07T16-13-09Z | Almacenamiento S3-compatible |
| Apache Iceberg | 1.10.1 | Tabla Lakehouse |
| Prometheus | 3.12.0 | Métricas |
| Grafana | 13.0.2 | Dashboard |

## Estructura del Repositorio

```text
.
├── install.sh
├── practica.sh
├── docker-compose.yml
├── data/
│   ├── origin_dest_distances.jsonl
│   └── simple_flight_delay_features.jsonl.bz2
├── docker/
│   ├── airflow/
│   ├── flask/
│   ├── kafka/
│   ├── prometheus/
│   ├── grafana/
│   └── spark/
├── flight_prediction/
├── shared-jars/
├── k8s/
├── k8s-gke/
└── resources/web/
```

- `install.sh`: prepara una VM Debian 12 o Ubuntu 22.04 limpia.
- `practica.sh`: gestiona arranque, reentrenamiento, diagnósticos y mantenimiento.
- `docker-compose.yml`: define el escenario Docker.
- `docker/kafka/` y `docker/spark/`: contextos de build requeridos por Docker Compose y GKE; Spark incluye el runtime Iceberg versionado.
- `docker/airflow/`: imagen Airflow 2.10.4 sobre Python 3.10 con `kubectl` fijado para ejecutar el DAG dentro de GKE.
- `data/`: datos de entrenamiento y distancias incluidos para reproducibilidad.
- `shared-jars/`: JAR Scala compilado.
- `k8s-gke/`: manifests usados por el despliegue GKE.
- `resources/web/`: aplicación Flask y frontend WebSocket.

---

**Para evaluación:** ejecuta `practica.sh` → `10` para comprobar todos los componentes y `10` → `d` para demostrar que los drivers Spark están ejecutándose dentro de workers.
