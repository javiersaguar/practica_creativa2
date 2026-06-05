#!/usr/bin/env bash

# Prepara una VM Debian 12 o Ubuntu 22.04 de Google Cloud para ejecutar practica.sh.
# No despliega servicios ni modifica la logica de la practica.

set -u
set -o pipefail

PROJECT_HOME="${PROJECT_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
ZONE="${ZONE:-europe-southwest1-a}"
CLUSTER="${CLUSTER:-practica-k8s}"
VENV_DIR="${VENV_DIR:-$PROJECT_HOME/.venv}"
APT_UPDATED=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

header() {
  clear 2>/dev/null || true
  echo ""
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo -e "${BOLD}${BLUE}  $1${NC}"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
}

info() {
  echo -e "  ${CYAN}INFO${NC} $*"
}

ok() {
  echo -e "  ${GREEN}OK${NC} $*"
}

warn() {
  echo -e "  ${YELLOW}AVISO${NC} $*"
}

err() {
  echo -e "  ${RED}ERROR${NC} $*"
}

pause() {
  if [ ! -t 0 ]; then
    return 0
  fi
  echo ""
  read -r -p "  Pulsa ENTER para continuar..." _ || true
}

run_root() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    err "Se necesita sudo para ejecutar: $*"
    return 1
  fi
}

apt_update() {
  if [ "$APT_UPDATED" -eq 0 ]; then
    info "Actualizando indice APT..."
    run_root apt-get update || return 1
    APT_UPDATED=1
  fi
}

install_base_packages() {
  apt_update || return 1
  run_root apt-get install -y ca-certificates curl gnupg lsb-release apt-transport-https || return 1
}

target_user() {
  if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
    printf '%s\n' "$SUDO_USER"
  else
    id -un
  fi
}

install_docker() {
  header "INSTALACION -- DOCKER ENGINE + COMPOSE"

  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    ok "Docker Engine y Docker Compose plugin ya estan instalados"
  else
    install_base_packages || return 1
    run_root install -m 0755 -d /etc/apt/keyrings || return 1

    . /etc/os-release
    local docker_distro
    case "${ID:-}" in
      debian|ubuntu) docker_distro="$ID" ;;
      *)
        err "Sistema operativo no soportado para instalacion automatica de Docker: ${PRETTY_NAME:-${ID:-desconocido}}"
        return 1
        ;;
    esac

    curl -fsSL "https://download.docker.com/linux/$docker_distro/gpg" -o /tmp/docker.asc || return 1
    run_root install -m 0644 /tmp/docker.asc /etc/apt/keyrings/docker.asc || return 1

    local arch
    arch="$(dpkg --print-architecture)"
    printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/%s %s stable\n' \
      "$arch" "$docker_distro" "$VERSION_CODENAME" > /tmp/docker.list
    run_root install -m 0644 /tmp/docker.list /etc/apt/sources.list.d/docker.list || return 1

    APT_UPDATED=0
    apt_update || return 1
    run_root apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin || return 1
    ok "Docker Engine y Docker Compose plugin instalados"
  fi

  local user
  user="$(target_user)"
  if id -nG "$user" | tr ' ' '\n' | grep -qx docker; then
    ok "El usuario $user ya pertenece al grupo docker"
  else
    run_root usermod -aG docker "$user" || return 1
    warn "Usuario $user anadido al grupo docker. Cierra la sesion SSH y vuelve a entrar."
  fi

  run_root systemctl enable --now docker >/dev/null 2>&1 || warn "No se pudo habilitar Docker con systemctl"
}

install_java() {
  header "INSTALACION -- JAVA 17"

  if command -v java >/dev/null 2>&1 && java -version 2>&1 | head -1 | grep -q '"17'; then
    ok "Java 17 ya esta instalado"
    return 0
  fi

  apt_update || return 1
  run_root apt-get install -y openjdk-17-jdk || return 1
  ok "Java 17 instalado"
}

install_gcloud() {
  header "INSTALACION -- GCLOUD CLI"

  if command -v gcloud >/dev/null 2>&1 && gcloud version >/dev/null 2>&1; then
    ok "gcloud CLI ya esta instalado: $(command -v gcloud)"
    return 0
  fi

  install_base_packages || return 1
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg -o /tmp/google-cloud.asc || return 1
  gpg --dearmor --yes --output /tmp/google-cloud.gpg /tmp/google-cloud.asc || return 1
  run_root install -m 0644 /tmp/google-cloud.gpg /usr/share/keyrings/cloud.google.gpg || return 1

  printf '%s\n' \
    'deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main' \
    > /tmp/google-cloud-sdk.list
  run_root install -m 0644 /tmp/google-cloud-sdk.list /etc/apt/sources.list.d/google-cloud-sdk.list || return 1

  APT_UPDATED=0
  apt_update || return 1
  run_root apt-get install -y google-cloud-cli || return 1
  ok "gcloud CLI instalado"
}

install_kubectl() {
  header "INSTALACION -- KUBECTL"

  if command -v kubectl >/dev/null 2>&1; then
    ok "kubectl ya esta instalado: $(command -v kubectl)"
    return 0
  fi

  install_base_packages || return 1
  local version arch
  version="$(curl -L -s https://dl.k8s.io/release/stable.txt)" || return 1
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *)
      err "Arquitectura no soportada automaticamente: $(uname -m)"
      return 1
      ;;
  esac

  curl -fsSL "https://dl.k8s.io/release/${version}/bin/linux/${arch}/kubectl" -o /tmp/kubectl || return 1
  run_root install -m 0755 /tmp/kubectl /usr/local/bin/kubectl || return 1
  ok "kubectl $version instalado en /usr/local/bin/kubectl"
}

install_gke_auth_plugin() {
  header "INSTALACION -- GKE GCLOUD AUTH PLUGIN"

  if command -v gke-gcloud-auth-plugin >/dev/null 2>&1 && gke-gcloud-auth-plugin --version >/dev/null 2>&1; then
    ok "gke-gcloud-auth-plugin ya esta instalado"
    return 0
  fi

  install_gcloud || return 1
  apt_update || return 1

  if run_root apt-get install -y google-cloud-cli-gke-gcloud-auth-plugin 2>/dev/null ||
     run_root apt-get install -y google-cloud-sdk-gke-gcloud-auth-plugin 2>/dev/null; then
    ok "Plugin oficial gke-gcloud-auth-plugin instalado"
    return 0
  fi

  warn "No se encontro el paquete oficial; instalando el shim compatible usado por practica.sh"
  cat > /tmp/gke-gcloud-auth-plugin <<'SHIM'
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
  run_root install -m 0755 /tmp/gke-gcloud-auth-plugin /usr/local/bin/gke-gcloud-auth-plugin || return 1
  ok "Shim gke-gcloud-auth-plugin instalado en /usr/local/bin"
}

install_python_dependencies() {
  header "INSTALACION -- DEPENDENCIAS PYTHON"

  apt_update || return 1
  run_root apt-get install -y python3 python3-pip python3-venv python3-dev build-essential || return 1

  if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR" || return 1
    ok "Entorno virtual creado en $VENV_DIR"
  else
    ok "El entorno virtual ya existe en $VENV_DIR"
  fi

  "$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel || return 1
  "$VENV_DIR/bin/python" -m pip install -r "$PROJECT_HOME/requirements.txt" || return 1
  ok "Dependencias de requirements.txt instaladas"
  info "Activa el entorno cuando sea necesario: source .venv/bin/activate"
}

ensure_repo_data() {
  header "VERIFICACION -- DATOS DEL REPOSITORIO"

  info "Verificando presencia e integridad de los datos"
  bash "$PROJECT_HOME/resources/download_data.sh" || return 1
  [ -s "$PROJECT_HOME/data/origin_dest_distances.jsonl" ] &&
    [ -s "$PROJECT_HOME/data/simple_flight_delay_features.jsonl.bz2" ] ||
    { err "Los datos siguen ausentes tras la descarga"; return 1; }
  ok "Datos descargados y verificados"
}

configure_gcloud() {
  header "CONFIGURACION -- AUTENTICACION GCLOUD"

  install_gcloud || return 1

  local active_account current_project selected_project current_zone selected_zone
  active_account="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -1)"
  if [ -z "$active_account" ]; then
    if [ ! -t 0 ]; then
      err "No hay cuenta gcloud activa. Ejecuta primero: gcloud auth login --no-launch-browser"
      return 1
    fi
    warn "No hay una cuenta activa. Iniciando autenticacion interactiva."
    gcloud auth login --no-launch-browser || return 1
    active_account="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -1)"
  fi
  ok "Cuenta activa: $active_account"

  current_project="$(gcloud config get-value project 2>/dev/null || true)"
  if [ -t 0 ]; then
    read -r -p "  Project ID [${PROJECT_ID:-${current_project:-sin configurar}}]: " selected_project || return 1
  else
    selected_project=""
  fi
  selected_project="${selected_project:-${PROJECT_ID:-$current_project}}"
  if [ -z "$selected_project" ]; then
    err "Debes indicar un Project ID"
    return 1
  fi
  gcloud config set project "$selected_project" >/dev/null || return 1

  current_zone="$(gcloud config get-value compute/zone 2>/dev/null || true)"
  if [ -t 0 ]; then
    read -r -p "  Zona del cluster GKE [${current_zone:-$ZONE}]: " selected_zone || return 1
  else
    selected_zone=""
  fi
  selected_zone="${selected_zone:-${current_zone:-$ZONE}}"
  gcloud config set compute/zone "$selected_zone" >/dev/null || return 1

  info "Habilitando APIs necesarias..."
  gcloud services enable compute.googleapis.com container.googleapis.com artifactregistry.googleapis.com || return 1
  ok "Proyecto $selected_project y zona $selected_zone configurados"
  info "Cluster esperado por practica.sh: $CLUSTER"
}

verify_installation() {
  header "VERIFICACION FINAL"

  local failures=0

  if command -v docker >/dev/null 2>&1; then
    ok "Docker: $(docker --version)"
    docker compose version >/dev/null 2>&1 && ok "Docker Compose: $(docker compose version --short)" ||
      { err "Docker Compose plugin no disponible"; failures=$((failures + 1)); }
  else
    err "Docker no disponible"
    failures=$((failures + 1))
  fi

  if command -v java >/dev/null 2>&1; then
    ok "Java: $(java -version 2>&1 | head -1)"
  else
    err "Java no disponible"
    failures=$((failures + 1))
  fi

  if command -v gcloud >/dev/null 2>&1 && gcloud version >/dev/null 2>&1; then
    ok "gcloud: $(gcloud version | head -1)"
  else
    err "gcloud no disponible"
    failures=$((failures + 1))
  fi

  if command -v kubectl >/dev/null 2>&1; then
    ok "kubectl: $(kubectl version --client 2>/dev/null | head -1)"
  else
    err "kubectl no disponible"
    failures=$((failures + 1))
  fi

  if command -v gke-gcloud-auth-plugin >/dev/null 2>&1; then
    ok "gke-gcloud-auth-plugin: $(gke-gcloud-auth-plugin --version 2>/dev/null | head -1)"
  else
    err "gke-gcloud-auth-plugin no disponible"
    failures=$((failures + 1))
  fi

  if [ -x "$VENV_DIR/bin/python" ]; then
    ok "Python virtualenv: $("$VENV_DIR/bin/python" --version)"
  else
    err "Entorno Python no disponible en $VENV_DIR"
    failures=$((failures + 1))
  fi

  local required
  for required in \
    docker/spark/Dockerfile \
    docker/spark/download_jars.sh \
    docker/spark/iceberg-spark-runtime.jar \
    docker/spark/flight_prediction_2.13-0.1.jar \
    docker/kafka/Dockerfile \
    docker/kafka/start-kafka.sh \
    data/origin_dest_distances.jsonl \
    data/simple_flight_delay_features.jsonl.bz2 \
    resources/download_data.sh \
    shared-jars/flight_prediction_2.13-0.1.jar; do
    if [ -f "$PROJECT_HOME/$required" ]; then
      ok "Recurso del repositorio presente: $required"
    else
      err "Falta recurso del repositorio: $required"
      failures=$((failures + 1))
    fi
  done
  if ! cmp -s \
      "$PROJECT_HOME/shared-jars/flight_prediction_2.13-0.1.jar" \
      "$PROJECT_HOME/docker/spark/flight_prediction_2.13-0.1.jar"; then
    err "Las copias del JAR flight_prediction en shared-jars/ y docker/spark/ no coinciden"
    failures=$((failures + 1))
  fi

  if command -v docker >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
    warn "Docker esta instalado, pero la sesion actual no puede usar el daemon. Vuelve a entrar por SSH."
  fi

  if [ "$failures" -eq 0 ]; then
    ok "VM preparada para ejecutar practica.sh"
  else
    err "Verificacion terminada con $failures problema(s)"
    return 1
  fi
}

install_all() {
  local step
  for step in \
    install_docker \
    install_java \
    install_gcloud \
    install_kubectl \
    install_gke_auth_plugin \
    install_python_dependencies \
    ensure_repo_data \
    verify_installation; do
    "$step" || {
      err "Fallo el paso $step"
      return 1
    }
  done
}

run_cli() {
  case "${1:-}" in
    --all) install_all ;;
    --configure-gcloud) configure_gcloud ;;
    --verify) verify_installation ;;
    --docker) install_docker ;;
    --help|-h)
      cat <<'EOF'
Uso:
  ./install.sh --all               Instala/verifica herramientas y datos sin autenticacion interactiva
  ./install.sh --configure-gcloud  Configura cuenta, proyecto, zona y APIs de Google Cloud
  ./install.sh --verify            Verifica herramientas y recursos
  ./install.sh --docker            Instala/verifica Docker
  ./install.sh                     Abre el menu interactivo
EOF
      ;;
    *) err "Argumento no reconocido: ${1:-}"; return 2 ;;
  esac
}

if [ "$#" -gt 0 ]; then
  run_cli "$@"
  exit $?
fi

while true; do
  header "INSTALACION VM -- PRACTICA BIGDATA"
  echo -e "  ${GREEN}0)${NC} Instalar/verificar herramientas y datos"
  echo ""
  echo -e "  ${GREEN}1)${NC} Docker Engine + Docker Compose plugin"
  echo -e "  ${GREEN}2)${NC} Java 17"
  echo -e "  ${GREEN}3)${NC} gcloud CLI"
  echo -e "  ${GREEN}4)${NC} kubectl"
  echo -e "  ${GREEN}5)${NC} gke-gcloud-auth-plugin"
  echo -e "  ${GREEN}6)${NC} Paquetes Python de requirements.txt"
  echo -e "  ${GREEN}7)${NC} Autenticacion gcloud + proyecto + zona"
  echo -e "  ${GREEN}8)${NC} Verificacion final"
  echo ""
  echo -e "  ${GREEN}9)${NC} Salir"
  echo -e "${BOLD}${BLUE}============================================${NC}"
  echo ""
  if ! read -r -p "  Selecciona una opcion: " option; then
    echo ""
    info "Entrada cerrada; instalacion finalizada"
    break
  fi

  case "$option" in
    0) install_all; pause ;;
    1) install_docker; pause ;;
    2) install_java; pause ;;
    3) install_gcloud; pause ;;
    4) install_kubectl; pause ;;
    5) install_gke_auth_plugin; pause ;;
    6) install_python_dependencies; pause ;;
    7) configure_gcloud; pause ;;
    8) verify_installation; pause ;;
    9) echo ""; info "Instalacion finalizada"; echo ""; break ;;
    *) warn "Opcion no valida"; pause ;;
  esac
done
