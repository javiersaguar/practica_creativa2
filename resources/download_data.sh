#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$PROJECT_HOME/data"
BASE_URL="https://s3.amazonaws.com/agile_data_science"

download() {
  local name="$1" destination="$2"
  local temporary="${destination}.tmp"
  echo "Descargando $name..."
  curl --fail --location --retry 3 --retry-delay 2 --output "$temporary" "$BASE_URL/$name"
  test -s "$temporary"
  mv "$temporary" "$destination"
}

ensure_checked_download() {
  local name="$1" destination="$2" expected_sha256="$3"
  if [ -s "$destination" ] &&
     printf '%s  %s\n' "$expected_sha256" "$destination" | sha256sum --check --status; then
    return 0
  fi
  rm -f "${destination}.tmp"
  download "$name" "$destination"
  printf '%s  %s\n' "$expected_sha256" "$destination" | sha256sum --check --status
}

mkdir -p "$DATA_DIR"

ensure_checked_download \
  simple_flight_delay_features.jsonl.bz2 \
  "$DATA_DIR/simple_flight_delay_features.jsonl.bz2" \
  "2dde67c56fcb06c8b279706152ee0051f33903c0ee86119e154febf8abbbf049"
ensure_checked_download \
  origin_dest_distances.jsonl \
  "$DATA_DIR/origin_dest_distances.jsonl" \
  "b8d2907f62b3a0facc9d35d27df52c87d08995db01844a089398eccee026d74e"

echo "Datos descargados y verificados."
