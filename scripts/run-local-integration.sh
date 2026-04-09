#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="/Users/lixucheng/Documents/oss/apache/ozone/hadoop-ozone/dist/target/compose/ozone"
OVERRIDE_FILE="${ROOT_DIR}/docker/docker-compose.ozone.override.yml"
COMPOSE_ARGS=(-f "${COMPOSE_DIR}/docker-compose.yaml" -f "${OVERRIDE_FILE}")

docker compose "${COMPOSE_ARGS[@]}" up -d

for port in 8981 9858 9859 9874; do
  for _ in $(seq 1 60); do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
done

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:9874" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

cd "${ROOT_DIR}"
OZONE_OM_ENDPOINT="http://127.0.0.1:8981" cargo test --test ozone_cluster -- --ignored
