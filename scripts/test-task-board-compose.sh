#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
project_suffix=$(python3 -c 'import secrets; print(secrets.token_hex(8))')
scratch=$(mktemp -d)
export COMPOSE_PROJECT_NAME="factory-task-board-$project_suffix"
export COMPOSE_FILE=docker-compose.yml:scripts/fixtures/task-board-pi.compose.yml
export COMPOSE_DISABLE_ENV_FILE=1
export COMPOSE_PROFILES=
export FACTORY_COMPOSE_CONFIG_DIR="$scratch/config"
export FACTORY_COMPOSE_PI_DIR="$scratch/pi"
export FACTORY_COMPOSE_HARNESS_DIR="$PWD/scripts/fixtures"
cleanup() {
  docker compose down --volumes >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
mkdir -m 755 "$FACTORY_COMPOSE_PI_DIR"
python3 scripts/compose-local.py
if ! docker compose up --build -d --wait --wait-timeout 90; then
  docker compose logs --tail 50 remote-worker pi-provider >&2
  exit 1
fi
docker compose exec -T remote-worker /usr/local/bin/pi --version
node frontend/e2e/task-board-compose.mjs "$FACTORY_COMPOSE_CONFIG_DIR"
