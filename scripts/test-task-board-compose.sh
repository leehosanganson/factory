#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
project_suffix=$(python3 -c 'import secrets; print(secrets.token_hex(8))')
scratch=$(mktemp -d)
export COMPOSE_PROJECT_NAME="factory-task-board-$project_suffix"
export FACTORY_TASK_BOARD_API_IMAGE="factory-web:$COMPOSE_PROJECT_NAME"
export FACTORY_TASK_BOARD_PI_IMAGE="factory-pi-worker:$COMPOSE_PROJECT_NAME"
export COMPOSE_FILE=docker-compose.yml:scripts/fixtures/task-board-pi.compose.yml
export COMPOSE_DISABLE_ENV_FILE=1
export COMPOSE_PROFILES=
export FACTORY_COMPOSE_CONFIG_DIR="$scratch/config"
export FACTORY_COMPOSE_PI_DIR="$scratch/pi"
export FACTORY_COMPOSE_HARNESS_DIR="$PWD/scripts/fixtures"
compose_started=0
cleanup_failed() {
  printf 'Task board smoke cleanup failed: %s\n' "$1" >&2
  cleanup_status=1
}
cleanup() {
  test_status=$1
  cleanup_status=0
  if [ "$compose_started" = 1 ]; then
    docker compose down --volumes || cleanup_failed 'compose down --volumes'
    for image in "$FACTORY_TASK_BOARD_API_IMAGE" "$FACTORY_TASK_BOARD_PI_IMAGE"; do
      if image_id=$(docker image ls --quiet "$image"); then
        if [ -n "$image_id" ]; then
          docker image rm --no-prune "$image" || cleanup_failed "remove image $image"
        fi
      else
        cleanup_failed "list image $image"
      fi
    done
  fi
  rm -rf -- "$scratch" || cleanup_failed "remove scratch directory $scratch"
  if [ "$test_status" -ne 0 ]; then
    exit "$test_status"
  fi
  exit "$cleanup_status"
}
trap 'cleanup "$?"' EXIT
trap 'exit 1' HUP INT TERM
mkdir -m 755 "$FACTORY_COMPOSE_PI_DIR"
python3 scripts/compose-local.py
compose_started=1
docker compose up --build -d --wait --wait-timeout 90 || {
  up_status=$?
  docker compose logs --tail 50 remote-worker pi-provider >&2 || true
  exit "$up_status"
}
docker compose exec -T remote-worker /usr/local/bin/pi --version
node frontend/e2e/task-board-compose.mjs "$FACTORY_COMPOSE_CONFIG_DIR"
