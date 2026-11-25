#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
case "${1:-}" in
  demo) auto_login=${FACTORY_DEMO_AUTO_LOGIN:-0} ;;
  smoke) auto_login=0 ;;
  *) exit 2 ;;
esac
case "$auto_login" in
  0|1) ;;
  *) printf 'FACTORY_DEMO_AUTO_LOGIN must be 0 or 1\n' >&2; exit 2 ;;
esac
project_suffix=$(python3 -c 'import secrets; print(secrets.token_hex(8))')
scratch=$(mktemp -d)
export COMPOSE_PROJECT_NAME="factory-task-board-$project_suffix"
export FACTORY_TASK_BOARD_API_IMAGE="factory-web:$COMPOSE_PROJECT_NAME"
export FACTORY_TASK_BOARD_PI_IMAGE="factory-pi-worker:$COMPOSE_PROJECT_NAME"
export COMPOSE_FILE=docker-compose.yml:scripts/fixtures/task-board-pi.compose.yml
if [ "$1" = demo ]; then
  COMPOSE_FILE="$COMPOSE_FILE:scripts/fixtures/task-board-demo.compose.yml"
  if [ "$auto_login" = 1 ]; then
    COMPOSE_FILE="$COMPOSE_FILE:scripts/fixtures/task-board-demo-auto-login.compose.yml"
  fi
fi
unset COMPOSE_ENV_FILES COMPOSE_PATH_SEPARATOR
export COMPOSE_DISABLE_ENV_FILE=1
export COMPOSE_PROFILES=
export FACTORY_COMPOSE_CONFIG_DIR="$scratch/config"
export FACTORY_COMPOSE_PI_DIR="$scratch/pi"
export FACTORY_COMPOSE_HARNESS_DIR="$PWD/scripts/fixtures"
compose_started=0
cleanup_failed() {
  printf 'Mock task board cleanup failed: %s\n' "$1" >&2
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
if [ "$1" = smoke ]; then
  node frontend/e2e/task-board-compose.mjs "$FACTORY_COMPOSE_CONFIG_DIR"
else
  printf '\nMock task board: https://localhost:19443/ui\n'
  if [ "$auto_login" = 0 ]; then
    printf 'Submitter token file (private): %s/owner-token\n' "$FACTORY_COMPOSE_CONFIG_DIR"
  fi
  printf 'Press Enter or Ctrl-C to stop and remove the demo.\n'
  read -r _ || true
fi
