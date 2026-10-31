#!/bin/sh
set -eu
umask 077

config_volume=/target/factory-config
worker_config_volume=/target/worker-config
secrets_volume=/target/postgres-secrets
config_names='server.json api-key operator-verifier.json'
worker_config_names='worker.json'
secret_names='postgres-password'
config_stage_dir=
worker_config_stage_dir=
secrets_stage_dir=
mode=${1:-default}

config_source() {
  case "$1" in
    server.json|worker.json|api-key|operator-verifier.json) printf '/source/%s' "$1" ;;
    postgres-password) printf '/source/postgres-password' ;;
    *) return 1 ;;
  esac
}
config_target() {
  case "$1" in
    worker.json) printf '%s/%s' "$worker_config_volume" "$1" ;;
    server.json|api-key|operator-verifier.json) printf '%s/%s' "$config_volume" "$1" ;;
    postgres-password) printf '%s/postgres-password' "$secrets_volume" ;;
    *) return 1 ;;
  esac
}
stage_dir() {
  case "$1" in
    worker.json) printf '%s' "$worker_config_stage_dir" ;;
    server.json|api-key|operator-verifier.json) printf '%s' "$config_stage_dir" ;;
    postgres-password) printf '%s' "$secrets_stage_dir" ;;
    *) return 1 ;;
  esac
}
fail() {
  printf '%s\n' "$1" >&2
  exit 1
}
cleanup() {
  # These mode-0700 directories were uniquely created by this PID; never touch a final path.
  [ -z "$config_stage_dir" ] || rm -rf "$config_stage_dir"
  [ -z "$worker_config_stage_dir" ] || rm -rf "$worker_config_stage_dir"
  [ -z "$secrets_stage_dir" ] || rm -rf "$secrets_stage_dir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

[ "$#" -le 1 ] && { [ "$mode" = default ] || [ "$mode" = worker ]; } || fail 'usage: provision-compose-config.sh [default|worker]'
[ -d /source ] && [ ! -L /source ] && [ "$(stat -c %a /source)" = 700 ] || fail 'invalid source directory'
if [ "$mode" = default ]; then
  for name in $config_names; do
    source=$(config_source "$name")
    [ -f "$source" ] && [ ! -L "$source" ] && [ "$(stat -c %h "$source")" = 1 ] && [ -s "$source" ] && [ "$(stat -c %a "$source")" = 600 ] || fail 'invalid source configuration'
  done
  for name in $secret_names; do
    source=$(config_source "$name")
    [ -f "$source" ] && [ ! -L "$source" ] && [ "$(stat -c %h "$source")" = 1 ] && [ -s "$source" ] && [ "$(stat -c %a "$source")" = 600 ] || fail 'invalid source credentials'
  done
else
  name=worker.json
  source=$(config_source "$name")
  [ -f "$source" ] && [ ! -L "$source" ] && [ "$(stat -c %h "$source")" = 1 ] && [ -s "$source" ] && [ "$(stat -c %a "$source")" = 600 ] || fail 'invalid worker source configuration'
fi

validate_target() {
  name=$1
  expected_digest=$2
  target=$(config_target "$name")
  [ -f "$target" ] && [ ! -L "$target" ] || return 1
  [ "$(stat -c %a "$target")" = 400 ] || return 1
  if [ "$name" = postgres-password ]; then
    [ "$(stat -c %u:%g "$target")" = 999:999 ] || return 1
  else
    [ "$(stat -c %u:%g "$target")" = 65532:65532 ] || return 1
  fi
  [ "$(stat -c %h "$target")" = 1 ] && [ "$expected_digest" = "$(sha256sum "$target" | cut -d ' ' -f 1)" ]
}

if [ "$mode" = default ]; then
  config_stage_dir=$(mktemp -d "$config_volume/.factory-provision-$$.XXXXXX") || fail 'cannot create private config staging directory'
  secrets_stage_dir=$(mktemp -d "$secrets_volume/.factory-provision-$$.XXXXXX") || fail 'cannot create private secret staging directory'
  chmod 0700 "$config_stage_dir" "$secrets_stage_dir"
  names="$config_names $secret_names"
else
  worker_config_stage_dir=$(mktemp -d "$worker_config_volume/.factory-provision-$$.XXXXXX") || fail 'cannot create private worker config staging directory'
  chmod 0700 "$worker_config_stage_dir"
  names=$worker_config_names
fi

for name in $names; do
  source=$(config_source "$name")
  source_digest=$(sha256sum "$source" | cut -d ' ' -f 1)
  target=$(config_target "$name")
  directory=$(stage_dir "$name")
  stage="$directory/$name"

  # Preserve and verify any existing final before preparing a replacement.
  if [ -e "$target" ] || [ -L "$target" ]; then
    validate_target "$name" "$source_digest" || fail 'existing provisioning data does not match; preserving it and refusing to overwrite'
    continue
  fi

  cp "$source" "$stage"
  [ "$(sha256sum "$stage" | cut -d ' ' -f 1)" = "$source_digest" ] || fail 'source changed while staging; refusing to publish'
  chmod 0400 "$stage"
  if [ "$name" = postgres-password ]; then chown 999:999 "$stage"; else chown 65532:65532 "$stage"; fi

  if [ -n "${FACTORY_PROVISION_TEST_START_GATE:-}" ] && [ "$name" = api-key ]; then
    gate=${FACTORY_PROVISION_TEST_START_GATE}
    : > "$gate/ready-${FACTORY_PROVISION_TEST_START_ID:-$$}"
    while [ ! -e "$gate/release" ]; do sleep 0.02; done
  fi

  # Hard-link publication is atomic and cannot replace a concurrently-created target.
  if ! ln -T "$stage" "$target" 2>/dev/null; then
    validate_target "$name" "$source_digest" || fail 'provisioning target exists or cannot be created safely; preserving it and refusing to overwrite'
  else
    rm -f "$stage"
  fi
  validate_target "$name" "$source_digest" || fail 'provisioned file failed validation; preserving it'

  # Optional explicit gate used by the disposable smoke test to simulate interruption.
  if [ "${FACTORY_PROVISION_TEST_GATE_AFTER:-}" = "$name" ]; then
    gate=${FACTORY_PROVISION_TEST_GATE_DIR:-}
    [ -n "$gate" ] || fail 'test gate directory is required'
    : > "$gate/promoted-$name"
    while [ ! -e "$gate/release-$name" ]; do sleep 0.05; done
  fi
done
