#!/usr/bin/env bash
set -euo pipefail

validator="$(dirname "$0")/validate-release-tag.sh"

for tag in v0.0.0 v1.2.3 v10.20.30; do
  bash "$validator" "$tag"
done

for tag in v v1 v1.2 v01.2.3 v1.02.3 v1.2.03 v1.2.3-rc.1 v1.2.3+build.1 vv1.2.3; do
  if bash "$validator" "$tag" >/dev/null 2>&1; then
    echo "validator unexpectedly accepted: $tag" >&2
    exit 1
  fi
done

echo "Release tag validation tests passed."
