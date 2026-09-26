#!/usr/bin/env bash
set -euo pipefail

tag=${1:?usage: validate-release-tag.sh <tag>}
if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "Refusing non-stable semantic version tag: $tag" >&2
  exit 1
fi
