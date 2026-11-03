#!/usr/bin/env bash
set -euo pipefail

output=${GITHUB_OUTPUT:?GITHUB_OUTPUT must name the workflow output file}
event_name=${1:?usage: classify-ci-changes.sh <event-name> <base-sha> <head-sha>}
base_sha=${2:-}
head_sha=${3:-}

# Keep checks enabled unless a verified pull request diff proves they are safe to skip.
printf 'run_full=true\nrun_macos=true\n' >> "$output"

case $event_name in
  push)
    exit 0
    ;;
  pull_request)
    ;;
  *)
    echo "unsupported event for CI classification: $event_name" >&2
    exit 1
    ;;
esac

if [[ -z $base_sha || -z $head_sha ]]; then
  echo 'pull request base and head SHAs are required' >&2
  exit 1
fi

changed_paths=$(mktemp)
trap 'rm -f "$changed_paths"' EXIT
if ! git diff --no-renames --name-only -z "$base_sha...$head_sha" > "$changed_paths"; then
  echo 'could not read pull request base-to-head diff; running full CI' >&2
  exit 1
fi

has_changes=false
all_docs_only=true
all_docker_only=true
while IFS= read -r -d '' path; do
  has_changes=true
  if [[ $path != README.md && $path != docs/* ]]; then
    all_docs_only=false
  fi
  if [[ $path == *$'\n'* || $path == *$'\t'* || ($path != Dockerfile && $path != docker-compose*.yml) ]]; then
    all_docker_only=false
  fi
done < "$changed_paths"

# An empty diff is not evidence that either platform's checks can be skipped.
if [[ $has_changes == true && $all_docs_only == true ]]; then
  printf 'run_full=false\nrun_macos=false\n' >> "$output"
elif [[ $has_changes == true && $all_docker_only == true ]]; then
  printf 'run_macos=false\n' >> "$output"
fi
