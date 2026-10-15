#!/usr/bin/env bash
set -euo pipefail

base_sha=${1:?usage: check-commit-attribution.sh <base-sha> <head-sha>}
head_sha=${2:?usage: check-commit-attribution.sh <base-sha> <head-sha>}

if ! git cat-file -e "${base_sha}^{commit}" 2>/dev/null; then
  echo "base commit is unavailable: $base_sha" >&2
  exit 2
fi
if ! git cat-file -e "${head_sha}^{commit}" 2>/dev/null; then
  echo "head commit is unavailable: $head_sha" >&2
  exit 2
fi

if ! git merge-base "$base_sha" "$head_sha" >/dev/null 2>&1; then
  echo "base and head do not have a common history" >&2
  exit 2
fi

failed=0
while IFS= read -r commit; do
  identity=$(git show -s --format='%an%n%ae%n%cn%n%ce' "$commit")
  trailers=$(git show -s --format='%B' "$commit" | grep -Ei '^[[:space:]]*co-authored-by:' || true)
  if printf '%s\n%s\n' "$identity" "$trailers" | grep -Eiq 'factory[[:space:]_-]*agent|factory-agent@users\.noreply\.github\.com'; then
    printf 'disallowed Factory Agent attribution in commit %s\n' "$commit" >&2
    git show -s --format='  %h %s%n  Author: %an <%ae>%n  Committer: %cn <%ce>' "$commit" >&2
    if [[ -n "$trailers" ]]; then
      printf '  Co-author trailer: %s\n' "$trailers" >&2
    fi
    failed=1
  fi
done < <(git rev-list --reverse "${base_sha}..${head_sha}")

if (( failed )); then
  echo 'Do not attribute commits or co-authorship to Factory Agent. Use the human contributor as author and omit agent identity trailers.' >&2
  exit 1
fi

echo 'No Factory Agent commit author, committer, or co-author attribution found.'
