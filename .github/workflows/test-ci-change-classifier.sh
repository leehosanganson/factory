#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
classifier="$repo_root/.github/workflows/classify-ci-changes.sh"
tmp_root=$(mktemp -d)
trap 'rm -rf "$tmp_root"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

new_repo() {
  local name=$1
  TEST_REPO="$tmp_root/$name"
  mkdir -p "$TEST_REPO"
  git -C "$TEST_REPO" init -q
  git -C "$TEST_REPO" config user.name 'CI test'
  git -C "$TEST_REPO" config user.email 'ci-test@example.invalid'
  printf 'baseline\n' > "$TEST_REPO/README.md"
  git -C "$TEST_REPO" add README.md
  git -C "$TEST_REPO" commit -qm baseline
  BASE=$(git -C "$TEST_REPO" rev-parse HEAD)
}

classify() {
  local expected=$1
  local head=${2:-$(git -C "$TEST_REPO" rev-parse HEAD)}
  local should_fail=${3:-false}
  local event_name=${4:-pull_request}
  local output="$TEST_REPO/github-output"
  : > "$output"
  if (cd "$TEST_REPO" && GITHUB_OUTPUT="$output" "$classifier" "$event_name" "$BASE" "$head" 2>"$TEST_REPO/classifier-error.log"); then
    [[ $should_fail == false ]] || fail 'classifier unexpectedly succeeded'
  else
    [[ $should_fail == true ]] || fail 'classifier unexpectedly failed'
  fi
  local actual expected_output
  actual=$(grep '^run_full=' "$output" | tail -n1)
  expected_output="run_full=$expected"
  [[ $actual == "$expected_output" ]] || fail "expected $expected_output, got: $(<"$output")"
  if [[ $should_fail == true ]]; then
    grep -q '^could not read pull request base-to-head diff; running full CI$' "$TEST_REPO/classifier-error.log" || fail 'classifier error was not reported'
  fi
}

assert_docs_only() {
  local name=$1
  new_repo "$name"
  mkdir -p "$TEST_REPO/docs/guide"
  printf 'content\n' > "$TEST_REPO/docs/guide/topic.md"
  git -C "$TEST_REPO" add docs
  git -C "$TEST_REPO" commit -qm docs
  classify false
}

assert_full_for_path() {
  local name=$1 path=$2
  new_repo "$name"
  mkdir -p "$(dirname "$TEST_REPO/$path")"
  printf 'content\n' > "$TEST_REPO/$path"
  git -C "$TEST_REPO" add -- "$path"
  git -C "$TEST_REPO" commit -qm change
  classify true
}

assert_docs_only 'docs-only'
new_repo 'readme-only'
printf 'updated\n' >> "$TEST_REPO/README.md"
git -C "$TEST_REPO" add README.md
git -C "$TEST_REPO" commit -qm 'update README'
classify false
assert_full_for_path 'code-change' 'internal/factory/change.go'
new_repo 'mixed-change'
mkdir -p "$TEST_REPO/docs" "$TEST_REPO/cmd"
printf 'content\n' > "$TEST_REPO/docs/update.md"
git -C "$TEST_REPO" add docs
printf 'content\n' > "$TEST_REPO/cmd/change.go"
git -C "$TEST_REPO" add cmd/change.go
git -C "$TEST_REPO" commit -qm mixed
BASE=$(git -C "$TEST_REPO" rev-list --max-parents=0 HEAD)
classify true
assert_full_for_path 'workflow-change' '.github/workflows/ci.yml'
assert_full_for_path 'config-change' '.golangci.yml'

new_repo 'deletion'
mkdir -p "$TEST_REPO/docs"
printf 'old\n' > "$TEST_REPO/docs/removed.md"
git -C "$TEST_REPO" add docs/removed.md
git -C "$TEST_REPO" commit -qm 'add docs file'
BASE=$(git -C "$TEST_REPO" rev-parse HEAD)
rm "$TEST_REPO/docs/removed.md"
git -C "$TEST_REPO" add -u
git -C "$TEST_REPO" commit -qm 'delete docs file'
classify false

new_repo 'rename'
mkdir -p "$TEST_REPO/docs"
printf 'rename me\n' > "$TEST_REPO/docs/renamed.md"
git -C "$TEST_REPO" add docs/renamed.md
git -C "$TEST_REPO" commit -qm 'add docs file'
BASE=$(git -C "$TEST_REPO" rev-parse HEAD)
mkdir -p "$TEST_REPO/scripts"
git -C "$TEST_REPO" mv docs/renamed.md scripts/renamed.md
git -C "$TEST_REPO" commit -qm 'rename docs file to script'
classify true

new_repo 'unusual-doc-paths'
mkdir -p "$TEST_REPO/docs"
printf 'line break\n' > "$TEST_REPO/docs/line
break.md"
printf 'tab\n' > "$TEST_REPO/docs/tab\tname.md"
git -C "$TEST_REPO" add --all
git -C "$TEST_REPO" commit -qm 'add unusual docs paths'
classify false

new_repo 'unusual-code-path'
mkdir -p "$TEST_REPO/cmd"
printf 'unknown path\n' > "$TEST_REPO/cmd/unusual\nname.go"
git -C "$TEST_REPO" add --all
git -C "$TEST_REPO" commit -qm 'add unusual code path'
classify true

new_repo 'empty-diff'
classify true "$BASE"
classify true "$BASE" false push

new_repo 'classifier-error'
classify true 'not-a-commit' true

workflow="$repo_root/.github/workflows/ci.yml"
for required in \
  '  pull_request:' \
  '    branches: [main]' \
  '    run_full: ${{ steps.changes.outputs.run_full }}' \
  "if: always() && needs.classify.outputs.run_full != 'false'" \
  '  ci-result:' \
  '    name: CI result' \
  '    if: always()' \
  'test "$CLASSIFY_RESULT" = success' \
  'test "$ATTRIBUTION_RESULT" = success' \
  'test "$BUILD_RESULT" = skipped' \
  'test "$BUILD_RESULT" = success' \
  'test "$IMAGE_RESULT" = success' \
  'test "$POSTGRES_RESULT" = success'; do
  grep -Fq -- "$required" "$workflow" || fail "workflow is missing required CI contract: $required"
done
if grep -A4 '^  commit-attribution:' "$workflow" | grep -Fq 'if:'; then
  fail 'commit attribution must run unconditionally'
fi

echo 'All CI change classifier and workflow contract tests passed.'
