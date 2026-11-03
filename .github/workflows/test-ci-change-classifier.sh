#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
classifier="$repo_root/.github/workflows/classify-ci-changes.sh"
validator="$repo_root/.github/workflows/validate-ci-results.sh"
workflow="$repo_root/.github/workflows/ci.yml"
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
  local full=$1 macos=$2 head=${3:-$(git -C "$TEST_REPO" rev-parse HEAD)}
  local should_fail=${4:-false} event_name=${5:-pull_request}
  local output="$TEST_REPO/github-output"
  : > "$output"
  if (cd "$TEST_REPO" && GITHUB_OUTPUT="$output" "$classifier" "$event_name" "$BASE" "$head" 2>"$TEST_REPO/classifier-error.log"); then
    [[ $should_fail == false ]] || fail 'classifier unexpectedly succeeded'
  else
    [[ $should_fail == true ]] || fail 'classifier unexpectedly failed'
  fi
  [[ $(grep '^run_full=' "$output" | tail -n1) == "run_full=$full" ]] || fail "expected run_full=$full, got: $(<"$output")"
  [[ $(grep '^run_macos=' "$output" | tail -n1) == "run_macos=$macos" ]] || fail "expected run_macos=$macos, got: $(<"$output")"
  if [[ $should_fail == true ]]; then
    [[ $(grep '^run_full=' "$output" | tail -n1) == 'run_full=true' ]] || fail 'classification failure did not default to full CI'
    [[ $(grep '^run_macos=' "$output" | tail -n1) == 'run_macos=true' ]] || fail 'classification failure did not default to macOS CI'
  fi
}

add_and_commit() {
  local path=$1 content=${2:-content}
  mkdir -p "$(dirname "$TEST_REPO/$path")"
  printf '%s\n' "$content" > "$TEST_REPO/$path"
  git -C "$TEST_REPO" add --all
  git -C "$TEST_REPO" commit -qm change
}

assert_path() {
  local name=$1 path=$2 full=$3 macos=$4
  new_repo "$name"
  add_and_commit "$path"
  classify "$full" "$macos"
}

assert_path docs-only docs/guide/topic.md false false
assert_path readme-only README.md false false
assert_path dockerfile-only Dockerfile true false
assert_path compose-only docker-compose.custom.yml true false
assert_path go-source internal/factory/change.go true true
assert_path go-test internal/factory/change_test.go true true
assert_path module-change go.mod true true
assert_path workflow-change .github/workflows/ci.yml true true
assert_path makefile-change Makefile true true
assert_path unknown-path new-tool-config true true

new_repo mixed-docker-and-go
add_and_commit Dockerfile
mkdir -p "$TEST_REPO/cmd"
printf 'code\n' > "$TEST_REPO/cmd/change.go"
git -C "$TEST_REPO" add cmd/change.go
git -C "$TEST_REPO" commit -qm 'add code beside Dockerfile'
classify true true

new_repo mixed-docs-and-docker
add_and_commit Dockerfile
mkdir -p "$TEST_REPO/docs"
printf 'docs\n' > "$TEST_REPO/docs/update.md"
git -C "$TEST_REPO" add docs/update.md
git -C "$TEST_REPO" commit -qm 'add docs beside Dockerfile'
classify true true

new_repo docs-deletion
mkdir -p "$TEST_REPO/docs"
printf 'old\n' > "$TEST_REPO/docs/removed.md"
git -C "$TEST_REPO" add docs/removed.md
git -C "$TEST_REPO" commit -qm 'add docs file'
BASE=$(git -C "$TEST_REPO" rev-parse HEAD)
rm "$TEST_REPO/docs/removed.md"
git -C "$TEST_REPO" add -u
git -C "$TEST_REPO" commit -qm 'delete docs file'
classify false false

new_repo docker-rename
add_and_commit docker-compose.yml
BASE=$(git -C "$TEST_REPO" rev-parse HEAD)
git -C "$TEST_REPO" mv docker-compose.yml docker-compose.local.yml
git -C "$TEST_REPO" commit -qm 'rename compose file'
classify true false

new_repo docker-delete
add_and_commit Dockerfile
BASE=$(git -C "$TEST_REPO" rev-parse HEAD)
rm "$TEST_REPO/Dockerfile"
git -C "$TEST_REPO" add -u
git -C "$TEST_REPO" commit -qm 'delete Dockerfile'
classify true false

new_repo unusual-docker-path
add_and_commit $'docker-compose.local\n.yml'
classify true true

new_repo unusual-doc-paths
mkdir -p "$TEST_REPO/docs"
printf 'line break\n' > "$TEST_REPO/docs/line\nbreak.md"
printf 'tab\n' > "$TEST_REPO/docs/tab\tname.md"
git -C "$TEST_REPO" add --all
git -C "$TEST_REPO" commit -qm 'add unusual docs paths'
classify false false

new_repo unusual-code-path
add_and_commit $'cmd/unusual\nname.go'
classify true true

new_repo empty-diff
classify true true "$BASE"
classify true true "$BASE" false push

new_repo classifier-error
classify true true not-a-commit true
classify true true "$BASE" true invalid-event
missing_sha_output="$TEST_REPO/missing-sha-output"
if GITHUB_OUTPUT="$missing_sha_output" "$classifier" pull_request '' "$BASE" 2>"$TEST_REPO/missing-sha-error.log"; then
  fail 'classifier unexpectedly accepted a missing pull request SHA'
fi
grep -Fxq 'run_full=true' "$missing_sha_output" || fail 'missing SHA did not default to full CI'
grep -Fxq 'run_macos=true' "$missing_sha_output" || fail 'missing SHA did not default to macOS CI'

validator_succeeds() {
  local full=$1 macos=$2 build=$3 mac=$4 image=$5 postgres=$6
  RUN_FULL=$full RUN_MACOS=$macos CLASSIFY_RESULT=success ATTRIBUTION_RESULT=success \
    BUILD_RESULT=$build MACOS_RESULT=$mac IMAGE_RESULT=$image POSTGRES_RESULT=$postgres \
    "$validator"
}
validator_fails() {
  if validator_succeeds "$@" >/dev/null 2>&1; then
    fail "aggregate accepted invalid job results: $*"
  fi
}
validator_succeeds false false skipped skipped skipped skipped
validator_succeeds true false success skipped success success
validator_succeeds true true success success success success
validator_fails true false success cancelled success success
validator_fails true true success abandoned success success
validator_fails true true success success success missing
validator_fails false true success skipped skipped skipped
validator_fails true false success success success success
validator_fails invalid true success success success success
validator_fails true invalid success success success success
validator_fails true true failure success success success
validator_fails true true success failure success success
validator_succeeds false false skipped skipped skipped skipped
if RUN_FULL=false RUN_MACOS=false CLASSIFY_RESULT=failure ATTRIBUTION_RESULT=success \
  BUILD_RESULT=skipped MACOS_RESULT=skipped IMAGE_RESULT=skipped POSTGRES_RESULT=skipped "$validator" >/dev/null 2>&1; then
  fail 'aggregate accepted classifier failure'
fi
if RUN_FULL=false RUN_MACOS=false CLASSIFY_RESULT=success ATTRIBUTION_RESULT=failure \
  BUILD_RESULT=skipped MACOS_RESULT=skipped IMAGE_RESULT=skipped POSTGRES_RESULT=skipped "$validator" >/dev/null 2>&1; then
  fail 'aggregate accepted attribution failure'
fi

for required in \
  '  pull_request:' \
  '    branches: [main]' \
  '      run_full: ${{ steps.changes.outputs.run_full }}' \
  '      run_macos: ${{ steps.changes.outputs.run_macos }}' \
  "if: always() && needs.classify.outputs.run_full != 'false'" \
  "if: always() && needs.classify.outputs.run_macos != 'false'" \
  '  linux-build:' \
  '  macos-build:' \
  '  ci-result:' \
  '    name: CI result' \
  '    if: always()' \
  'run: .github/workflows/validate-ci-results.sh'; do
  grep -Fq -- "$required" "$workflow" || fail "workflow is missing required CI contract: $required"
done
if grep -A4 '^  commit-attribution:' "$workflow" | grep -Fq 'if:'; then
  fail 'commit attribution must run unconditionally'
fi
for required in \
  'test "$CLASSIFY_RESULT" = success' \
  'test "$ATTRIBUTION_RESULT" = success' \
  'test "$BUILD_RESULT" = skipped' \
  'test "$MACOS_RESULT" = skipped' \
  'test "$BUILD_RESULT" = success' \
  'test "$MACOS_RESULT" = success' \
  'test "$IMAGE_RESULT" = success' \
  'test "$POSTGRES_RESULT" = success'; do
  grep -Fq -- "$required" "$validator" || fail "aggregate validator is missing contract: $required"
done

echo 'All CI change classifier and workflow contract tests passed.'
