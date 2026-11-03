#!/usr/bin/env bash
set -euo pipefail

: "${CLASSIFY_RESULT:?CLASSIFY_RESULT is required}"
: "${RUN_FULL:?RUN_FULL is required}"
: "${RUN_MACOS:?RUN_MACOS is required}"
: "${ATTRIBUTION_RESULT:?ATTRIBUTION_RESULT is required}"
: "${BUILD_RESULT:?BUILD_RESULT is required}"
: "${MACOS_RESULT:?MACOS_RESULT is required}"
: "${IMAGE_RESULT:?IMAGE_RESULT is required}"
: "${POSTGRES_RESULT:?POSTGRES_RESULT is required}"

test "$CLASSIFY_RESULT" = success
test "$ATTRIBUTION_RESULT" = success
case $RUN_FULL in true|false) ;; *) exit 1 ;; esac
case $RUN_MACOS in true|false) ;; *) exit 1 ;; esac

if [[ $RUN_FULL == false ]]; then
  test "$RUN_MACOS" = false
  test "$BUILD_RESULT" = skipped
  test "$MACOS_RESULT" = skipped
  test "$IMAGE_RESULT" = skipped
  test "$POSTGRES_RESULT" = skipped
else
  test "$BUILD_RESULT" = success
  test "$IMAGE_RESULT" = success
  test "$POSTGRES_RESULT" = success
  if [[ $RUN_MACOS == false ]]; then
    test "$MACOS_RESULT" = skipped
  else
    test "$MACOS_RESULT" = success
  fi
fi
