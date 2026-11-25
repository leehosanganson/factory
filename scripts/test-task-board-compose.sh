#!/bin/sh
set -eu
cd "$(dirname "$0")"
node --test fixtures/task-board-demo-session.test.mjs
exec sh fixtures/run-task-board-mock.sh smoke
