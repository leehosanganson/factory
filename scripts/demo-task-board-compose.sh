#!/bin/sh
set -eu
cd "$(dirname "$0")"
exec sh fixtures/run-task-board-mock.sh demo
