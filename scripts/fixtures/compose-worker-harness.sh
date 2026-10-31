#!/bin/sh
set -eu

for arg do
  if [ "$arg" = 'compose fixture: create the expected worker result' ]; then
    printf '%s\n' 'compose worker fixture completed' > compose-worker-result.txt
  fi
done
printf '%s\n' PASS
