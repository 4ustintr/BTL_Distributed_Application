#!/bin/sh
set -eu
ruff check .
for suite in tests/common tests/node_api tests/hq_api tests/consumer; do
  python -m pytest "$suite" -q "$@"
done
