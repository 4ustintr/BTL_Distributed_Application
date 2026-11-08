#!/usr/bin/env bash
# Chờ các container healthy (mặc định: pg-hq, kafka, kafka-connect, pg-dc1..3), tối đa 180 giây.
# Dùng: bash infra/scripts/wait-healthy.sh [container...]     (WAIT_TIMEOUT=giây để đổi hạn)
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [ "$#" -eq 0 ]; then
  set -- pg-hq kafka kafka-connect pg-dc1 pg-dc2 pg-dc3
fi
step "cho healthy: $*"
wait_healthy "${WAIT_TIMEOUT:-180}" "$@"
