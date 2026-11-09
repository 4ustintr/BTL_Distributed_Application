#!/usr/bin/env bash
# make check-invariant (spec F12, F7 GET /invariant). Máy A.
# In 4 số của bất biến và chênh lệch: tồn 3 kho + đi đường + đã xuất = đã nhập (demo.md).
#   bash scripts/check-invariant.sh        SKU A (product_id 1)
#   P=2 bash scripts/check-invariant.sh    SKU khác
# Thoát 1 khi HQ không trả 200 (vd 503 vì một kho không truy cập được, F7).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../infra/scripts/lib.sh"

load_env

HQ_API="http://${HOST_A_IP}:8000" # F2: hq-api
PRODUCT_ID="${P:-1}"              # tests.md mục 2: SKU A

step "GET /invariant?product_id=${PRODUCT_ID}"
BODY="$(curl -s --max-time 10 -w '\n%{http_code}' "$HQ_API/invariant?product_id=${PRODUCT_ID}" || true)"
CODE="${BODY##*$'\n'}"
BODY="${BODY%$'\n'*}"
[ "$CODE" = 200 ] || die "HQ tra ${CODE:-loi mang}: ${BODY}"

field() { # field <tên>: số nguyên của trường trong thân JSON (F7)
  [[ "$BODY" =~ \"$1\":\ *(-?[0-9]+) ]] || die "thieu truong $1 trong: $BODY"
  echo "${BASH_REMATCH[1]}"
}

STOCK="$(field stock)"
IN_TRANSIT="$(field in_transit)"
SHIPPED_OUT="$(field shipped_out)"
RECEIVED_IN="$(field received_in)"
DIFF="$(field diff)"

echo "stock=${STOCK} in_transit=${IN_TRANSIT} shipped_out=${SHIPPED_OUT} received_in=${RECEIVED_IN} diff=${DIFF}"
if [ "$DIFF" = 0 ]; then
  info "bat bien dung: ${STOCK} + ${IN_TRANSIT} + ${SHIPPED_OUT} = ${RECEIVED_IN}"
else
  info "LECH ${DIFF}: ${STOCK} + ${IN_TRANSIT} + ${SHIPPED_OUT} != ${RECEIVED_IN}"
fi
