#!/usr/bin/env bash
# make replay-again (spec F12, demo.md KB3 bước 12). Máy A.
# Gọi thẳng POST /replay của kho Trung với 2 request_id cũ của Kịch bản 3 (REQ_KB3_IN nhập 30,
# REQ_KB3_OUT xuất 10). Kho đã áp dụng hai lệnh này nên phải trả 200 kèm duplicate: true và tồn
# không đổi (55). Thoát 1 nếu có lệnh không trả duplicate.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../infra/scripts/lib.sh"

load_env
read_env_file scripts/demo_ids.env
[ -n "${INTERNAL_KEY:-}" ] || die "thieu INTERNAL_KEY trong ${ENV_FILE}"

NODE_TRUNG="http://${HOST_B_IP}:8002" # F2: node-api-dc2
PRODUCT_ID=1                          # tests.md mục 2: SKU A
CREATED_BY="replay-again"
TMP_BODY="$(mktemp)"
trap 'rc=$?; rm -f "$TMP_BODY"; (exit "$rc"); _on_exit' EXIT
ALL_DUPLICATE=1

# replay <request_id> <txn_type> <qty>: body theo common.models.ReplayRequest (F6)
replay() {
  local req="$1" txn="$2" qty="$3" code
  code="$(curl -s -o "$TMP_BODY" -w '%{http_code}' --max-time 10 -X POST "$NODE_TRUNG/replay" \
    -H 'Content-Type: application/json' -H "X-Internal-Key: ${INTERNAL_KEY}" \
    -d "{\"request_id\":\"${req}\",\"txn_type\":\"${txn}\",\"product_id\":${PRODUCT_ID},\"qty\":${qty},\"created_by\":\"${CREATED_BY}\"}" ||
    true)"
  info "${txn} ${qty} (${req:0:8}..): HTTP ${code:-loi mang} $(cat "$TMP_BODY")"
  [ "$code" = 200 ] && grep -q '"duplicate": *true' "$TMP_BODY" || ALL_DUPLICATE=0
}

step "goi lai /replay kho Trung voi REQ_KB3_IN, REQ_KB3_OUT"
replay "$REQ_KB3_IN" INBOUND 30
replay "$REQ_KB3_OUT" OUTBOUND 10

if [ "$ALL_DUPLICATE" = 1 ]; then
  info "dung: ca hai lenh tra duplicate=true, kho khong ghi trung"
else
  die "co lenh khong tra 200 duplicate=true (da chay Kich ban 3 chua? kho Trung co dang ONLINE?)"
fi
