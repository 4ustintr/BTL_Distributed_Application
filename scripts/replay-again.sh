#!/usr/bin/env bash
# make replay-again (spec F12, demo.md KB3 bước 12). Chạy được trên máy A hoặc máy B.
# Gọi thẳng POST /replay của kho Trung với request_id của các giao dịch ghi hộ mà HQ đã đánh dấu
# APPLIED cho kho Trung (pending_txn, dc_id = 2). Đó đúng là các lệnh nhân viên vừa gửi qua WMS ở
# KB3 bước 3–4 (WMS tự sinh request_id, nên không dùng mã cố định REQ_KB3_*). Kho đã áp dụng các
# lệnh này nên phải trả 200 kèm duplicate: true và tồn không đổi. Thoát 1 nếu có lệnh không trả
# duplicate. Chỉ gửi lại dòng APPLIED, nên script không thể tạo giao dịch mới ở kho.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../infra/scripts/lib.sh"

load_env
[ -n "${INTERNAL_KEY:-}" ] || die "thieu INTERNAL_KEY trong ${ENV_FILE}"

DC_TRUNG=2
NODE_TRUNG="http://${HOST_B_IP}:$((8000 + DC_TRUNG))" # F2: node-api-dc2
CREATED_BY="replay-again"
TMP_BODY="$(mktemp)"
trap 'rc=$?; rm -f "$TMP_BODY"; (exit "$rc"); _on_exit' EXIT
ALL_DUPLICATE=1

# ton_trung: qty_on_hand của kho Trung (GET /inventory, F6)
ton_trung() {
  curl -s --max-time 5 "$NODE_TRUNG/inventory" | grep -o '"qty_on_hand": *[0-9]*' | head -1 | grep -o '[0-9]*$' ||
    echo "?"
}

# replay <request_id> <txn_type> <product_id> <qty>: body theo common.models.ReplayRequest (F6)
replay() {
  local req="$1" txn="$2" product="$3" qty="$4" code
  code="$(curl -s -o "$TMP_BODY" -w '%{http_code}' --max-time 10 -X POST "$NODE_TRUNG/replay" \
    -H 'Content-Type: application/json' -H "X-Internal-Key: ${INTERNAL_KEY}" \
    -d "{\"request_id\":\"${req}\",\"txn_type\":\"${txn}\",\"product_id\":${product},\"qty\":${qty},\"created_by\":\"${CREATED_BY}\"}" ||
    true)"
  info "${txn} ${qty} (${req:0:8}..): HTTP ${code:-loi mang} $(cat "$TMP_BODY")"
  [ "$code" = 200 ] && grep -q '"duplicate": *true' "$TMP_BODY" || ALL_DUPLICATE=0
}

step "lay giao dich ghi ho da APPLIED cua kho Trung (pending_txn tren hq_db)"
rows="$(psql_val pg-hq hq_db "SELECT txn_id || '|' || txn_type || '|' || product_id || '|' || qty
  FROM pending_txn WHERE dc_id = ${DC_TRUNG} AND status = 'APPLIED' ORDER BY created_at")"
[ -n "$rows" ] ||
  die "kho Trung chua co giao dich ghi ho nao APPLIED (da chay Kich ban 3 toi buoc 11 chua?)"
info "$(printf '%s\n' "$rows" | wc -l | tr -d ' ') giao dich; ton kho Trung truoc: $(ton_trung)"

step "goi lai /replay kho Trung voi dung cac request_id do"
while IFS='|' read -r req txn product qty; do
  replay "$req" "$txn" "$product" "$qty"
done <<<"$rows"
info "ton kho Trung sau: $(ton_trung)"

if [ "$ALL_DUPLICATE" = 1 ]; then
  info "dung: moi lenh tra duplicate=true, kho khong ghi trung"
else
  die "co lenh khong tra 200 duplicate=true (kho Trung co dang ONLINE?)"
fi
