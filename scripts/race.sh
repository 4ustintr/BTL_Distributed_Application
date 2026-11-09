#!/usr/bin/env bash
# make race / make race-unsafe (spec F12, demo.md KB1 bước 0 và 2, tests.md TC03, TC04). Máy B.
# Bắn cùng lúc 2 lệnh POST /outbound 30 thùng vào kho Bắc với REQ_RACE_1, REQ_RACE_2.
#   bash scripts/race.sh          kho Bắc chạy LOCK_MODE=optimistic: 1 lệnh 200, 1 lệnh 422
#   bash scripts/race.sh unsafe   khởi động lại node-api-dc1 với LOCK_MODE=none rồi bắn: cả 2 lệnh
#                                 200, tồn chỉ giảm 30 (lost update); chạy make reset trước bước 1
# Chạy từ gốc repo.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../infra/scripts/lib.sh"

MODE="${1:-safe}"
case "$MODE" in safe | unsafe) ;; *) die "tham so phai la 'safe' hoac 'unsafe': $MODE" ;; esac

load_env
read_env_file scripts/demo_ids.env

DC_BAC=1
NODE_CONTAINER="node-api-dc1"         # F2
NODE_BAC="http://${HOST_B_IP}:8001"   # F2: node-api-dc1
RACE_PRODUCT_ID=1                     # tests.md mục 2: SKU A
RACE_QTY=30                           # tests.md mục 2: mỗi lệnh xuất 30 thùng
RACE_CREATED_BY="race"
ONLINE_WAIT_S=60
OUT_1="$(mktemp)"
OUT_2="$(mktemp)"
trap 'rc=$?; rm -f "$OUT_1" "$OUT_2"; (exit "$rc"); _on_exit' EXIT

container_lock_mode() {
  dk inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$NODE_CONTAINER" 2>/dev/null |
    sed -n 's/^LOCK_MODE=//p'
}

# wait_bac_online: kho vừa khởi động ở RECOVERING (F9) cho tới khi HQ gọi /recovery/complete
wait_bac_online() {
  local deadline=$((SECONDS + ONLINE_WAIT_S)) body
  while :; do
    body="$(curl -s --max-time 2 "$NODE_BAC/health" || true)"
    [[ "$body" =~ \"mode\":\ *\"ONLINE\" ]] && break
    [ "$SECONDS" -lt "$deadline" ] ||
      die "kho Bac chua ONLINE sau ${ONLINE_WAIT_S}s (/health: ${body:-khong tra loi}); HQ co dang chay?"
    sleep 1
  done
  info "kho Bac: $body"
}

if [ "$MODE" = unsafe ]; then
  step "khoi dong lai ${NODE_CONTAINER} voi LOCK_MODE=none (F2)"
  # Biến môi trường của shell thắng giá trị trong --env-file khi compose thay biến
  LOCK_MODE=none dk compose --env-file "$ENV_FILE" -f infra/compose.dc.yml \
    up -d --no-deps --no-build --force-recreate "$NODE_CONTAINER"
else
  step "kiem tra ${NODE_CONTAINER} dang chay LOCK_MODE=optimistic"
  [ "$(container_lock_mode)" != none ] ||
    die "${NODE_CONTAINER} con chay LOCK_MODE=none tu lan make race-unsafe; chay make reset truoc"
fi
info "LOCK_MODE cua ${NODE_CONTAINER}: $(container_lock_mode)"

step "cho kho Bac ONLINE"
wait_bac_online

outbound_body() {
  printf '{"request_id":"%s","product_id":%s,"qty":%s,"ref_type":"ISSUE","created_by":"%s"}' \
    "$1" "$RACE_PRODUCT_ID" "$RACE_QTY" "$RACE_CREATED_BY"
}

fire() { # fire <request_id> <file>: in mã HTTP vào dòng đầu file, thân phản hồi ở dòng sau
  local code
  code="$(curl -s -o "$2.body" -w '%{http_code}' --max-time 10 -X POST "$NODE_BAC/outbound" \
    -H 'Content-Type: application/json' -d "$(outbound_body "$1")" || true)"
  { echo "${code:-000}"; cat "$2.body" 2>/dev/null; } >"$2"
  rm -f "$2.body"
}

step "ban cung luc 2 lenh xuat ${RACE_QTY} thung vao kho Bac (REQ_RACE_1, REQ_RACE_2)"
fire "$REQ_RACE_1" "$OUT_1" &
fire "$REQ_RACE_2" "$OUT_2" &
wait
info "REQ_RACE_1: HTTP $(sed -n 1p "$OUT_1") $(sed -n '2,$p' "$OUT_1")"
info "REQ_RACE_2: HTTP $(sed -n 1p "$OUT_2") $(sed -n '2,$p' "$OUT_2")"

step "ton kho Bac sau khi ban"
info "$(curl -s --max-time 3 "$NODE_BAC/inventory?product_id=${RACE_PRODUCT_ID}" || echo 'khong doc duoc /inventory')"
if [ "$MODE" = unsafe ]; then
  info "Dang chay LOCK_MODE=none: chay make check-invariant (may A) de thay lech, roi make reset truoc buoc 1"
fi

step "xong"
