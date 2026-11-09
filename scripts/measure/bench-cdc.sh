#!/usr/bin/env bash
# make bench-cdc (spec F12, docs/tests.md mục 5 – M1). Máy A, có container hq-consumer.
# Gửi COUNT lệnh xuất 1 thùng, cách nhau INTERVAL giây vào kho DC (mặc định kho Trung, nhập trước
# COUNT thùng để không thiếu hàng), rồi đọc log hq-consumer và in p50, p95, max (ms).
#   make bench-cdc                       200 lệnh, 0.1 giây, kho 2
#   make bench-cdc ARGS="DC=1 OUT=m1.json"
# Đồng hồ hai máy phải đồng bộ NTP, hoặc đặt kho và HQ cùng một máy khi đo (tests.md mục 5).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../../infra/scripts/lib.sh"

load_env
DC="${DC:-2}"
COUNT="${COUNT:-200}"         # tests.md mục 5
INTERVAL="${INTERVAL:-0.1}"   # tests.md mục 5: cách nhau 100 ms
OUT="${OUT:-}"
CONSUMER_CONTAINER="hq-consumer" # F2
CATCH_UP_S=120
WORK="$(mktemp -d)"
trap 'rc=$?; rm -rf "$WORK"; (exit "$rc"); _on_exit' EXIT

M1=(python scripts/measure/m1_cdc.py)
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

step "gui ${COUNT} lenh xuat 1 thung vao kho ${DC} (cach ${INTERVAL}s)"
"${M1[@]}" send --node-url "http://${HOST_B_IP}:$((8000 + DC))" --count "$COUNT" \
  --interval "$INTERVAL" --prepare-stock --output "$WORK/requests.json"

step "cho hq-consumer xu ly xong roi tinh p50, p95, max (toi da ${CATCH_UP_S}s)"
deadline=$((SECONDS + CATCH_UP_S))
report=("${M1[@]}" report --requests "$WORK/requests.json" --log "$WORK/consumer.log")
[ -z "$OUT" ] || report+=(--output "$OUT")
until dk logs --since "$SINCE" "$CONSUMER_CONTAINER" >"$WORK/consumer.log" 2>&1 &&
  "${report[@]}" 2>"$WORK/report.err"; do
  [ "$SECONDS" -lt "$deadline" ] || die "chua du mau sau ${CATCH_UP_S}s: $(cat "$WORK/report.err")"
  sleep 3
done
[ -z "$OUT" ] || info "da ghi ket qua vao ${OUT}"
