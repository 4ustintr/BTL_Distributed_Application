#!/usr/bin/env bash
# Bước đầu của make reset-hq (máy A, F12): 3 kho phải vừa được make reset-dc trên máy B.
# Kho còn subscription sub_master cũ nghĩa là chưa reset: HQ xóa sạch rồi dựng lại sẽ không khớp
# slot replication, offset Debezium và tồn kho cũ ở kho, nên dừng ngay và báo thứ tự đúng.
# Chạy từ gốc repo: bash infra/scripts/expect-fresh-dc.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

load_env

for n in 1 2 3; do
  c="pg-dc${n}"
  step "kiem tra ${c} (${HOST_B_IP}:$((5432 + n)))"
  st="$(container_state "$c")"
  [ "$st" = healthy ] ||
    die "${c} chua san sang (trang thai: ${st}); tren may B chay make reset-dc truoc"
  subs="$(psql_val "$c" "dc${n}" "SELECT count(*) FROM pg_subscription WHERE subname = 'sub_master'")" ||
    die "khong doc duoc ${c}; kiem tra HOST_B_IP, tuong lua cong $((5432 + n)), POSTGRES_PASSWORD"
  [ "$subs" = 0 ] ||
    die "${c} con subscription cu: tren may B chay make reset-dc truoc, roi chay lai make reset-hq"
  info "${c}: san sang, chua co subscription"
done

step "xong"
