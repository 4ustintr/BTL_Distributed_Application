#!/usr/bin/env bash
# Logical replication danh mục HQ -> kho (spec F3, db.md 2.3).
# Trên pg-dc1..3 tạo SUBSCRIPTION sub_master nối PUBLICATION pub_master của hq_db qua
# HOST_A_IP:5432, user repl_sub; mỗi kho một slot riêng trên HQ: sub_master_dc1..3.
# Idempotent: kho đã có subscription thì bỏ qua.
# Cần: pg-hq và pg-dc1..3 đang chạy. Chạy từ gốc repo: bash infra/scripts/setup-replication.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

load_env

step "kiem tra pg-hq co publication pub_master"
[ "$(psql_val pg-hq hq_db "SELECT count(*) FROM pg_publication WHERE pubname = 'pub_master'")" = 1 ] ||
  die "hq_db chua co publication pub_master (migration 001_hq.sql chua chay?)"

conninfo="host=${HOST_A_IP} port=5432 dbname=hq_db user=repl_sub"
conninfo+=" password=$(conninfo_quote "$POSTGRES_PASSWORD") connect_timeout=5"

for n in 1 2 3; do
  c="pg-dc${n}"
  db="dc${n}"
  slot="sub_master_dc${n}"

  step "subscription sub_master tren ${c} (slot ${slot})"
  has_sub="$(psql_val "$c" "$db" "SELECT count(*) FROM pg_subscription s
      JOIN pg_database d ON d.oid = s.subdbid
      WHERE s.subname = 'sub_master' AND d.datname = current_database()")"
  if [ "$has_sub" != 0 ]; then
    info "da co, bo qua"
    continue
  fi

  # Kho chưa có subscription mà HQ còn slot cùng tên: slot mồ côi (ví dụ chỉ xóa volume kho).
  # Slot đang được dùng thì không xóa, báo lỗi.
  slot_active="$(psql_val pg-hq hq_db "SELECT active FROM pg_replication_slots WHERE slot_name = '${slot}'")"
  if [ "$slot_active" = t ]; then
    die "slot ${slot} tren HQ dang duoc dung boi ket noi khac; kiem tra pg_stat_replication tren pg-hq"
  elif [ "$slot_active" = f ]; then
    info "HQ con slot mo coi ${slot}, xoa truoc khi tao lai"
    psql_val pg-hq hq_db "SELECT pg_drop_replication_slot('${slot}')" >/dev/null
  fi

  psql_in "$c" "$db" -v conninfo="$conninfo" -v slot="$slot" <<'SQL' ||
CREATE SUBSCRIPTION sub_master
  CONNECTION :'conninfo'
  PUBLICATION pub_master
  WITH (slot_name = :'slot');
SQL
    die "CREATE SUBSCRIPTION tren ${c} loi (HQ co nghe o ${HOST_A_IP}:5432? xem infra/README.md muc loi hay gap)"
  info "da tao"
done

step "cho worker cua 3 subscription chay (toi da 30s)"
deadline=$((SECONDS + 30))
for n in 1 2 3; do
  until [ "$(psql_val "pg-dc${n}" "dc${n}" \
    "SELECT count(*) FROM pg_stat_subscription WHERE subname = 'sub_master' AND pid IS NOT NULL")" -ge 1 ]; do
    [ "$SECONDS" -lt "$deadline" ] ||
      die "worker sub_master tren pg-dc${n} khong chay; xem: docker logs pg-dc${n} --tail 50"
    sleep 1
  done
  info "pg-dc${n}: sub_master dang chay"
done

step "xong"
