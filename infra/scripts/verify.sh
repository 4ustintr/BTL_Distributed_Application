#!/usr/bin/env bash
# Kiểm tra hạ tầng sau make reset (infra/README.md mục "Kiểm tra sau make reset"):
#   (a) danh mục và 3 dòng dc_status ở hq_db; danh mục ở cả 3 kho; bản ghi mới ở HQ xuống 3 kho
#       trong 10 giây
#   (b) hq_app đọc được dc1..dc3.inventory và v_inventory_all qua FDW
#   (c) 3 connector và task RUNNING
#   (d) một dòng outbox_event ở dc1 xuất hiện trên topic outbox.event.INVENTORY
#   (e) chỉ khi có --failover: kill-dc2 -> task dc2-outbox FAILED (connector vẫn RUNNING),
#       dc1/dc3 RUNNING;
#       start-dc2 + restart-connector-dc2 -> dc2-outbox RUNNING, subscription dc2 vẫn chạy
# Dùng: bash infra/scripts/verify.sh [--failover]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

load_env
FAILOVER=0
[ "${1:-}" = "--failover" ] && FAILOVER=1

pass() { echo "    OK  $*"; }

# Xóa dòng thử và trả sequence IDENTITY về max, để mã danh mục thật vẫn liền nhau.
drop_test_category() {
  psql_val pg-hq hq_db "DELETE FROM product_category WHERE category_id = $1;
    SELECT setval(pg_get_serial_sequence('product_category', 'category_id'),
                  (SELECT coalesce(max(category_id), 1) FROM product_category))" >/dev/null
}

# wait_on_dcs <sql> <giá trị mong đợi> <giây> <container...>: chờ mọi kho trả đúng giá trị.
wait_on_dcs() {
  local sql="$1" want="$2" timeout="$3" n got deadline
  shift 3
  deadline=$((SECONDS + timeout))
  for n in "$@"; do
    while :; do
      got="$(psql_val "pg-dc${n}" "dc${n}" "$sql" 2>/dev/null || echo loi)"
      [ "$got" = "$want" ] && break
      [ "$SECONDS" -lt "$deadline" ] || die "pg-dc${n}: '$sql' = '$got', mong doi '$want' trong ${timeout}s"
      sleep 1
    done
  done
}

CATALOG_SQL="SELECT (SELECT count(*) FROM dc WHERE dc_id IN (1,2,3))
  || '|' || (SELECT count(*) FROM product WHERE product_id = 1 AND sku = 'SKU-A' AND weight_kg = 12.5)
  || '|' || (SELECT count(*) FROM product_category WHERE category_id = 1 AND category_code = 'CAT-BEV')"

# ---------------------------------------------------------------------------------------------
step "(a) danh muc o hq_db"
got="$(psql_val pg-hq hq_db "$CATALOG_SQL")"
[ "$got" = "3|1|1" ] || die "hq_db: dc|SKU-A|CAT-BEV = '$got', mong doi 3|1|1 (da chay make seed?)"
pass "hq_db: 3 dong dc, SKU-A 12.5 kg, CAT-BEV"

step "(a) dc_status o hq_db co du 3 dong (seed tao, Health Monitor chi UPDATE)"
got="$(psql_val pg-hq hq_db "SELECT count(*) FILTER (WHERE dc_id IN (1,2,3)) || '|' || count(*) FROM dc_status")"
[ "$got" = "3|3" ] || die "dc_status: dong dc 1..3 | tong = '$got', mong doi 3|3 (da chay make seed?)"
pass "dc_status: $(psql_val pg-hq hq_db "SELECT string_agg(dc_id || '=' || status, ',' ORDER BY dc_id) FROM dc_status")"

step "(a) danh muc o dc1, dc2, dc3 (toi da 10s)"
wait_on_dcs "$CATALOG_SQL" "3|1|1" 10 1 2 3
pass "ca 3 kho thay du danh muc"

step "(a) chen product_category thu o hq_db, cho 3 kho thay (10s)"
psql_val pg-hq hq_db "DELETE FROM product_category WHERE category_code = 'CAT-TEST'" >/dev/null
test_id="$(psql_val pg-hq hq_db "INSERT INTO product_category (category_code, category_name)
  VALUES ('CAT-TEST', 'Thu replication') RETURNING category_id")"
t0=$SECONDS
wait_on_dcs "SELECT count(*) FROM product_category WHERE category_id = ${test_id}" 1 10 1 2 3
pass "CAT-TEST (id ${test_id}) xuong 3 kho sau ~$((SECONDS - t0))s"
drop_test_category "$test_id"
wait_on_dcs "SELECT count(*) FROM product_category WHERE category_id = ${test_id}" 0 10 1 2 3
pass "da xoa dong thu o HQ va 3 kho"

# ---------------------------------------------------------------------------------------------
step "(b) FDW bang user hq_app"
got="$(psql_val pg-hq hq_db "SELECT (SELECT count(*) FROM dc1.inventory), (SELECT count(*) FROM dc2.inventory),
  (SELECT count(*) FROM dc3.inventory)" hq_app)" || die "hq_app khong doc duoc dcN.inventory"
pass "count(*) dc1|dc2|dc3.inventory = ${got}"
psql_val pg-hq hq_db "SELECT * FROM v_inventory_all" hq_app >/dev/null || die "hq_app khong doc duoc v_inventory_all"
pass "SELECT * FROM v_inventory_all chay duoc"

# ---------------------------------------------------------------------------------------------
step "(c) 3 connector RUNNING"
for n in 1 2 3; do
  connector_running "dc${n}-outbox" || die "dc${n}-outbox: '$(connector_states "dc${n}-outbox")'"
  pass "dc${n}-outbox: $(connector_states "dc${n}-outbox")"
done

# ---------------------------------------------------------------------------------------------
step "(d) outbox_event o dc1 -> topic outbox.event.INVENTORY"
event_id="$(psql_val pg-dc1 dc1 "SELECT gen_random_uuid()")"
# version 0: Consumer chỉ áp dụng khi version > last_version (>= 0), nên sự kiện thử không đổi snapshot.
psql_in pg-dc1 dc1 -v eid="$event_id" <<'SQL'
INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload)
VALUES (:'eid', 'INVENTORY', '1:1', 'InventoryChanged',
        jsonb_build_object(
          'event_id', :'eid', 'event_type', 'InventoryChanged', 'source_dc', 1,
          'occurred_at', clock_timestamp(), 'product_id', 1,
          'qty_on_hand', 0, 'qty_reserved', 0, 'version', 0,
          'request_id', gen_random_uuid(), 'ref_type', 'RECEIPT'));
SQL
info "da chen event_id ${event_id}, doc topic (toi da ~20s)"
msg="$(dk exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic outbox.event.INVENTORY --from-beginning --timeout-ms 20000 \
  --property print.key=true --property key.separator=' => ' 2>/dev/null | grep "$event_id" || true)"
psql_val pg-dc1 dc1 "DELETE FROM outbox_event WHERE event_id = '${event_id}'" >/dev/null
[ -n "$msg" ] || die "khong thay event ${event_id} tren outbox.event.INVENTORY sau 20s"
case "$msg" in "1:1 => "*) ;; *) die "key Kafka khong phai '1:1': ${msg}" ;; esac
pass "nhan duoc: ${msg}"
pass "da xoa dong thu o dc1"

# ---------------------------------------------------------------------------------------------
if [ "$FAILOVER" -eq 0 ]; then
  step "xong (a)-(d); (e) can --failover"
  exit 0
fi

step "(e) make kill-dc2, cho dc2-outbox FAILED (toi da 240s)"
make --no-print-directory kill-dc2
t0=$SECONDS
until [[ " $(connector_states dc2-outbox) " == *" FAILED "* ]]; do
  [ $((SECONDS - t0)) -lt 240 ] || die "dc2-outbox chua FAILED sau 240s: '$(connector_states dc2-outbox)'"
  sleep 3
done
pass "dc2-outbox: $(connector_states dc2-outbox) sau ~$((SECONDS - t0))s"
for n in 1 3; do
  connector_running "dc${n}-outbox" || die "dc${n}-outbox bi anh huong: '$(connector_states "dc${n}-outbox")'"
  pass "dc${n}-outbox van RUNNING"
done

step "(e) make start-dc2, make restart-connector-dc2"
make --no-print-directory start-dc2
wait_healthy 120 pg-dc2
make --no-print-directory restart-connector-dc2
pass "dc2-outbox: $(connector_states dc2-outbox)"

step "(e) subscription cua dc2 van chay"
deadline=$((SECONDS + 30))
until [ "$(psql_val pg-dc2 dc2 "SELECT count(*) FROM pg_stat_subscription
    WHERE subname = 'sub_master' AND pid IS NOT NULL")" -ge 1 ]; do
  [ "$SECONDS" -lt "$deadline" ] || die "worker sub_master tren pg-dc2 khong chay lai"
  sleep 1
done
test_id="$(psql_val pg-hq hq_db "INSERT INTO product_category (category_code, category_name)
  VALUES ('CAT-TEST', 'Thu replication') RETURNING category_id")"
wait_on_dcs "SELECT count(*) FROM product_category WHERE category_id = ${test_id}" 1 10 1 2 3
pass "dong moi o HQ xuong ca 3 kho, ke ca dc2"
drop_test_category "$test_id"
wait_on_dcs "SELECT count(*) FROM product_category WHERE category_id = ${test_id}" 0 10 1 2 3
pass "da xoa dong thu"

step "(e) dc2 sau khi len lai: outbox -> Kafka"
event_id="$(psql_val pg-dc2 dc2 "SELECT gen_random_uuid()")"
psql_in pg-dc2 dc2 -v eid="$event_id" <<'SQL'
INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload)
VALUES (:'eid', 'INVENTORY', '2:1', 'InventoryChanged',
        jsonb_build_object(
          'event_id', :'eid', 'event_type', 'InventoryChanged', 'source_dc', 2,
          'occurred_at', clock_timestamp(), 'product_id', 1,
          'qty_on_hand', 0, 'qty_reserved', 0, 'version', 0,
          'request_id', gen_random_uuid(), 'ref_type', 'RECEIPT'));
SQL
msg="$(dk exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic outbox.event.INVENTORY --from-beginning --timeout-ms 20000 2>/dev/null |
  grep "$event_id" || true)"
psql_val pg-dc2 dc2 "DELETE FROM outbox_event WHERE event_id = '${event_id}'" >/dev/null
[ -n "$msg" ] || die "dc2-outbox RUNNING nhung khong day su kien ${event_id} len Kafka"
pass "su kien tu dc2 len Kafka"

step "xong (a)-(e)"
