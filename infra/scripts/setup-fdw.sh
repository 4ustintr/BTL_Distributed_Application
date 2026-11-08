#!/usr/bin/env bash
# postgres_fdw từ hq_db tới 3 kho (spec F3, F11; db.md "Phân bố dữ liệu", 3.5).
#   - SERVER srv_dc1..3 -> HOST_B_IP:5433..5435, dbname dc1..dc3,
#     OPTIONS connect_timeout '3', keep_connections 'off'.
#   - USER MAPPING cho hq_app (role fdw_reader ở kho). Thêm mapping cho postgres: IMPORT FOREIGN
#     SCHEMA chạy bằng postgres, và view v_inventory_all (chủ là postgres) truy cập bảng ngoại
#     bằng mapping của chủ view.
#   - IMPORT FOREIGN SCHEMA public LIMIT TO (inventory, stock_movement, transfer_order,
#     transfer_item) vào schema dc1..dc3; view v_inventory_all; quyền SELECT cho hq_app.
# Idempotent: chạy lại thì cập nhật OPTIONS, nhập lại bảng ngoại, tạo lại view (một transaction).
# Cần: pg-hq và pg-dc1..3 đang chạy. Chạy từ gốc repo: bash infra/scripts/setup-fdw.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

load_env

TABLES="inventory, stock_movement, transfer_order, transfer_item"

dc_sql() {
  local n="$1" port=$((5432 + $1)) u
  cat <<SQL
-- kho ${n}: srv_dc${n}, cong ${port}, dbname dc${n}
CREATE SERVER IF NOT EXISTS srv_dc${n} FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host :'host_b', port '${port}', dbname 'dc${n}',
           connect_timeout '3', keep_connections 'off');
ALTER SERVER srv_dc${n}
  OPTIONS (SET host :'host_b', SET port '${port}', SET dbname 'dc${n}',
           SET connect_timeout '3', SET keep_connections 'off');
SQL
  for u in hq_app postgres; do
    cat <<SQL
CREATE USER MAPPING IF NOT EXISTS FOR ${u} SERVER srv_dc${n}
  OPTIONS (user 'fdw_reader', password :'pw');
ALTER USER MAPPING FOR ${u} SERVER srv_dc${n}
  OPTIONS (SET user 'fdw_reader', SET password :'pw');
SQL
  done
  cat <<SQL
CREATE SCHEMA IF NOT EXISTS dc${n};
DROP FOREIGN TABLE IF EXISTS
  dc${n}.inventory, dc${n}.stock_movement, dc${n}.transfer_order, dc${n}.transfer_item;
IMPORT FOREIGN SCHEMA public
  LIMIT TO (${TABLES})
  FROM SERVER srv_dc${n} INTO dc${n};
GRANT USAGE ON SCHEMA dc${n} TO hq_app;
GRANT SELECT ON ALL TABLES IN SCHEMA dc${n} TO hq_app;
SQL
}

step "kiem tra 3 kho nhan ket noi tu ${HOST_B_IP}:5433..5435"
for n in 1 2 3; do
  [ "$(container_state "pg-dc${n}")" = healthy ] || die "pg-dc${n} chua healthy"
done

step "tao server, user mapping, bang ngoai, view v_inventory_all tren hq_db"
{
  echo "SET client_min_messages = warning;"
  echo "BEGIN;"
  echo "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"
  echo "DROP VIEW IF EXISTS v_inventory_all;"
  for n in 1 2 3; do dc_sql "$n"; done
  cat <<'SQL'
-- db.md 3.5
CREATE VIEW v_inventory_all AS
SELECT * FROM dc1.inventory
UNION ALL
SELECT * FROM dc2.inventory
UNION ALL
SELECT * FROM dc3.inventory;

GRANT SELECT ON v_inventory_all TO hq_app;
COMMIT;
SQL
} | psql_in pg-hq hq_db -v host_b="$HOST_B_IP" -v pw="$POSTGRES_PASSWORD" ||
  die "SQL FDW loi (kho co nghe o ${HOST_B_IP}:5433..5435? mat khau fdw_reader dung?)"

step "thu truy van bang user hq_app"
for n in 1 2 3; do
  cnt="$(psql_val pg-hq hq_db "SELECT count(*) FROM dc${n}.inventory" hq_app)" ||
    die "hq_app khong doc duoc dc${n}.inventory"
  info "dc${n}.inventory: ${cnt} dong"
done
cnt="$(psql_val pg-hq hq_db "SELECT count(*) FROM v_inventory_all" hq_app)" ||
  die "hq_app khong doc duoc v_inventory_all"
info "v_inventory_all: ${cnt} dong"

step "xong"
