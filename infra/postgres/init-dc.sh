#!/usr/bin/env bash
# Khởi tạo pg-dcN (chạy một lần trong docker-entrypoint-initdb.d, khi volume dữ liệu còn trống).
# DC_ID (1, 2, 3) do compose đặt. Tạo dcN, chạy 002_dc.sql với -v dc_id=N bằng user postgres
# (chủ bảng outbox_event, cần cho publication dbz_outbox), rồi đặt mật khẩu role.
# Chạy trong subshell: entrypoint có thể "source" file .sh không có quyền thực thi.
(
set -euo pipefail

case "${DC_ID:-}" in
  1 | 2 | 3) ;;
  *) echo "init-dc: DC_ID phai la 1, 2 hoac 3 (dang la '${DC_ID:-}')" >&2; exit 1 ;;
esac
DB="dc${DC_ID}"
PSQL=(psql -v ON_ERROR_STOP=1 --no-psqlrc -U postgres)

echo "init-dc: tao CSDL ${DB}"
"${PSQL[@]}" -d postgres -c "CREATE DATABASE ${DB}"

echo "init-dc: chay 002_dc.sql voi dc_id=${DC_ID}"
"${PSQL[@]}" -d "${DB}" -v dc_id="${DC_ID}" -f /migrations/002_dc.sql

echo "init-dc: dat mat khau role"
"${PSQL[@]}" -d "${DB}" -v pw="$POSTGRES_PASSWORD" <<'SQL'
ALTER ROLE node_app PASSWORD :'pw';
ALTER ROLE debezium PASSWORD :'pw';
ALTER ROLE fdw_reader PASSWORD :'pw';
SQL

echo "init-dc: xong ${DB}"
)
