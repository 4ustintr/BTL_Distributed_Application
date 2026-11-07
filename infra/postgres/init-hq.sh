#!/usr/bin/env bash
# Khởi tạo pg-hq (chạy một lần trong docker-entrypoint-initdb.d, khi volume dữ liệu còn trống).
# Tạo hq_db, chạy 001_hq.sql bằng user postgres, rồi đặt mật khẩu cho role bằng POSTGRES_PASSWORD.
# Chạy trong subshell: entrypoint có thể "source" file .sh không có quyền thực thi.
(
set -euo pipefail

PSQL=(psql -v ON_ERROR_STOP=1 --no-psqlrc -U postgres)

echo "init-hq: tao CSDL hq_db"
"${PSQL[@]}" -d postgres -c "CREATE DATABASE hq_db"

echo "init-hq: chay 001_hq.sql"
"${PSQL[@]}" -d hq_db -f /migrations/001_hq.sql

echo "init-hq: dat mat khau role"
"${PSQL[@]}" -d hq_db -v pw="$POSTGRES_PASSWORD" <<'SQL'
ALTER ROLE hq_app PASSWORD :'pw';
ALTER ROLE repl_sub PASSWORD :'pw';
SQL

echo "init-hq: xong"
)
