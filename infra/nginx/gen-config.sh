#!/bin/sh
# Sinh config.js từ biến môi trường rồi chạy nginx (spec F1: địa chỉ API đặt trong config.js).
# Dashboard: HQ_URL. WMS: DC_ID, NODE_URL, HQ_URL (F2).
set -eu

OUT=/etc/nginx/runtime/config.js
mkdir -p "$(dirname "$OUT")"

{
  echo "// Sinh lúc container khởi động bởi infra/nginx/gen-config.sh. Không sửa tay."
  echo "window.CONFIG = {"
  echo "  HQ_URL: \"${HQ_URL:?thieu HQ_URL}\","
  if [ -n "${NODE_URL:-}" ]; then
    echo "  NODE_URL: \"${NODE_URL}\","
  fi
  if [ -n "${DC_ID:-}" ]; then
    echo "  DC_ID: ${DC_ID},"
  fi
  echo "};"
} > "$OUT"

echo "gen-config: da ghi $OUT"
cat "$OUT"

exec /docker-entrypoint.sh nginx -g "daemon off;"
