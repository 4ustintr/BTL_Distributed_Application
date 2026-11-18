#!/usr/bin/env bash
# Khởi động lại một connector sau khi kho lên lại (spec F4), rồi chờ RUNNING tối đa 60 giây.
# Chờ CSDL của kho nhận kết nối trước (tối đa 60 giây), để chạy ngay sau make start-dc2 mà task
# không lỗi lại. Chạy được từ máy A hoặc máy B (Kafka Connect ở HOST_A_IP:8083).
# Dùng: bash infra/scripts/restart-connector.sh dc2-outbox
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

name="${1:?Dung: restart-connector.sh <ten connector, vd dc2-outbox>}"
load_env

case "$name" in
  dc[1-3]-outbox)
    pg="pg-${name%-outbox}"
    step "cho ${pg} nhan ket noi (toi da 60s)"
    wait_healthy 60 "$pg"
    ;;
esac

step "trang thai truoc khi restart ${name}"
info "$(connector_states "$name")"

step "POST /connectors/${name}/restart?includeTasks=true&onlyFailed=true"
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "$CONNECT_URL/connectors/${name}/restart?includeTasks=true&onlyFailed=true")"
case "$code" in
  202 | 204) info "HTTP ${code}" ;;
  *) die "restart ${name} tra HTTP ${code}" ;;
esac

step "cho ${name} RUNNING (toi da 60s)"
wait_connector_running "$name" 60
