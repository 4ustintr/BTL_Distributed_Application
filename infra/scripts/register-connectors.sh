#!/usr/bin/env bash
# Đăng ký 3 connector Debezium dc1-outbox, dc2-outbox, dc3-outbox (spec F4).
# Đọc infra/connectors/dcN-outbox.json, thay ${HOST_B_IP} và ${POSTGRES_PASSWORD} bằng sed
# (không dùng envsubst: nó xóa luôn ${routedByValue} của Outbox Event Router).
# Connector đã có thì xóa rồi tạo lại (offset và slot debezium_dcN được giữ, nên không mất sự kiện).
# Chờ connector và task RUNNING, mỗi connector tối đa 60 giây.
# Cần: kafka-connect (máy A) và pg-dc1..3 (máy B). Chạy: bash infra/scripts/register-connectors.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

load_env

host_b="$(sed_escape "$HOST_B_IP")"
pw="$(sed_escape "$POSTGRES_PASSWORD")"
NAMES=(dc1-outbox dc2-outbox dc3-outbox)

step "cho Kafka Connect ${CONNECT_URL}"
wait_connect_api 120

for name in "${NAMES[@]}"; do
  file="infra/connectors/${name}.json"
  step "dang ky ${name} tu ${file}"
  [ -f "$file" ] || die "khong thay ${file}"
  body="$(sed -e "s|\${HOST_B_IP}|${host_b}|g" -e "s|\${POSTGRES_PASSWORD}|${pw}|g" "$file")"
  case "$body" in *'${HOST_B_IP}'* | *'${POSTGRES_PASSWORD}'*) die "con cho giu chua thay trong ${file}" ;; esac
  case "$body" in *'${routedByValue}'*) ;; *) die "mat \${routedByValue} sau khi thay bien" ;; esac

  code="$(curl -s -o /dev/null -w '%{http_code}' "$CONNECT_URL/connectors/${name}")"
  if [ "$code" = 200 ]; then
    info "da co, xoa de tao lai"
    code="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$CONNECT_URL/connectors/${name}")"
    case "$code" in 204 | 404) ;; *) die "DELETE ${name} tra HTTP ${code}" ;; esac
    deadline=$((SECONDS + 30))
    until [ "$(curl -s -o /dev/null -w '%{http_code}' "$CONNECT_URL/connectors/${name}")" = 404 ]; do
      [ "$SECONDS" -lt "$deadline" ] || die "${name} van con sau 30s ke tu khi xoa"
      sleep 1
    done
  fi

  # 409 = Connect đang cân bằng lại sau khi xóa; thử lại vài lần.
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    resp="$(printf '%s' "$body" | curl -s -w '\n%{http_code}' -X POST \
      -H 'Content-Type: application/json' --data-binary @- "$CONNECT_URL/connectors")"
    code="${resp##*$'\n'}"
    [ "$code" = 409 ] || break
    info "HTTP 409 (lan ${attempt}), thu lai sau 2s"
    sleep 2
  done
  if [ "$code" != 201 ]; then
    msg="${resp%$'\n'*}"
    die "POST ${name} tra HTTP ${code}: ${msg//"$POSTGRES_PASSWORD"/***}"
  fi
  info "da tao"
done

for name in "${NAMES[@]}"; do
  step "cho ${name} RUNNING (toi da 60s)"
  wait_connector_running "$name" 60
done

step "xong"
