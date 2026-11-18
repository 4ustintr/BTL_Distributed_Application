#!/usr/bin/env bash
# Hàm dùng chung cho script hạ tầng (infra/scripts/*.sh, scripts/seed.sh).
# Dùng:  . "$(dirname "${BASH_SOURCE[0]}")/lib.sh"   (script gọi phải tự đặt set -euo pipefail)
#
# Biến môi trường tùy chọn:
#   ENV_FILE     file env, tương đối với gốc repo (mặc định .env); Makefile tự export.
#   CONNECT_URL  REST của Kafka Connect (mặc định http://HOST_A_IP:8083, đặt trong load_env),
#                nên lệnh connector chạy được từ cả máy A lẫn máy B.
#
# Hai máy (F2): container PostgreSQL không có trên máy đang chạy script thì psql_in, psql_val,
# container_state tự đi qua mạng (HOST_A_IP:5432 cho pg-hq, HOST_B_IP:5433..5435 cho pg-dc1..3),
# dùng psql có sẵn trong một container PostgreSQL của máy này. Một máy thì vẫn docker exec như cũ.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-.env}"
CONNECT_URL="${CONNECT_URL:-}"
SCRIPT_NAME="$(basename "${BASH_SOURCE[1]:-$0}" .sh)"
# Git Bash đổi tham số dạng /duong/dan thành C:/... khi gọi chương trình Windows. Chỉ tắt cho docker
# (đường dẫn nằm trong container); curl vẫn cần đổi /dev/null nên không export toàn cục.
dk() { MSYS_NO_PATHCONV=1 docker "$@"; }

STEP="khoi dong"

# step "mô tả": ghi lại bước hiện tại để báo lỗi rõ ràng.
step() {
  STEP="$*"
  echo "==> [${SCRIPT_NAME}] $*"
}

info() { echo "    $*"; }

# die "lý do": in lỗi kèm bước đang chạy rồi thoát.
die() {
  echo "LOI [${SCRIPT_NAME}] buoc '${STEP}': $*" >&2
  exit 1
}

_on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "THAT BAI [${SCRIPT_NAME}] tai buoc: ${STEP} (ma thoat ${rc})" >&2
  fi
}
trap _on_exit EXIT

cd "$ROOT_DIR" || die "khong vao duoc thu muc goc repo ${ROOT_DIR}"

# read_env_file <file>: export từng dòng KEY=VALUE như docker compose (không thực thi nội dung,
# vì .env có thể chứa JSON như NODE_URLS={"1": ...}). Bỏ dòng trống, dòng #, \r cuối dòng,
# tiền tố "export ", và một cặp nháy đơn/kép bao quanh giá trị.
read_env_file() {
  local line k v
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    case "$line" in '' | '#'*) continue ;; esac
    line="${line#export }"
    [[ "$line" == *=* ]] || continue
    k="${line%%=*}"
    v="${line#*=}"
    k="${k%"${k##*[![:space:]]}"}"
    [[ "$k" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    if [[ "$v" =~ ^\"(.*)\"$ || "$v" =~ ^\'(.*)\'$ ]]; then v="${BASH_REMATCH[1]}"; fi
    export "$k=$v"
  done <"$1"
}

# Nạp .env và kiểm tra biến bắt buộc.
load_env() {
  local f="$ENV_FILE" v
  case "$f" in /* | [A-Za-z]:*) ;; *) f="$ROOT_DIR/$f" ;; esac
  [ -f "$f" ] || die "khong thay file env: $f (cp .env.example .env roi sua)"
  read_env_file "$f"
  for v in HOST_A_IP HOST_B_IP POSTGRES_PASSWORD; do
    [ -n "${!v:-}" ] || die "thieu bien $v trong $f"
  done
  CONNECT_URL="${CONNECT_URL:-http://${HOST_A_IP}:8083}"
  export CONNECT_URL
}

# ---- PostgreSQL ở máy này hoặc máy kia --------------------------------------------------------
PG_CONTAINERS=(pg-hq pg-dc1 pg-dc2 pg-dc3)

# is_local <container>: container có trên Docker của máy này (đang chạy hay đã dừng đều tính).
is_local() { dk inspect "$1" >/dev/null 2>&1; }

# pg_addr <container>: "host port" theo F2, dùng khi container nằm ở máy kia.
pg_addr() {
  case "$1" in
    pg-hq) echo "${HOST_A_IP} 5432" ;;
    pg-dc[1-3]) echo "${HOST_B_IP} $((5432 + ${1#pg-dc}))" ;;
    *) return 1 ;;
  esac
}

# pg_client: một container PostgreSQL đang chạy trên máy này, dùng psql của nó để nối qua mạng.
pg_client() {
  local c
  for c in "${PG_CONTAINERS[@]}"; do
    [ "$(dk inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = true ] && { echo "$c"; return 0; }
  done
  die "may nay khong co container PostgreSQL nao dang chay de noi toi may kia (make up-hq hoac make up-dc truoc)"
}

# _psql <container> <user> [tham số psql...]: psql tại chỗ (socket) hoặc qua mạng (mật khẩu).
_psql() {
  local c="$1" user="$2" host port
  shift 2
  if is_local "$c"; then
    dk exec -i "$c" psql -U "$user" "$@"
  else
    read -r host port <<<"$(pg_addr "$c")" || die "khong biet dia chi cua ${c}"
    dk exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" -e PGCONNECT_TIMEOUT=5 "$(pg_client)" \
      psql -h "$host" -p "$port" -U "$user" "$@"
  fi
}

# psql_in <container> <db> [tham số psql...]: chạy SQL đọc từ stdin bằng user postgres.
psql_in() {
  local c="$1" db="$2"
  shift 2
  _psql "$c" postgres -v ON_ERROR_STOP=1 --no-psqlrc -q -d "$db" "$@"
}

# psql_val <container> <db> <sql> [user]: in kết quả không định dạng (một giá trị mỗi dòng).
psql_val() {
  _psql "$1" "${4:-postgres}" -v ON_ERROR_STOP=1 --no-psqlrc -qtAX -d "$2" -c "$3" </dev/null
}

# Thoát ký tự đặc biệt cho vế thay thế của sed (dấu phân cách là |).
sed_escape() {
  printf '%s' "$1" | sed -e 's/[\\&|]/\\&/g'
}

# Thoát chuỗi để đặt trong '...' của conninfo libpq.
conninfo_quote() {
  local s="${1//\\/\\\\}"
  printf "'%s'" "${s//\'/\\\'}"
}

# container_state <container>: healthy | unhealthy | starting | running | exited | paused | missing
# PostgreSQL ở máy kia: healthy nếu pg_isready qua mạng trả lời, ngược lại missing.
container_state() {
  local host port
  if is_local "$1"; then
    dk inspect -f '{{if .State.Paused}}paused{{else if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "$1" 2>/dev/null || echo missing
  elif read -r host port <<<"$(pg_addr "$1" 2>/dev/null)" && [ -n "${host:-}" ]; then
    if dk exec "$(pg_client)" pg_isready -q -h "$host" -p "$port" -t 3 >/dev/null 2>&1; then
      echo healthy
    else
      echo missing
    fi
  else
    echo missing
  fi
}

# wait_healthy <giây> <container>...: chờ từng container healthy, quá hạn thì die.
wait_healthy() {
  local timeout="$1" c st deadline
  shift
  deadline=$((SECONDS + timeout))
  for c in "$@"; do
    while :; do
      st="$(container_state "$c")"
      [ "$st" = healthy ] && break
      [ "$SECONDS" -lt "$deadline" ] || die "$c chua healthy sau ${timeout}s (trang thai: $st)"
      sleep 2
    done
    info "$c: healthy"
  done
}

# connector_states <tên>: "CONNECTOR_STATE TASK0_STATE ..."; rỗng nếu không có connector.
connector_states() {
  curl -s "$CONNECT_URL/connectors/$1/status" 2>/dev/null |
    grep -o '"state":"[A-Z_]*"' | cut -d'"' -f4 | tr '\n' ' ' | sed 's/ $//' || true
}

# connector_running <tên>: đúng khi connector và mọi task (ít nhất 1) đều RUNNING.
connector_running() {
  local s
  read -r -a s <<<"$(connector_states "$1")"
  [ "${#s[@]}" -ge 2 ] || return 1
  local x
  for x in "${s[@]}"; do [ "$x" = RUNNING ] || return 1; done
}

# wait_connector_running <tên> [giây=60]
wait_connector_running() {
  local name="$1" timeout="${2:-60}" deadline
  deadline=$((SECONDS + timeout))
  until connector_running "$name"; do
    [ "$SECONDS" -lt "$deadline" ] ||
      die "$name chua RUNNING sau ${timeout}s (trang thai: '$(connector_states "$name")'; xem: curl $CONNECT_URL/connectors/$name/status)"
    sleep 2
  done
  info "$name: $(connector_states "$name")"
}

# wait_connect_api [giây=120]: chờ REST Kafka Connect trả lời.
wait_connect_api() {
  local timeout="${1:-120}" deadline
  deadline=$((SECONDS + timeout))
  until curl -sf "$CONNECT_URL/connector-plugins" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || die "Kafka Connect ($CONNECT_URL) khong tra loi sau ${timeout}s"
    sleep 2
  done
}
