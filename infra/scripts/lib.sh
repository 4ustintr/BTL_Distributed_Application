#!/usr/bin/env bash
# Hàm dùng chung cho script hạ tầng (infra/scripts/*.sh, scripts/seed.sh).
# Dùng:  . "$(dirname "${BASH_SOURCE[0]}")/lib.sh"   (script gọi phải tự đặt set -euo pipefail)
#
# Biến môi trường tùy chọn:
#   ENV_FILE     file env, tương đối với gốc repo (mặc định .env); Makefile tự export.
#   CONNECT_URL  REST của Kafka Connect (mặc định http://localhost:8083).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-.env}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
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
}

# psql_in <container> <db> [tham số psql...]: chạy SQL đọc từ stdin bằng user postgres.
psql_in() {
  local c="$1" db="$2"
  shift 2
  dk exec -i "$c" psql -v ON_ERROR_STOP=1 --no-psqlrc -q -U postgres -d "$db" "$@"
}

# psql_val <container> <db> <sql> [user]: in kết quả không định dạng (một giá trị mỗi dòng).
psql_val() {
  dk exec "$1" psql -v ON_ERROR_STOP=1 --no-psqlrc -qtAX -U "${4:-postgres}" -d "$2" -c "$3"
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
container_state() {
  dk inspect -f '{{if .State.Paused}}paused{{else if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "$1" 2>/dev/null || echo missing
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
