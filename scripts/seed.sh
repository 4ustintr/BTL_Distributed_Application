#!/usr/bin/env bash
# make seed (spec F12, docs/tests.md mục 2): ghi danh mục thẳng vào hq_db bằng mã cố định,
# tạo 3 dòng dc_status ở trạng thái RECOVERING, rồi đặt lại sequence IDENTITY.
# Logical replication (pub_master) chép danh mục xuống 3 kho. Sau đó chờ cả 3 kho ONLINE và nhập
# tồn ban đầu qua POST /inbound (Bắc 100, Trung 40, Nam 10).
# Chạy lại được: INSERT ... ON CONFLICT (danh mục DO UPDATE, dc_status DO NOTHING); /inbound gửi
# lại cùng request_id nên kho trả duplicate.
# Chạy từ gốc repo: bash scripts/seed.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../infra/scripts/lib.sh"

load_env
read_env_file scripts/demo_ids.env

step "ghi danh muc vao hq_db (dc 1..3, CAT-BEV, SKU-A) va dc_status 1..3"
psql_in pg-hq hq_db <<'SQL'
SET client_encoding = 'UTF8';
BEGIN;

INSERT INTO dc (dc_id, dc_code, dc_name, region, fdw_server) VALUES
    (1, 'DC-BAC',   'Bắc',   'BAC',   'srv_dc1'),
    (2, 'DC-TRUNG', 'Trung', 'TRUNG', 'srv_dc2'),
    (3, 'DC-NAM',   'Nam',   'NAM',   'srv_dc3')
ON CONFLICT (dc_id) DO UPDATE
    SET dc_code = EXCLUDED.dc_code, dc_name = EXCLUDED.dc_name,
        region = EXCLUDED.region, fdw_server = EXCLUDED.fdw_server;

INSERT INTO product_category (category_id, category_code, category_name, parent_id) VALUES
    (1, 'CAT-BEV', 'Đồ uống', NULL)
ON CONFLICT (category_id) DO UPDATE
    SET category_code = EXCLUDED.category_code, category_name = EXCLUDED.category_name,
        parent_id = EXCLUDED.parent_id;

INSERT INTO product (product_id, sku, product_name, category_id, unit, weight_kg) VALUES
    (1, 'SKU-A', 'Nước suối thùng 24 chai', 1, 'thùng', 12.5)
ON CONFLICT (product_id) DO UPDATE
    SET sku = EXCLUDED.sku, product_name = EXCLUDED.product_name,
        category_id = EXCLUDED.category_id, unit = EXCLUDED.unit,
        weight_kg = EXCLUDED.weight_kg;

-- Trạng thái kho ở HQ (F9, F12): kho khởi động ở RECOVERING. Seed tạo dòng, Health Monitor chỉ
-- UPDATE. DO NOTHING: chạy lại seed không kéo trạng thái Health Monitor đã đổi về RECOVERING.
INSERT INTO dc_status (dc_id, status, node_mode, missed_count) VALUES
    (1, 'RECOVERING', 'RECOVERING', 0),
    (2, 'RECOVERING', 'RECOVERING', 0),
    (3, 'RECOVERING', 'RECOVERING', 0)
ON CONFLICT (dc_id) DO NOTHING;

-- Mã ghi tay nên sequence IDENTITY chưa tăng; đặt lại để lần INSERT không ghi mã sau lấy max + 1.
DO $$
BEGIN
    PERFORM setval(pg_get_serial_sequence('product_category', 'category_id'),
                   (SELECT max(category_id) FROM product_category));
    PERFORM setval(pg_get_serial_sequence('product', 'product_id'),
                   (SELECT max(product_id) FROM product));
END
$$;

COMMIT;
SQL
info "dc: $(psql_val pg-hq hq_db "SELECT string_agg(dc_code, ',' ORDER BY dc_id) FROM dc")"
info "product: $(psql_val pg-hq hq_db "SELECT sku || ' ' || weight_kg || ' kg' FROM product WHERE product_id = 1")"
info "dc_status: $(psql_val pg-hq hq_db "SELECT string_agg(dc_id || '=' || status, ',' ORDER BY dc_id) FROM dc_status")"

# ---- Nhập tồn ban đầu qua API (F12, tests.md mục 2) ------------------------------------------
HQ_API="http://${HOST_A_IP}:8000"     # F2: hq-api
SEED_PRODUCT_ID=1                     # F12: SKU-A
SEED_CREATED_BY="seed"                # stock_movement.created_by của 3 dòng nhập ban đầu
ONLINE_WAIT_S=180                     # kho khởi động RECOVERING; Replay Worker chu kỳ 2 giây (F11)
TMP_BODY="$(mktemp)"
# Giữ mã thoát cho _on_exit (lib.sh) để vẫn in bước bị lỗi
trap 'rc=$?; rm -f "$TMP_BODY"; (exit "$rc"); _on_exit' EXIT

# online_count: số kho HQ thấy ONLINE. Theo F12 đọc GET /dc-status; GIẢ ĐỊNH: endpoint này (Ngọc
# Chung, PR #6) chưa có thì trả 404, khi đó đọc thẳng bảng dc_status – cùng một nguồn dữ liệu.
online_count() {
  local code
  code="$(curl -s -o "$TMP_BODY" -w '%{http_code}' --max-time 3 "$HQ_API/dc-status" || true)"
  case "$code" in
    200) grep -o '"status": *"ONLINE"' "$TMP_BODY" | wc -l | tr -d ' ' ;;
    404) psql_val pg-hq hq_db "SELECT count(*) FROM dc_status WHERE status = 'ONLINE'" ;;
    *) echo 0 ;;
  esac
}

dc_status_line() {
  psql_val pg-hq hq_db "SELECT string_agg(dc_id || '=' || status, ',' ORDER BY dc_id) FROM dc_status"
}

step "cho ca 3 kho ONLINE (GET /dc-status, toi da ${ONLINE_WAIT_S}s)"
deadline=$((SECONDS + ONLINE_WAIT_S))
until [ "$(online_count)" = 3 ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    die "chua du 3 kho ONLINE sau ${ONLINE_WAIT_S}s ($(dc_status_line)); can hq-api co Health Monitor, Replay Worker va 3 node-api dang chay"
  fi
  sleep 2
done
info "dc_status: $(dc_status_line)"

# seed_inbound <dc_id> <qty> <request_id>: POST /inbound (F6, body theo common.models.InboundRequest).
# Chạy lại seed gửi cùng request_id nên kho trả duplicate, không nhập hai lần.
seed_inbound() {
  local dc="$1" qty="$2" req="$3" url code
  url="http://${HOST_B_IP}:$((8000 + dc))/inbound" # F2: node-api-dc1..3 cổng 8001..8003
  code="$(curl -s -o "$TMP_BODY" -w '%{http_code}' --max-time 10 -X POST "$url" \
    -H 'Content-Type: application/json' \
    -d "{\"request_id\":\"${req}\",\"product_id\":${SEED_PRODUCT_ID},\"qty\":${qty},\"ref_type\":\"RECEIPT\",\"created_by\":\"${SEED_CREATED_BY}\"}" ||
    true)"
  [ "$code" = 200 ] || die "kho ${dc}: POST /inbound tra ${code:-loi mang}: $(cat "$TMP_BODY")"
  info "kho ${dc}: nhap ${qty} -> $(cat "$TMP_BODY")"
}

step "nhap ton ban dau qua API: Bac 100, Trung 40, Nam 10 (REQ_SEED_1..3)"
seed_inbound 1 100 "$REQ_SEED_1"
seed_inbound 2 40 "$REQ_SEED_2"
seed_inbound 3 10 "$REQ_SEED_3"

step "xong"
