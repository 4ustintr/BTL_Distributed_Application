#!/usr/bin/env bash
# make seed (spec F12, docs/tests.md mục 2): ghi danh mục thẳng vào hq_db bằng mã cố định,
# tạo 3 dòng dc_status ở trạng thái RECOVERING, rồi đặt lại sequence IDENTITY.
# Logical replication (pub_master) chép danh mục xuống 3 kho.
# Chạy lại được: INSERT ... ON CONFLICT (danh mục DO UPDATE, dc_status DO NOTHING).
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

step "nhap ton ban dau qua API (TODO)"
# TODO(Tuấn Minh, Nhật Khánh): cần Node API ONLINE. Theo F12: chờ GET ${HQ}/dc-status báo cả 3 kho
# ONLINE (kho khởi động ở RECOVERING cho tới khi Replay Worker gọi /recovery/complete), rồi
# POST /inbound ở từng kho với ref_type RECEIPT:
#   Bắc  (http://${HOST_B_IP}:8001) qty 100, request_id ${REQ_SEED_1}
#   Trung(http://${HOST_B_IP}:8002) qty 40,  request_id ${REQ_SEED_2}
#   Nam  (http://${HOST_B_IP}:8003) qty 10,  request_id ${REQ_SEED_3}
# Body theo common.models.InboundRequest. Khi API xong, thay khối echo dưới bằng lệnh gọi thật.
info "TODO: chua nhap ton Bac 100 / Trung 40 / Nam 10 (REQ_SEED_1..3: ${REQ_SEED_1:0:8}.., ${REQ_SEED_2:0:8}.., ${REQ_SEED_3:0:8}..)"
info "      can Node API ONLINE (phan cua Tuan Minh va Nhat Khanh)"

step "xong"
