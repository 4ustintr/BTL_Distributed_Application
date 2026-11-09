# Lệnh theo spec F12. Các lệnh còn TODO sẽ được hoàn thiện dần.
TEST_DIRS = tests/common tests/node_api tests/hq_api tests/consumer
# Test JavaScript của giao diện (node --test); thư mục chưa có file .js/.mjs thì bỏ qua
JS_TEST_DIRS = tests/wms tests/dashboard

.PHONY: up-hq up-dc down-hq down-dc ps logs reset seed race race-unsafe check-invariant \
        pause-dc2 unpause-dc2 kill-dc2 start-dc2 restart-connector-dc2 replay-again bench-cdc \
        test test-all test-ci test-js lint measure-failover

# Docker Compose (F2, F12). Biến đọc từ .env ở gốc repo; đổi file bằng: make up-hq ENV_FILE=...
ENV_FILE ?= .env
# Script trong infra/scripts, scripts/ đọc ENV_FILE và CONNECT_URL từ môi trường.
CONNECT_URL ?= http://localhost:8083
export ENV_FILE CONNECT_URL
COMPOSE_HQ = docker compose --env-file $(ENV_FILE) -f infra/compose.hq.yml -f consumer/compose.yml
COMPOSE_DC = docker compose --env-file $(ENV_FILE) -f infra/compose.dc.yml
# make logs S=hq-api  (để trống = mọi service); make logs N=200
S ?=
N ?= 100

up-hq:
	$(COMPOSE_HQ) up -d --build

up-dc:
	$(COMPOSE_DC) up -d --build

# Giữ volume (dữ liệu CSDL, Kafka). Xóa volume là việc của make reset.
down-hq:
	$(COMPOSE_HQ) down

down-dc:
	$(COMPOSE_DC) down

ps:
	@$(COMPOSE_HQ) ps -a
	@$(COMPOSE_DC) ps -a

logs:
	@$(COMPOSE_HQ) ps -q $(S) >/dev/null 2>&1 && $(COMPOSE_HQ) logs --tail=$(N) $(S) || true
	@$(COMPOSE_DC) ps -q $(S) >/dev/null 2>&1 && $(COMPOSE_DC) logs --tail=$(N) $(S) || true

lint:
	ruff check .

# CI: không cần container
test-ci:
	@for d in $(TEST_DIRS); do pytest -m "not integration" $$d || [ $$? -eq 5 ] || exit 1; done

# CI: test JS của wms/ và dashboard/ bằng node --test (không cần npm)
test-js:
	@files=$$(find $(JS_TEST_DIRS) -type f \( -name '*.js' -o -name '*.mjs' \) 2>/dev/null); \
	if [ -z "$$files" ]; then echo "test-js: chua co file test trong $(JS_TEST_DIRS), bo qua"; \
	else node --test $$files; fi

# 10 test bắt buộc (cần hệ thống đang chạy)
test:
	@for d in $(TEST_DIRS); do pytest -m "not optional" $$d || [ $$? -eq 5 ] || exit 1; done

test-all:
	@for d in $(TEST_DIRS); do pytest $$d || [ $$? -eq 5 ] || exit 1; done

# ---- Khởi tạo lại toàn bộ (F12) ---------------------------------------------------------------
# Thứ tự cố định: xóa cả hai phía kèm volume (cả Kafka, để Consumer không áp dụng lại sự kiện cũ);
# máy A; máy B; subscription; FDW; connector; seed. Bước nào lỗi thì script in tên bước và dừng.
reset:
	@echo "== reset 1/8: down -v ca hai compose"
	$(COMPOSE_DC) down -v
	$(COMPOSE_HQ) down -v
	@echo "== reset 2/8: up-hq"
	$(MAKE) --no-print-directory up-hq
	@echo "== reset 3/8: up-dc"
	$(MAKE) --no-print-directory up-dc
	@echo "== reset 4/8: cho pg-hq, kafka, kafka-connect, pg-dc1..3 healthy"
	bash infra/scripts/wait-healthy.sh
	@echo "== reset 5/8: logical replication danh muc"
	bash infra/scripts/setup-replication.sh
	@echo "== reset 6/8: postgres_fdw, v_inventory_all"
	bash infra/scripts/setup-fdw.sh
	@echo "== reset 7/8: connector Debezium"
	bash infra/scripts/register-connectors.sh
	@echo "== reset 8/8: seed"
	$(MAKE) --no-print-directory seed
	@echo "== reset xong. Kiem tra: bash infra/scripts/verify.sh"

seed:
	bash scripts/seed.sh

# ---- Tạo sự cố cho kho Trung (máy B) ---------------------------------------------------------
kill-dc2:
	docker kill pg-dc2 node-api-dc2

start-dc2:
	docker start pg-dc2 node-api-dc2

pause-dc2:
	docker pause pg-dc2 node-api-dc2

unpause-dc2:
	docker unpause pg-dc2 node-api-dc2

# Máy A: F4, chỉ khởi động lại connector/task đang FAILED rồi chờ RUNNING.
restart-connector-dc2:
	bash infra/scripts/restart-connector.sh dc2-outbox

# Số đo M2, M4 (docs/tests.md mục 5), xem scripts/measure/README.md
measure-failover:
	@echo "TODO: $@ (goi python scripts/measure/m2_m4.py, xem scripts/measure/README.md)"

# ---- Tranh chấp đồng thời và bất biến (F12) ---------------------------------------------------
# Máy B: 2 lệnh xuất 30 thùng cùng lúc vào kho Bắc (REQ_RACE_1, REQ_RACE_2).
race:
	bash scripts/race.sh

# Máy B: khởi động lại node-api-dc1 với LOCK_MODE=none rồi bắn như trên; xong chạy make reset.
race-unsafe:
	bash scripts/race.sh unsafe

# Máy A: GET /invariant, in 4 số và chênh lệch. SKU khác: make check-invariant P=2
check-invariant:
	@P=$(P) bash scripts/check-invariant.sh

replay-again bench-cdc:
	@echo "TODO: $@ (xem docs/spec.md mục F12)"
