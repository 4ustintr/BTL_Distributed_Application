# Lệnh theo spec F12. Các lệnh còn TODO sẽ được hoàn thiện dần.
TEST_DIRS = tests/common tests/node_api tests/hq_api tests/consumer
# Test JavaScript của giao diện (node --test); thư mục chưa có file .js/.mjs thì bỏ qua
JS_TEST_DIRS = tests/wms tests/dashboard

.PHONY: up-hq up-dc down-hq down-dc ps logs reset reset-dc reset-hq seed race race-unsafe \
        check-invariant pause-dc2 unpause-dc2 kill-dc2 start-dc2 restart-connector-dc2 replay-again \
        preflight bench-cdc test test-all test-ci test-js lint measure-failover

# Docker Compose (F2, F12). Biến đọc từ .env ở gốc repo; đổi file bằng: make up-hq ENV_FILE=...
ENV_FILE ?= .env
# Script trong infra/scripts, scripts/ đọc ENV_FILE và CONNECT_URL từ môi trường.
# CONNECT_URL để trống thì lib.sh dùng http://HOST_A_IP:8083 (chạy được từ cả máy A và máy B).
CONNECT_URL ?=
export ENV_FILE CONNECT_URL
COMPOSE_HQ = docker compose --env-file $(ENV_FILE) -f infra/compose.hq.yml
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

# ---- Khởi tạo lại trên hai máy (F12): máy B chạy reset-dc TRƯỚC, xong máy A chạy reset-hq ------
reset-dc:
	@echo "== reset-dc 1/3 (may B): down -v compose kho"
	$(COMPOSE_DC) down -v
	@echo "== reset-dc 2/3: up-dc"
	$(MAKE) --no-print-directory up-dc
	@echo "== reset-dc 3/3: cho pg-dc1..3 healthy"
	bash infra/scripts/wait-healthy.sh pg-dc1 pg-dc2 pg-dc3
	@echo "== reset-dc xong. Tiep theo: may A chay make reset-hq"

reset-hq:
	@echo "== reset-hq 1/8 (may A): kiem tra 3 kho vua reset-dc (chua co subscription cu)"
	bash infra/scripts/expect-fresh-dc.sh
	@echo "== reset-hq 2/8: down -v compose HQ"
	$(COMPOSE_HQ) down -v
	@echo "== reset-hq 3/8: up-hq"
	$(MAKE) --no-print-directory up-hq
	@echo "== reset-hq 4/8: cho pg-hq, kafka, kafka-connect healthy va pg-dc1..3 tra loi qua mang"
	bash infra/scripts/wait-healthy.sh
	@echo "== reset-hq 5/8: logical replication danh muc"
	bash infra/scripts/setup-replication.sh
	@echo "== reset-hq 6/8: postgres_fdw, v_inventory_all"
	bash infra/scripts/setup-fdw.sh
	@echo "== reset-hq 7/8: connector Debezium"
	bash infra/scripts/register-connectors.sh
	@echo "== reset-hq 8/8: seed"
	$(MAKE) --no-print-directory seed
	@echo "== reset-hq xong. Kiem tra: make preflight"

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
# Tham số thêm: make measure-failover ARGS="--only m2 --runs 5 --out ket-qua.json"
ARGS ?=
measure-failover:
	python scripts/measure/m2_m4.py $(ARGS)

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

# Máy A hoặc B: gọi lại /replay kho Trung với request_id của các giao dịch ghi hộ đã APPLIED;
# phải trả duplicate, tồn không đổi (KB3 bước 12).
replay-again:
	bash scripts/replay-again.sh

# Máy A hoặc B, chỉ đọc: kiểm tra trước giờ demo (IP, 3 kho ONLINE, connector, dòng trạng thái).
# Bước khác KB1-0: make preflight STEP=KB2-1
STEP ?= KB1-0
preflight:
	python scripts/preflight.py $(STEP)

# M1 (docs/tests.md mục 5), máy A: make bench-cdc ARGS="DC=2 COUNT=200 OUT=m1.json"
bench-cdc:
	env $(ARGS) bash scripts/measure/bench-cdc.sh
