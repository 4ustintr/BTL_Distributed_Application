# Lệnh theo spec F12. Các lệnh còn TODO sẽ được hoàn thiện dần.
TEST_DIRS = tests/common tests/node_api tests/hq_api tests/consumer

.PHONY: up-hq up-dc down-hq down-dc ps logs reset seed race race-unsafe check-invariant \
        pause-dc2 unpause-dc2 kill-dc2 start-dc2 restart-connector-dc2 replay-again bench-cdc \
        test test-all test-ci lint

# Docker Compose (F2, F12). Biến đọc từ .env ở gốc repo; đổi file bằng: make up-hq ENV_FILE=...
ENV_FILE ?= .env
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

# 10 test bắt buộc (cần hệ thống đang chạy)
test:
	@for d in $(TEST_DIRS); do pytest -m "not optional" $$d || [ $$? -eq 5 ] || exit 1; done

test-all:
	@for d in $(TEST_DIRS); do pytest $$d || [ $$? -eq 5 ] || exit 1; done

reset seed race race-unsafe check-invariant pause-dc2 unpause-dc2 kill-dc2 start-dc2 \
restart-connector-dc2 replay-again bench-cdc:
	@echo "TODO: $@ (xem docs/spec.md mục F12)"
