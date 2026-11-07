# Lệnh theo spec F12. Các lệnh còn TODO sẽ được hoàn thiện dần.
TEST_DIRS = tests/common tests/node_api tests/hq_api tests/consumer

.PHONY: up-hq up-dc reset seed race race-unsafe check-invariant pause-dc2 unpause-dc2 \
        kill-dc2 start-dc2 restart-connector-dc2 replay-again bench-cdc test test-all test-ci lint

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

up-hq up-dc reset seed race race-unsafe check-invariant pause-dc2 unpause-dc2 kill-dc2 start-dc2 \
restart-connector-dc2 replay-again bench-cdc:
	@echo "TODO: $@ (xem docs/spec.md mục F12)"
