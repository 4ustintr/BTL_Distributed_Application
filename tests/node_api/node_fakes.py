"""Đồ giả cho unit test của node-api (không cần CSDL thật).

Hợp đồng với node-api/services/stock.py mà các test dựa vào:
- Mỗi câu SQL bắt đầu bằng đúng từ khóa của lệnh (SET LOCAL, UPDATE inventory, INSERT INTO ...,
  SELECT ... FROM ...), để classify() nhận ra loại câu lệnh.
- Câu có RETURNING hoặc SELECT dùng fetchrow/fetchval; SET LOCAL và INSERT outbox_event dùng
  execute.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import Any, NamedTuple


def classify(sql: str) -> str:
    """Loại câu lệnh theo từ khóa đầu câu."""
    s = " ".join(sql.lower().split())
    if s.startswith("set local"):
        return "set_local"
    if s.startswith("update inventory"):
        return "update_inventory"
    if s.startswith("insert into inventory"):
        return "upsert_inventory"
    if s.startswith("insert into stock_movement"):
        return "insert_movement"
    if s.startswith("insert into outbox_event"):
        return "insert_outbox"
    if s.startswith("select") and "from stock_movement" in s:
        return "select_movement"
    if s.startswith("select") and "from inventory" in s:
        return "select_inventory"
    if s.startswith("select") and "from product" in s:
        return "select_product"
    return "other"


class Call(NamedTuple):
    kind: str
    sql: str  # đã chuẩn hóa khoảng trắng, chữ thường
    args: tuple[Any, ...]

    @property
    def str_args(self) -> list[str]:
        return [str(a) for a in self.args]


class FakeTransaction:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False


class FakeConn:
    """Kết nối giả: ghi lại mọi câu lệnh, trả kết quả theo kịch bản của từng loại câu.

    script: {kind: [kết quả lần 1, lần 2, ...]}; hết danh sách thì lặp lại kết quả cuối.
    Loại câu không có trong script trả None. Kết quả là dict (một dòng) hoặc None (không có dòng).
    """

    def __init__(
        self,
        script: Mapping[str, list[Any]] | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.script = {k: deque(v) for k, v in (script or {}).items()}
        self.calls: list[Call] = []
        self.events = events if events is not None else []
        self.transactions: list[FakeTransaction] = []

    def kinds(self) -> list[str]:
        return [c.kind for c in self.calls]

    def calls_of(self, kind: str) -> list[Call]:
        return [c for c in self.calls if c.kind == kind]

    def _answer(self, sql: str, args: tuple[Any, ...]) -> Any:
        kind = classify(sql)
        self.calls.append(Call(kind, " ".join(sql.lower().split()), args))
        self.events.append(f"sql:{kind}")
        queue = self.script.get(kind)
        if not queue:
            return None
        return queue.popleft() if len(queue) > 1 else queue[0]

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        value = self._answer(sql, args)
        if value is None or isinstance(value, Mapping):
            return value
        return {"?column?": value}

    async def fetchval(self, sql: str, *args: Any) -> Any:
        value = self._answer(sql, args)
        if isinstance(value, Mapping):
            return next(iter(value.values()))
        return value

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        value = self._answer(sql, args)
        return [] if value is None else list(value)

    async def execute(self, sql: str, *args: Any) -> str:
        self._answer(sql, args)
        return "OK"

    def transaction(self) -> FakeTransaction:
        tx = FakeTransaction()
        self.transactions.append(tx)
        return tx


class _Acquire:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


class FakePool:
    """Pool giả: acquire() dùng với async with; fetchval phục vụ /health."""

    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)

    async def fetchval(self, _sql: str) -> int:
        return 1

    async def close(self) -> None:
        pass
