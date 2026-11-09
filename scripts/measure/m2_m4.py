"""Số đo M2, M4 (docs/tests.md mục 5) – Nhật Khánh.

Chạy ở gốc repo: python scripts/measure/m2_m4.py [--runs 5] [--n 2,20,200] [--only m2|m4]
                 [--out kq.json]

M2 – thời gian phát hiện kho chết: dc_status.updated_at lúc chuyển OFFLINE trừ thời điểm gọi
     docker kill (make kill-dc2); lặp 5 lần; báo trung bình và max. Dự kiến 4–6 giây (3 × 2 giây).
M4 – thời gian phục hồi: dc_status.updated_at lúc chuyển ONLINE trừ thời điểm gọi docker start
     (make start-dc2), với N giao dịch ghi hộ đang chờ, N = 2, 20, 200.

Mọi mốc thời gian lấy theo đồng hồ của pg-hq (clock_timestamp()), cùng đồng hồ ghi updated_at,
nên không lệch giờ giữa các máy. Script chạy make kill-dc2 / start-dc2 / restart-connector-dc2
ngay trên máy này, nên cần HQ và kho cùng một máy (HOST_A_IP = HOST_B_IP), như test tích hợp.

Chạy sau make reset. M4 ghi hộ N lệnh nhập 1 thùng vào kho Trung (request_id tất định theo N,
xem m4_request_ids), nên tồn Trung tăng 222 thùng sau cả 3 lần đo: make reset lại trước khi demo.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import sys
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import asyncpg  # noqa: E402
import httpx  # noqa: E402

from common.constants import (  # noqa: E402
    ENV_HOST_A_IP,
    ENV_HOST_B_IP,
    DcStatus,
    PendingTxnStatus,
)
from common.models import FailoverRequest, FailoverResponse  # noqa: E402

DC_TRUNG = 2  # docs/demo.md: kho bị tắt trong Kịch bản 2, 3 (make kill-dc2)
SKU_A_PRODUCT_ID = 1  # docs/tests.md mục 2
HQ_PORT = 8000  # F2
HQ_DB_PORT = 5432  # F2
ENV_POSTGRES_PASSWORD = "POSTGRES_PASSWORD"  # .env.example
M2_RUNS = 5  # docs/tests.md mục 5: lặp 5 lần
M4_NS = (2, 20, 200)  # docs/tests.md mục 5: N = 2, 20, 200
M4_CREATED_BY = "m4"
M4_ID_NAMESPACE = "btl-m4"

POLL_S = 0.2
ONLINE_WAIT_S = 180.0  # chờ kho về ONLINE trước mỗi lần đo
OFFLINE_WAIT_S = 30.0  # M2 dự kiến 4–6 giây
RECOVERY_WAIT_S = 900.0  # M4 với N = 200: Replay Worker gửi từng dòng, chờ CDC


# ---------------------------------------------------------------------------
# Hàm thuần (có unit test ở tests/hq_api/test_measure_m2_m4.py)
# ---------------------------------------------------------------------------
def read_env_file(path: Path) -> dict[str, str]:
    """Đọc .env kiểu KEY=VALUE; bỏ dòng trống, chú thích, dòng không có '='; thiếu file trả {}."""
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if sep:
            values[key.strip()] = value.strip().strip("\"'")
    return values


def same_host_problem(host_a: str | None, host_b: str | None) -> str | None:
    """Lý do không chạy được, hoặc None khi HQ và kho cùng một máy."""
    if not host_a or not host_b:
        return f"thiếu {ENV_HOST_A_IP} hoặc {ENV_HOST_B_IP} trong .env"
    if host_a != host_b:
        return f"cần HQ và kho cùng một máy ({ENV_HOST_A_IP} = {ENV_HOST_B_IP})"
    return None


def m4_request_ids(n: int) -> list[uuid.UUID]:
    """N request_id tất định cho lần đo M4 với N giao dịch: uuid5 như common/ids.py."""
    return [uuid.uuid5(uuid.NAMESPACE_URL, f"{M4_ID_NAMESPACE}:{n}:{i}") for i in range(n)]


def seconds_between(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds()


def summarize(values: Iterable[float]) -> dict[str, Any]:
    """M2: các lần đo, trung bình và max (giây, làm tròn 2 chữ số)."""
    runs = [round(v, 2) for v in values]
    if not runs:
        return {"runs_s": [], "mean_s": None, "max_s": None}
    return {"runs_s": runs, "mean_s": round(statistics.fmean(runs), 2), "max_s": max(runs)}


def status_counts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """{"APPLIED": n, "REJECTED": n, "PENDING": n} từ các dòng (status, count)."""
    counts = {s.value: 0 for s in PendingTxnStatus}
    for row in rows:
        counts[row["status"]] = row["count"]
    return counts


# ---------------------------------------------------------------------------
# Đo trên hệ thống thật
# ---------------------------------------------------------------------------
def make(target: str) -> None:
    print(f"  $ make {target}", flush=True)
    subprocess.run(["make", target], cwd=REPO_ROOT, check=True)


class Ctx:
    def __init__(self, hq_url: str, conn: asyncpg.Connection, http: httpx.AsyncClient) -> None:
        self.hq_url = hq_url
        self.conn = conn
        self.http = http

    async def db_now(self) -> datetime:
        return await self.conn.fetchval("SELECT clock_timestamp()")

    async def wait_status(self, status: DcStatus, timeout_s: float) -> datetime:
        """Chờ dc_status kho Trung = status; trả updated_at (lần đổi status gần nhất)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while True:
            row = await self.conn.fetchrow(
                "SELECT status, updated_at FROM dc_status WHERE dc_id = $1", DC_TRUNG
            )
            if row is not None and row["status"] == status:
                return row["updated_at"]
            if loop.time() >= deadline:
                got = None if row is None else row["status"]
                raise TimeoutError(f"hết {timeout_s:.0f} giây chờ kho {DC_TRUNG} {status}: {got}")
            await asyncio.sleep(POLL_S)

    async def recover(self) -> datetime:
        """docker start + restart connector (KB3 bước 7–8), chờ ONLINE; trả lúc gọi start."""
        started = await self.db_now()
        make("start-dc2")
        make("restart-connector-dc2")
        await self.wait_status(DcStatus.ONLINE, RECOVERY_WAIT_S)
        return started


async def measure_m2(ctx: Ctx, runs: int) -> dict[str, Any]:
    values: list[float] = []
    for k in range(1, runs + 1):
        await ctx.wait_status(DcStatus.ONLINE, ONLINE_WAIT_S)
        killed = await ctx.db_now()  # thời điểm gọi docker kill
        make("kill-dc2")
        offline_at = await ctx.wait_status(DcStatus.OFFLINE, OFFLINE_WAIT_S)
        values.append(seconds_between(killed, offline_at))
        print(f"M2 lần {k}/{runs}: {values[-1]:.2f} giây", flush=True)
        await ctx.recover()
    return summarize(values)


async def measure_m4(ctx: Ctx, n: int) -> dict[str, Any]:
    ids = m4_request_ids(n)
    await ctx.wait_status(DcStatus.ONLINE, ONLINE_WAIT_S)
    make("kill-dc2")
    await ctx.wait_status(DcStatus.OFFLINE, OFFLINE_WAIT_S)
    for request_id in ids:
        body = FailoverRequest(
            request_id=request_id, product_id=SKU_A_PRODUCT_ID, qty=1, created_by=M4_CREATED_BY
        )
        r = await ctx.http.post(
            f"{ctx.hq_url}/failover/dc/{DC_TRUNG}/inbound", json=body.model_dump(mode="json")
        )
        if r.status_code != 200 or FailoverResponse.model_validate(r.json()).duplicate:
            raise RuntimeError(
                f"ghi hộ {request_id} không nhận như mong đợi: {r.status_code} {r.text}"
            )
    started = await ctx.recover()
    online_at = await ctx.wait_status(DcStatus.ONLINE, 1)
    rows = await ctx.conn.fetch(
        """SELECT status, count(*) AS count FROM pending_txn
           WHERE txn_id = ANY($1::uuid[]) GROUP BY status""",
        ids,
    )
    result = {"n": n, "recovery_s": round(seconds_between(started, online_at), 2)}
    result.update(status_counts(rows))
    print(f"M4 N = {n}: {result['recovery_s']:.2f} giây {status_counts(rows)}", flush=True)
    return result


async def run(args: argparse.Namespace) -> dict[str, Any]:
    env_file = REPO_ROOT / os.environ.get("ENV_FILE", ".env")
    env = {**read_env_file(env_file), **os.environ}
    problem = same_host_problem(env.get(ENV_HOST_A_IP), env.get(ENV_HOST_B_IP))
    if problem is not None:
        raise SystemExit(f"m2_m4: {problem}")
    host = env[ENV_HOST_A_IP]
    dsn = f"postgresql://hq_app:{env[ENV_POSTGRES_PASSWORD]}@{host}:{HQ_DB_PORT}/hq_db"
    ns = [int(x) for x in args.n.split(",")] if args.n else list(M4_NS)

    conn = await asyncpg.connect(dsn)
    try:
        if args.only != "m2":
            used = await conn.fetchval(
                "SELECT count(*) FROM pending_txn WHERE txn_id = ANY($1::uuid[])",
                [i for n in ns for i in m4_request_ids(n)],
            )
            if used:
                raise SystemExit(
                    "m2_m4: request_id của M4 đã có trong pending_txn, chạy make reset"
                )
        report: dict[str, Any] = {
            "measured_at": (await conn.fetchval("SELECT now()")).isoformat(),
            "machine": {
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "python": platform.python_version(),
                "same_host": True,
            },
        }
        async with httpx.AsyncClient(timeout=10) as http:
            ctx = Ctx(f"http://{host}:{HQ_PORT}", conn, http)
            if args.only != "m4":
                report["m2"] = await measure_m2(ctx, args.runs)
            if args.only != "m2":
                report["m4"] = [await measure_m4(ctx, n) for n in ns]
    finally:
        await conn.close()
    return report


def use_utf8_console() -> None:
    """Console Windows mặc định cp1252, in tiếng Việt sẽ lỗi UnicodeEncodeError."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    use_utf8_console()
    parser = argparse.ArgumentParser(description="Số đo M2, M4 (docs/tests.md mục 5)")
    parser.add_argument("--runs", type=int, default=M2_RUNS, help="số lần đo M2 (mặc định 5)")
    parser.add_argument("--n", default=None, help="các N của M4, phân cách bằng dấu phẩy")
    parser.add_argument("--only", choices=["m2", "m4"], default=None, help="chỉ đo M2 hoặc M4")
    parser.add_argument("--out", type=Path, default=None, help="ghi kết quả JSON vào file này")
    args = parser.parse_args(argv)
    report = asyncio.run(run(args))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.out is not None:
        args.out.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
