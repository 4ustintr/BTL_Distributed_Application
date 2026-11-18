"""Kiểm tra trước giờ demo (make preflight). Chỉ đọc, chạy được trên máy A hoặc máy B, từ gốc repo:

    python scripts/preflight.py            # mặc định so với bước KB1-0 (vừa make reset)
    python scripts/preflight.py KB2-1      # trước Kịch bản 2

Kiểm: IP trong .env thuộc máy này; HQ API trả lời; 3 kho ONLINE ở cả Node API lẫn HQ; không còn
giao dịch ghi hộ đang chờ; 3 connector Debezium RUNNING; dòng trạng thái (docs/tests.md mục 3)
khớp bảng chuẩn của bước. Mỗi mục sai in kèm cách xử lý. Thoát 1 nếu có mục sai.
"""

from __future__ import annotations

import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(REPO_ROOT), str(REPO_ROOT / "tests")]

import httpx  # noqa: E402
from fixtures import format_status_line, read_env_file  # noqa: E402

from common.constants import (  # noqa: E402
    ENV_HOST_A_IP,
    ENV_HOST_B_IP,
    DcStatus,
    NodeMode,
    PendingTxnStatus,
)

HQ_API_PORT = 8000  # F2: hq-api
NODE_API_PORT_BASE = 8000  # F2: node-api-dc1..3 cổng 8001..8003
CONNECT_PORT = 8083  # F2: kafka-connect
SKU_A_PRODUCT_ID = 1  # tests.md mục 2
TIMEOUT_S = 3.0
INVARIANT_TIMEOUT_S = 10.0  # /invariant đọc 3 kho qua FDW (F11)
CDC_WAIT_S = 20.0  # chờ snapshot bắt kịp khi dòng trạng thái chưa khớp (như kb1_walk.py)
RUNNING = "RUNNING"  # trạng thái connector và task của Kafka Connect (F4)
DC_NAME = {1: "Bắc", 2: "Trung", 3: "Nam"}

# docs/tests.md mục 3: các trạng thái ổn định dùng làm điểm xuất phát của từng phần demo
EXPECTED = {
    "KB1-0": "BAC=100/100 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=0 RECEIVED_IN=150 DIFF=0",
    "KB1-5b": "BAC=20/20 TRUNG=40/40 NAM=60/60 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0",
    "KB2-1": "BAC=20/20 TRUNG=35/35 NAM=60/60 TRANSIT=0 SHIPPED_OUT=35 RECEIVED_IN=150 DIFF=0",
    "KB3-11": "BAC=20/20 TRUNG=55/55 NAM=60/60 TRANSIT=0 SHIPPED_OUT=45 RECEIVED_IN=180 DIFF=0",
    "KB3-12": "BAC=20/20 TRUNG=55/55 NAM=60/60 TRANSIT=0 SHIPPED_OUT=45 RECEIVED_IN=180 DIFF=0",
}


@dataclass(frozen=True)
class Observed:
    """Số liệu thu được. None hoặc rỗng nghĩa là không đọc được."""

    local_ips: frozenset[str]
    host_a: str
    host_b: str
    hq_ok: bool
    dc_status: dict[int, str]  # theo HQ (GET /dc-status)
    node_modes: dict[int, str | None]  # GET /health của từng Node API
    pending: dict[int, int]  # số pending_txn PENDING theo kho
    connectors: dict[int, str]  # "CONNECTOR_STATE TASK_STATE..." hoặc "" nếu không đọc được
    status_line: str | None
    step: str


@dataclass(frozen=True)
class Check:
    ok: bool
    title: str
    hint: str = ""


def assess(o: Observed) -> list[Check]:
    checks: list[Check] = []

    on_a, on_b = o.host_a in o.local_ips, o.host_b in o.local_ips
    if on_a or on_b:
        role = "cả HQ và 3 kho" if on_a and on_b else ("máy A (HQ)" if on_a else "máy B (3 kho)")
        checks.append(Check(True, f"IP trong .env thuộc máy này: {role}"))
    else:
        checks.append(
            Check(
                False,
                "IP trong .env không thuộc máy này",
                (
                    f"HOST_A_IP={o.host_a}, HOST_B_IP={o.host_b} nhưng máy này có "
                    f"{sorted(o.local_ips)}. Máy vừa đổi mạng? Sửa .env theo ipconfig rồi "
                    "make reset (hai máy: reset-dc, reset-hq)."
                ),
            )
        )

    if o.hq_ok:
        checks.append(Check(True, f"HQ API trả lời (http://{o.host_a}:{HQ_API_PORT})"))
    else:
        checks.append(
            Check(
                False,
                f"HQ API không trả lời (http://{o.host_a}:{HQ_API_PORT})",
                (
                    "Trên máy A: make ps xem hq-api có chạy không; "
                    f"kiểm tra HOST_A_IP và tường lửa cổng {HQ_API_PORT}."
                ),
            )
        )

    for dc, name in DC_NAME.items():
        port = NODE_API_PORT_BASE + dc
        node, hq = o.node_modes.get(dc), o.dc_status.get(dc)
        title = f"Kho {name} (DC {dc})"
        if node is None:
            checks.append(
                Check(
                    False,
                    f"{title}: Node API không trả lời",
                    (
                        f"Trên máy B: make ps; kiểm tra HOST_B_IP và tường lửa cổng {port} "
                        f"(http://{o.host_b}:{port}/health)."
                    ),
                )
            )
        elif node == NodeMode.RECOVERING:
            checks.append(
                Check(
                    False,
                    f"{title}: kho đang RECOVERING (HQ thấy {hq or '?'})",
                    (
                        "Kho chờ HQ gọi /recovery/complete. HQ phải gọi được "
                        f"http://{o.host_b}:{port}; chờ 10 giây rồi chạy lại. Nếu vẫn kẹt: "
                        "kiểm tra IP trong .env rồi make reset; quản trị viên có thể gọi tay "
                        f"POST http://{o.host_b}:{port}/recovery/complete kèm header "
                        "X-Internal-Key."
                    ),
                )
            )
        elif hq is not None and hq != DcStatus.ONLINE:
            checks.append(
                Check(
                    False,
                    f"{title}: kho ONLINE nhưng HQ thấy {hq}",
                    (
                        "HQ chưa gọi được kho: kiểm tra HOST_B_IP và tường lửa cổng "
                        f"{port} trên máy B, chờ 6 giây rồi chạy lại."
                    ),
                )
            )
        else:
            checks.append(Check(True, f"{title}: ONLINE"))

    waiting = {dc: n for dc, n in o.pending.items() if n}
    if o.hq_ok and waiting:
        detail = ", ".join(f"{DC_NAME[dc]} {n}" for dc, n in waiting.items())
        checks.append(
            Check(
                False,
                f"Còn giao dịch ghi hộ đang chờ: {detail}",
                ("Bình thường chỉ có giữa Kịch bản 3. Trước giờ demo: make reset."),
            )
        )
    elif o.hq_ok:
        checks.append(Check(True, "Không có giao dịch ghi hộ đang chờ"))

    for dc in DC_NAME:
        name = f"dc{dc}-outbox"
        states = o.connectors.get(dc, "").split()
        if not states:
            checks.append(
                Check(
                    False,
                    f"Không đọc được connector {name}",
                    (
                        f"Kafka Connect ở http://{o.host_a}:{CONNECT_PORT} (máy A): "
                        "make ps xem kafka-connect."
                    ),
                )
            )
        elif len(states) >= 2 and all(s == RUNNING for s in states):
            checks.append(Check(True, f"Connector {name}: RUNNING"))
        else:
            restart = (
                f"make restart-connector-dc{dc}"
                if dc == 2
                else f'curl -X POST "http://{o.host_a}:{CONNECT_PORT}/connectors/{name}/restart'
                '?includeTasks=true&onlyFailed=true"'
            )
            checks.append(
                Check(
                    False,
                    f"Connector {name}: {' '.join(states)}",
                    (f"Sau khi kho {DC_NAME[dc]} đã chạy: {restart}"),
                )
            )

    if o.status_line is not None:
        want = EXPECTED.get(o.step)
        if want is not None:
            ok = o.status_line == f"[{o.step}] {want}"
            hint = (
                ""
                if ok
                else (
                    f"Mong đợi: [{o.step}] {want}. Có thao tác ngoài kịch bản? make reset "
                    "rồi làm lại (python scripts/kb1_walk.py để đi nhanh hết KB1)."
                )
            )
        else:
            ok = o.status_line.endswith("DIFF=0")
            hint = (
                ""
                if ok
                else "Bất biến lệch: dừng demo, xem log (make logs S=hq-api) rồi make reset."
            )
        checks.append(Check(ok, f"Dòng trạng thái: {o.status_line}", hint))
    return checks


# ---- Thu thập số liệu (HTTP; không cần docker nên chạy được trên máy A hoặc B) ---------------
def local_ips() -> frozenset[str]:
    ips = {"127.0.0.1"}
    try:
        ips.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("192.0.2.1", 9))  # không gửi gói nào; chỉ để hệ điều hành chọn card mạng
            ips.add(s.getsockname()[0])
    except OSError:
        pass
    return frozenset(ips)


def _get(http: httpx.Client, url: str, **kw):  # noqa: ANN202
    try:
        return http.get(url, **kw)
    except httpx.HTTPError:
        return None


def _status_line(http: httpx.Client, hq: str, step: str) -> str | None:
    params = {"product_id": SKU_A_PRODUCT_ID}
    r = _get(http, f"{hq}/dashboard/inventory", params={**params, "mode": "snapshot"})
    inv = _get(http, f"{hq}/invariant", params=params, timeout=INVARIANT_TIMEOUT_S)
    if r is None or r.status_code != 200 or inv is None:
        return None
    invariant = inv.json() if inv.status_code == 200 else None
    return format_status_line(step, r.json()["items"], invariant)


def observe(step: str) -> Observed:
    env = {
        **read_env_file(REPO_ROOT / os.environ.get("ENV_FILE", ".env")),
        **{k: v for k, v in os.environ.items() if v},
    }
    host_a, host_b = env.get(ENV_HOST_A_IP, ""), env.get(ENV_HOST_B_IP, "")
    if not host_a or not host_b:
        raise SystemExit(
            "Thiếu HOST_A_IP hoặc HOST_B_IP trong .env (cp .env.example .env rồi sửa)."
        )
    hq = f"http://{host_a}:{HQ_API_PORT}"
    connect = env.get("CONNECT_URL") or f"http://{host_a}:{CONNECT_PORT}"
    with httpx.Client(timeout=TIMEOUT_S) as http:
        r = _get(http, f"{hq}/health")
        hq_ok = r is not None and r.status_code == 200
        dc_status: dict[int, str] = {}
        pending: dict[int, int] = {dc: 0 for dc in DC_NAME}
        status_line = None
        if hq_ok:
            r = _get(http, f"{hq}/dc-status")
            if r is not None and r.status_code == 200:
                dc_status = {int(x["dc_id"]): x["status"] for x in r.json()}
            r = _get(http, f"{hq}/pending-txn")
            if r is not None and r.status_code == 200:
                for x in r.json():
                    if x["status"] == PendingTxnStatus.PENDING:
                        pending[int(x["dc_id"])] = pending.get(int(x["dc_id"]), 0) + 1
            # Ngay sau reset, sự kiện CDC đầu tiên mất vài giây mới tới snapshot: chờ tối đa
            # CDC_WAIT_S cho tới khi dòng trạng thái khớp bảng chuẩn (như scripts/kb1_walk.py).
            want = f"[{step}] {EXPECTED[step]}" if step in EXPECTED else None
            deadline = time.monotonic() + CDC_WAIT_S
            while True:
                status_line = _status_line(http, hq, step)
                if want is None or status_line == want or time.monotonic() >= deadline:
                    break
                time.sleep(1)
        node_modes: dict[int, str | None] = {}
        for dc in DC_NAME:
            r = _get(http, f"http://{host_b}:{NODE_API_PORT_BASE + dc}/health")
            node_modes[dc] = (
                r.json().get("mode") if r is not None and r.status_code == 200 else None
            )
        connectors: dict[int, str] = {}
        for dc in DC_NAME:
            r = _get(http, f"{connect}/connectors/dc{dc}-outbox/status")
            if r is not None and r.status_code == 200:
                body = r.json()
                states = [body["connector"]["state"], *(t["state"] for t in body.get("tasks", []))]
                connectors[dc] = " ".join(states)
            else:
                connectors[dc] = ""
    return Observed(
        local_ips(),
        host_a,
        host_b,
        hq_ok,
        dc_status,
        node_modes,
        pending,
        connectors,
        status_line,
        step,
    )


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):  # terminal Windows mặc định cp1252
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    args = sys.argv[1:] if argv is None else argv
    step = args[0] if args else "KB1-0"
    checks = assess(observe(step))
    for c in checks:
        print(f"{'ĐẠT' if c.ok else 'SAI'}  {c.title}")
        if not c.ok and c.hint:
            print(f"      -> {c.hint}")
    bad = sum(not c.ok for c in checks)
    print(f"\n{'SẴN SÀNG DEMO' if not bad else f'CÓ {bad} MỤC CẦN XỬ LÝ'} (bước {step})")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
