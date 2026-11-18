"""Đi từng bước Kịch bản 1 (docs/demo.md) trên hệ thống thật, so dòng trạng thái với tests.md mục 3.

Chạy từ gốc repo, ngay sau make reset: python scripts/kb1_walk.py
Script tự chạy make race, nên dữ liệu sau đó không còn như vừa reset.
"""

import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO), str(REPO / "tests")]
for _stream in (sys.stdout, sys.stderr):  # terminal Windows mặc định cp1252, không in được "ĐẠT"
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8")

import httpx  # noqa: E402
from fixtures import format_status_line, integration_target  # noqa: E402

EXPECTED = {
    "KB1-0": "BAC=100/100 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=0 RECEIVED_IN=150 DIFF=0",
    "KB1-1": "BAC=100/50 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=0 RECEIVED_IN=150 DIFF=0",
    "KB1-2": "BAC=70/20 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0",
    "KB1-3": "BAC=20/20 TRUNG=40/40 NAM=10/10 TRANSIT=50 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0",
    "KB1-4": "BAC=20/20 TRUNG=40/40 NAM=60/60 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0",
    "KB1-5a": "BAC=20/10 TRUNG=40/40 NAM=60/60 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0",
    "KB1-5b": "BAC=20/20 TRUNG=40/40 NAM=60/60 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0",
}
t = integration_target()
http = httpx.Client(timeout=30)
ok_all = True


def line(step: str) -> str:
    items = http.get(f"{t.hq}/dashboard/inventory", params={"product_id": 1}).json()["items"]
    inv = http.get(f"{t.hq}/invariant", params={"product_id": 1})
    return format_status_line(step, items, inv.json() if inv.status_code == 200 else None)


def check(step: str, note: str) -> None:
    global ok_all
    want = f"[{step}] {EXPECTED[step]}"
    deadline = time.monotonic() + 20  # chờ CDC đưa snapshot về HQ
    got = line(step)
    while got != want and time.monotonic() < deadline:
        time.sleep(0.5)
        got = line(step)
    ok = got == want
    ok_all &= ok
    print(f"{'ĐẠT' if ok else 'SAI'}  {got}   <- {note}")
    if not ok:
        print(f"     mong đợi: {want}")


def post(url: str, body: dict) -> httpx.Response:
    return http.post(url, json=body)


def transfer(from_dc: int, to_dc: int, qty: int) -> httpx.Response:
    body = {"from_dc_id": from_dc, "to_dc_id": to_dc, "items": [{"product_id": 1, "qty": qty}]}
    return post(f"{t.hq}/transfers", body)


check("KB1-0", "make reset xong")

r = transfer(1, 3, 50)
print(f"     POST /transfers -> {r.status_code} {r.json()}")
t1 = r.json()["transfer_id"]
check("KB1-1", "phiếu 1 Bắc -> Nam 50, Bắc giữ chỗ 50")

race = subprocess.run(
    ["make", "race"], cwd=REPO, capture_output=True, text=True, encoding="utf-8", errors="replace"
)
print("     make race:", " | ".join(x.strip() for x in race.stdout.splitlines() if "REQ_RACE" in x))
check("KB1-2", "race: 1 lệnh 200, 1 lệnh 422")

r = post(f"{t.nodes[1]}/transfers/{t1}/ship", {"created_by": "nv-bac"})
print(f"     ship Bắc -> {r.status_code} {r.json()}")
check("KB1-3", "Bắc xuất, 50 đi đường tới Nam")

deadline = time.monotonic() + 15  # HQ đồng bộ SHIPPED xuống kho Nam (chạy nền)
while True:
    r = post(f"{t.nodes[3]}/transfers/{t1}/receive", {"created_by": "nv-nam"})
    if r.status_code != 409 or time.monotonic() > deadline:
        break
    time.sleep(0.5)
print(f"     receive Nam -> {r.status_code} {r.json()}")
check("KB1-4", "Nam nhận 50")
summary = http.get(f"{t.hq}/transfers/{t1}").json()
print(f"     phiếu 1: {summary['transfer_code']} {summary['status']}")

r = transfer(1, 2, 10)
print(f"     POST /transfers (phiếu 2) -> {r.status_code} {r.json()}")
t2 = r.json()["transfer_id"]
check("KB1-5a", "phiếu 2 Bắc -> Trung 10, Bắc giữ chỗ 10")

r = post(f"{t.nodes[2]}/transfers/{t2}/decline", {"reason": "DECLINED"})
print(f"     decline Trung -> {r.status_code} {r.json()}")
check("KB1-5b", "Trung từ chối, Bắc nhả giữ chỗ")
summary = http.get(f"{t.hq}/transfers/{t2}").json()
print(f"     phiếu 2: {summary['transfer_code']} {summary['status']} {summary['cancel_reason']}")

print("\nKẾT QUẢ:", "KB1 khớp bảng chuẩn ở mọi bước" if ok_all else "CÓ BƯỚC LỆCH")
sys.exit(0 if ok_all else 1)
