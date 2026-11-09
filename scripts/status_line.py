"""In dòng trạng thái của một bước demo (docs/tests.md mục 3). Chạy trên máy A, từ gốc repo:

    python scripts/status_line.py KB1-2
    -> [KB1-2] BAC=70/20 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0

Số liệu lấy từ GET /dashboard/inventory (snapshot) và GET /invariant (F7). /invariant trả 503 khi
một kho không đọc được (Kịch bản 2): SHIPPED_OUT, RECEIVED_IN, DIFF in NA, TRANSIT lấy từ dashboard.
Định dạng dùng chung tests/fixtures.format_status_line, nên dòng in ra khớp với test.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(REPO_ROOT), str(REPO_ROOT / "tests")]

from fixtures import format_status_line, read_env_file  # noqa: E402

from common.constants import ENV_HOST_A_IP, HTTP_TIMEOUT_S  # noqa: E402

HQ_API_PORT = 8000  # F2: hq-api
SKU_A_PRODUCT_ID = 1  # tests.md mục 2
INVARIANT_TIMEOUT_S = 10.0  # /invariant đọc 3 kho qua FDW, mỗi truy vấn tối đa 5 giây (F11)


def hq_url() -> str:
    env = {**read_env_file(REPO_ROOT / os.environ.get("ENV_FILE", ".env")), **os.environ}
    return f"http://{env[ENV_HOST_A_IP]}:{HQ_API_PORT}"


def use_utf8_console() -> None:
    """Terminal Windows mặc định cp1252, không in được tiếng Việt."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    use_utf8_console()
    parser = argparse.ArgumentParser(description="Dòng trạng thái demo (tests.md mục 3)")
    parser.add_argument("step", help="mã bước, ví dụ KB1-2, KB2-5")
    parser.add_argument("--product-id", type=int, default=SKU_A_PRODUCT_ID)
    args = parser.parse_args(argv)

    base = hq_url()
    params = {"product_id": args.product_id}
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT_S) as http:
            r = http.get(f"{base}/dashboard/inventory", params={**params, "mode": "snapshot"})
            if r.status_code != 200:
                print(f"GET /dashboard/inventory trả {r.status_code}: {r.text}", file=sys.stderr)
                return 1
            items = r.json()["items"]
            inv = http.get(f"{base}/invariant", params=params, timeout=INVARIANT_TIMEOUT_S)
    except httpx.HTTPError as exc:
        print(f"Không gọi được HQ API {base}: {exc!r}", file=sys.stderr)
        return 1
    if inv.status_code == 200:
        invariant = inv.json()
    elif inv.status_code == 503:
        invariant = None  # F7: một kho không truy cập được
    else:
        print(f"GET /invariant trả {inv.status_code}: {inv.text}", file=sys.stderr)
        return 1
    print(format_status_line(args.step, items, invariant))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
