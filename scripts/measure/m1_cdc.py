"""M1 – độ trễ CDC (docs/tests.md mục 5) – Ngọc Chung. Chạy qua make bench-cdc (máy A).

  send:   gửi COUNT lệnh POST /outbound 1 thùng, cách nhau INTERVAL giây, vào một kho; lưu
          request_id từng lệnh vào file JSON (lưu trước khi gửi, để lệnh hết giờ vẫn có mã).
  report: đọc log của hq-consumer (mỗi sự kiện một dòng JSON có latency_ms = synced_at −
          occurred_at), lấy đúng các request_id đã gửi, tính p50, p95, max (ms).

Không đọc inventory_snapshot: sự kiện sau ghi đè dòng đó nên mất mẫu. occurred_at theo đồng hồ
kho, synced_at theo đồng hồ HQ: hai máy phải đồng bộ NTP, hoặc đo khi kho và HQ cùng một máy.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import httpx  # noqa: E402

from common.constants import RefType  # noqa: E402
from common.ids import new_request_id  # noqa: E402


def summarize(request_ids: list[str], lines: list[str]) -> dict[str, float | int]:
    samples = {}
    for line in lines:
        # Accept plain docker logs and docker-compose's service-name prefixes.
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        request_id = record.get("request_id")
        if request_id in request_ids and "latency_ms" in record:
            latency = float(record["latency_ms"])
            if not math.isfinite(latency) or latency < 0:
                raise ValueError("Negative/invalid latency: synchronize clocks before measuring M1")
            samples.setdefault(request_id, latency)
    missing = set(request_ids) - samples.keys()
    if missing:
        raise ValueError(
            f"Missing {len(missing)} CDC samples; wait for catch-up and collect logs again"
        )
    if not samples:
        raise ValueError("No CDC measurements")
    ordered = sorted(samples.values())

    def percentile(fraction: float) -> float:
        return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]

    return {
        "count": len(ordered),
        "p50_ms": percentile(0.5),
        "p95_ms": percentile(0.95),
        "max_ms": ordered[-1],
    }


async def send(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("Output already exists; use a new filename to preserve run IDs")
    requests = []
    run = {
        "started_at": datetime.now(UTC).isoformat(),
        "node_url": args.node_url,
        "product_id": args.product_id,
        "requests": requests,
    }
    # Persist partial runs too: a failed HTTP request must not erase earlier successful IDs.
    args.output.write_text(json.dumps(run, indent=2), encoding="utf-8")
    async with httpx.AsyncClient(base_url=args.node_url, timeout=30) as client:
        if args.prepare_stock:
            response = await client.post(
                "/inbound",
                json={
                    "request_id": str(new_request_id()),
                    "product_id": args.product_id,
                    "qty": args.count,
                    "ref_type": RefType.RECEIPT,
                    "created_by": "bench-cdc",
                },
            )
            response.raise_for_status()
        loop = asyncio.get_running_loop()
        started = loop.time()
        for index in range(args.count):
            request_id = str(new_request_id())
            # Save before sending: a timeout can occur after the warehouse committed.
            entry = {"request_id": request_id, "status": "sending"}
            requests.append(entry)
            args.output.write_text(json.dumps(run, indent=2), encoding="utf-8")
            response = await client.post(
                "/outbound",
                json={
                    "request_id": request_id,
                    "product_id": args.product_id,
                    "qty": 1,
                    "ref_type": RefType.ISSUE,
                    "created_by": "bench-cdc",
                },
            )
            response.raise_for_status()
            entry["status"] = "committed"
            args.output.write_text(json.dumps(run, indent=2), encoding="utf-8")
            await asyncio.sleep(max(0, started + (index + 1) * args.interval - loop.time()))
    print(f"Sent {args.count} writes; collect Consumer logs, then run report.")


def main() -> None:
    for stream in (
        sys.stdout,
        sys.stderr,
    ):  # terminal Windows mặc định cp1252, không in được tiếng Việt
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sender = commands.add_parser("send")
    sender.add_argument("--node-url", required=True)
    sender.add_argument("--product-id", type=int, default=1)
    sender.add_argument("--count", type=int, default=200)
    sender.add_argument("--interval", type=float, default=0.1)
    sender.add_argument("--prepare-stock", action="store_true", help="Add count units via /inbound")
    sender.add_argument("--output", type=Path, required=True)
    report = commands.add_parser("report")
    report.add_argument("--requests", type=Path, required=True)
    report.add_argument("--log", type=Path, required=True)
    report.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "send":
            if args.count <= 0 or args.interval < 0 or args.product_id <= 0:
                parser.error("count/product-id must be positive; interval must be nonnegative")
            asyncio.run(send(args))
        else:
            run = json.loads(args.requests.read_text(encoding="utf-8-sig"))
            ids = [item["request_id"] for item in run["requests"]]
            result = summarize(ids, args.log.read_text(encoding="utf-8-sig").splitlines())
            result["started_at"] = run["started_at"]
            text = json.dumps(result, indent=2, ensure_ascii=False)
            if args.output:
                args.output.write_text(text, encoding="utf-8")
            print(text)
    except (ValueError, OSError, httpx.HTTPError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
