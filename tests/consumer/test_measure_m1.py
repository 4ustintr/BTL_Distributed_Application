"""Unit test scripts/measure/m1_cdc.py (số đo M1, docs/tests.md mục 5)."""

import argparse
import importlib.util
import json

import httpx
import pytest
from fixtures import REPO_ROOT

SCRIPT = REPO_ROOT / "scripts" / "measure" / "m1_cdc.py"
_spec = importlib.util.spec_from_file_location("m1_cdc", SCRIPT)
assert _spec is not None and _spec.loader is not None
m1_cdc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m1_cdc)
send, summarize = m1_cdc.send, m1_cdc.summarize


def test_m1_percentiles_and_replayed_logs_are_deduplicated():
    ids = [str(index) for index in range(200)]
    lines = [
        json.dumps({"request_id": str(index), "latency_ms": index + 1}) for index in range(200)
    ]
    assert summarize(ids, lines + lines) == {
        "count": 200,
        "p50_ms": 100,
        "p95_ms": 190,
        "max_ms": 200,
    }


def test_m1_missing_events_are_not_silently_reported_as_success():
    with pytest.raises(ValueError, match="Missing"):
        summarize(["missing"], [])


@pytest.mark.parametrize("latency", [-1, float("nan"), float("inf")])
def test_m1_invalid_clock_measurement_rejected(latency):
    with pytest.raises(ValueError, match="synchronize clocks"):
        summarize(["request"], [json.dumps({"request_id": "request", "latency_ms": latency})])


async def test_m1_sender_uses_api_prefills_and_saves_request_ids(monkeypatch, tmp_path):
    calls = []

    def handle(request):
        calls.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"qty_on_hand": 200, "version": 1})

    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handle)),
    )
    args = argparse.Namespace(
        node_url="http://node",
        product_id=1,
        count=2,
        interval=0,
        prepare_stock=True,
        output=tmp_path / "requests.json",
    )
    await send(args)
    assert [call[0] for call in calls] == ["/inbound", "/outbound", "/outbound"]
    assert calls[0][1]["qty"] == 2
    manifest = json.loads(args.output.read_text())
    assert [item["request_id"] for item in manifest["requests"]] == [
        calls[1][1]["request_id"],
        calls[2][1]["request_id"],
    ]
    assert all(item["status"] == "committed" for item in manifest["requests"])
