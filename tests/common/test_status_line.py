"""tests/fixtures.py: nạp demo_ids.env và dòng trạng thái theo docs/tests.md mục 3."""

import os
import uuid

from fixtures import DEMO_IDS, format_status_line

EXPECTED_KEYS = {
    "REQ_SEED_1", "REQ_SEED_2", "REQ_SEED_3", "REQ_RACE_1", "REQ_RACE_2",
    "REQ_KB2_OUT", "REQ_KB3_IN", "REQ_KB3_OUT", "REQ_KB3_OUT_BIG",
}


def test_demo_ids_loaded_and_are_uuids(demo_ids: dict[str, str]) -> None:
    assert set(demo_ids) == EXPECTED_KEYS
    assert demo_ids == DEMO_IDS
    for key, value in demo_ids.items():
        uuid.UUID(value)
        assert os.environ[key] == value
    assert len(set(demo_ids.values())) == len(demo_ids)


def _item(dc_id: int, on_hand: int, reserved: int, source: str, transit: int = 0) -> dict:
    return {
        "dc_id": dc_id,
        "qty_on_hand": on_hand,
        "qty_reserved": reserved,
        "qty_in_transit": transit,
        "source": source,
    }


def test_status_line_kb1_2() -> None:
    items = [_item(1, 70, 50, "snapshot"), _item(2, 40, 0, "snapshot"), _item(3, 10, 0, "snapshot")]
    inv = {"stock": 120, "in_transit": 0, "shipped_out": 30, "received_in": 150, "diff": 0}
    assert format_status_line("KB1-2", items, inv) == (
        "[KB1-2] BAC=70/20 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0"
    )


def test_status_line_kb2_5_invariant_unavailable() -> None:
    items = [_item(3, 60, 0, "snapshot"), _item(1, 20, 0, "snapshot"), _item(2, 35, 0, "offline")]
    assert format_status_line("KB2-5", items, None) == (
        "[KB2-5] BAC=20/20 TRUNG=35/35(offline) NAM=60/60 "
        "TRANSIT=0 SHIPPED_OUT=NA RECEIVED_IN=NA DIFF=NA"
    )


def test_status_line_prints(status_line, capsys) -> None:  # type: ignore[no-untyped-def]
    items = [_item(1, 100, 0, "snapshot"), _item(2, 40, 0, "snapshot"), _item(3, 10, 0, "snapshot")]
    inv = {"stock": 150, "in_transit": 0, "shipped_out": 0, "received_in": 150, "diff": 0}
    line = status_line("KB1-0", items, inv)
    assert capsys.readouterr().out.strip() == line
    assert line.startswith("[KB1-0] BAC=100/100 ")
