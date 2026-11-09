"""Đối chiếu common/constants.py với spec F5, F8, F9, F11 và db.md."""

from common import constants as c
from common.constants import ErrorCode


def test_error_http_status_matches_f5_table() -> None:
    assert c.ERROR_HTTP_STATUS == {
        ErrorCode.VALIDATION_ERROR: 400,
        ErrorCode.UNAUTHORIZED: 401,
        ErrorCode.NOT_FOUND: 404,
        ErrorCode.VERSION_CONFLICT: 409,
        ErrorCode.INVALID_STATE: 409,
        ErrorCode.DC_NOT_OFFLINE: 409,
        ErrorCode.INSUFFICIENT_STOCK: 422,
        ErrorCode.DC_RECOVERING: 503,
        ErrorCode.DC_UNREACHABLE: 503,
        ErrorCode.UPSTREAM_TIMEOUT: 504,
    }
    assert set(c.ERROR_HTTP_STATUS) == set(ErrorCode)


def test_error_code_value_equals_name() -> None:
    for code in ErrorCode:
        assert code.value == code.name


def test_topics_and_group_f8() -> None:
    assert c.TOPIC_INVENTORY == "outbox.event.INVENTORY"
    assert c.TOPIC_TRANSFER == "outbox.event.TRANSFER"
    assert c.TOPIC_DLQ == "outbox.event.DLQ"
    assert c.CONSUMER_GROUP == "hq-consumer"
    # Debezium định tuyến outbox.event.${aggregate_type} (F4)
    assert {f"outbox.event.{t}" for t in c.AggregateType} == {c.TOPIC_INVENTORY, c.TOPIC_TRANSFER}


def test_event_types_match_outbox_check_db_2_11() -> None:
    assert [e.value for e in c.EventType] == [
        "InventoryChanged",
        "InventoryChangedDirect",
        "TransferCreated",
        "TransferShipped",
        "TransferReceived",
        "TransferCancelled",
    ]


def test_status_values_match_db_checks() -> None:
    assert [s.value for s in c.TransferStatus] == ["CREATED", "SHIPPED", "RECEIVED", "CANCELLED"]
    assert [s.value for s in c.DcStatus] == ["ONLINE", "OFFLINE", "RECOVERING"]
    assert [s.value for s in c.NodeMode] == ["ONLINE", "RECOVERING"]
    assert [s.value for s in c.PendingTxnStatus] == ["PENDING", "APPLIED", "REJECTED"]
    assert [s.value for s in c.TxnType] == ["INBOUND", "OUTBOUND"]
    assert [s.value for s in c.TransferDirection] == ["OUTBOUND", "INBOUND"]
    assert [s.value for s in c.CancelReason] == ["DECLINED", "INSUFFICIENT_STOCK", "TIMEOUT"]
    assert [s.value for s in c.MovementType] == [
        "INBOUND",
        "OUTBOUND",
        "TRANSFER_OUT",
        "TRANSFER_IN",
        "ADJUST",
    ]
    assert [s.value for s in c.RefType] == [
        "ISSUE",
        "RECEIPT",
        "TRANSFER",
        "REPLAY",
        "STOCKTAKE",
        "DIRECT",
    ]
    assert [s.value for s in c.InventorySource] == [
        "snapshot",
        "realtime",
        "offline",
        "provisional",
    ]
    assert [s.value for s in c.TransferEventSource] == ["HQ", "DC1", "DC2", "DC3"]


def test_transfer_state_machine_f9() -> None:
    S = c.TransferStatus
    assert c.TRANSFER_PREV_STATUS == {
        S.SHIPPED: S.CREATED,
        S.RECEIVED: S.SHIPPED,
        S.CANCELLED: S.CREATED,
    }
    assert c.TRANSFER_PROGRESS_ORDER == (S.CREATED, S.SHIPPED, S.RECEIVED)


def test_node_starts_recovering_f9() -> None:
    assert c.NODE_START_MODE is c.NodeMode.RECOVERING


def test_transfer_event_source_for_dc() -> None:
    assert c.TransferEventSource.for_dc(2) is c.TransferEventSource.DC2


def test_f11_parameters() -> None:
    assert (c.HEALTH_CHECK_INTERVAL_S, c.HEALTH_CHECK_TIMEOUT_S) == (2, 1)
    assert c.OFFLINE_MISSED_THRESHOLD == 3
    assert (c.WMS_HEALTH_INTERVAL_S, c.WMS_FAILOVER_MISSED) == (2, 2)
    assert c.WMS_HEALTH_TIMEOUT_S == 1.0  # F11: timeout health check của WMS
    assert c.FDW_CONNECT_TIMEOUT_S == 3
    assert c.FDW_QUERY_TIMEOUT_S == 5
    assert (c.HTTP_TIMEOUT_S, c.HTTP_MAX_RETRIES, c.HTTP_RETRY_INTERVAL_S) == (2, 4, 2)
    assert c.VERSION_CONFLICT_MAX_RETRIES == 3
    assert c.RECONCILE_INTERVAL_S == 60
    assert c.REPLAY_ROUND_MAX_S == 30
    assert c.REPLAY_INTERVAL_S == 2
    assert c.RECOVERING_WARN_AFTER_S == 30
    assert c.CONNECTOR_ERRORS_MAX_RETRIES == 3


def test_env_names_f2() -> None:
    assert {
        c.ENV_HOST_A_IP,
        c.ENV_HOST_B_IP,
        c.ENV_INTERNAL_KEY,
        c.ENV_POSTGRES_PASSWORD,
        c.ENV_DC_ID,
        c.ENV_NODE_DB_URL,
        c.ENV_HQ_URL,
        c.ENV_LOCK_MODE,
        c.ENV_HQ_DB_URL,
        c.ENV_NODE_URLS,
        c.ENV_KAFKA_BOOTSTRAP,
        c.ENV_NODE_URL,
    } == {
        "HOST_A_IP",
        "HOST_B_IP",
        "INTERNAL_KEY",
        "POSTGRES_PASSWORD",
        "DC_ID",
        "NODE_DB_URL",
        "HQ_URL",
        "LOCK_MODE",
        "HQ_DB_URL",
        "NODE_URLS",
        "KAFKA_BOOTSTRAP",
        "NODE_URL",
    }
    assert [m.value for m in c.LockMode] == ["optimistic", "none"]


def test_cors_regex_allows_ports_3000_to_3003_only() -> None:
    import re

    rx = re.compile(c.CORS_ORIGIN_REGEX)
    for port in (3000, 3001, 3002, 3003):
        assert rx.match(f"http://192.168.1.20:{port}")
    assert rx.match("http://localhost:3000")
    for bad in ("http://192.168.1.20:3004", "http://192.168.1.20:8000", "http://evil:30001"):
        assert not rx.match(bad)
