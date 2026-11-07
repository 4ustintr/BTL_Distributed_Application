"""Hằng số dùng chung. Mỗi hằng số ghi mục spec nguồn; không thêm tên ngoài docs/."""

from __future__ import annotations

from enum import StrEnum

# ---------------------------------------------------------------------------
# Biến môi trường (spec F2)
# ---------------------------------------------------------------------------
ENV_HOST_A_IP = "HOST_A_IP"  # F2 – chung
ENV_HOST_B_IP = "HOST_B_IP"  # F2 – chung
ENV_INTERNAL_KEY = "INTERNAL_KEY"  # F2 – chung, khóa cho lệnh gọi nội bộ HQ <-> kho
ENV_DC_ID = "DC_ID"  # F2 – Node API, WMS
ENV_NODE_DB_URL = "NODE_DB_URL"  # F2 – Node API
ENV_HQ_URL = "HQ_URL"  # F2 – Node API, WMS
ENV_LOCK_MODE = "LOCK_MODE"  # F2 – Node API
ENV_HQ_DB_URL = "HQ_DB_URL"  # F2 – HQ API và Consumer
ENV_NODE_URLS = "NODE_URLS"  # F2 – HQ API và Consumer, JSON {"1": url, "2": url, "3": url}
ENV_KAFKA_BOOTSTRAP = "KAFKA_BOOTSTRAP"  # F2 – HQ API và Consumer
ENV_NODE_URL = "NODE_URL"  # F2 – WMS


class LockMode(StrEnum):
    """Giá trị LOCK_MODE của Node API (F2)."""

    OPTIMISTIC = "optimistic"  # F2 – mặc định
    NONE = "none"  # F2 – chỉ dùng cho make race-unsafe


# ---------------------------------------------------------------------------
# Quy ước API chung (spec F5)
# ---------------------------------------------------------------------------
INTERNAL_KEY_HEADER = "X-Internal-Key"  # F5 – header của lệnh gọi nội bộ HQ <-> kho
CORS_ORIGIN_REGEX = r"^https?://[^/]+:300[0-3]$"  # F5, F2 – Dashboard 3000, WMS 3001–3003


class ErrorCode(StrEnum):
    """error_code theo bảng F5."""

    VALIDATION_ERROR = "VALIDATION_ERROR"  # F5 – 400
    UNAUTHORIZED = "UNAUTHORIZED"  # F5 – 401
    NOT_FOUND = "NOT_FOUND"  # F5 – 404
    VERSION_CONFLICT = "VERSION_CONFLICT"  # F5 – 409
    INVALID_STATE = "INVALID_STATE"  # F5 – 409
    DC_NOT_OFFLINE = "DC_NOT_OFFLINE"  # F5 – 409
    INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK"  # F5 – 422
    DC_RECOVERING = "DC_RECOVERING"  # F5 – 503
    DC_UNREACHABLE = "DC_UNREACHABLE"  # F5 – 503
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"  # F5 – 504


# HTTP status của từng error_code (bảng F5)
ERROR_HTTP_STATUS: dict[ErrorCode, int] = {
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

# ---------------------------------------------------------------------------
# Kafka (spec F8, db.md 2.11, 3.4)
# ---------------------------------------------------------------------------
TOPIC_INVENTORY = "outbox.event.INVENTORY"  # F8
TOPIC_TRANSFER = "outbox.event.TRANSFER"  # F8 – Consumer bỏ qua, chỉ để quan sát
TOPIC_DLQ = "outbox.event.DLQ"  # F8, db.md 3.4 bước 6
TOPIC_PARTITIONS = 3  # F8 – INVENTORY và TRANSFER có 3 partition
CONSUMER_GROUP = "hq-consumer"  # F8, db.md 3.4 bước 1


class AggregateType(StrEnum):
    """outbox_event.aggregate_type, dùng để định tuyến topic (db.md 2.11, F4)."""

    INVENTORY = "INVENTORY"  # db.md 2.11
    TRANSFER = "TRANSFER"  # db.md 2.11


class EventType(StrEnum):
    """outbox_event.event_type (F8, db.md 2.11)."""

    INVENTORY_CHANGED = "InventoryChanged"  # F8
    INVENTORY_CHANGED_DIRECT = "InventoryChangedDirect"  # F8 – dự phòng, bản nộp chưa sinh
    TRANSFER_CREATED = "TransferCreated"  # F8
    TRANSFER_SHIPPED = "TransferShipped"  # F8
    TRANSFER_RECEIVED = "TransferReceived"  # F8
    TRANSFER_CANCELLED = "TransferCancelled"  # F8


# ---------------------------------------------------------------------------
# Trạng thái (spec F9, db.md)
# ---------------------------------------------------------------------------
class TransferStatus(StrEnum):
    """transfer_summary.status, transfer_order.status, transfer_event.to_status (F9, db.md 2.5)."""

    CREATED = "CREATED"  # F9, db.md 2.5, 2.9
    SHIPPED = "SHIPPED"  # F9, db.md 2.5, 2.9
    RECEIVED = "RECEIVED"  # F9, db.md 2.5, 2.9
    CANCELLED = "CANCELLED"  # F9, db.md 2.5, 2.9


# Trạng thái liền trước hợp lệ của từng trạng thái phiếu (F9; db.md 3.2 quy tắc 2)
TRANSFER_PREV_STATUS: dict[TransferStatus, TransferStatus] = {
    TransferStatus.SHIPPED: TransferStatus.CREATED,
    TransferStatus.RECEIVED: TransferStatus.SHIPPED,
    TransferStatus.CANCELLED: TransferStatus.CREATED,
}

# Thứ tự tiến của /sync-status: CREATED < SHIPPED < RECEIVED (F6 /sync-status)
TRANSFER_PROGRESS_ORDER: tuple[TransferStatus, ...] = (
    TransferStatus.CREATED,
    TransferStatus.SHIPPED,
    TransferStatus.RECEIVED,
)


class TransferDirection(StrEnum):
    """transfer_order.direction (db.md 2.9; F6 /transfers/notice)."""

    OUTBOUND = "OUTBOUND"  # db.md 2.9 – node là from_dc
    INBOUND = "INBOUND"  # db.md 2.9 – node là to_dc


class CancelReason(StrEnum):
    """cancel_reason (db.md 2.5, 2.9; F10.f, F10.g)."""

    DECLINED = "DECLINED"  # db.md 2.5 – kho đích từ chối
    INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK"  # db.md 2.5 – kho nguồn không giữ chỗ được
    TIMEOUT = "TIMEOUT"  # db.md 2.5 – nút Hủy phiếu hoặc lỗi mạng khi tạo phiếu


class TransferEventSource(StrEnum):
    """transfer_event.source (db.md 2.13)."""

    HQ = "HQ"  # db.md 2.13
    DC1 = "DC1"  # db.md 2.13
    DC2 = "DC2"  # db.md 2.13
    DC3 = "DC3"  # db.md 2.13

    @classmethod
    def for_dc(cls, dc_id: int) -> TransferEventSource:
        """Nguồn ứng với kho dc_id (1, 2, 3)."""
        return cls(f"DC{dc_id}")


class DcStatus(StrEnum):
    """dc_status.status ở HQ (F9, db.md 2.14)."""

    ONLINE = "ONLINE"  # F9
    OFFLINE = "OFFLINE"  # F9
    RECOVERING = "RECOVERING"  # F9 – giá trị mặc định của cột (db.md 2.14)


class NodeMode(StrEnum):
    """Chế độ của Node API, trả trong GET /health và dc_status.node_mode (F6, F9, db.md 2.14)."""

    ONLINE = "ONLINE"  # F9
    RECOVERING = "RECOVERING"  # F9 – luôn là chế độ khi khởi động


NODE_START_MODE = NodeMode.RECOVERING  # F9 – (khởi động) -> RECOVERING, luôn luôn


class PendingTxnStatus(StrEnum):
    """pending_txn.status (F9, db.md 2.12)."""

    PENDING = "PENDING"  # F9, db.md 2.12
    APPLIED = "APPLIED"  # F9, db.md 2.12
    REJECTED = "REJECTED"  # F9, db.md 2.12


class TxnType(StrEnum):
    """pending_txn.txn_type và trường txn_type của /replay (db.md 2.12, F6)."""

    INBOUND = "INBOUND"  # db.md 2.12
    OUTBOUND = "OUTBOUND"  # db.md 2.12


class WmsMode(StrEnum):
    """Chế độ của WMS (F9). WMS là JavaScript; giữ ở đây để đối chiếu."""

    NORMAL = "NORMAL"  # F9
    FAILOVER = "FAILOVER"  # F9
    WAITING = "WAITING"  # F9


class MovementType(StrEnum):
    """stock_movement.movement_type (db.md 2.8)."""

    INBOUND = "INBOUND"  # db.md 2.8
    OUTBOUND = "OUTBOUND"  # db.md 2.8
    TRANSFER_OUT = "TRANSFER_OUT"  # db.md 2.8, F10.c
    TRANSFER_IN = "TRANSFER_IN"  # db.md 2.8, F10.d
    ADJUST = "ADJUST"  # db.md 2.8, 3.3


class RefType(StrEnum):
    """stock_movement.ref_type (db.md 2.8; F6 /inbound, /outbound; F8 payload)."""

    ISSUE = "ISSUE"  # db.md 2.8 – xuất cho đại lý
    RECEIPT = "RECEIPT"  # db.md 2.8 – nhập từ nhà cung cấp
    TRANSFER = "TRANSFER"  # db.md 2.8 – điều chuyển
    REPLAY = "REPLAY"  # db.md 2.8 – giao dịch HQ ghi hộ
    STOCKTAKE = "STOCKTAKE"  # db.md 2.8 – kiểm kê
    DIRECT = "DIRECT"  # db.md 2.8 – dự phòng cho ghi nhận sửa tay


class InventorySource(StrEnum):
    """Trường source của GET /dashboard/inventory (F7, db.md 2.4)."""

    SNAPSHOT = "snapshot"  # F7
    REALTIME = "realtime"  # F7
    OFFLINE = "offline"  # F7
    PROVISIONAL = "provisional"  # F7


class DashboardMode(StrEnum):
    """Tham số mode của GET /dashboard/inventory (F7)."""

    SNAPSHOT = "snapshot"  # F7
    REALTIME = "realtime"  # F7


HQ_HEALTH_OK = "ok"  # F7 – GET /health của HQ trả {"status": "ok"}
NODE_DB_OK = "ok"  # F6 – GET /health của kho trả "db": "ok"

# ---------------------------------------------------------------------------
# Danh mục kho (db.md 2.1, F12)
# ---------------------------------------------------------------------------
DC_IDS: tuple[int, ...] = (1, 2, 3)  # db.md 2.1 – 1 = Bắc, 2 = Trung, 3 = Nam


class Region(StrEnum):
    """dc.region (db.md 2.1)."""

    BAC = "BAC"  # db.md 2.1
    TRUNG = "TRUNG"  # db.md 2.1
    NAM = "NAM"  # db.md 2.1


DC_REGION: dict[int, Region] = {1: Region.BAC, 2: Region.TRUNG, 3: Region.NAM}  # db.md 2.1, F12

# ---------------------------------------------------------------------------
# Quy trình ở kho (spec F10)
# ---------------------------------------------------------------------------
SQL_SET_APP_SOURCE = "SET LOCAL app.source = 'api'"  # F10 – lệnh đầu tiên của mọi transaction ghi
TRANSFER_CODE_PREFIX = "TR"  # F10.f, db.md 3.1 – 'TR-' + năm + '-' + 6 chữ số
TRANSFER_CODE_SEQ_DIGITS = 6  # F10.f – số thứ tự 6 chữ số từ transfer_code_seq

# ---------------------------------------------------------------------------
# Tham số cấu hình (spec F11). Đơn vị giây trừ khi ghi khác.
# ---------------------------------------------------------------------------
HEALTH_CHECK_INTERVAL_S = 2.0  # F11, F10.j – chu kỳ health check của HQ
HEALTH_CHECK_TIMEOUT_S = 1.0  # F11, F10.j – timeout GET /health
OFFLINE_MISSED_THRESHOLD = 3  # F11, F9 – số lần lỗi liên tiếp để coi là OFFLINE
WMS_HEALTH_INTERVAL_S = 2.0  # F11 – chu kỳ WMS kiểm tra Node API (WMS là JS, giữ để đối chiếu)
WMS_FAILOVER_MISSED = 2  # F11, F9 – số lần lỗi để WMS chuyển FAILOVER (WMS là JS)
FDW_CONNECT_TIMEOUT_S = 3  # F11, F3 – connect_timeout của foreign server
FDW_QUERY_TIMEOUT_S = 5.0  # F11, F3 – timeout mỗi truy vấn FDW ở tầng ứng dụng
HTTP_TIMEOUT_S = 2.0  # F11, F5 – timeout lệnh gọi giữa các service
HTTP_MAX_RETRIES = 4  # F11, F5 – thử lại tối đa sau lần gọi đầu: tổng tối đa 5 lần gọi
HTTP_RETRY_INTERVAL_S = 2.0  # F11, F5 – khoảng cách giữa hai lần thử
VERSION_CONFLICT_MAX_RETRIES = 3  # F11, F10.a, db.md 2.7 – thử lại khi xung đột version
RECONCILE_INTERVAL_S = 60.0  # F11, F10.k – chu kỳ đối soát (tùy chọn)
REPLAY_ROUND_MAX_S = 30.0  # F11 – thời gian tối đa một vòng Replay Worker
REPLAY_INTERVAL_S = 2.0  # F11, F10.i – chu kỳ Replay Worker
RECOVERING_WARN_AFTER_S = 30.0  # F11 – kho ở RECOVERING quá lâu thì ghi cảnh báo vào log
CONNECTOR_ERRORS_MAX_RETRIES = 3  # F11, F4 – errors.max.retries của connector (JSON connector)
