# Kiểm tra và bàn giao — Ngọc Chung

Ngày: 08/11/2026. Base: `24ca53e`. Nhánh: `chung/cdc/consumer-dashboard`.
Python 3.11 trong Docker; PostgreSQL 16; Kafka 3.8.1; Debezium 2.7.3.Final.
Không sửa `common/`, `db/`, `docs/`, `infra/`, `scripts/`, main.py hoặc code nghiệp vụ của người khác.

## Kết quả đã chạy

| Kiểm tra | Kết quả |
|---|---|
| Ruff toàn repo; format các file Python đã thêm/sửa | Đạt |
| tests/common | 41 đạt |
| tests/node_api | 4 đạt |
| tests/hq_api | 14 đạt; 2 probe sự cố skip trong lượt bình thường |
| tests/consumer | 28 đạt |
| Tổng lượt pytest đầy đủ | **87 đạt**, 2 skip có chủ đích |
| TC12, kill PostgreSQL test bằng SIGKILL | Đạt; API realtime **0.055 giây**, giữ kết quả Bắc/Nam |
| TC13, pause PostgreSQL test thật | Đạt; **5.039 giây** kể cả hủy truy vấn; giữ kết quả Bắc/Nam |
| Node built-in tests | 3 đạt |
| Cú pháp module JavaScript | Đạt |
| nginx HTML/CSS/JS/config.js và API đọc | HTTP 200, MIME chính xác |
| Build Consumer và compose overlay | Đạt |
| Consumer image khởi động, SIGTERM sau khi subscribe | Exit code 0 |
| git diff --check | Đạt |

TC10 dùng Kafka thật: phát sự kiện, chờ commit, dừng Consumer, reset offset về 0,
chạy lại rồi so nguyên snapshot và số dòng processed_event. Payload lỗi đi vào DLQ;
sự kiện hợp lệ sau đó vẫn được xử lý. Test khác đi qua WAL/Debezium thật.
TC11 kiểm tra version nhỏ hơn **và bằng** last_version. Các test còn kiểm tra cạnh tranh
nhận trùng, rollback khi CSDL lỗi, APPLIED kể cả khi sự kiện replay đã cũ,
và công thức provisional chỉ cộng/trừ dòng PENDING.

Các warning pytest hiện tại đến từ bộ TestClient nền với Starlette/httpx;
không thêm dependency ngoài danh sách được phép để loại warning này.

## M1 đã đo

200 transaction nguồn, khoảng cách 100 ms, một host Docker:

| p50 | p95 | max |
|---|---|---|
| 520.270 ms | 779.491 ms | 858.517 ms |

Chi tiết: `validation/M1-local.json`. Nguồn là emitter SQL trong test; đường
PostgreSQL → WAL → Debezium → Kafka → Consumer → HQ là thật.
Kết quả này chưa đại diện cho Node API hoặc hai máy demo. Công cụ
`python -m consumer.benchmark send/report` đã có để đo lại khi nhóm ghép xong.

## Tự duyệt checklist PR

| Ý trong mẫu PR | Đánh giá và bằng chứng |
|---|---|
| Đúng tên/hằng số/model theo spec | Đạt: processing.py dùng common.constants/models/ids; hai router dùng model F7 và enum nguồn/trạng thái |
| Thứ tự giao dịch ghi kho, chống trùng request trước trừ kho | Không áp dụng: phần này không cung cấp nghiệp vụ ghi kho |
| Outbox cùng transaction, không HTTP trong transaction | Không áp dụng cho nghiệp vụ kho; production Consumer không gọi HTTP |
| UPDATE inventory có version/khả dụng, không tự tăng version | Không áp dụng: production chỉ đọc inventory qua FDW |
| Trạng thái có điều kiện trạng thái liền trước | Đạt: InventoryStore.apply chỉ đổi PENDING → APPLIED |
| Consumer processed_event, last_version, commit offset sau CSDL | Đạt: InventoryStore.apply và handle_message; được xác minh bằng PostgreSQL/Kafka thật |
| Gọi service qua client common | Không áp dụng: Consumer dùng Kafka/PG, router đọc PG; benchmark là công cụ gọi API công khai của WMS |
| Test tương ứng xanh | Đạt trong bộ tích hợp riêng: TC10, TC11, TC12, TC13 |

Không thêm bảng, endpoint, error_code, topic, payload field hoặc biến môi trường production.
`consumer/compose.yml` là overlay để trưởng nhóm ghép service; chưa thay Makefile hay compose chính.

## Mô tả PR có thể sử dụng

**Tiêu đề:** feat(cdc): implement inventory projection and hq monitoring

**Mục spec:** F5, F7, F8; db.md 2.4/3.4; TC10–TC13; M1.

Consumer đưa tồn kho từ Kafka vào snapshot HQ bằng giao dịch chống trùng và so version.
HQ có API snapshot/realtime, fallback riêng từng kho, số provisional và kiểm tra bảo toàn hàng.
Dashboard hiển thị nguồn và độ mới; có luồng tạo/hủy phiếu theo API của Saga.

**Kiểm chứng:** 87 pytest và 3 test JavaScript đạt; 2 probe kill/pause container thật đạt;
luồng CDC qua Debezium/Kafka thật đạt; Docker image build và shutdown đạt.

**Giới hạn bàn giao:** Node API nghiệp vụ, Saga và Health/Replay của các thành viên khác
chưa hiện thực trên base này; chưa chạy trọn ba kịch bản trên hai máy.
Kiểm tra trực quan bị công cụ chặn quyền dùng Microsoft Edge; chỉ xác nhận được
logic JS, cú pháp, file tĩnh, MIME và HTTP. Không tuyên bố UI đã qua kiểm tra trực quan.

Hướng dẫn chạy, lệnh đo và phần cần ghép: `consumer/README.md`.
