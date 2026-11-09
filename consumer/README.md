# Phần Ngọc Chung: CDC và đọc phân tán

Nguồn hợp đồng: spec F5, F7, F8; `docs/db.md` mục 2.4 và 3.4; `docs/demo.md` Kịch bản 2;
TC10–TC13 và M1 trong `docs/tests.md`.

## Code

- `processing.py`: kiểm payload và key Kafka, chặn trùng bằng `processed_event`, upsert
  `inventory_snapshot` chỉ khi version mới hơn, chuyển `pending_txn` sang APPLIED (kèm
  `applied_at`, `updated_at`) trong cùng transaction, rồi mới commit offset Kafka (db.md 3.4).
  Sự kiện sai định dạng được gửi DLQ và chờ broker xác nhận trước khi commit offset.
  Lỗi ghi CSDL/commit làm tiến trình thoát để phát lại. Lỗi poll tạm thời và topic chưa có
  chỉ ghi log rồi tiếp tục; lỗi quyền truy cập/fatal vẫn được báo ra.
- `main.py`: group `hq-consumer`, chỉ đọc `outbox.event.INVENTORY`, tắt auto commit và auto
  offset store; đóng kết nối khi nhận SIGTERM. Consumer không ghi `transfer_summary`.
  Mỗi snapshot được cập nhật ghi một dòng log JSON có `latency_ms` (dùng cho M1).
- `hq-api/routers/dashboard.py`: `/dashboard/inventory` (snapshot, realtime FDW từng kho
  riêng, offline, tạm tính), `/dc-status`, `/pending-txn`.
- `hq-api/routers/invariant.py`: `/invariant` đọc đủ ba kho, trả 503 khi thiếu kho.
- `dashboard/`: HTML/CSS/JS thuần; cập nhật mỗi 2 giây, đối soát, kiểm tra bất biến,
  tạo và hủy phiếu (TIMEOUT), xem giao dịch ghi hộ.

## Chạy

Consumer là service `hq-consumer` trong `infra/compose.hq.yml`, dựng cùng `make up-hq` và
`make reset`. Xem log: `make logs S=hq-consumer`.

## Kiểm thử

| Test | File | Loại |
| :---- | :---- | :---- |
| Consumer: commit offset sau CSDL, DLQ, key sai | `tests/consumer/test_consumer_processing.py` | unit (CI) |
| Router đọc: lỗi F5, SKU lạ | `tests/hq_api/test_dashboard_router.py` | unit (CI) |
| Script M1 | `tests/consumer/test_measure_m1.py` | unit (CI) |
| TC10 – đọc lại toàn bộ topic, snapshot và processed_event không đổi | `tests/consumer/test_tc10_consumer_replay.py` | tích hợp |
| TC11 – sự kiện version cũ không ghi đè (tùy chọn) | `tests/consumer/test_tc11_out_of_order.py` | tích hợp |
| TC12 – kho chết, realtime dưới 1 giây | `tests/hq_api/test_tc12_fdw_dead_dc.py` | tích hợp |
| TC13 – kho treo, realtime 3–5 giây (tùy chọn) | `tests/hq_api/test_tc13_fdw_hung_dc.py` | tích hợp |

Test tích hợp chạy trên hệ thống thật khi HQ và kho cùng một máy (`HOST_A_IP = HOST_B_IP`),
mỗi test tự gọi `make reset`: `make test` (bắt buộc) hoặc `make test-all` (cả tùy chọn).

## M1

`make bench-cdc` (máy A): gửi 200 lệnh xuất 1 thùng cách nhau 100 ms vào kho Trung (nhập trước
200 thùng), đọc log `hq-consumer`, in p50, p95, max (ms). Đổi tham số:
`make bench-cdc ARGS="DC=1 COUNT=200 OUT=m1.json"`. Script: `scripts/measure/m1_cdc.py`.
`latency_ms = synced_at − occurred_at` dùng đồng hồ của kho và HQ: đồng bộ NTP hai máy, hoặc đo
khi kho và HQ cùng một máy, và ghi rõ trong báo cáo. Report thiếu mẫu hoặc độ trễ âm sẽ báo lỗi.

## Bổ sung sau review PR #6

- Realtime không gọi FDW khi kho đã OFFLINE, kể cả đang có pending. Snapshot mode vẫn
  tính provisional cho OFFLINE/RECOVERING có giao dịch PENDING.
- TC12 đợi snapshot seed và trạng thái OFFLINE trước khi đo; sau test bật lại kho/connector.
- GET của Dashboard không gửi Content-Type; chỉ JSON POST mới thêm header này.
- Hồi quy poll error và transaction: `tests/consumer/test_main.py`, `test_projection.py`.
- Kết quả kiểm chứng và phạm vi tích hợp: [REVIEW_FIXES.md](REVIEW_FIXES.md).
