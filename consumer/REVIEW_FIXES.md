# Kiểm chứng sau review PR #6 — 09/11/2026

PR #6 đã được owner merge trong lúc sửa. Nhánh đã ghép các bản cập nhật master:
service Consumer trong compose chính, M1 trong scripts/measure, tên test chuẩn,
APPLIED cập nhật updated_at. Không đưa lại compose/schema/role/connector của stack riêng.

## Bổ sung còn lại

1. Poll lỗi topic chưa tồn tại, lỗi kết nối và broker tạm thời: log rồi tiếp tục,
   không commit offset và không gửi DLQ cho lỗi poll. Xử lý cả KafkaException từ poll.
   Lỗi fatal/quyền truy cập và lỗi ghi CSDL/commit vẫn lan ra để tránh bỏ qua dữ liệu.
2. Realtime không gọi FDW cho kho OFFLINE. Kho OFFLINE/RECOVERING còn pending hiện tạm tính ở
   mọi chế độ (db.md 2.4; Minh Danh chỉnh khi merge), không pending thì snapshot/offline. Có test
   chứng minh không gọi FDW ở cả hai trường hợp.
3. Dashboard chỉ đặt Content-Type khi có body; test-js kiểm tra GET và POST.
4. TC10 so cả synced_at sau reset offset. TC12 chờ snapshot seed và OFFLINE trước khi
   đo dưới 1 giây; finally phục hồi cả kho lẫn connector.

## Kết quả đã chạy

| Lệnh / kiểm tra | Kết quả |
|---|---|
| make lint | Đạt |
| make test-ci | 204 passed; 2 skipped thuộc Node API chưa có reserve/release trên master |
| make test-js | 29 passed, gồm 4 test Dashboard |
| TC10 Consumer replay | Passed trên Kafka/Consumer thật; không skip |
| TC11 sự kiện cũ | Passed trên stack chung; không skip |
| TC12 FDW, kho đã OFFLINE | Passed; realtime 0.055 giây; không skip |
| TC13 kho pause | Passed; timeout trong 3–5.5 giây; không skip |
| Consumer restart sau make reset | 0 |
| make bench-cdc, 200 lệnh Node API thật | Đủ 200 mẫu; p50 525.403 ms, p95 903.040 ms, max 990.435 ms |
| git diff --check | Đạt |

Lệnh TC đã chạy cùng nhau:

```bash
pytest tests/consumer/test_tc10_consumer_replay.py tests/consumer/test_tc11_out_of_order.py \
  tests/hq_api/test_tc12_fdw_dead_dc.py tests/hq_api/test_tc13_fdw_hung_dc.py -q -s
```

Mỗi TC gọi make reset, dùng chính infra/compose.hq.yml và infra/compose.dc.yml.
Không còn kiểm tra hostname riêng để skip. Quy tắc HOST_A_IP = HOST_B_IP của nhóm vẫn áp dụng.
Hai unit test Node API bị skip là test_transfer_cancel.py và test_transfer_notice.py,
do phần services/stock.py trên master chưa ghép đủ; không phải TC10/TC12 bị bỏ qua.

## Môi trường và giới hạn

Để tránh cổng 8000/8080 và container Kafka của dự án khác đang có trên máy, lượt tích hợp
dùng overlay cổng/tên và định tuyến TCP cục bộ ngoài repo. CSDL, migration, replication,
FDW, Kafka, Debezium, connector và API đều là stack infra chuẩn của nhóm.

Master lúc kiểm tra chưa ghép đủ Node API nhập/xuất và recovery. Worktree kiểm thử ghép
thêm refs của Tuấn Minh `66a413a` (transfer-out và các phụ thuộc nhập/xuất/replay) và
`ba1cf85` (recovery). Các thay đổi Node API đó không được đưa vào nhánh Chung.
Kết quả M1 là một host Docker, qua Node API thật, không dùng SQL emitter;
chưa thay cho số đo hai máy demo. JSON: scripts/measure/results/m1-2026-11-09.json.

Chưa xác nhận trọn các kịch bản Saga trên hai máy; kiểm tra trực quan Dashboard vẫn là
bước riêng. Bản sửa này chỉ báo các kiểm tra thực tế nêu trong bảng.
