# CDC và đọc phân tán — Ngọc Chung

Hợp đồng: F5, F7, F8; db.md 2.4/3.4; TC10–TC13 và M1.

## Chạy cùng hệ thống nhóm

Dùng Git Bash, Python 3.11 và Docker Desktop theo README ở gốc. Đặt `.env` theo
`infra/README.md`. Makefile ghép `consumer/compose.yml` vào compose HQ nên các lệnh
`make up-hq`, `make reset`, `make down-hq` quản lý cả hq-consumer.

```bash
make reset
make lint
make test-ci
make test-js
```

Consumer chỉ subscribe `outbox.event.INVENTORY`, group `hq-consumer`, tắt auto commit
và auto offset store. Topic chưa có hoặc lỗi poll tạm thời chỉ được ghi log rồi tiếp tục
poll. Lỗi quyền truy cập/fatal, lỗi ghi CSDL và lỗi commit vẫn được báo ra.
Snapshot, processed_event và APPLIED cùng transaction; commit offset sau commit CSDL.

## TC bắt buộc trên stack chung

```bash
pytest tests/consumer/test_tc10_redelivery.py -q -s
pytest tests/hq_api/test_tc12_offline.py -q -s
```

Hai TC đều gọi `make reset` và dùng cấu hình từ `tests/fixtures.py`/`.env`.
TC10 chèn outbox ở pg-dc1, đợi CDC, dừng hq-consumer, reset offset Kafka về earliest,
khởi động lại rồi so nguyên snapshot và số dòng processed_event.
TC12 kill pg-dc2/node-api-dc2, đợi Health Monitor đánh dấu OFFLINE, rồi đo realtime
phải dưới 1 giây, Bắc/Nam realtime và Trung offline. Kho và connector được bật lại
trong finally.

Không có điều kiện skip theo hostname riêng. Thiếu service/configuration hoặc reset lỗi
thì TC thất bại. Chỉ quy tắc toàn nhóm trong tests/conftest.py được dùng để skip tích hợp
khi HOST_A_IP khác HOST_B_IP. Chạy từng TC, không chạy song song vì make reset xóa dữ liệu.

## TC tùy chọn

```bash
pytest tests/consumer/test_tc11_old_version.py -q -s
pytest tests/hq_api/test_tc13_timeout.py -q -s
```

TC11 phát version mới rồi version cũ qua Kafka; snapshot không bị ghi đè.
TC13 pause kho thật, yêu cầu phản hồi khoảng 3–5 giây, giữ số hai kho còn lại,
rồi unpause trong finally. Có dung sai 0.5 giây cho hủy truy vấn sau deadline F11.
Test bổ sung cho startup, transaction, parser và API dùng tên thành phần, không mượn ID TC.

`tests/consumer/stack_helpers.py` chỉ điều khiển stack trong `infra/`, không tạo schema,
role, connector hoặc một cụm Docker riêng. `tests/consumer/Dockerfile` chỉ là image công cụ
Python/Node/make/Docker CLI nếu máy chưa có tool; không chứa stack CSDL/Kafka.

## Dashboard và số liệu

HTML/CSS/JS thuần, config.js do nginx sinh, polling snapshot mỗi 2 giây. GET không
có Content-Type nên tránh preflight CORS không cần thiết; JSON POST có header này.
Bấm Đối soát: mỗi kho một truy vấn FDW riêng có timeout. Kho đã OFFLINE trả snapshot
với source=offline ngay, kể cả có pending; snapshot mode vẫn có số provisional cho
OFFLINE/RECOVERING có giao dịch PENDING. Kết quả đối soát được giữ 15 giây.

Dashboard còn gọi API phiếu của Minh Hiếu; phần tạo/hủy phiếu cần API Saga đã ghép.
API/worker của người khác không được mô phỏng trong production.

## M1

Consumer ghi một dòng JSON cho mỗi snapshot thực sự cập nhật, gồm request_id,
occurred_at, synced_at và latency_ms. Sắp sự kiện bằng version, không dùng timestamp.

```bash
python -m consumer.benchmark send --node-url http://HOST_B_IP:8002 --prepare-stock --output requests.json
docker logs hq-consumer > consumer.log 2>&1
python -m consumer.benchmark report --requests requests.json --log consumer.log --output cdc-results.json
```

Mặc định 200 lệnh xuất, cách nhau 100 ms. --prepare-stock nhập thêm 200 thùng qua API.
Report kiểm tra đủ mẫu và độ trễ âm; đồng bộ đồng hồ hai máy trước khi đo.
PowerShell 5 xuất log UTF-8 bằng `docker logs hq-consumer 2>&1 | Out-File consumer.log -Encoding utf8`.
`validation/M1-local.json` là kết quả lịch sử ngày 08/11 dùng SQL emitter; không coi đó
là phép đo mới trên stack demo. Kết quả kiểm chứng review ở VALIDATION.md.