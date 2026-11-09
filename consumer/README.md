# Phần Ngọc Chung: CDC và đọc phân tán

Nhánh: `chung/cdc/consumer-dashboard`. Nguồn hợp đồng: spec F5, F7, F8;
`docs/db.md` mục 2.4 và 3.4; `docs/demo.md`; TC10–TC13, M1 trong `docs/tests.md`.

## Code

- `processing.py`: xác thực payload/key, chống trùng bằng `processed_event`, upsert snapshot
  theo version, chuyển pending sang APPLIED cùng transaction, rồi mới commit offset Kafka.
  Sự kiện lỗi định dạng được gửi DLQ và chờ broker xác nhận trước khi commit offset.
  Lỗi CSDL/broker làm tiến trình thoát; restart policy phát lại sự kiện chưa xác nhận.
- `main.py`: group `hq-consumer`, chỉ đọc INVENTORY, tắt auto commit và auto offset store;
  đóng kết nối khi nhận SIGTERM. Consumer không ghi `transfer_summary`.
- `hq-api/routers/dashboard.py`: snapshot, realtime FDW song song, offline fallback,
  số tạm tính, trạng thái kho và danh sách giao dịch ghi hộ.
- `hq-api/routers/invariant.py`: đọc đủ ba kho, tính bảo toàn hàng; trả 503 khi thiếu kho.
- `dashboard/`: HTML/CSS/JS thuần, dùng config.js do nginx sinh; cập nhật mỗi 2 giây,
  đối soát, kiểm tra bất biến, tạo/hủy phiếu và xem PENDING/APPLIED/REJECTED.
  Kết quả đối soát được giữ 15 giây; nút Làm mới quay về snapshot ngay.

## Chạy Consumer cùng hạ tầng nhóm

Tại gốc repo, sau khi có `.env` và PostgreSQL/Kafka của nhóm:

```bash
docker compose --env-file .env -f infra/compose.hq.yml -f consumer/compose.yml up -d --build hq-consumer
docker logs -f hq-consumer
```

Overlay cung cấp service còn thiếu mà không sửa `infra/compose.hq.yml`.
Khi trưởng nhóm tích hợp, ghép service này vào compose chính và nối Makefile với các lệnh M1.
HQ main.py đã include hai router; không cần sửa file đó.

## Kiểm thử riêng, không dùng dữ liệu demo

Chỉ yêu cầu Docker Desktop. Python 3.11 và thư viện chạy trong container.
Không có thư viện mới ngoài requirements hiện có. Các lệnh sau chạy từ gốc repo:

```bash
docker compose -f tests/consumer/compose.integration.yml up -d --wait
docker compose -f tests/consumer/compose.integration.yml build chung-runner
docker compose -f tests/consumer/compose.integration.yml run --rm --use-aliases chung-runner
```

Lệnh cuối chạy ruff và cả bốn bộ pytest theo thứ tự riêng để tránh trùng module `routers`.
Các fixture chỉ reset dữ liệu khi `HQ_DB_URL` trỏ đến `chung-hq` của bộ test này.
Không chạy nhiều bộ tích hợp đồng thời vì chúng cùng khởi tạo lại dữ liệu và topic test.

Chỉ chạy test không cần hạ tầng:

```bash
docker compose -f tests/consumer/compose.integration.yml run --rm chung-runner sh tests/consumer/run-tests.sh -m "not integration"
```

Test giao diện dùng Node có sẵn các module kiểm thử chuẩn, không npm/build:

```bash
docker run --rm -v "${PWD}:/workspace" -w /workspace node:20-alpine node --experimental-default-type=module --test tests/dashboard/test_dashboard.mjs
```

Hai probe kill/pause thật được skip khi không bật sự cố. Sau lượt test đầy đủ,
chạy trong PowerShell để tạo đúng sự cố trên kho test và tự bật lại:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/consumer/verify-faults.ps1
```

Port `127.0.0.1:15434` chỉ dành cho probe này. Nó mô phỏng đúng host máy B vẫn sống
trong khi container kho bị kill; dùng DNS của container đã dừng sẽ đo thêm lỗi DNS.
TC13 chấp nhận 0.5 giây sai số cho thời gian hủy truy vấn sau deadline 5 giây.

## Mở Dashboard kiểm tra

Sau khi build runner, chạy test sau để đưa bộ test về tồn ban đầu 100/40/10:

```bash
docker compose -f tests/consumer/compose.integration.yml run --rm chung-runner python -m pytest tests/hq_api/test_tc12_database.py::test_tc12_live_stock_and_invariant -q
docker compose -f tests/consumer/compose.integration.yml --profile preview up -d chung-api chung-dashboard
```

Mở http://localhost:13000. Đây là HQ API thật với hai router của Chung, trên CSDL test.
Phần API phiếu của Hiếu chưa hiện thực nên danh sách/tạo/hủy phiếu chưa chạy trọn;
Dashboard hiển thị lỗi và giữ các phần khác hoạt động. Không có API giả cho phần đó.

## M1

Consumer ghi một dòng JSON cho mỗi snapshot thực sự được cập nhật, gồm event_id,
request_id, occurred_at, synced_at và latency_ms. Không dùng timestamp để sắp sự kiện.
Chạy ở môi trường đo riêng và đồng bộ đồng hồ hai máy trước khi lấy kết quả báo cáo.

```bash
python -m consumer.benchmark send --node-url http://HOST_B_IP:8002 --prepare-stock --output requests.json
docker logs hq-consumer > consumer.log 2>&1
python -m consumer.benchmark report --requests requests.json --log consumer.log --output cdc-results.json
```

Mặc định 200 lệnh xuất, cách nhau 100 ms; `--prepare-stock` nhập thêm 200 thùng qua API.
Mỗi request có UUID riêng, được lưu trước khi gửi để tra cứu cả khi HTTP timeout.
Report thiếu mẫu hoặc độ trễ âm sẽ báo lỗi, không âm thầm bỏ mẫu.
Trên Windows PowerShell 5, xuất log UTF-8 bằng `docker logs hq-consumer 2>&1 | Out-File consumer.log -Encoding utf8`.

`validation/M1-local.json` là phép đo trên cùng máy Docker qua PostgreSQL → Debezium → Kafka
→ Consumer → HQ, bằng nguồn phát SQL trong test. Nó chưa thay thế phép đo qua Node API
và hai máy demo. Test tích hợp thực hiện lại được 200 sự kiện này.

## Phần cần ghép với nhóm

- Tuấn Minh: nhập/xuất và outbox thật theo F6/F10; seed tồn qua API.
- Minh Hiếu: API phiếu và trạng thái SHIPPED để tính hàng đi đường.
- Nhật Khánh: Health Monitor, failover và Replay Worker. Chung đọc trạng thái;
  Consumer là bên đánh dấu APPLIED sau khi CDC về.
- Minh Danh: ghép overlay Consumer và lệnh benchmark/check-invariant vào hạ tầng chính.

Chưa xác nhận trọn ba kịch bản trên hai máy. Kiểm tra trực quan bằng Edge bị công cụ
computer-use từ chối; đã kiểm tra cú pháp JS, logic JS, HTTP và MIME của file tĩnh.

Dọn bộ test sau khi xem xong (chỉ xóa container/volume của project btl-chung-tests):

```bash
docker compose -f tests/consumer/compose.integration.yml --profile preview --profile validation down -v
```
