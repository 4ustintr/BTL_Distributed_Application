# infra/ – Chạy hệ thống bằng Docker Compose

Spec: `docs/spec.md` F1–F4, F12. Môi trường đã thử: Windows + Docker Desktop, terminal Git Bash.
Mọi lệnh chạy **từ gốc repo**.

## 1. Chuẩn bị `.env` (gốc repo)

```bash
cp .env.example .env    # rồi sửa giá trị
```

Biến compose cần (thiếu biến bắt buộc thì compose dừng và báo tên biến):

| Biến | Bắt buộc | Dùng ở |
|---|---|---|
| `HOST_A_IP` | có | IP máy A (HQ). Trình duyệt và máy B gọi HQ API qua `http://HOST_A_IP:8000` |
| `HOST_B_IP` | có | IP máy B (3 kho). `NODE_URLS` của HQ, `NODE_URL` của WMS, connector |
| `INTERNAL_KEY` | có | Header `X-Internal-Key` giữa HQ và kho (F5) |
| `POSTGRES_PASSWORD` | có | Mật khẩu `postgres` và **mọi role** ứng dụng: hq_app, repl_sub, node_app, debezium, fdw_reader |
| `LOCK_MODE` | không | Mặc định `optimistic`; `none` chỉ cho `make race-unsafe` |

Chạy cả hai phần trên **một máy**: đặt `HOST_A_IP = HOST_B_IP = IP LAN của máy này`
(`ipconfig`, dòng IPv4 của card mạng đang dùng). Không dùng `localhost`/`127.0.0.1`: container
gọi nhau qua IP này, và trong container `localhost` là chính container đó.

Các biến còn lại của F2 do compose tự ghép, không cần đặt trong `.env`:
`DC_ID`, `NODE_DB_URL`, `HQ_URL`, `HQ_DB_URL`, `NODE_URLS`, `KAFKA_BOOTSTRAP=kafka:9092`, `NODE_URL`.
Vì nằm trong URL, `POSTGRES_PASSWORD` không được chứa `@ : / ? # %`.

## 2. Lệnh

| Lệnh | Việc |
|---|---|
| `make up-hq` | Máy A: pg-hq, kafka, kafka-connect, kafka-ui, hq-api, dashboard (build hq-api) |
| `make up-dc` | Máy B: pg-dc1..3, node-api-dc1..3, wms-dc1..3 (build node-api) |
| `make down-hq`, `make down-dc` | Dừng và xóa container, **giữ volume** (dữ liệu CSDL, Kafka) |
| `make ps` | Trạng thái container của cả hai file compose |
| `make logs [S=hq-api] [N=200]` | Log (mặc định mọi service, 100 dòng cuối) |

Dùng file env khác: `make up-hq ENV_FILE=duong/dan/khac.env`.

Lần đầu (volume trống), postgres chạy `postgres/init-hq.sh` hoặc `postgres/init-dc.sh`:
tạo CSDL, chạy migration bằng user `postgres`, đặt mật khẩu role. Lần sau volume đã có dữ liệu
nên script **không chạy lại**; muốn khởi tạo lại phải xóa volume (`docker compose ... down -v`, hoặc
`make reset` khi đã có).

## 3. Kiểm tra nhanh

```bash
make ps                                                   # mọi container "running"/"healthy"
curl localhost:8000/health                                # {"status":"ok"}
curl localhost:8001/health                                # {"dc_id":1,"mode":"RECOVERING","db":"ok"}
docker exec pg-hq  psql -U postgres -d hq_db -c "\dt"     # 9 bảng
docker exec pg-dc1 psql -U postgres -d dc1   -c "\dRp+"   # publication dbz_outbox
curl localhost:8083/connector-plugins                     # có io.debezium.connector.postgresql.PostgresConnector
curl localhost:3000/config.js                             # HQ_URL
curl localhost:3001/config.js                             # DC_ID, NODE_URL, HQ_URL
```

Kafka UI: http://localhost:8080.

Kafka có hai listener: container dùng `kafka:9092`; công cụ chạy trên máy host (pytest, bench-cdc)
dùng `localhost:29092` hoặc `HOST_A_IP:29092` (listener EXTERNAL, quảng bá `HOST_A_IP:29092`).

## 4. Thành phần

| File | Nội dung |
|---|---|
| `compose.hq.yml` | Máy A. Project `btl-hq`. Volume `pg-hq-data`, `kafka-data` |
| `compose.dc.yml` | Máy B. Project `btl-dc`. Volume `pg-dc1-data`..`pg-dc3-data` |
| `postgres/postgresql.conf` | F3: `wal_level=logical`, slot/wal_sender = 10, `max_slot_wal_keep_size=2GB`, `listen_addresses='*'` |
| `postgres/pg_hba.conf` | Socket trong container: trust; qua mạng: scram-sha-256 (kể cả replication) |
| `postgres/init-hq.sh`, `init-dc.sh` | Khởi tạo CSDL (xem mục 2). `init-dc.sh` đọc `DC_ID` do compose đặt cho từng pg-dc |
| `nginx/gen-config.sh` | Lúc container khởi động, ghi `config.js` từ biến môi trường rồi chạy nginx |
| `nginx/default.conf` | Phục vụ `dashboard/` hoặc `wms/`; `/config.js` lấy từ file vừa sinh, không cache |
| `connectors/dc1..3-outbox.json` | F4. **Chưa đăng ký** (xem mục 5) |

`config.js` có dạng:

```js
window.CONFIG = { HQ_URL: "http://HOST_A_IP:8000", NODE_URL: "http://HOST_B_IP:8001", DC_ID: 1 };
```

(Dashboard chỉ có `HQ_URL`.) Giao diện nạp bằng `<script src="config.js"></script>`.

## 5. Đăng ký connector (thủ công, khi cần)

File JSON chứa chỗ giữ `${HOST_B_IP}` và `${POSTGRES_PASSWORD}`. Thay bằng `sed`, **không dùng
`envsubst`** vì nó sẽ xóa `${routedByValue}` của Outbox Event Router:

```bash
set -a; . ./.env; set +a
for n in 1 2 3; do
  sed -e "s|\${HOST_B_IP}|$HOST_B_IP|" -e "s|\${POSTGRES_PASSWORD}|$POSTGRES_PASSWORD|" \
      infra/connectors/dc$n-outbox.json \
  | curl -s -X POST -H "Content-Type: application/json" --data @- localhost:8083/connectors
done
```

## 6. Ghi chú Windows

- `infra/.gitattributes` buộc `*.sh`, `*.conf`, `*.yml`, `*.json` dùng LF. Nếu script khởi tạo lỗi
  `$'\r': command not found`, chạy `git add --renormalize infra && git checkout -- infra`.
- Script khởi tạo chạy được dù file mount vào không có quyền thực thi (entrypoint postgres
  `source` file; nginx gọi qua `sh`).
- Nếu máy có **PostgreSQL cài trên Windows** (dịch vụ `postgresql-x64-*`), nó giữ cổng 5432 trên IP LAN:
  `docker exec` và hq-api (gọi `pg-hq:5432` trong mạng Docker) vẫn chạy, nhưng mọi kết nối tới
  `HOST_A_IP:5432` (subscription `sub_master` của kho) sẽ rơi vào bản Windows. Kiểm tra:
  `netstat -ano | grep ":5432"` rồi `tasklist //FI "PID eq <pid>"`; dừng dịch vụ đó (cần quyền admin).
- Cổng 5432, 3000, 8000, 8080... phải trống. `docker ps --format "{{.Names}} {{.Ports}}"` để tìm container chiếm cổng.
