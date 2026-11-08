# infra/ – Chạy hệ thống bằng Docker Compose

Spec: `docs/spec.md` F1–F4, F11, F12. Môi trường đã thử: Windows + Docker Desktop, terminal Git Bash.
Mọi lệnh chạy **từ gốc repo**. Không cần `psql`, `jq` trên máy: script gọi `docker exec` và `curl`.

## 1. Chuẩn bị `.env` (gốc repo)

```bash
cp .env.example .env    # rồi sửa giá trị
```

Biến compose cần (thiếu biến bắt buộc thì compose dừng và báo tên biến):

| Biến | Bắt buộc | Dùng ở |
|---|---|---|
| `HOST_A_IP` | có | IP máy A (HQ). Trình duyệt và máy B gọi HQ API qua `http://HOST_A_IP:8000`; subscription của kho nối `HOST_A_IP:5432` |
| `HOST_B_IP` | có | IP máy B (3 kho). `NODE_URLS` của HQ, `NODE_URL` của WMS, FDW, connector |
| `INTERNAL_KEY` | có | Header `X-Internal-Key` giữa HQ và kho (F5) |
| `POSTGRES_PASSWORD` | có | Mật khẩu `postgres` và **mọi role** ứng dụng: hq_app, repl_sub, node_app, debezium, fdw_reader |
| `LOCK_MODE` | không | Mặc định `optimistic`; `none` chỉ cho `make race-unsafe` |

Chạy cả hai phần trên **một máy**: đặt `HOST_A_IP = HOST_B_IP = IP LAN của máy này`
(`ipconfig`, dòng IPv4 của card mạng đang dùng). Không dùng `localhost`/`127.0.0.1`: container
gọi nhau qua IP này, và trong container `localhost` là chính container đó.

Các biến còn lại của F2 do compose tự ghép, không cần đặt trong `.env`:
`DC_ID`, `NODE_DB_URL`, `HQ_URL`, `HQ_DB_URL`, `NODE_URLS`, `KAFKA_BOOTSTRAP=kafka:9092`, `NODE_URL`.
Vì nằm trong URL, `POSTGRES_PASSWORD` không được chứa `@ : / ? # %` và khoảng trắng.

`NODE_URLS` **không** đặt trong `.env`: `compose.hq.yml` ghép từ `HOST_B_IP`
(`{"1": "http://HOST_B_IP:8001", ...}`) và đưa vào hq-api. Compose chỉ dùng `.env` để thay biến
trong file yml, nên một dòng `NODE_URLS=...` còn sót trong `.env` cũ bị bỏ qua (dễ gây nhầm khi đổi
IP); nên xóa dòng đó.

Script trong `infra/scripts/` đọc `.env` theo kiểu `KEY=VALUE` của docker compose (không `source`
bằng bash, nên dòng JSON như `NODE_URLS={...}` không làm hỏng script).

## 2. Lệnh make

### Thứ tự lần đầu (hoặc khi muốn làm sạch toàn bộ)

```bash
make reset                          # ~2-4 phút; in "== reset k/8" cho từng bước
bash infra/scripts/verify.sh        # kiểm tra (a)-(d), xem mục 3
```

`make reset` chạy theo thứ tự cố định của F12, bước nào lỗi thì dừng và in tên bước:

| Bước | Việc | Script |
|---|---|---|
| 1/8 | `docker compose down -v` cả hai file (xóa volume CSDL **và Kafka**) | |
| 2/8 | `make up-hq` | |
| 3/8 | `make up-dc` | |
| 4/8 | Chờ `pg-hq`, `kafka`, `kafka-connect`, `pg-dc1..3` healthy (tối đa 180 giây) | `infra/scripts/wait-healthy.sh` |
| 5/8 | Logical replication danh mục: `SUBSCRIPTION sub_master` ở mỗi kho, slot `sub_master_dc1..3` trên HQ | `infra/scripts/setup-replication.sh` |
| 6/8 | postgres_fdw trên hq_db: `srv_dc1..3`, user mapping, schema `dc1..dc3`, view `v_inventory_all` | `infra/scripts/setup-fdw.sh` |
| 7/8 | Đăng ký `dc1-outbox`, `dc2-outbox`, `dc3-outbox`, chờ RUNNING | `infra/scripts/register-connectors.sh` |
| 8/8 | `make seed` | `scripts/seed.sh` |

Các script 5/8–7/8 **chạy lại được** (idempotent), dùng khi chỉ một phần hỏng mà không muốn reset:
subscription đã có thì bỏ qua; FDW cập nhật OPTIONS và nhập lại bảng ngoại; connector đã có thì
xóa rồi tạo lại (offset và slot `debezium_dcN` giữ nguyên nên không mất sự kiện).

### Danh sách lệnh

| Lệnh | Máy | Việc |
|---|---|---|
| `make up-hq` | A | pg-hq, kafka, kafka-connect, kafka-ui, hq-api, dashboard (build hq-api) |
| `make up-dc` | B | pg-dc1..3, node-api-dc1..3, wms-dc1..3 (build node-api) |
| `make down-hq`, `make down-dc` | A, B | Dừng và xóa container, **giữ volume** |
| `make reset` | cả hai | Xóa sạch và dựng lại theo bảng trên |
| `make seed` | A | Ghi danh mục vào hq_db (mục 2 của `docs/tests.md`): dc 1..3 (`srv_dc1..3`), `CAT-BEV`, `SKU-A` 12.5 kg; tạo 3 dòng `dc_status` ở RECOVERING (`node_mode` RECOVERING, `missed_count` 0; Health Monitor chỉ UPDATE; chạy lại seed không ghi đè dòng đã có); đặt lại sequence IDENTITY. **TODO**: nhập tồn Bắc 100 / Trung 40 / Nam 10 qua `POST /inbound` (REQ_SEED_1..3) khi Node API ONLINE |
| `make kill-dc2`, `make start-dc2` | B | `docker kill` / `docker start` pg-dc2 và node-api-dc2 |
| `make pause-dc2`, `make unpause-dc2` | B | `docker pause` / `docker unpause` pg-dc2 và node-api-dc2 |
| `make restart-connector-dc2` | A | `POST /connectors/dc2-outbox/restart?includeTasks=true&onlyFailed=true` (F4), chờ RUNNING tối đa 60 giây |
| `make ps` | | Trạng thái container của cả hai file compose |
| `make logs [S=hq-api] [N=200]` | | Log (mặc định mọi service, 100 dòng cuối) |

Dùng file env khác: `make reset ENV_FILE=duong/dan/khac.env`. Kafka Connect ở chỗ khác:
`CONNECT_URL=http://HOST_A_IP:8083` (mặc định `http://localhost:8083`).

Volume trống thì postgres chạy `postgres/init-hq.sh` hoặc `postgres/init-dc.sh` (tạo CSDL, chạy
migration, đặt mật khẩu role). Volume đã có dữ liệu thì script **không chạy lại**; muốn khởi tạo lại
phải `make reset`.

## 3. Kiểm tra sau `make reset`

```bash
bash infra/scripts/verify.sh              # (a)-(d), ~30 giây
bash infra/scripts/verify.sh --failover   # thêm (e): GIẾT kho Trung rồi dựng lại, ~1-2 phút
```

Mỗi mục in `OK ...`; sai thì in `LOI [verify] buoc '...'` và thoát mã khác 0. Dòng thử được xóa
sau khi kiểm tra. Làm tay từng mục:

**(a) Logical replication danh mục** – hq_db có 3 dòng dc, SKU-A và 3 dòng `dc_status`; 3 kho
thấy đủ danh mục trong 10 giây; dòng mới ở HQ xuống cả 3 kho trong 10 giây.

```bash
docker exec pg-hq  psql -U postgres -d hq_db -c "select dc_id, dc_code, fdw_server from dc; select sku, weight_kg from product"
docker exec pg-hq  psql -U postgres -d hq_db -c "select dc_id, status, node_mode, missed_count from dc_status"
for n in 1 2 3; do docker exec pg-dc$n psql -U postgres -d dc$n -tAc "select count(*) from dc; select sku from product"; done
docker exec pg-hq  psql -U postgres -d hq_db -c "insert into product_category (category_code, category_name) values ('CAT-TEST','Thu')"
for n in 1 2 3; do docker exec pg-dc$n psql -U postgres -d dc$n -tAc "select category_code from product_category"; done
docker exec pg-hq  psql -U postgres -d hq_db -c "delete from product_category where category_code = 'CAT-TEST'"
docker exec pg-dc1 psql -U postgres -d dc1 -c "select subname, pid, received_lsn from pg_stat_subscription"   # pid khác rỗng
docker exec pg-hq  psql -U postgres -d hq_db -c "select slot_name, active from pg_replication_slots"         # sub_master_dc1..3
```

**(b) FDW** – chạy bằng user `hq_app`:

```bash
docker exec pg-hq psql -U hq_app -d hq_db -c "select (select count(*) from dc1.inventory), (select count(*) from dc2.inventory), (select count(*) from dc3.inventory)"
docker exec pg-hq psql -U hq_app -d hq_db -c "select * from v_inventory_all"
```

**(c) Connector** – connector và task đều `RUNNING`:

```bash
for n in 1 2 3; do curl -s localhost:8083/connectors/dc$n-outbox/status; echo; done
```

**(d) Outbox → Kafka** – chèn một dòng ở dc1, đọc topic, rồi xóa:

```bash
docker exec pg-dc1 psql -U postgres -d dc1 -c "insert into outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload)
  select id, 'INVENTORY', '1:1', 'InventoryChanged', jsonb_build_object('event_id', id, 'event_type', 'InventoryChanged',
    'source_dc', 1, 'occurred_at', clock_timestamp(), 'product_id', 1, 'qty_on_hand', 0, 'qty_reserved', 0,
    'version', 0, 'request_id', gen_random_uuid(), 'ref_type', 'RECEIPT') from gen_random_uuid() id"
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic outbox.event.INVENTORY --from-beginning --timeout-ms 10000 --property print.key=true
docker exec pg-dc1 psql -U postgres -d dc1 -c "delete from outbox_event where aggregate_id = '1:1' and payload->>'version' = '0'"
```

Key phải là `1:1`, value là payload JSON. Dòng thử dùng `version 0` để Consumer (chỉ áp dụng khi
`version > last_version`) không đổi snapshot. Từ máy host thì dùng `localhost:29092`.

**(e) Sự cố kho Trung**

```bash
make kill-dc2
curl -s localhost:8083/connectors/dc2-outbox/status    # sau ~15-30 giây: task "FAILED" (connector vẫn RUNNING)
curl -s localhost:8083/connectors/dc1-outbox/status    # dc1, dc3 vẫn RUNNING
make start-dc2                                          # chờ pg-dc2 healthy (make ps)
make restart-connector-dc2                              # task dc2 RUNNING lại
docker exec pg-dc2 psql -U postgres -d dc2 -c "select subname, pid from pg_stat_subscription"   # subscription tự nối lại
```

Đã đo trên Debezium 2.7.3 (`errors.max.retries=3`): task dc2 chuyển `FAILED` sau 16–26 giây; ở mức
connector trạng thái vẫn là `RUNNING`, chỉ task `FAILED`. Subscription không cần thao tác gì: worker
của kho tự nối lại HQ khi pg-dc2 lên.

## 4. Thành phần

| File | Nội dung |
|---|---|
| `compose.hq.yml` | Máy A. Project `btl-hq`. Volume `pg-hq-data`, `kafka-data` |
| `compose.dc.yml` | Máy B. Project `btl-dc`. Volume `pg-dc1-data`..`pg-dc3-data` |
| `postgres/postgresql.conf` | F3: `wal_level=logical`, slot/wal_sender = 10, `max_slot_wal_keep_size=2GB`, `listen_addresses='*'` |
| `postgres/pg_hba.conf` | Socket trong container: trust; qua mạng: scram-sha-256 (kể cả replication) |
| `postgres/init-hq.sh`, `init-dc.sh` | Khởi tạo CSDL khi volume trống. `init-dc.sh` đọc `DC_ID` do compose đặt cho từng pg-dc |
| `nginx/gen-config.sh` | Lúc container khởi động, ghi `config.js` từ biến môi trường rồi chạy nginx |
| `nginx/default.conf` | Phục vụ `dashboard/` hoặc `wms/`; `/config.js` lấy từ file vừa sinh, không cache |
| `connectors/dc1..3-outbox.json` | F4, có chỗ giữ `${HOST_B_IP}`, `${POSTGRES_PASSWORD}`; `register-connectors.sh` thay bằng `sed` |
| `scripts/lib.sh` | Hàm dùng chung: đọc `.env`, `psql` qua `docker exec`, chờ healthy, đọc trạng thái connector |
| `scripts/*.sh` | Các bước của `make reset` (mục 2), `restart-connector.sh`, `verify.sh` (mục 3) |

Đối tượng do script tạo (không nằm trong migration vì cần `HOST_A_IP`/`HOST_B_IP`):

- Mỗi kho: `SUBSCRIPTION sub_master ... PUBLICATION pub_master WITH (slot_name = 'sub_master_dcN')`.
- hq_db: extension `postgres_fdw`; `SERVER srv_dc1..3` (`HOST_B_IP`, cổng 5433..5435,
  `connect_timeout '3'`, `keep_connections 'off'`); `USER MAPPING` cho `hq_app` và `postgres`
  (cùng dùng `fdw_reader`); schema `dc1..dc3` chứa bảng ngoại `inventory, stock_movement,
  transfer_order, transfer_item`; view `v_inventory_all` (db.md 3.5); `SELECT` cho `hq_app`.
  Mapping cho `postgres` cần vì `IMPORT FOREIGN SCHEMA` chạy bằng `postgres`, và view (chủ là
  `postgres`) truy cập bảng ngoại bằng mapping của chủ view, kể cả khi `hq_app` là người đọc.
- Kafka Connect: 3 connector; slot `debezium_dc1..3` ở từng kho do connector tự tạo.

`config.js` của giao diện có dạng:

```js
window.CONFIG = { HQ_URL: "http://HOST_A_IP:8000", NODE_URL: "http://HOST_B_IP:8001", DC_ID: 1 };
```

(Dashboard chỉ có `HQ_URL`.) Giao diện nạp bằng `<script src="config.js"></script>`.
Kafka UI: http://localhost:8080. Kafka có hai listener: container dùng `kafka:9092`; công cụ trên
máy host (pytest, bench-cdc) dùng `localhost:29092` hoặc `HOST_A_IP:29092`.

## 5. Lỗi hay gặp

| Triệu chứng | Nguyên nhân, cách xử lý |
|---|---|
| `CREATE SUBSCRIPTION ... could not connect` hoặc `password authentication failed for user "repl_sub"` | Có **PostgreSQL cài trên Windows** (dịch vụ `postgresql-x64-*`) giữ cổng 5432 trên IP LAN, nên kết nối tới `HOST_A_IP:5432` rơi vào bản Windows. Kiểm tra `netstat -ano \| grep ":5432"` rồi `tasklist //FI "PID eq <pid>"`; dừng dịch vụ đó (cần quyền admin). Hoặc `HOST_A_IP` sai (đổi mạng Wi-Fi thì IP đổi) |
| `setup-fdw` báo `could not connect to server "srv_dcN"` | `HOST_B_IP` sai hoặc pg-dcN chưa chạy; thử `docker exec pg-hq pg_isready -h $HOST_B_IP -p 5433` |
| `user mapping not found for "..."` khi đọc `v_inventory_all` | FDW chưa dựng hoặc dựng thiếu; chạy lại `bash infra/scripts/setup-fdw.sh` |
| `register-connectors` báo `POST ... tra HTTP 400` | Thường là mật khẩu/IP sai trong JSON sau khi thay, hoặc slot `debezium_dcN` đang bị connector cũ giữ; xem `docker logs kafka-connect --tail 100` |
| Connector RUNNING nhưng task `FAILED` | Kho từng sập. Sau khi kho lên: `make restart-connector-dc2` (kho 2) hoặc `curl -X POST "localhost:8083/connectors/dcN-outbox/restart?includeTasks=true&onlyFailed=true"` |
| Script dừng ở `cho healthy` | Xem `make ps`, `make logs S=pg-dc1`. Hay gặp: cổng bị container khác chiếm (`docker ps --format "{{.Names}} {{.Ports}}"`) |
| `$'\r': command not found` | Script bị đổi sang CRLF. `.gitattributes` ở gốc repo buộc `*.sh` dùng LF; chạy `git add --renormalize . && git checkout -- infra scripts` |
| `docker exec ... /opt/...: no such file` trong Git Bash | Git Bash đổi `/opt/...` thành `C:/Program Files/Git/opt/...`. Thêm `MSYS_NO_PATHCONV=1` trước lệnh `docker` (script đã tự làm) |
| Subscription trên kho đã có nhưng không nhận dữ liệu sau khi chỉ xóa volume HQ | Slot `sub_master_dcN` trên HQ mất. Không xóa volume riêng lẻ; dùng `make reset` |
| `make seed` không có tồn kho | Đúng như hiện tại: bước nhập tồn qua API là TODO, chờ Node API (mục 2) |

Ghi chú Windows khác: script khởi tạo chạy được dù file mount vào không có quyền thực thi (entrypoint
postgres `source` file; nginx gọi qua `sh`). Cổng 5432, 3000, 8000, 8080... phải trống.
