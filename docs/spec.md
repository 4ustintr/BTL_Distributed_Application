<!-- File sinh tự động từ Google Docs bằng scripts/sync_docs.py. Không sửa tay: sửa Google Docs rồi chạy lại script. -->

# Đặc tả kỹ thuật


Thẻ này là hợp đồng kỹ thuật cho cả nhóm và cho công cụ sinh code: khi thẻ này mâu thuẫn với chỗ khác, lấy thẻ này làm chuẩn. Mục F1–F4 mô tả môi trường, F5–F9 mô tả hợp đồng giữa các service, F10–F12 mô tả quy trình và tham số.

## F1. Công nghệ và phiên bản

| Thành phần | Công nghệ | Phiên bản | Ghi chú |
| :---- | :---- | :---- | :---- |
| CSDL | PostgreSQL | 16 | Tối thiểu 14, vì cần tùy chọn keep\_connections của postgres\_fdw |
| CDC | Debezium PostgreSQL connector chạy trên Kafka Connect | 2.7 trở lên | Plugin pgoutput, Outbox Event Router |
| Hàng đợi | Apache Kafka, chế độ KRaft | 3.7 trở lên, 1 broker | Replication factor 1 (xem Giới hạn ở thẻ Tổng quan) |
| Giao diện Kafka | Kafka UI | Bản ổn định mới nhất | Xem connector, topic, consumer lag |
| HQ API, Node API | Python, FastAPI, asyncpg, httpx | Python 3.11 | Mỗi API một service riêng |
| CDC Consumer | Python, confluent-kafka, asyncpg | Python 3.11 | Tắt tự commit offset |
| Dashboard, WMS | HTML, CSS, JavaScript thuần (không cần build), phục vụ bằng nginx; dùng chung cho Dashboard và WMS |  | Chỉ gọi API bằng fetch, không truy cập CSDL; địa chỉ API đặt trong config.js của từng container |
| Triển khai | Docker Compose |  | compose.hq.yml cho máy A, compose.dc.yml cho máy B |

## F2. Bố trí máy, container và cổng

| Container | Máy | Cổng | Vai trò |
| :---- | :---- | :---- | :---- |
| pg-hq | A | 5432 | hq\_db |
| hq-api | A | 8000 | HQ API, kèm Health Monitor và Replay Worker chạy nền |
| hq-consumer | A | không mở | CDC Consumer |
| kafka | A | 9092 | Kafka broker |
| kafka-connect | A | 8083 | Debezium, 3 connector dc1-outbox, dc2-outbox, dc3-outbox |
| kafka-ui | A | 8080 | Giao diện Kafka |
| dashboard | A | 3000 | Dashboard HQ |
| pg-dc1, pg-dc2, pg-dc3 | B | 5433, 5434, 5435 | CSDL kho Bắc, Trung, Nam (dbname dc1, dc2, dc3) |
| node-api-dc1, node-api-dc2, node-api-dc3 | B | 8001, 8002, 8003 | Node API |
| wms-dc1, wms-dc2, wms-dc3 | B | 3001, 3002, 3003 | WMS |

Biến môi trường, đặt trong file .env của mỗi máy:

* Chung: HOST\_A\_IP, HOST\_B\_IP, INTERNAL\_KEY (khóa dùng chung cho lệnh gọi nội bộ giữa HQ và kho).  
* Node API: DC\_ID, NODE\_DB\_URL, HQ\_URL \= http\://HOST\_A\_IP:8000, LOCK\_MODE \= optimistic (giá trị none chỉ dùng cho make race-unsafe).  
* HQ API và Consumer: HQ\_DB\_URL, NODE\_URLS \= {"1": "http\://HOST\_B\_IP:8001", "2": "http\://HOST\_B\_IP:8002", "3": "http\://HOST\_B\_IP:8003"}, KAFKA\_BOOTSTRAP \= kafka:9092.  
* WMS: DC\_ID, NODE\_URL, HQ\_URL.

Mạng: các pg-dc đặt listen\_addresses \= '\*' và pg\_hba.conf cho phép HOST\_A\_IP (FDW, Debezium); pg-hq cho phép HOST\_B\_IP (subscription của 3 kho).

## F3. Cấu hình PostgreSQL

Cả 4 CSDL dùng cùng các tham số sau trong postgresql.conf; bảng dưới đó là các role và quyền:

```text
wal_level = logical max_replication_slots = 10 max_wal_senders = 10 max_slot_wal_keep_size = 2GB   # chặn đầy đĩa khi connector hoặc kho dừng lâu
```

| CSDL | Role | Quyền |
| :---- | :---- | :---- |
| hq\_db | hq\_app | Ứng dụng HQ API và Consumer |
| hq\_db | repl\_sub | LOGIN REPLICATION, SELECT trên dc, product\_category, product; dùng cho subscription của 3 kho |
| dcX | node\_app | SELECT trên bản sao danh mục; SELECT, INSERT, UPDATE trên inventory, transfer\_order, transfer\_item; SELECT, INSERT trên stock\_movement, outbox\_event |
| dcX | debezium | LOGIN REPLICATION, SELECT trên outbox\_event |
| dcX | fdw\_reader | SELECT trên inventory, stock\_movement, transfer\_order, transfer\_item |

Logical replication danh mục. Mỗi subscription phải có slot riêng trên HQ, nếu không cả 3 kho cùng tên slot sub\_master và kho thứ hai sẽ bị từ chối:

```text
-- Trên hq_db CREATE PUBLICATION pub_master FOR TABLE dc, product_category, product; -- Trên dc1 (đổi dc1 thành dc2, dc3 ở hai kho còn lại) CREATE SUBSCRIPTION sub_master   CONNECTION 'host=HOST_A_IP port=5432 dbname=hq_db user=repl_sub password=***'   PUBLICATION pub_master   WITH (slot_name = 'sub_master_dc1');
```

FDW từ HQ tới kho:

```text
CREATE EXTENSION IF NOT EXISTS postgres_fdw; CREATE SERVER srv_dc1 FOREIGN DATA WRAPPER postgres_fdw   OPTIONS (host 'HOST_B_IP', port '5433', dbname 'dc1',            connect_timeout '3', keep_connections 'off'); CREATE USER MAPPING FOR hq_app SERVER srv_dc1   OPTIONS (user 'fdw_reader', password '***'); CREATE SCHEMA dc1; IMPORT FOREIGN SCHEMA public   LIMIT TO (inventory, stock_movement, transfer_order, transfer_item)   FROM SERVER srv_dc1 INTO dc1; -- Lặp lại cho srv_dc2 (cổng 5434, schema dc2) và srv_dc3 (cổng 5435, schema dc3)
```

HQ API chạy 3 truy vấn FDW song song trên 3 kết nối riêng trong pool, mỗi truy vấn bọc timeout 5 giây ở tầng ứng dụng.

## F4. Cấu hình Debezium connector

Mẫu cho kho Trung; hai kho còn lại đổi tên, cổng, dbname, slot.name, publication.name và topic.prefix.

```text
{   "name": "dc2-outbox",   "config": {     "connector.class": "io.debezium.connector.postgresql.PostgresConnector",     "plugin.name": "pgoutput",     "database.hostname": "HOST_B_IP",     "database.port": "5434",     "database.user": "debezium",     "database.password": "***",     "database.dbname": "dc2",     "topic.prefix": "dc2",     "slot.name": "debezium_dc2",     "publication.name": "dbz_outbox_dc2",     "publication.autocreate.mode": "filtered",     "table.include.list": "public.outbox_event",     "tombstones.on.delete": "false",     "transforms": "outbox",     "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",     "transforms.outbox.table.field.event.id": "event_id",     "transforms.outbox.table.field.event.key": "aggregate_id",     "transforms.outbox.table.field.event.payload": "payload",     "transforms.outbox.route.by.field": "aggregate_type",     "transforms.outbox.route.topic.replacement": "outbox.event.${routedByValue}",     "transforms.outbox.table.expand.json.payload": "true",     "key.converter": "org.apache.kafka.connect.storage.StringConverter",     "value.converter": "org.apache.kafka.connect.json.JsonConverter",     "value.converter.schemas.enable": "false",     "errors.max.retries": "3"   } }
```

Khởi động lại connector sau khi kho lên lại: POST http\://HOST\_A\_IP:8083/connectors/dc2-outbox/restart?includeTasks=true\&onlyFailed=true. Nên chạy thử trước buổi demo để xác nhận connector chuyển FAILED đúng như mô tả, vì hành vi thử lại có thể khác nhau giữa các bản Debezium.

## F5. Quy ước API chung

* Dữ liệu JSON UTF-8; thời gian dạng ISO 8601 có múi giờ; mọi mã định danh UUID truyền dạng chuỗi. HQ API và Node API bật CORS cho Dashboard và WMS (các cổng 3000–3003), vì giao diện gọi API thẳng từ trình duyệt.  
* Lệnh gọi nội bộ giữa HQ và kho gửi header X-Internal-Key \= INTERNAL\_KEY; sai khóa trả 401\.  
* Mọi lệnh ghi mang request\_id hoặc transfer\_id nên gửi lại an toàn. Gửi trùng trả 200 kèm "duplicate": true và trạng thái hiện tại.  
* Lỗi luôn có dạng {"error\_code": "...", "message": "...", "details": {...}}.  
* Lệnh gọi giữa các service dùng timeout 2 giây, thử lại tối đa 5 lần, mỗi lần cách 2 giây, trừ khi mục riêng ghi khác.

| HTTP | error\_code | Khi nào |
| :---- | :---- | :---- |
| 400 | VALIDATION\_ERROR | Thiếu trường, qty ≤ 0, SKU không tồn tại trong danh mục |
| 401 | UNAUTHORIZED | Sai X-Internal-Key |
| 404 | NOT\_FOUND | Không có phiếu hoặc SKU |
| 409 | VERSION\_CONFLICT | Hết 3 lần thử lại do xung đột version |
| 409 | INVALID\_STATE | Thao tác không hợp lệ với trạng thái hiện tại của phiếu |
| 409 | DC\_NOT\_OFFLINE | Gọi failover khi HQ vẫn thấy kho còn sống |
| 422 | INSUFFICIENT\_STOCK | Không đủ hàng khả dụng |
| 503 | DC\_RECOVERING | Kho đang đồng bộ, chưa nhận giao dịch từ WMS |
| 503 | DC\_UNREACHABLE | HQ không gọi được kho cần thiết cho thao tác |
| 504 | UPSTREAM\_TIMEOUT | Truy vấn FDW hoặc lệnh gọi xuống kho quá thời gian |

## F6. API của Node API (mỗi kho)

| Endpoint | Gọi bởi | Request body | Kết quả |
| :---- | :---- | :---- | :---- |
| GET /health | WMS, Health Monitor |  | 200 {"dc\_id": 2, "mode": "ONLINE", "db": "ok"}; CSDL lỗi thì 503 |
| GET /inventory?product\_id=1 | WMS |  | 200 {"items": \[{"product\_id": 1, "qty\_on\_hand": 100, "qty\_reserved": 50, "available": 50, "version": 3}\]} |
| POST /inbound | WMS | {"request\_id", "product\_id", "qty", "ref\_type": "RECEIPT", "created\_by"} | 200 {"qty\_on\_hand", "version", "duplicate"}; 400; 503 DC\_RECOVERING |
| POST /outbound | WMS | {"request\_id", "product\_id", "qty", "ref\_type": "ISSUE", "created\_by"} | 200; 409 VERSION\_CONFLICT; 422; 503 DC\_RECOVERING |
| POST /transfers/notice | HQ | {"transfer\_id", "transfer\_code", "from\_dc\_id", "to\_dc\_id", "direction", "items": \[{"line\_no": 1, "product\_id": 1, "qty": 50}\]} | 200 {"status": "CREATED", "duplicate"}; 422 khi kho nguồn không đủ hàng để giữ chỗ |
| POST /transfers/{id}/ship | WMS kho nguồn | {"created\_by"} | 200 {"status": "SHIPPED"}; 409 INVALID\_STATE |
| POST /transfers/{id}/receive | WMS kho đích | {"created\_by"} | 200 {"status": "RECEIVED"}; 409 INVALID\_STATE nếu phiếu chưa SHIPPED |
| POST /transfers/{id}/decline | WMS kho đích | {"reason"} | Chuyển tiếp đồng bộ tới HQ /decline, trả nguyên kết quả của HQ |
| POST /transfers/{id}/cancel | HQ, gọi kho nguồn | {"reason": "DECLINED"} | 200 {"status": "CANCELLED"}, kể cả khi đã hủy từ trước; 409 INVALID\_STATE nếu đã SHIPPED |
| POST /transfers/{id}/sync-status | HQ | {"status": "SHIPPED"} | 200; chỉ cập nhật khi trạng thái tiến lên theo thứ tự CREATED \< SHIPPED \< RECEIVED (CANCELLED chỉ ghi khi đang CREATED), ngược lại bỏ qua và vẫn trả 200 |
| POST /replay | HQ | {"request\_id", "txn\_type": "OUTBOUND", "product\_id", "qty", "created\_by"} | 200 {"qty\_on\_hand", "duplicate"}; 422; nhận ở cả hai chế độ |
| POST /recovery/complete | HQ |  | 200 {"mode": "ONLINE"} |

Sau khi commit bước ship hoặc receive, Node API gọi HQ /shipped hoặc /received theo quy ước thử lại ở F5. Hết số lần thử thì trả 503 DC\_UNREACHABLE cho WMS; nhân viên bấm lại thao tác, kho trả duplicate và gửi lại callback.

## F7. API của HQ API

| Endpoint | Gọi bởi | Request body | Kết quả |
| :---- | :---- | :---- | :---- |
| GET /health | Dashboard, script demo |  | 200 {"status": "ok"} |
| POST /transfers | Dashboard | {"from\_dc\_id": 1, "to\_dc\_id": 3, "items": \[{"product\_id": 1, "qty": 50}\]} | 201 {"transfer\_id", "transfer\_code", "status": "CREATED"}; 422 khi kho nguồn không giữ chỗ được (phiếu CANCELLED, lý do INSUFFICIENT\_STOCK); 503 DC\_UNREACHABLE nếu một trong hai kho không ONLINE, khi đó không tạo phiếu |
| GET /transfers/{id} | Dashboard |  | 200 bản ghi transfer\_summary |
| GET /transfers?status=SHIPPED | Dashboard |  | 200 danh sách phiếu |
| POST /transfers/{id}/shipped | Node API kho nguồn | {"shipped\_at"} | 200 |
| POST /transfers/{id}/received | Node API kho đích | {"received\_at"} | 200 |
| POST /transfers/{id}/decline | Node API kho đích | {"reason"} | 200 {"status": "CANCELLED"}; 409 INVALID\_STATE nếu kho nguồn đã xuất. Dashboard cũng gọi endpoint này cho nút Hủy phiếu, với reason \= TIMEOUT |
| POST /failover/dc/{dc\_id}/inbound | WMS | {"request\_id", "product\_id", "qty", "created\_by"} | 200 {"txn\_id", "status": "PENDING", "provisional\_qty": 65}; 409 DC\_NOT\_OFFLINE |
| POST /failover/dc/{dc\_id}/outbound | WMS | Như trên | 200; 409 DC\_NOT\_OFFLINE; 422 nếu khả dụng tạm tính không đủ |
| GET /dashboard/inventory?mode=snapshot\&product\_id=1 | Dashboard | mode \= snapshot hoặc realtime | 200 {"items": \[{"dc\_id", "qty\_on\_hand", "qty\_reserved", "qty\_in\_transit", "source", "as\_of"}\]}; source là snapshot, realtime, offline hoặc provisional |
| GET /dc-status | Dashboard |  | 200 \[{"dc\_id", "status", "updated\_at"}\] |
| GET /pending-txn?dc\_id=2 | Dashboard |  | 200 danh sách pending\_txn |
| GET /invariant?product\_id=1 | make check-invariant |  | 200 {"stock": 135, "in\_transit": 0, "shipped\_out": 45, "received\_in": 180, "diff": 0}; 503 nếu một kho không truy cập được |

## F8. Hợp đồng sự kiện

| Topic | Key | Sự kiện | Ghi chú |
| :---- | :---- | :---- | :---- |
| outbox.event.INVENTORY | "dc\_id:product\_id" | InventoryChanged, InventoryChangedDirect (dự phòng, bản nộp chưa sinh) | 3 partition |
| outbox.event.TRANSFER | transfer\_id | TransferCreated, TransferShipped, TransferReceived, TransferCancelled (Consumer bỏ qua, chỉ để quan sát) | 3 partition |
| outbox.event.DLQ | Giữ nguyên key gốc | Sự kiện Consumer không xử lý được | Chỉ để điều tra |

Consumer group: hq-consumer. Trường occurred\_at ghi bằng clock\_timestamp() ở kho, chỉ để hiển thị và đo độ trễ, không dùng để sắp thứ tự. Mẫu payload sự kiện tồn kho:

```text
{   "event_id": "8f0c2a4e-1d3b-4c5a-9e7f-0a1b2c3d4e5f",   "event_type": "InventoryChanged",   "source_dc": 2,   "occurred_at": "2026-11-20T09:15:02.481+07:00",   "product_id": 1,   "qty_on_hand": 65,   "qty_reserved": 0,   "version": 8,   "request_id": "3b1e7c90-5a2d-4f61-8b3c-9d0e1f2a3b4c",   "ref_type": "REPLAY" }
```

InventoryChangedDirect (dự phòng, bản nộp chưa sinh) có cùng các trường, thêm changed\_by (current\_user) và client\_addr (inet\_client\_addr()). Mẫu payload sự kiện phiếu:

```text
{   "event_id": "c4d5e6f7-0a1b-4c2d-8e3f-5a6b7c8d9e0f",   "event_type": "TransferShipped",   "source_dc": 1,   "occurred_at": "2026-11-20T09:05:40.117+07:00",   "transfer_id": "5e6f7a8b-9c0d-4e1f-a2b3-c4d5e6f7a8b9",   "transfer_code": "TR-2026-000001",   "direction": "OUTBOUND",   "status": "SHIPPED",   "shipped_at": "2026-11-20T09:05:40.117+07:00",   "received_at": null,   "cancel_reason": null }
```

TransferCreated, TransferReceived, TransferCancelled dùng cùng cấu trúc với mẫu trên; chỉ khác event\_type, status và trường thời gian tương ứng.

## F9. Máy trạng thái

### Phiếu điều chuyển (transfer\_summary ở HQ)

| Từ | Sang | Kích hoạt bởi | Điều kiện |
| :---- | :---- | :---- | :---- |
| (chưa có) | CREATED | POST /transfers | Hai kho đều ONLINE |
| CREATED | SHIPPED | /shipped từ kho nguồn | Trạng thái hiện tại đúng là trạng thái liền trước |
| SHIPPED | RECEIVED | /received từ kho đích | Trạng thái hiện tại đúng là trạng thái liền trước |
| CREATED | CANCELLED | Kho nguồn trả 200 cho /cancel | Không có đường nào khác tới CANCELLED |

### Trạng thái kho ở HQ (dc\_status)

| Từ | Sang | Kích hoạt bởi | Điều kiện |
| :---- | :---- | :---- | :---- |
| ONLINE, RECOVERING | OFFLINE | Health Monitor | missed\_count ≥ 3 |
| OFFLINE | RECOVERING | Health Monitor | Health check thành công và (mode \= RECOVERING hoặc kho còn pending\_txn PENDING) |
| OFFLINE | ONLINE | Health Monitor | Health check thành công, mode \= ONLINE, không còn PENDING (kho chỉ mất mạng, không khởi động lại) |
| RECOVERING | ONLINE | Replay Worker | Không còn PENDING và /recovery/complete trả 200 |

### Chế độ của Node API

| Từ | Sang | Kích hoạt bởi | Điều kiện |
| :---- | :---- | :---- | :---- |
| (khởi động) | RECOVERING | Tiến trình Node API | Luôn luôn |
| RECOVERING | ONLINE | HQ gọi /recovery/complete | Không có |
| RECOVERING | ONLINE | Quản trị viên | HQ không chạy; gọi tay POST /recovery/complete (xem Giới hạn) |

### Giao dịch nhận hộ (pending\_txn)

| Từ | Sang | Kích hoạt bởi | Điều kiện |
| :---- | :---- | :---- | :---- |
| (chưa có) | PENDING | POST /failover | Kho OFFLINE; lệnh xuất cần đủ khả dụng tạm tính |
| PENDING | APPLIED | Consumer | Thấy request\_id trong InventoryChanged |
| PENDING | APPLIED | Replay Worker | Kho trả 200 kèm duplicate: true (giao dịch đã có ở kho từ trước, sẽ không có sự kiện mới) |
| PENDING | REJECTED | Replay Worker | Kho trả 422 |

### Chế độ của WMS

| Từ | Sang | Điều kiện | Hành vi |
| :---- | :---- | :---- | :---- |
| NORMAL | FAILOVER | 2 lần GET /health tới Node API thất bại | Gửi nhập, xuất tới HQ /failover; gặp 409 DC\_NOT\_OFFLINE thì hiện thông báo và gửi lại sau 2 giây |
| FAILOVER | WAITING | Node API trả lời với mode \= RECOVERING | Khóa nút nhập, xuất; hiện "kho đang đồng bộ" |
| FAILOVER, WAITING | NORMAL | Node API trả lời với mode \= ONLINE | Gọi lại Node API như bình thường |

WMS sinh request\_id một lần cho mỗi thao tác của người dùng và giữ nguyên khi gửi lại, kể cả khi chuyển từ Node API sang HQ.

## F10. Quy trình xử lý chuẩn

Mỗi quy trình ở kho chạy trong một transaction duy nhất và bắt đầu bằng SET LOCAL app.source \= 'api'. Thứ tự các bước là bắt buộc.

### a. Nhập, xuất tại kho (POST /inbound, /outbound, /replay)

1. Đang RECOVERING mà lệnh đến từ WMS thì trả 503 DC\_RECOVERING (/replay vẫn chạy). Tiếp theo kiểm tra trùng: SELECT 1 FROM stock\_movement WHERE request\_id \= :request\_id; đã có thì trả 200 kèm duplicate: true ngay, không trừ kho. Bước này bắt buộc chạy trước bước trừ kho: nếu không, một lệnh xuất đã áp dụng được gửi lại đúng lúc tồn không còn đủ sẽ nhận nhầm 422 và bị đánh dấu REJECTED.  
2. Xuất: UPDATE inventory có điều kiện (Thiết kế CSDL, mục 2.7), thử lại tối đa 3 lần khi xung đột version. Nhập: INSERT … ON CONFLICT (dc\_id, product\_id) DO UPDATE SET qty\_on\_hand \= inventory.qty\_on\_hand \+ EXCLUDED.qty\_on\_hand.  
3. INSERT stock\_movement … ON CONFLICT (request\_id) DO NOTHING. Không có dòng nào được ghi thì ROLLBACK cả transaction và trả 200 kèm duplicate: true. Đây là chốt chặn cuối cho trường hợp hai lệnh trùng đến cùng lúc; trường hợp thường đã bị chặn ở bước 1\.  
4. INSERT outbox\_event InventoryChanged với version, qty\_on\_hand, qty\_reserved lấy từ RETURNING ở bước 2, kèm request\_id.  
5. COMMIT.

Ghi chú: inventory phải được cập nhật trước stock\_movement vì stock\_movement có khóa ngoại tới inventory (nhập SKU mới chưa có dòng). Nếu INSERT stock\_movement gặp trùng request\_id thì ROLLBACK cả transaction, nên thay đổi tồn kho ở bước 2 cũng bị hủy.

### b. Nhận Transfer Notice tại kho

1. INSERT transfer\_order … ON CONFLICT (transfer\_code) DO NOTHING. Không có dòng nào thì ROLLBACK, trả 200 duplicate.  
2. INSERT các dòng transfer\_item.  
3. Nếu direction \= OUTBOUND: giữ chỗ từng dòng (Thiết kế CSDL, mục 2.7); một dòng không đủ hàng thì ROLLBACK, trả 422\. Ghi InventoryChanged cho từng SKU.  
4. Ghi TransferCreated, COMMIT.

### c. Xuất điều chuyển tại kho nguồn (POST /transfers/{id}/ship)

1. UPDATE transfer\_order SET status \= 'SHIPPED', shipped\_at \= now() WHERE transfer\_id \= :id AND direction \= 'OUTBOUND' AND status \= 'CREATED'. Không cập nhật được: đã SHIPPED hoặc RECEIVED thì trả 200 duplicate và gửi lại callback /shipped; đã CANCELLED thì 409\.  
2. Từng dòng: giảm qty\_on\_hand và qty\_reserved cùng một lượng; ghi stock\_movement TRANSFER\_OUT với request\_id tất định; đặt qty\_shipped \= qty\_requested.  
3. Ghi InventoryChanged và TransferShipped, COMMIT, rồi gọi HQ /shipped.

### d. Nhận điều chuyển tại kho đích (POST /transfers/{id}/receive)

1. Bản INBOUND chưa SHIPPED thì trả 409 INVALID\_STATE (HQ đồng bộ trạng thái qua /sync-status ngay sau khi kho nguồn xuất).  
2. UPDATE transfer\_order SET status \= 'RECEIVED', received\_at \= now() WHERE direction \= 'INBOUND' AND status \= 'SHIPPED'. Đã RECEIVED thì trả 200 duplicate và gửi lại callback /received.  
3. Từng dòng: tăng qty\_on\_hand; ghi stock\_movement TRANSFER\_IN với request\_id tất định; đặt qty\_received \= qty\_requested.  
4. Ghi InventoryChanged và TransferReceived, COMMIT, rồi gọi HQ /received.

### e. Hủy tại kho nguồn (POST /transfers/{id}/cancel)

1. UPDATE transfer\_order SET status \= 'CANCELLED', cancel\_reason \= :reason WHERE direction \= 'OUTBOUND' AND status \= 'CREATED'. Đã CANCELLED hoặc chưa có phiếu thì trả 200 và không làm gì; đã SHIPPED hoặc RECEIVED thì 409\.  
2. Từng dòng: giảm qty\_reserved đúng bằng qty\_requested.  
3. Ghi InventoryChanged và TransferCancelled, COMMIT.

### f. Tạo phiếu tại HQ (POST /transfers)

1. Hai kho không cùng ONLINE thì trả 503, không tạo phiếu.  
2. Một transaction: sinh transfer\_code \= 'TR-' \+ năm \+ '-' \+ số thứ tự 6 chữ số từ transfer\_code\_seq; INSERT transfer\_summary CREATED kèm items; ghi transfer\_event (NULL → CREATED, source HQ).  
3. Gọi kho nguồn /transfers/notice. 422: chuyển phiếu CANCELLED với lý do INSUFFICIENT\_STOCK, ghi transfer\_event, trả 422 cho Dashboard. Lỗi mạng sau khi thử lại theo F5: gọi /cancel cho kho nguồn; chỉ khi /cancel trả 200 mới chuyển phiếu CANCELLED (lý do TIMEOUT) và trả 503\. /cancel cũng lỗi thì giữ phiếu CREATED, trả 503; nhân viên HQ bấm Hủy phiếu trên Dashboard khi kho liên lạc lại được (bước g), để kho nguồn không giữ chỗ mãi cho một phiếu đã hủy.  
4. Gọi kho đích /transfers/notice. Lỗi mạng sau khi thử lại: gọi /cancel cho kho nguồn để nhả giữ chỗ, xử lý kết quả như bước 3\.

### g. Kho đích từ chối hoặc HQ hủy phiếu, xử lý tại HQ (POST /transfers/{id}/decline)

1. Gọi kho nguồn /cancel với lý do DECLINED (kho đích từ chối) hoặc TIMEOUT (nút Hủy phiếu trên Dashboard).  
2. Kho nguồn trả 200: một transaction chuyển transfer\_summary sang CANCELLED (điều kiện status \= CREATED) và ghi transfer\_event; sau đó gọi kho đích /sync-status CANCELLED; trả 200\.  
3. Kho nguồn trả 409: trả 409 INVALID\_STATE. Không gọi được kho nguồn: trả 503 DC\_UNREACHABLE, phiếu giữ nguyên.

### h. Ghi hộ tại HQ (POST /failover/dc/{dc\_id}/…)

1. dc\_status của kho khác OFFLINE thì trả 409 DC\_NOT\_OFFLINE.  
2. Đọc inventory\_snapshot của (dc\_id, product\_id) và các pending\_txn đang PENDING của kho đó; demo chỉ có một người thao tác nên không khóa dòng (xem Giới hạn).  
3. Tính khả dụng tạm tính (Thiết kế CSDL, mục 2.4). Lệnh xuất không đủ thì ROLLBACK, trả 422\.  
4. INSERT pending\_txn với txn\_id \= request\_id … ON CONFLICT (txn\_id) DO NOTHING, COMMIT, trả 200 kèm provisional\_qty.

### i. Replay Worker (mỗi 2 giây)

1. Với mỗi kho đang RECOVERING, lấy các pending\_txn đang PENDING theo thứ tự created\_at.  
2. Gửi lần lượt từng dòng tới /replay. Trả 200 thì sang dòng kế; nếu kèm duplicate: true thì chuyển thẳng APPLIED. Trả 422 thì chuyển REJECTED và ghi error\_msg. Lỗi mạng hoặc 5xx thì dừng vòng này. Dòng đã gửi nhưng chưa APPLIED (đang chờ sự kiện CDC) sẽ được gửi lại ở vòng sau; việc này an toàn vì kho chặn trùng theo request\_id.  
3. Khi kho không còn dòng PENDING nào, gọi /recovery/complete; trả 200 thì chuyển dc\_status sang ONLINE.

### j. Health Monitor (mỗi 2 giây, cho từng kho)

1. Gọi GET /health với timeout 1 giây.  
2. Thành công: missed\_count \= 0, ghi last\_heartbeat\_at và node\_mode, rồi chuyển trạng thái theo bảng dc\_status ở mục F9.  
3. Thất bại: tăng missed\_count; khi đạt 3 và kho chưa OFFLINE thì chuyển OFFLINE, ghi updated\_at.

### k. Đối soát định kỳ (tùy chọn, mỗi 60 giây)

1. Với mỗi kho ONLINE, đọc tồn và version qua FDW, so với inventory\_snapshot.  
2. Lệch thì ghi log và hiện cảnh báo trên Dashboard; snapshot là dữ liệu dẫn xuất nên được ghi đè bằng số và version đọc từ kho.  
3. Nếu thiếu thời gian, bỏ tác vụ này; nút Đối soát trên Dashboard là đủ cho demo.

## F11. Tham số cấu hình

| Tham số | Giá trị | Dùng ở |
| :---- | :---- | :---- |
| Chu kỳ health check của HQ | 2 giây, timeout 1 giây | Health Monitor |
| Số lần lỗi liên tiếp để coi là OFFLINE | 3 (khoảng 6 giây) | Health Monitor |
| Chu kỳ và số lần lỗi để WMS chuyển failover | 2 giây, 2 lần (khoảng 4 giây) | WMS |
| Timeout kết nối FDW | 3 giây (connect\_timeout) | Foreign server |
| Timeout mỗi truy vấn FDW ở tầng ứng dụng | 5 giây | HQ API |
| Timeout và thử lại khi gọi giữa các service | 2 giây, 5 lần, cách 2 giây | HQ API, Node API |
| Số lần thử lại khi xung đột version | 3 | Node API |
| Chu kỳ đối soát định kỳ | 60 giây (tùy chọn, xem F10.k) | HQ API |
| Thời gian tối đa một vòng Replay Worker | 30 giây | HQ API |
| Chu kỳ Replay Worker | 2 giây | HQ API |
| Thời gian kho ở RECOVERING trước khi ghi cảnh báo vào log | 30 giây | Node API |
| errors.max.retries của connector | 3 | Debezium |
| Nguồn thời gian cho nhãn "dữ liệu lúc hh:mm" | synced\_at, theo đồng hồ HQ | Dashboard |

## F12. Dữ liệu ban đầu và lệnh make

Dữ liệu danh mục ghi thẳng vào hq\_db bằng script seed; tồn kho nhập qua API. Bảng thứ hai liệt kê các lệnh make dùng khi demo và kiểm thử.

| Bảng | Dữ liệu |
| :---- | :---- |
| dc | (1, DC-BAC, Bắc, srv\_dc1), (2, DC-TRUNG, Trung, srv\_dc2), (3, DC-NAM, Nam, srv\_dc3) |
| product\_category | (1, CAT-BEV, Đồ uống, parent NULL) |
| product | (1, SKU-A, Nước suối thùng 24 chai, category 1, đơn vị thùng, 12.5 kg) |
| Tồn kho ban đầu | POST /inbound tại từng kho: Bắc 100, Trung 40, Nam 10, ref\_type RECEIPT |

| Lệnh | Chạy trên | Việc làm |
| :---- | :---- | :---- |
| make up-hq, make up-dc | Máy A, máy B | Dựng các container của máy đó |
| make reset | Cả hai máy | Thứ tự cố định: máy A trước (hq\_db, Kafka, Kafka Connect, HQ API), rồi máy B (3 kho và subscription), rồi tạo 3 connector, cuối cùng make seed. Mỗi máy chạy docker compose down \-v rồi dựng lại. Phải xóa cả volume Kafka: nếu chỉ xóa CSDL, Consumer sẽ áp dụng lại sự kiện cũ còn trong topic vì processed\_event đã bị xóa |
| make seed | Máy A | Ghi danh mục; chờ GET /dc-status báo cả 3 kho ONLINE (kho khởi động ở RECOVERING và từ chối lệnh nhập từ WMS cho tới khi Replay Worker gọi /recovery/complete); rồi gọi API nhập tồn ban đầu |
| make race, make race-unsafe | Máy B | Bắn 2 lệnh xuất 30 thùng cùng lúc vào kho Bắc; bản unsafe khởi động lại node-api-dc1 với LOCK\_MODE=none |
| make check-invariant | Máy A | Gọi GET /invariant, in 4 số và chênh lệch |
| make pause-dc2, unpause-dc2, kill-dc2, start-dc2 | Máy B | Tạo sự cố cho kho Trung |
| make restart-connector-dc2 | Máy A | Gọi REST restart connector dc2-outbox |
| make replay-again | Máy A | Gọi thẳng /replay của kho Trung với 2 request\_id cũ |
| make bench-cdc | Máy A | Đo độ trễ CDC (thẻ Kiểm thử và đánh giá) |
| make test | Máy A | Chạy 10 test bắt buộc; make test-all chạy cả test tùy chọn |
