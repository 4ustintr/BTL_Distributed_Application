# Thiết kế cơ sở dữ liệu

## Phân bố dữ liệu

Hệ thống dùng 4 CSDL PostgreSQL: 1 CSDL trung tâm hq\_db và 3 node pg\_dc1 (Bắc), pg\_dc2 (Trung), pg\_dc3 (Nam) có cùng lược đồ. Tổng cộng 14 bảng: 9 bảng ở HQ (3 bảng danh mục gốc, inventory\_snapshot, transfer\_summary, processed\_event, pending\_txn, transfer\_event, dc\_status), 5 bảng nghiệp vụ ở mỗi node, cộng 3 bản sao danh mục ở mỗi node.

* Phân mảnh ngang theo dc\_id: tồn kho, phiếu điều chuyển và sổ nhập–xuất của DC nào nằm ở node DC đó. Mỗi bảng giữ cột dc\_id (hoặc from\_dc\_id, to\_dc\_id) kèm CHECK để node chỉ nhận dữ liệu của mình; hợp các mảnh (UNION ALL) cho ra toàn bộ dữ liệu.  
* Nhân bản toàn phần dữ liệu danh mục: dc, product\_category, product chỉ được ghi tại HQ, phát xuống 3 node bằng logical replication (PUBLICATION ở HQ, SUBSCRIPTION ở node). Ở node đây là bản chỉ đọc.  
* Đồng bộ lên HQ bằng CDC outbox: mỗi giao dịch ở node ghi thêm dòng outbox\_event trong cùng transaction. Debezium đọc WAL và đẩy lên Kafka; CDC Consumer cập nhật inventory\_snapshot; bảng processed\_event chặn xử lý trùng. Trạng thái phiếu điều chuyển do HQ API ghi trực tiếp vì HQ là bên điều phối. Mỗi sự kiện tồn kho mang version của dòng inventory; Consumer chỉ áp dụng sự kiện có version mới hơn bản đang có (mục 3.4).  
* Truy vấn phân tán: HQ khai báo postgres\_fdw tới 3 node và nhập bảng của mỗi node vào một schema riêng (dc1, dc2, dc3), ví dụ dc1.inventory, dc2.transfer\_order. View v\_inventory\_all hợp 3 mảnh bằng UNION ALL để minh họa trong suốt phân mảnh. Khi đối soát, Dashboard không dùng view này mà truy vấn riêng từng schema (mục 2.4).  
* Nhận hộ khi kho sập: khi một kho không truy cập được, HQ ghi tạm giao dịch của kho đó vào pending\_txn rồi đẩy về khi kho hoạt động lại; cột stock\_movement.request\_id (UNIQUE) ở node chặn ghi trùng.  
* Theo dõi trạng thái kho: bảng dc\_status ở HQ lưu trạng thái ONLINE, OFFLINE hoặc RECOVERING của từng kho, do Health Monitor cập nhật. HQ chỉ nhận ghi hộ cho kho đang OFFLINE.  
* Khóa chính: dữ liệu danh mục dùng khóa số do HQ cấp; dữ liệu phát sinh ở node (transfer\_id, event\_id) dùng UUID để không trùng giữa các node.

## Biểu đồ cơ sở dữ liệu

\[Chèn hình ERD đã vẽ lại tại đây: thêm pending\_txn, transfer\_event, dc\_status ở khối HQ; bỏ qty\_in\_transit và đổi last\_event\_at thành last\_version ở inventory\_snapshot; thêm các cột mới của transfer\_summary, transfer\_order, pending\_txn.\]

ERD · 9 bảng HQ, 5 bảng nghiệp vụ \+ 3 bản sao danh mục mỗi node DC.

Các bảng nghiệp vụ chỉ tham chiếu bản sao danh mục ngay trong node, nên mọi khóa ngoại đều kiểm tra cục bộ; giữa HQ và node có 4 đường trao đổi (CDC, logical replication, FDW, API) nhưng không có khóa ngoại xuyên CSDL.

## 1\. Thiết kế bảng tổng quát

CSDL gồm 14 bảng; 3 bảng danh mục có bản gốc ở HQ và bản sao ở cả 3 node.

| STT | Tên bảng | Vị trí | Mô tả |
| :---- | :---- | :---- | :---- |
| 1 | dc | HQ (gốc) → 3 node | Danh mục trung tâm phân phối: mã, tên, vùng, tên foreign server dùng cho FDW |
| 2 | product\_category | HQ (gốc) → 3 node | Nhóm hàng hóa, phân cấp cha–con |
| 3 | product | HQ (gốc) → 3 node | Danh mục hàng hóa (SKU) dùng chung toàn hệ thống |
| 4 | inventory\_snapshot | HQ | Bản chụp tồn kho từng SKU tại từng DC (tồn thực và đã giữ chỗ), cập nhật từ sự kiện CDC theo version; nguồn số liệu mặc định cho Dashboard |
| 5 | transfer\_summary | HQ | Trạng thái hiện tại và danh sách hàng của từng phiếu điều chuyển; HQ API ghi khi điều phối Saga |
| 6 | processed\_event | HQ | Nhật ký sự kiện CDC đã xử lý, bảo đảm mỗi sự kiện chỉ áp dụng một lần |
| 7 | inventory | Node DC | Tồn kho hiện tại của từng SKU tại DC |
| 8 | stock\_movement | Node DC | Sổ nhập–xuất–điều chỉnh: mỗi lần tồn kho thay đổi là một dòng |
| 9 | transfer\_order | Node DC | Phiếu điều chuyển: phiếu xuất đi ở node nguồn, phiếu nhận về ở node đích |
| 10 | transfer\_item | Node DC | Chi tiết từng dòng hàng của phiếu điều chuyển |
| 11 | outbox\_event | Node DC | Hàng đợi sự kiện (transactional outbox) để Debezium đọc và đẩy lên Kafka |
| 12 | pending\_txn | HQ | Giao dịch HQ nhận hộ khi một kho bị sập, chờ đẩy về kho khi kho hoạt động lại |
| 13 | transfer\_event | HQ | Lịch sử chuyển trạng thái của phiếu điều chuyển |
| 14 | dc\_status | HQ | Trạng thái ONLINE, OFFLINE, RECOVERING của từng kho, do Health Monitor cập nhật |

## 2\. Thiết kế chi tiết từng bảng

Kiểu dữ liệu theo PostgreSQL. PK \= khóa chính, FK \= khóa ngoại.

### 2.1. Bảng dc

Danh mục trung tâm phân phối. Gốc ở HQ, nhân bản xuống 3 node.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| dc\_id | SMALLINT | PK | Mã DC: 1 \= Bắc, 2 \= Trung, 3 \= Nam |
| dc\_code | VARCHAR(10) | NOT NULL, UNIQUE | Mã viết tắt: DC-BAC, DC-TRUNG, DC-NAM |
| dc\_name | VARCHAR(100) | NOT NULL | Tên trung tâm phân phối |
| region | VARCHAR(10) | NOT NULL, CHECK IN ('BAC','TRUNG','NAM') | Vùng phụ trách |
| address | VARCHAR(255) |  | Địa chỉ kho |
| fdw\_server | VARCHAR(50) | UNIQUE | Tên foreign server trên HQ trỏ tới node (srv\_dc1, …) |
| is\_active | BOOLEAN | NOT NULL, DEFAULT TRUE | DC còn hoạt động |
| created\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm tạo |

### 2.2. Bảng product\_category

Nhóm hàng, phân cấp cha–con. Gốc ở HQ, nhân bản xuống 3 node.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| category\_id | INTEGER | PK, IDENTITY (chỉ ở HQ) | Mã nhóm hàng |
| category\_code | VARCHAR(20) | NOT NULL, UNIQUE | Mã nghiệp vụ của nhóm |
| category\_name | VARCHAR(100) | NOT NULL | Tên nhóm hàng |
| parent\_id | INTEGER | FK → product\_category(category\_id), NULL | Nhóm cha; NULL nếu là nhóm gốc |

### 2.3. Bảng product

Danh mục hàng hóa dùng chung. Gốc ở HQ, nhân bản xuống 3 node.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| product\_id | BIGINT | PK, IDENTITY (chỉ ở HQ) | Mã hàng hóa |
| sku | VARCHAR(30) | NOT NULL, UNIQUE | Mã đơn vị lưu kho (SKU) |
| product\_name | VARCHAR(200) | NOT NULL | Tên hàng hóa |
| category\_id | INTEGER | NOT NULL, FK → product\_category | Nhóm hàng |
| unit | VARCHAR(20) | NOT NULL | Đơn vị tính: thùng, cái, kg, … |
| weight\_kg | NUMERIC(10,3) | CHECK (weight\_kg \>= 0\) | Khối lượng một đơn vị |
| is\_active | BOOLEAN | NOT NULL, DEFAULT TRUE | Còn kinh doanh |
| updated\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Lần sửa cuối |

Ở mỗi node, 3 bảng dc, product\_category, product có cùng tên cột, kiểu và ràng buộc như ở HQ, trừ một điểm: khóa chính ở node không dùng IDENTITY (khai báo category\_id INTEGER PRIMARY KEY, product\_id BIGINT PRIMARY KEY), vì giá trị khóa do HQ cấp và được replication chép xuống. Dữ liệu nhận qua SUBSCRIPTION sub\_master; role ứng dụng ở node chỉ có quyền SELECT trên 3 bảng này.

### 2.4. Bảng inventory\_snapshot (HQ)

Bản chụp tồn kho toàn hệ thống, do CDC Consumer ghi. Cột qty\_in\_transit và last\_event\_at của bản cũ đã bỏ: hàng đi đường tính bằng view, thứ tự sự kiện so bằng version.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| dc\_id | SMALLINT | PK, FK → dc | DC sở hữu tồn kho |
| product\_id | BIGINT | PK, FK → product | Hàng hóa |
| qty\_on\_hand | INTEGER | NOT NULL, DEFAULT 0, CHECK (\>= 0\) | Số lượng thực có theo sự kiện mới nhất |
| qty\_reserved | INTEGER | NOT NULL, DEFAULT 0, CHECK (\>= 0\) | Số lượng đã giữ chỗ |
| last\_version | BIGINT | NOT NULL, DEFAULT 0 | Bằng inventory.version của sự kiện cuối đã áp dụng; sự kiện có version nhỏ hơn hoặc bằng bị bỏ qua |
| last\_event\_id | UUID |  | Sự kiện cuối đã áp dụng |
| synced\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm HQ cập nhật dòng, theo đồng hồ HQ; dùng cho nhãn "dữ liệu lúc hh:mm" |

Hàng đang đi đường không lưu trong bảng này mà tính bằng view từ transfer\_summary, để chỉ có một nơi ghi số liệu này:

```sql
CREATE VIEW v_in_transit AS
SELECT s.to_dc_id AS dc_id,
       (i->>'product_id')::BIGINT AS product_id,
       SUM((i->>'qty')::INT) AS qty_in_transit
FROM transfer_summary s
CROSS JOIN LATERAL jsonb_array_elements(s.items) AS i
WHERE s.status = 'SHIPPED'
GROUP BY s.to_dc_id, (i->>'product_id')::BIGINT;
```

Dashboard đọc số tồn kho theo quy tắc sau:

| Tình huống | Nguồn số liệu | Nhãn hiển thị |
| :---- | :---- | :---- |
| Bình thường | inventory\_snapshot | "cập nhật lúc hh:mm:ss" (synced\_at) |
| Bấm nút Đối soát | FDW: 3 truy vấn song song trên 3 kết nối riêng, mỗi truy vấn timeout 5 giây, bắt lỗi riêng | "trực tiếp" |
| Kho OFFLINE, hoặc truy vấn FDW tới kho đó lỗi hay quá thời gian | inventory\_snapshot của kho đó | "offline, dữ liệu lúc hh:mm" |
| Kho OFFLINE hoặc RECOVERING và có pending\_txn đang PENDING | Công thức tạm tính bên dưới | "tạm tính" |

Tồn tạm tính \= qty\_on\_hand \+ Σ qty của pending\_txn INBOUND đang PENDING − Σ qty của pending\_txn OUTBOUND đang PENDING.

Khả dụng tạm tính \= tồn tạm tính − qty\_reserved. HQ dùng khả dụng tạm tính để duyệt lệnh xuất ghi hộ.

### 2.5. Bảng transfer\_summary (HQ)

Trạng thái hiện tại của từng phiếu điều chuyển. Chỉ HQ API ghi bảng này, vì HQ là bên điều phối Saga (mục 3.2). Các bước hợp lệ: CREATED → SHIPPED → RECEIVED và CREATED → CANCELLED.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| transfer\_id | UUID | PK | Trùng transfer\_id ở node |
| transfer\_code | VARCHAR(20) | NOT NULL, UNIQUE | Sinh từ sequence transfer\_code\_seq tại HQ, ví dụ TR-2026-000123; duy nhất toàn hệ thống |
| from\_dc\_id | SMALLINT | NOT NULL, FK → dc | DC xuất |
| to\_dc\_id | SMALLINT | NOT NULL, FK → dc, CHECK (to\_dc\_id \<\> from\_dc\_id) | DC nhận |
| status | VARCHAR(15) | NOT NULL, CHECK IN ('CREATED','SHIPPED','RECEIVED','CANCELLED') | Trạng thái phiếu |
| total\_lines | INTEGER | CHECK (\>= 0\) | Số dòng hàng, tính từ items |
| total\_qty | INTEGER | CHECK (\>= 0\) | Tổng số lượng yêu cầu, tính từ items |
| items | JSONB | NOT NULL | Danh sách hàng, ví dụ \[{"product\_id": 1, "qty": 50}\]; HQ ghi khi tạo phiếu |
| cancel\_reason | VARCHAR(30) | CHECK IN ('DECLINED','INSUFFICIENT\_STOCK','TIMEOUT') | Lý do hủy; chỉ có giá trị khi status \= CANCELLED |
| cancelled\_at | TIMESTAMPTZ |  | Thời điểm phiếu bị hủy |
| last\_sync\_at | TIMESTAMPTZ |  | Lần gần nhất HQ gọi /sync-status cho hai kho |
| created\_at | TIMESTAMPTZ | NOT NULL | Thời điểm tạo phiếu |
| shipped\_at | TIMESTAMPTZ |  | Thời điểm node nguồn xuất hàng |
| received\_at | TIMESTAMPTZ |  | Thời điểm node đích xác nhận nhận |
| updated\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Lần cập nhật cuối tại HQ |

Hệ thống không hỗ trợ xuất thiếu hay nhận thiếu: qty\_shipped và qty\_received phải bằng qty\_requested (xem mục Giới hạn ở thẻ Tổng quan).

### 2.6. Bảng processed\_event (HQ)

Chống xử lý trùng: Consumer INSERT … ON CONFLICT DO NOTHING vào bảng này trong cùng transaction với lệnh cập nhật snapshot (mục 3.4).

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| event\_id | UUID | PK | Bằng outbox\_event.event\_id ở node |
| source\_dc | SMALLINT | NOT NULL | DC phát sinh sự kiện |
| event\_type | VARCHAR(50) | NOT NULL | Loại sự kiện |
| aggregate\_id | VARCHAR(64) | NOT NULL | Đối tượng bị tác động ("dc\_id:product\_id" hoặc transfer\_id) |
| processed\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm xử lý |

### 2.7. Bảng inventory (node DC)

Tồn kho hiện tại của DC. dc\_id là thuộc tính phân mảnh: mỗi node có CHECK (dc\_id \= mã của node), ví dụ pg\_dc1 chỉ nhận dc\_id \= 1\.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| dc\_id | SMALLINT | PK, FK → dc, CHECK (dc\_id \= mã node) | DC sở hữu |
| product\_id | BIGINT | PK, FK → product | Hàng hóa |
| qty\_on\_hand | INTEGER | NOT NULL, DEFAULT 0, CHECK (\>= 0\) | Số lượng thực có |
| qty\_reserved | INTEGER | NOT NULL, DEFAULT 0, CHECK (qty\_reserved BETWEEN 0 AND qty\_on\_hand) | Số lượng đã giữ, không vượt tồn thực |
| version | BIGINT | NOT NULL, DEFAULT 1 | Khóa lạc quan. Trigger BEFORE UPDATE tự đặt NEW.version \= OLD.version \+ 1 cho mọi lệnh UPDATE, kể cả sửa tay; ứng dụng chỉ dùng version trong mệnh đề WHERE, không tự tăng |
| updated\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Lần thay đổi cuối |

Ràng buộc mảnh được sinh theo từng node: script khởi tạo của pg\_dc1 tạo CHECK (dc\_id \= 1), của pg\_dc2 tạo CHECK (dc\_id \= 2), của pg\_dc3 tạo CHECK (dc\_id \= 3). Version bắt đầu từ 1 để sự kiện đầu tiên luôn lớn hơn last\_version mặc định 0 ở HQ. Trigger và lệnh mẫu:

```sql
CREATE FUNCTION bump_version() RETURNS trigger AS $$
BEGIN
  NEW.version := OLD.version + 1;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_inventory_version
BEFORE UPDATE ON inventory
FOR EACH ROW EXECUTE FUNCTION bump_version();

-- Xuất :qty thùng; :v là version vừa đọc
UPDATE inventory
SET qty_on_hand = qty_on_hand - :qty, updated_at = now()
WHERE dc_id = :dc AND product_id = :p
  AND version = :v
  AND qty_on_hand - qty_reserved >= :qty
RETURNING version, qty_on_hand, qty_reserved;

-- Giữ chỗ :qty thùng cho phiếu điều chuyển
UPDATE inventory
SET qty_reserved = qty_reserved + :qty, updated_at = now()
WHERE dc_id = :dc AND product_id = :p
  AND version = :v
  AND qty_on_hand - qty_reserved >= :qty
RETURNING version, qty_on_hand, qty_reserved;
```

Lệnh không trả về dòng nào thì đọc lại: version đã khác thì thử lại, tối đa 3 lần; version vẫn như cũ nghĩa là không đủ hàng, trả 422 INSUFFICIENT\_STOCK. Nhập hàng cho SKU chưa có dòng thì dùng INSERT … ON CONFLICT (dc\_id, product\_id) DO UPDATE.

### 2.8. Bảng stock\_movement (node DC)

Sổ nhập–xuất: mỗi lần inventory thay đổi ghi một dòng, tổng qty\_change của một SKU bằng qty\_on\_hand. Cột request\_id (UNIQUE) giúp node nhận ra giao dịch bị gửi trùng; quy trình đầy đủ ở thẻ Đặc tả kỹ thuật, mục F10.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| movement\_id | BIGINT | PK, IDENTITY | Mã dòng sổ |
| request\_id | UUID | NOT NULL, UNIQUE | Mã lệnh. WMS sinh cho nhập và xuất; khi replay dùng txn\_id của pending\_txn; dòng điều chuyển dùng mã tất định |
| dc\_id | SMALLINT | NOT NULL, FK (dc\_id, product\_id) → inventory | DC phát sinh |
| product\_id | BIGINT | NOT NULL | Hàng hóa |
| movement\_type | VARCHAR(15) | NOT NULL, CHECK IN ('INBOUND','OUTBOUND','TRANSFER\_OUT','TRANSFER\_IN','ADJUST') | Loại biến động |
| qty\_change | INTEGER | NOT NULL, CHECK (qty\_change \<\> 0\) | Dương là tăng, âm là giảm |
| ref\_type | VARCHAR(20) | NOT NULL, CHECK IN ('ISSUE','RECEIPT','TRANSFER','REPLAY','STOCKTAKE','DIRECT') | ISSUE: xuất cho đại lý; RECEIPT: nhập từ nhà cung cấp; TRANSFER: điều chuyển; REPLAY: giao dịch HQ ghi hộ; STOCKTAKE: kiểm kê; DIRECT: dự phòng cho ghi nhận sửa tay (hướng phát triển) |
| ref\_id | VARCHAR(64) |  | Mã chứng từ, ví dụ transfer\_id |
| created\_by | VARCHAR(50) |  | Người thao tác trên WMS |
| created\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm ghi |

Dòng sổ của bước xuất và nhận điều chuyển dùng mã tất định uuid5(NAMESPACE\_URL, "{transfer\_id}:{movement\_type}:{line\_no}") (Python: uuid.uuid5). Gọi lại cùng một bước bao nhiêu lần cũng sinh cùng mã, nên không ghi trùng. Role ứng dụng chỉ có quyền INSERT và SELECT trên bảng này: sổ chỉ ghi thêm, không sửa, không xóa.

Dữ liệu ban đầu cũng được nhập qua API như một lệnh INBOUND (ref\_type RECEIPT), để quy tắc "tổng qty\_change bằng qty\_on\_hand" đúng ngay từ đầu và câu kiểm tra bất biến chạy được.

### 2.9. Bảng transfer\_order (node DC)

Một phiếu điều chuyển nằm ở 2 node với cùng transfer\_id do HQ cấp trong Transfer Notice: bản OUTBOUND ở node nguồn, bản INBOUND ở node đích. Node đích gửi Receipt Confirmation khi nhận đủ hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| transfer\_id | UUID | PK | Mã phiếu, duy nhất toàn hệ thống, do HQ cấp |
| transfer\_code | VARCHAR(20) | NOT NULL, UNIQUE | Mã phiếu do HQ cấp; UNIQUE trong node để chặn nhận lặp Transfer Notice |
| from\_dc\_id | SMALLINT | NOT NULL, FK → dc | DC xuất |
| to\_dc\_id | SMALLINT | NOT NULL, FK → dc, CHECK (to\_dc\_id \<\> from\_dc\_id) | DC nhận |
| direction | VARCHAR(8) | NOT NULL, CHECK IN ('OUTBOUND','INBOUND') | OUTBOUND khi node là from\_dc, INBOUND khi node là to\_dc |
| status | VARCHAR(15) | NOT NULL, DEFAULT 'CREATED', CHECK IN ('CREATED','SHIPPED','RECEIVED','CANCELLED') | Bản OUTBOUND do kho nguồn đổi khi xuất hàng hoặc khi nhận lệnh hủy; bản INBOUND do kho đích đổi khi nhận hàng, và được HQ đồng bộ qua /sync-status |
| cancel\_reason | VARCHAR(30) | CHECK IN ('DECLINED','INSUFFICIENT\_STOCK','TIMEOUT') | Lý do hủy |
| note | TEXT |  | Ghi chú |
| created\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm tạo |
| shipped\_at | TIMESTAMPTZ |  | Thời điểm xuất hàng |
| received\_at | TIMESTAMPTZ |  | Thời điểm nhận hàng |

Ràng buộc mảnh ở mức bảng (ví dụ cho pg\_dc1, đổi số 1 theo từng node):

```sql
CHECK (
  (direction = 'OUTBOUND' AND from_dc_id = 1) OR
  (direction = 'INBOUND'  AND to_dc_id   = 1)
)
```

Kho đích chỉ cho nhận hàng khi bản INBOUND đã SHIPPED; HQ đồng bộ trạng thái này qua /sync-status ngay sau khi kho nguồn xuất hàng. Nếu chưa SHIPPED, trả 409 INVALID\_STATE.

### 2.10. Bảng transfer\_item (node DC)

Chi tiết dòng hàng của phiếu. Bảng đi theo transfer\_order (phân mảnh ngang dẫn xuất); hệ thống không hỗ trợ xuất thiếu hay nhận thiếu.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| transfer\_id | UUID | PK, FK → transfer\_order ON DELETE CASCADE | Phiếu chứa dòng |
| line\_no | SMALLINT | PK | Số thứ tự dòng |
| product\_id | BIGINT | NOT NULL, FK → product | Hàng hóa |
| qty\_requested | INTEGER | NOT NULL, CHECK (\> 0\) | Số lượng yêu cầu |
| qty\_shipped | INTEGER | CHECK (qty\_shipped IS NULL OR qty\_shipped \= qty\_requested) | Số lượng thực xuất, node nguồn ghi |
| qty\_received | INTEGER | CHECK (qty\_received IS NULL OR qty\_received \= qty\_requested) | Số lượng thực nhận, node đích ghi |

Ràng buộc mức bảng:

```sql
PRIMARY KEY (transfer_id, line_no),
UNIQUE (transfer_id, product_id)
```

### 2.11. Bảng outbox\_event (node DC)

Ghi trong cùng transaction với thay đổi nghiệp vụ. Debezium dùng Outbox Event Router (cấu hình ở thẻ Đặc tả kỹ thuật) để đẩy sự kiện lên Kafka. Dòng cũ có thể xóa định kỳ vì Debezium đọc từ WAL.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| event\_id | UUID | PK | Mã sự kiện; HQ lưu lại trong processed\_event |
| aggregate\_type | VARCHAR(30) | NOT NULL, CHECK IN ('INVENTORY','TRANSFER') | Dùng để định tuyến topic Kafka |
| aggregate\_id | VARCHAR(64) | NOT NULL | Sự kiện tồn kho: chuỗi "dc\_id:product\_id", ví dụ "2:1". Sự kiện phiếu: transfer\_id. Dùng làm key Kafka để giữ thứ tự sự kiện của cùng đối tượng |
| event\_type | VARCHAR(50) | NOT NULL, CHECK IN ('InventoryChanged','InventoryChangedDirect','TransferCreated','TransferShipped','TransferReceived','TransferCancelled') | Loại sự kiện |
| payload | JSONB | NOT NULL | Luôn có event\_id, event\_type, source\_dc, occurred\_at. Sự kiện tồn kho thêm product\_id, qty\_on\_hand, qty\_reserved, version, request\_id. Sự kiện phiếu thêm transfer\_id, transfer\_code, direction, status, cancel\_reason |
| created\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm phát sinh |

Mỗi lệnh ghi đúng một sự kiện InventoryChanged cho mỗi dòng inventory bị đổi, cộng một sự kiện Transfer\* nếu phiếu đổi trạng thái (sự kiện phiếu chỉ để quan sát trên Kafka UI, Consumer bỏ qua), tất cả trong cùng transaction với thay đổi nghiệp vụ. event\_id và event\_type được chép cả vào payload để Consumer không phải đọc header Kafka.

### 2.12. Bảng pending\_txn (HQ)

Lưu các giao dịch HQ nhận hộ khi một kho bị sập, để đẩy về kho khi kho hoạt động lại. Thêm chỉ mục (dc\_id, status, created\_at) để lấy nhanh các giao dịch đang chờ của một kho.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| txn\_id | UUID | PK | Bằng request\_id do WMS sinh ra; gửi kèm khi đẩy về kho |
| dc\_id | SMALLINT | NOT NULL, FK → dc | Kho mà giao dịch thuộc về |
| txn\_type | VARCHAR(10) | NOT NULL, CHECK IN ('INBOUND','OUTBOUND') | Nhập hoặc xuất |
| product\_id | BIGINT | NOT NULL, FK → product | Hàng hóa |
| qty | INTEGER | NOT NULL, CHECK (qty \> 0\) | Số lượng |
| status | VARCHAR(10) | NOT NULL, DEFAULT 'PENDING', CHECK IN ('PENDING','APPLIED','REJECTED') | PENDING: chưa áp dụng xong, kể cả khi kho đã nhận nhưng sự kiện CDC chưa về HQ. APPLIED: Consumer đã thấy request\_id trong sự kiện từ kho, hoặc kho trả duplicate: true khi replay. REJECTED: kho trả 422 khi replay |
| created\_by | VARCHAR(50) |  | Nhân viên kho thao tác |
| created\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm nhận; dùng làm thứ tự khi đẩy lại |
| updated\_at | TIMESTAMPTZ |  | Lần đổi trạng thái gần nhất |
| attempts | SMALLINT | NOT NULL, DEFAULT 0 | Số lần đã gửi tới kho, chỉ để theo dõi |
| applied\_at | TIMESTAMPTZ |  | Thời điểm kho áp dụng xong |
| error\_msg | TEXT |  | Lý do khi bị REJECTED |

### 2.13. Bảng transfer\_event (HQ)

Lịch sử chuyển trạng thái của phiếu điều chuyển, mỗi lần đổi trạng thái là một dòng. HQ API ghi transfer\_event trong cùng transaction với lệnh UPDATE có điều kiện trên transfer\_summary, và chỉ khi lệnh đó cập nhật đúng 1 dòng, nên callback trùng không tạo thêm dòng lịch sử.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| event\_id | UUID | PK | HQ sinh bằng gen\_random\_uuid() |
| transfer\_id | UUID | NOT NULL, FK → transfer\_summary | Phiếu điều chuyển |
| from\_status | VARCHAR(15) |  | Trạng thái trước; NULL khi phiếu mới tạo |
| to\_status | VARCHAR(15) | NOT NULL, CHECK IN ('CREATED','SHIPPED','RECEIVED','CANCELLED') | Trạng thái sau |
| source | VARCHAR(10) | NOT NULL, CHECK IN ('HQ','DC1','DC2','DC3') | Nơi phát sinh thay đổi |
| created\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Thời điểm chuyển trạng thái |

### 2.14. Bảng dc\_status (HQ)

Trạng thái của từng kho theo góc nhìn của HQ, do Health Monitor cập nhật. Không đặt trạng thái vào bảng dc vì bảng dc được nhân bản xuống kho; ghi vào đó mỗi 2 giây sẽ sinh luồng replication vô ích. Quy tắc chuyển trạng thái xem thẻ Đặc tả kỹ thuật, mục máy trạng thái. Seed tạo dòng ban đầu ở trạng thái RECOVERING; Health Monitor chỉ UPDATE, không INSERT.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :---- | :---- | :---- | :---- |
| dc\_id | SMALLINT | PK, FK → dc | Kho |
| status | VARCHAR(10) | NOT NULL, DEFAULT 'RECOVERING', CHECK IN ('ONLINE','OFFLINE','RECOVERING') | Trạng thái HQ dùng để quyết định có nhận ghi hộ và replay hay không |
| node\_mode | VARCHAR(10) | CHECK IN ('ONLINE','RECOVERING') | Chế độ kho tự báo trong lần health check thành công gần nhất |
| last\_heartbeat\_at | TIMESTAMPTZ |  | Lần cuối kho trả lời health check |
| missed\_count | SMALLINT | NOT NULL, DEFAULT 0 | Số lần health check thất bại liên tiếp |
| updated\_at | TIMESTAMPTZ | NOT NULL, DEFAULT now() | Lần đổi status gần nhất; Dashboard hiện "offline từ hh:mm" |

## 3\. Quy tắc đảm bảo tính đúng của dữ liệu

### 3.1. Mã phiếu điều chuyển duy nhất toàn hệ thống

Chỉ HQ được sinh transfer\_code, từ sequence transfer\_code\_seq trong hq\_db, theo dạng TR-2026-000123. Khi chỉ có một nơi sinh mã thì mã không thể trùng.

* HQ ghi phiếu vào transfer\_summary (UNIQUE transfer\_code) trước khi gửi Transfer Notice; ràng buộc này là chốt kiểm tra cho toàn hệ thống.  
* transfer\_order ở mỗi kho giữ UNIQUE (transfer\_code) để chặn nhận lặp cùng một Transfer Notice. Ràng buộc trong một CSDL chỉ chặn trùng trong kho đó, không tự đảm bảo duy nhất toàn cục.  
* Kho đang sập thì không tạo được phiếu liên quan đến kho đó, nên không có lúc hai nơi cùng sinh mã.  
* Nếu sau này cho kho tự tạo phiếu khi mất kết nối, mã sẽ gắn mã kho (TR-BAC-2026-000123) và mỗi kho dùng sequence riêng, để không gian mã được chia theo kho ngay từ đầu.

### 3.2. Lưu trạng thái giao dịch điều chuyển

Trạng thái được lưu ở ba nơi, mỗi nơi một vai trò:

* transfer\_summary (HQ): trạng thái hiện tại của cả phiếu, do HQ API ghi vì HQ là bên điều phối Saga.  
* transfer\_event (HQ): lịch sử chuyển trạng thái, mỗi lần đổi trạng thái là một dòng (mục 2.13).  
* transfer\_order và outbox\_event (kho): trạng thái phần việc của kho đó và sự kiện mỗi lần đổi, ghi cùng transaction với thay đổi tồn kho.

Bốn quy tắc giữ trạng thái đúng:

1. Ghi trước, gọi sau: HQ ghi phiếu CREATED vào transfer\_summary rồi mới gửi Transfer Notice tới hai kho. Nếu một lệnh gọi xuống kho vẫn lỗi sau khi thử lại, HQ gọi /cancel cho kho nguồn (an toàn kể cả khi kho chưa nhận notice) và chỉ chuyển phiếu CANCELLED khi kho nguồn xác nhận. Nếu /cancel cũng lỗi, phiếu giữ CREATED để HQ hủy lại bằng nút Hủy phiếu khi kho liên lạc được; nhờ vậy kho nguồn không giữ chỗ mãi cho một phiếu đã hủy. Bản nộp không có tác vụ chạy nền tự gửi lại hay tự hủy theo thời gian (xem Giới hạn ở thẻ Tổng quan).  
2. Trạng thái chỉ đi tới: lệnh cập nhật có điều kiện WHERE status \= trạng thái liền trước (CREATED trước SHIPPED, SHIPPED trước RECEIVED; CANCELLED chỉ từ CREATED). Callback trùng không cập nhật được dòng nào và được bỏ qua.  
3. Một nơi ghi trạng thái: chỉ HQ API ghi transfer\_summary; kho báo HQ bằng callback HTTP (/shipped, /received). Callback lỗi thì kho trả 503 cho WMS để nhân viên bấm lại; bấm lại an toàn vì kho trả duplicate và gửi lại callback. Lịch sử được ghi theo quy tắc ở mục 2.13.  
4. Kho nguồn quyết định việc hủy: mọi yêu cầu hủy (kho đích từ chối, quá thời gian chờ) đều đi qua HQ, và HQ gọi kho nguồn trước. Kho nguồn chỉ hủy khi bản OUTBOUND còn CREATED: nhả giữ chỗ, chuyển CANCELLED, trả 200\. Nếu đã SHIPPED, kho nguồn trả 409 INVALID\_STATE và HQ trả 409 cho bên yêu cầu: hàng đã rời kho nên kho đích phải nhận rồi tạo phiếu trả nếu cần. Chỉ khi kho nguồn đồng ý, HQ mới ghi CANCELLED vào transfer\_summary và đồng bộ xuống kho đích. Bước xuất hàng là bước then chốt (pivot) của Saga: trước nó mọi bước đều bù được; sau nó chỉ còn các bước đi tiếp, được thử lại tới khi thành công.

Lệnh cập nhật trạng thái theo quy tắc 2:

```sql
UPDATE transfer_summary
SET status      = :new,
    shipped_at  = COALESCE(shipped_at,  :shipped_at),
    received_at = COALESCE(received_at, :received_at),
    updated_at  = now()
WHERE transfer_id = :id
  AND status = :prev_status;  -- trạng thái liền trước
```

### 3.3. Chịu lỗi khi dữ liệu bị sửa trực tiếp vào CSDL

Nếu có người chạy UPDATE inventory trực tiếp, không qua API, thì không có dòng outbox\_event và stock\_movement đi kèm: HQ không biết thay đổi, sổ nhập–xuất lệch với tồn kho. Hệ thống xử lý theo bốn lớp:

1. Chặn bằng phân quyền: ứng dụng dùng role riêng; tài khoản người dùng không có quyền INSERT, UPDATE, DELETE trên bảng nghiệp vụ; bản sao danh mục ở kho đã bị thu hồi quyền ghi. Các CHECK (qty\_on\_hand \>= 0, dc\_id đúng mảnh) vẫn áp dụng khi sửa tay.  
2. Dấu vết bằng version: trigger BEFORE UPDATE ở mục 2.7 luôn tăng version, kể cả khi sửa tay. Bản nộp không tự sinh dòng sổ và sự kiện cho thay đổi sửa tay; thay đổi đó được phát hiện ở lớp 3 (trigger tự ghi sổ là hướng phát triển). Quyền DELETE trên inventory bị thu hồi với mọi role.  
3. Phát hiện bằng đối soát định kỳ: tại kho, kiểm tra qty\_on\_hand bằng tổng qty\_change trong stock\_movement theo từng SKU; tại HQ, so inventory\_snapshot với số đọc qua FDW từng kho. Lệch thì cảnh báo trên Dashboard; snapshot là dữ liệu dẫn xuất nên được tính lại từ kho.  
4. Khôi phục: sửa nhầm thì ghi một giao dịch ADJUST ngược lại để bù, không xóa lịch sử; sai nghiêm trọng thì phục hồi về thời điểm trước đó bằng WAL archiving (PITR); bản sao danh mục ở kho bị sửa thì đồng bộ lại bảng đó từ HQ.

### 3.4. Xử lý sự kiện ở CDC Consumer

Consumer bảo đảm mỗi sự kiện có hiệu quả đúng một lần dù Kafka có thể giao lại, và không bao giờ ghi đè số mới bằng số cũ.

1. Consumer thuộc group hq-consumer, đọc topic outbox.event.INVENTORY, tắt tự commit offset (enable.auto.commit \= false). Topic outbox.event.TRANSFER không cần đọc.  
2. Với mỗi sự kiện, mở một transaction trên hq\_db và chạy INSERT INTO processed\_event … ON CONFLICT (event\_id) DO NOTHING RETURNING event\_id. Không có dòng trả về nghĩa là đã xử lý: commit và bỏ qua.  
3. Sự kiện InventoryChanged (và InventoryChangedDirect, loại dự phòng chưa sinh trong bản nộp): upsert inventory\_snapshot theo lệnh bên dưới. Nếu payload có request\_id thì chạy UPDATE pending\_txn SET status \= 'APPLIED', applied\_at \= now() WHERE txn\_id \= :request\_id AND status \= 'PENDING'.  
4. Commit transaction. Trạng thái phiếu điều chuyển do HQ API ghi trực tiếp, nên Consumer không xử lý sự kiện phiếu.  
5. Chỉ commit offset Kafka sau khi transaction trên hq\_db commit xong. Consumer chết giữa chừng thì sự kiện được giao lại và bước 2 chặn trùng.  
6. Sự kiện lỗi không thể thử lại (payload sai định dạng): ghi log, đẩy sang topic outbox.event.DLQ, commit offset để không chặn cả partition.

```sql
INSERT INTO inventory_snapshot AS s
  (dc_id, product_id, qty_on_hand, qty_reserved,
   last_version, last_event_id, synced_at)
VALUES (:dc, :p, :on_hand, :reserved, :version, :event_id, now())
ON CONFLICT (dc_id, product_id) DO UPDATE
SET qty_on_hand   = EXCLUDED.qty_on_hand,
    qty_reserved  = EXCLUDED.qty_reserved,
    last_version  = EXCLUDED.last_version,
    last_event_id = EXCLUDED.last_event_id,
    synced_at     = now()
WHERE s.last_version < EXCLUDED.last_version;
```

Vì sao so version thay cho thời gian: trong PostgreSQL, now() là thời điểm bắt đầu transaction chứ không phải lúc commit. Transaction bắt đầu trước nhưng commit sau sẽ mang thời điểm nhỏ hơn dù số của nó mới hơn, nên so theo thời gian sẽ bỏ nhầm sự kiện đúng. Version tăng theo thứ tự các lệnh UPDATE trên cùng một dòng, nên luôn phản ánh đúng thứ tự.

### 3.5. Định nghĩa phân mảnh và tính đúng

Với i ∈ {1, 2, 3} là mã kho, các mảnh được định nghĩa bằng phép chọn σ và phép nửa kết ⋉:

```text
inventory_i      = σ[dc_id = i](inventory)
stock_movement_i = σ[dc_id = i](stock_movement)
transfer_order_i = σ[(direction = 'OUTBOUND' ∧ from_dc_id = i) ∨ (direction = 'INBOUND' ∧ to_dc_id = i)](transfer_order)
transfer_item_i  = transfer_item ⋉[transfer_id] transfer_order_i
```

* Đầy đủ: mọi dòng inventory có dc\_id thuộc {1, 2, 3} nhờ khóa ngoại tới dc, nên thuộc ít nhất một mảnh.  
* Tái thiết: inventory \= inventory₁ ∪ inventory₂ ∪ inventory₃, hiện thực bằng view v\_inventory\_all.  
* Tách biệt: các mảnh inventory và stock\_movement rời nhau nhờ CHECK (dc\_id \= i). Riêng transfer\_order cố ý không tách biệt theo transfer\_id: mỗi phiếu có 2 bản ở 2 kho, nên khóa của quan hệ toàn cục là (transfer\_id, direction) và phép hợp phải giữ cột direction.  
* transfer\_item là phân mảnh ngang dẫn xuất: dòng hàng đi theo phiếu mà nó thuộc về, nên khóa ngoại transfer\_item → transfer\_order luôn kiểm tra được trong một node.

```sql
CREATE VIEW v_inventory_all AS
SELECT * FROM dc1.inventory
UNION ALL
SELECT * FROM dc2.inventory
UNION ALL
SELECT * FROM dc3.inventory;
```

View này minh họa trong suốt phân mảnh: người dùng truy vấn như một bảng duy nhất. Khi một kho chết, cả câu truy vấn trên view bị lỗi; đó là lý do Dashboard truy vấn từng kho riêng.
