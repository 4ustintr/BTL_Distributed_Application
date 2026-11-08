# Kịch bản demo

Thứ tự trình bày: so với thứ tự đã báo, nhóm xếp lại để số liệu nối tiếp nhau. Chuyển hàng giữa các kho chạy trước, khi hệ thống còn bình thường. Sau đó kho Trung sập, HQ xem số liệu qua snapshot, rồi giao dịch của kho Trung đi qua trung tâm và được đồng bộ lại khi kho hoạt động trở lại.

Dữ liệu ban đầu (SKU A, đơn vị thùng): Bắc 100, Trung 40, Nam 10\. Tổng 150\.

Bất biến dùng xuyên suốt, cho từng SKU: tổng tồn 3 kho \+ hàng đang đi đường \+ hàng đã xuất cho khách \= tổng hàng đã nhập từ nhà cung cấp. Lúc đầu tổng đã nhập là 150, vì dữ liệu ban đầu cũng được nhập qua API. Lệnh make check-invariant tính bốn số này: tồn lấy từ 3 bảng inventory qua FDW; đi đường lấy từ view v\_in\_transit; đã xuất là tổng OUTBOUND trong stock\_movement (ref\_type ISSUE và REPLAY); đã nhập là tổng INBOUND. Lệnh chỉ chạy được khi cả 3 kho sống; trong lúc kho Trung sập thì đọc số trên Dashboard.

Bất biến chỉ được kiểm tra sau khi mỗi bước hoàn tất: FDW đọc từng kho ở những thời điểm khác nhau, nên đọc giữa lúc một giao dịch đang chạy có thể ra số lệch. Hệ phân tán không có ảnh chụp trạng thái toàn cục tức thời nếu không dùng thuật toán riêng, ví dụ snapshot Chandy–Lamport. Kết quả chuẩn của từng bước nằm ở thẻ Kiểm thử và đánh giá, mục 3\.

## Chuẩn bị trước demo

- [ ] Chạy make reset theo đúng thứ tự ở F12 (máy A, máy B, connector, seed); make seed chờ 3 kho ONLINE rồi mới nhập Bắc 100, Trung 40, Nam 10 qua API.  
- [ ] Kafka Connect: 3 connector dc1-outbox, dc2-outbox, dc3-outbox đều RUNNING.  
- [ ] Dashboard: 3 kho ONLINE, số trên snapshot khớp với số khi bấm Đối soát.  
- [ ] make check-invariant trả DIFF \= 0 và dòng trạng thái khớp bước KB1-0 ở thẻ Kiểm thử và đánh giá.  
- [ ] make test chạy xanh toàn bộ trên đúng bản code sẽ demo.  
- [ ] Đồng hồ hai máy được đồng bộ bằng NTP (chrony) nếu trình bày số đo độ trễ.  
- [ ] Mở sẵn Dashboard, WMS 3 kho, Kafka UI, terminal máy B, log HQ API; tắt chế độ ngủ trên hai máy và dùng mạng riêng (router hoặc hotspot) thay cho Wi-Fi của trường.

## Kịch bản 1: Giao dịch chuyển hàng giữa các chi nhánh

### Mục tiêu

Chứng minh một nghiệp vụ ghi vào CSDL của 2 kho khác nhau vẫn cho kết quả đúng mà không cần khóa xuyên node. HQ điều phối theo mô hình Saga.

### Mô tả kịch bản

Kho Nam sắp hết SKU A. HQ ra lệnh chuyển 50 thùng từ Bắc sang Nam. Trong lúc phiếu đang xử lý, hai nhân viên kho Bắc cùng lúc xuất hàng cho hai đại lý. Sau đó HQ tạo thêm một phiếu chuyển sang kho Trung nhưng phải hủy giữa chừng.

### Các bước thực hiện

Bước 0 (tùy chọn), vì sao cần khóa: trên dữ liệu vừa reset, chạy make race-unsafe. Node API chạy với LOCK\_MODE=none (đọc rồi ghi đè, không kiểm tra version), bắn cùng lúc 2 lệnh xuất 30 thùng vào kho Bắc. Cả hai cùng thành công nhưng tồn chỉ giảm 30, trong khi sổ ghi 60; make check-invariant báo lệch 30\. Đây là lỗi mất cập nhật (lost update). Chạy make reset rồi mới sang bước 1\.

1. HQ tạo phiếu: Dashboard gọi POST /transfers (Bắc → Nam, SKU A, 50 thùng). HQ ghi transfer\_summary CREATED với mã TR-2026-000001, rồi gửi Transfer Notice tới kho Bắc trước, kho Nam sau. Kho Bắc giữ chỗ 50 thùng (tồn 100, khả dụng 50); kho Nam tạo phiếu INBOUND CREATED.  
2. Tranh chấp đồng thời: make race bắn cùng lúc 2 lệnh POST /outbound 30 thùng, với 2 request\_id khác nhau, vào kho Bắc. Một lệnh thành công: tồn 70, khả dụng 20\. Lệnh còn lại gặp xung đột version, đọc lại, thấy khả dụng 20 \< 30 nên trả 422 INSUFFICIENT\_STOCK; log Node API hiện một dòng VERSION\_CONFLICT rồi một dòng INSUFFICIENT\_STOCK.  
3. Kho Bắc xuất hàng: nhân viên kho Bắc bấm Xuất trên phiếu (POST /transfers/{id}/ship). Trong một transaction: phiếu OUTBOUND chuyển SHIPPED, tồn 70 → 20, giữ chỗ 50 → 0, ghi 1 dòng TRANSFER\_OUT và 2 sự kiện outbox. Kho Bắc báo HQ qua /transfers/{id}/shipped; HQ chuyển transfer\_summary sang SHIPPED và gọi /sync-status cho kho Nam. Dashboard hiện 50 thùng đang đi đường tới kho Nam.  
4. Kho Nam nhận hàng: nhân viên kho Nam bấm Nhận (POST /transfers/{id}/receive). Node API Nam xác nhận phiếu đã SHIPPED, rồi trong một transaction: phiếu INBOUND chuyển RECEIVED, tồn 10 → 60, ghi 1 dòng TRANSFER\_IN. Kho Nam gửi Receipt Confirmation qua /transfers/{id}/received; HQ chuyển RECEIVED và đồng bộ trạng thái xuống kho Bắc.  
5. Hủy phiếu và giao dịch bù: HQ tạo phiếu thứ 2 chuyển 10 thùng Bắc → Trung; kho Bắc giữ chỗ 10 (khả dụng 20 → 10), kho Trung tạo phiếu INBOUND. Nhân viên kho Trung bấm Từ chối (lý do "hết chỗ chứa"); Node API Trung gọi POST /transfers/{id}/decline trên HQ. HQ gọi /transfers/{id}/cancel ở kho Bắc; phiếu OUTBOUND còn CREATED nên kho Bắc chạy giao dịch bù: nhả 10 thùng giữ chỗ (khả dụng về 20), phiếu chuyển CANCELLED. HQ ghi CANCELLED với cancel\_reason DECLINED rồi đồng bộ xuống kho Trung. Nếu kho Bắc đã xuất hàng thì /cancel trả 409 và HQ báo kho Trung không thể từ chối vì hàng đã rời kho (trình bày bằng lời hoặc chạy test TC06).

Sau mỗi bước, chạy make check-invariant; cột Bất biến trong bảng dưới lấy từ kết quả của lệnh này.

### Kết quả mong đợi

Số liệu đọc sau khi mỗi bước hoàn tất:

| Sau bước | Bắc (khả dụng) | Trung | Nam | Đang đi đường | Đã xuất cho khách | Bất biến |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| Ban đầu | 100 (100) | 40 | 10 | 0 | 0 | 150 |
| 1 | 100 (50) | 40 | 10 | 0 | 0 | 150 |
| 2 | 70 (20) | 40 | 10 | 0 | 30 | 150 |
| 3 | 20 (20) | 40 | 10 | 50 | 30 | 150 |
| 4 | 20 (20) | 40 | 60 | 0 | 30 | 150 |
| 5 | 20 (20) | 40 | 60 | 0 | 30 | 150 |

* Bất biến luôn bằng 150 sau mỗi bước.  
* Không có lúc nào hàng bị âm hay bị bán vượt.  
* Phiếu bị hủy không để lại phần giữ chỗ hay dữ liệu lệch ở kho nào.

### Kiến thức thể hiện

* Saga dạng điều phối (orchestration): HQ điều phối chuỗi giao dịch cục bộ; một bước thất bại thì chạy giao dịch bù thay vì rollback toàn cục. Không dùng 2PC.  
* Bước then chốt (pivot) của Saga: trước khi xuất hàng, mọi bước đều có giao dịch bù; sau khi xuất, phiếu chỉ đi tiếp. Kho nguồn là bên quyết định có hủy được hay không vì kho nguồn đang giữ hàng.  
* Điều khiển đồng thời trong một mảnh: giữ chỗ và khóa lạc quan, chứng minh bằng 2 request chạy song song thật.  
* Mất cập nhật khi thiếu điều khiển đồng thời (bước 0), và cách khóa lạc quan ngăn lỗi này (bước 2).  
* Bất biến để kiểm chứng tính đúng, và giới hạn của việc đọc trạng thái toàn cục.

### Ý nghĩa thực tế

Điều chuyển hàng giữa các kho logistics; đặt vé, đặt phòng và đơn thương mại điện tử đều đi theo chuỗi giữ chỗ, xác nhận, rồi hoàn tất hoặc hủy.

## Kịch bản 2: Chi nhánh sập, trung tâm xem qua snapshot

### Mục tiêu

Chứng minh khi một kho sập hẳn, các kho khác vẫn chạy, và HQ vẫn có số liệu của kho đó nhờ bản sao, kèm thời điểm của số liệu.

### Mô tả kịch bản

Kho Trung vừa xuất 5 thùng cho đại lý thì máy chủ mất điện. Ban điều hành ở HQ vẫn cần báo cáo tồn kho toàn quốc để ra quyết định.

### Các bước thực hiện

1. Kho Trung xuất 5 thùng: Trung 40 → 35\. Dashboard (đọc snapshot) hiện 35\. Chờ tới khi Dashboard hiện 35 kèm thời điểm cập nhật mới rồi mới làm bước tiếp, để chắc chắn sự kiện đã về HQ trước khi kho sập.  
2. Biến thể node treo (nếu còn thời gian): chạy docker pause pg-dc2 rồi bấm Đối soát. Truy vấn tới kho Trung chờ khoảng 3 giây (connect\_timeout) rồi báo timeout, hai kho còn lại vẫn trả số. Chạy docker unpause pg-dc2 để trả lại. Biến thể này chỉ đúng khi foreign server đặt keep\_connections 'off'; nếu không, HQ dùng lại kết nối cũ và truy vấn treo. Nếu tạm dừng quá 6 giây, Dashboard thấy kho Trung chuyển OFFLINE rồi ONLINE trở lại sau khi unpause; đây cũng là một minh họa đúng của cơ chế phát hiện lỗi.  
3. Gây sập: docker kill pg-dc2 node-api-dc2, sập đột ngột cả CSDL lẫn API của kho Trung. Khoảng 6 giây sau (3 lần health check thất bại liên tiếp), Dashboard chuyển kho Trung sang OFFLINE kèm dòng "offline từ hh:mm".  
4. Các kho khác không bị ảnh hưởng: kho Bắc và kho Nam vẫn tra cứu, nhập, xuất bình thường.  
5. HQ bấm Đối soát: HQ chạy 3 truy vấn FDW riêng, mỗi kho một câu. Bắc 20 và Nam 60 lấy trực tiếp từ kho; kho Trung báo lỗi ngay (connection refused) nên Dashboard dùng snapshot: 35, nhãn "offline, dữ liệu lúc hh:mm". Không gộp 3 bảng trong một câu UNION ALL, vì chỉ cần một kho chết là cả câu lỗi và mất luôn số của hai kho còn sống.  
6. transfer\_summary vẫn hiện đủ các phiếu liên quan đến kho Trung.  
7. Kafka Connect: task của connector dc2-outbox chuyển FAILED sau khoảng 15–30 giây (connector vẫn báo RUNNING), khi hết 3 lần thử lại (errors.max.retries \= 3); dc1-outbox và dc3-outbox vẫn RUNNING. Nếu tới bước này task chưa FAILED thì nói trước điều sẽ xảy ra và quay lại Kafka UI sau.

### Kết quả mong đợi

* Chỉ kho Trung dừng; HQ vẫn trả lời được toàn quốc còn bao nhiêu hàng: 20 \+ 35 \+ 60 \= 115 thùng tồn, cộng 35 thùng đã xuất cho khách (30 \+ 5), đúng bằng 150\.  
* Số 35 trên snapshot đúng bằng số thật tại thời điểm sập; điều này được kiểm chứng ở cuối Kịch bản 3\.

### Kiến thức thể hiện

* Lỗi một phần (partial failure): đặc trưng của hệ phân tán theo van Steen & Tanenbaum. Một node hỏng mà toàn hệ không dừng.  
* Nhân bản để tăng tính sẵn sàng: snapshot là bản sao của mảnh dữ liệu kho Trung, nằm ở HQ.  
* Đánh đổi độ mới và tính sẵn sàng: FDW cho số mới nhất nhưng phụ thuộc kho còn sống; snapshot luôn đọc được nhưng có thể cũ.  
* Phân biệt lỗi sập và lỗi treo, và vai trò của timeout trong việc phát hiện lỗi.  
* Suy giảm từ từ (graceful degradation): hệ thống vẫn phục vụ, chỉ đánh dấu rõ phần dữ liệu không còn mới.

### Ý nghĩa thực tế

Dashboard của doanh nghiệp vẫn phải trả lời được khi một nguồn dữ liệu ngừng hoạt động; nhãn "dữ liệu tính đến lúc…" cho người dùng biết độ tin cậy của con số.

## Kịch bản 3: Chi nhánh sập, giao dịch đi qua trung tâm, chi nhánh lên thì đồng bộ lại

### Mục tiêu

Chứng minh kho sập vẫn giao dịch được. HQ nhận và lưu tạm các giao dịch đó, đẩy về khi kho hoạt động lại, không mất và không ghi trùng.

### Mô tả kịch bản

Trong lúc máy chủ kho Trung còn hỏng, xe của nhà cung cấp tới và một đại lý đến lấy hàng. WMS của kho Trung chuyển sang gửi giao dịch lên HQ.

### Các bước thực hiện

1. WMS chuyển hướng: health check tới Node API Trung thất bại 2 lần liên tiếp (khoảng 4 giây), WMS chuyển sang gọi POST /failover/dc/2/inbound hoặc /outbound trên HQ API. Mỗi giao dịch mang một request\_id do WMS sinh ra. Nếu lúc đó HQ chưa kịp đánh dấu OFFLINE (HQ cần khoảng 6 giây), HQ trả 409 DC\_NOT\_OFFLINE; WMS hiện "đang xác nhận trạng thái kho" và tự gửi lại sau 2 giây.  
2. HQ kiểm tra chéo: HQ chỉ nhận failover khi chính HQ cũng xác định kho Trung OFFLINE. Nếu HQ vẫn thấy kho Trung sống, HQ từ chối và báo lỗi nằm ở phía WMS.  
3. Nhập 30 thùng: HQ ghi một dòng pending\_txn (PENDING, txn\_id \= request\_id).  
4. Xuất 10 thùng: HQ tính khả dụng tạm tính 35 \+ 30 \= 65 ≥ 10, rồi ghi pending\_txn bằng INSERT … ON CONFLICT (txn\_id) DO NOTHING. ON CONFLICT giúp WMS gửi lại cùng request\_id không bị ghi hai lần.  
5. Xuất 100 thùng: HQ từ chối ngay vì 65 − 10 \= 55 \< 100\.  
6. Dashboard: kho Trung hiện 55 (tạm tính), bằng snapshot 35 cộng 2 giao dịch đang chờ.  
7. Kho lên lại: chạy docker start pg-dc2 node-api-dc2. PostgreSQL tự phục hồi từ WAL, dữ liệu đã commit trước khi sập còn nguyên (35). Node API khởi động ở trạng thái RECOVERING: từ chối mọi giao dịch từ WMS (503 DC\_RECOVERING, "kho đang đồng bộ") nhưng nhận /replay từ HQ. Health Monitor thấy kho Trung trả lời lại với node\_mode \= RECOVERING nên chuyển dc\_status sang RECOVERING và ngừng nhận failover cho kho này, nhờ vậy không có giao dịch mới nào chen vào giữa lúc đang đẩy lại. Subscription danh mục tự kết nối lại.  
8. Khởi động lại connector: POST /connectors/dc2-outbox/restart?includeTasks=true\&onlyFailed=true. Connector RUNNING trở lại và đọc tiếp từ replication slot.  
9. Đẩy lại giao dịch: Replay Worker lấy các pending\_txn của kho Trung đang PENDING theo thứ tự created\_at, gửi từng dòng tới POST /replay. Kho trả 200 thì gửi dòng kế tiếp (dòng chưa APPLIED sẽ được gửi lại ở vòng sau, an toàn vì kho chặn trùng theo request\_id); kho trả 422 thì HQ đánh dấu REJECTED kèm lý do; lỗi mạng thì dừng, thử lại sau 2 giây và không nhảy sang dòng sau, để giữ đúng thứ tự. Ở kho, lệnh replay chạy đúng quy trình nhập/xuất thường với ref\_type REPLAY và request\_id \= txn\_id; lệnh bị từ chối thì rollback toàn bộ, không để lại dòng sổ nào.  
10. Khép vòng: CDC đưa sự kiện về HQ. Consumer thấy request\_id thì chuyển pending\_txn sang APPLIED trong cùng transaction với việc cập nhật snapshot, nên Dashboard không cộng hai lần hay tụt số. Dashboard đổi từ "55 (tạm tính)" thành 55\.  
11. Chuyển về bình thường: khi kho Trung không còn giao dịch PENDING, HQ gọi POST /recovery/complete. Kho chuyển ONLINE, WMS quay lại gọi Node API của kho; Health Monitor thấy node\_mode \= ONLINE ở lần health check kế tiếp và chuyển dc\_status sang ONLINE.  
12. Kiểm tra chống trùng: chạy make replay-again. Script gọi thẳng POST /replay của kho Trung với 2 request\_id cũ (Replay Worker không tự gửi lại dòng đã APPLIED). Kho trả 200 kèm duplicate: true; tồn vẫn 55\.  
13. Đối soát: FDW tới kho Trung ra 55, khớp với snapshot; điều này cũng xác nhận snapshot 35 ở Kịch bản 2 là đúng. Chạy make check-invariant: tồn 135 \+ đi đường 0 \+ đã xuất 45 \= đã nhập 180\.

### Kết quả mong đợi

| Thời điểm | Trung thật (pg\_dc2) | HQ hiển thị | pending\_txn | Trạng thái kho |
| :---- | :---- | :---- | :---- | :---- |
| Lúc sập | 35 | 35 (offline) | 0 | OFFLINE |
| Sau bước 5 | không truy cập được | 55 (tạm tính) | 2 PENDING | OFFLINE |
| Sau bước 7 | 35 | 55 (tạm tính) | 2 PENDING | RECOVERING |
| Sau bước 11 | 55 | 55 | 2 APPLIED | ONLINE |

* Bất biến cuối: 20 \+ 55 \+ 60 \= 135 thùng tồn, cộng 45 thùng đã xuất cho khách (30 \+ 5 \+ 10\) bằng 180, đúng bằng 150 ban đầu cộng 30 thùng mới nhập.  
* Giao dịch phát sinh trong lúc sập không bị mất; gửi lại nhiều lần không bị ghi trùng.  
* Sau khi đồng bộ, CSDL kho Trung và HQ khớp nhau.

### Trường hợp cần nói rõ

Nếu WMS mất kết nối nhưng kho thật ra vẫn sống, và HQ cũng không thấy kho (kho bị cô lập về mạng), thì có hai nơi cùng nhận ghi cho kho Trung. Khi đẩy lại, kho kiểm tra theo số thật; giao dịch nào không đủ hàng bị REJECTED và hiện trên Dashboard để đối soát thủ công. Nhóm chấp nhận rủi ro này vì đây là cái giá của việc chọn sẵn sàng thay vì nhất quán.

### Kiến thức thể hiện

* Failover và store-and-forward: Amazon Dynamo gọi cơ chế tương tự là hinted handoff, tức node khác nhận ghi hộ rồi trả lại khi node gốc sống lại.  
* Định lý CAP và xử lý xung đột khi hợp nhất dữ liệu.  
* Idempotency bằng cột request\_id UNIQUE.  
* Phục hồi dựa trên log: WAL, replication slot và Kafka giữ lại thay đổi để phát lại.  
* Tính bền vững (durability) của dữ liệu đã commit.  
* Trạng thái phục hồi (RECOVERING) để bảo đảm thứ tự áp dụng giao dịch.

### Ý nghĩa thực tế

* Máy POS ở cửa hàng vẫn bán khi mất kết nối rồi đồng bộ lại khi có mạng.  
* Chi nhánh ngân hàng gặp sự cố thì trung tâm xử lý tạm rồi đối soát sau.  
* Dynamo dùng hinted handoff để giỏ hàng luôn ghi được.

Số đo (nếu trình bày): thời gian từ docker kill tới khi dc\_status chuyển OFFLINE; thời gian từ docker start tới khi dc\_status chuyển ONLINE; số giao dịch được đẩy lại. Cách đo xem thẻ Kiểm thử và đánh giá.

## Kịch bản dự phòng: Trung tâm sập

Chỉ demo khi còn thời gian hoặc khi thầy hỏi "HQ chết thì sao".

### Mục tiêu

Chứng minh kho không cần HQ để nhập, xuất, và sự kiện không mất khi HQ chết.

### Các bước thực hiện

1. Trên máy A, chạy docker stop hq-api hq-consumer. Kafka và Kafka Connect vẫn chạy.  
2. Kho Bắc xuất 5 thùng cho đại lý: tồn 20 → 15\. WMS và Node API Bắc không bị ảnh hưởng.  
3. Kafka UI: topic outbox.event.INVENTORY có thêm 1 message; consumer group hq-consumer có lag \= 1\.  
4. Thử tạo phiếu điều chuyển trên Dashboard: không được vì HQ API không chạy, đúng như thiết kế.  
5. Chạy docker start hq-api hq-consumer. Consumer đọc tiếp từ offset đã commit, lag về 0, Dashboard hiện kho Bắc 15\.

### Kiến thức thể hiện

Tách rời theo thời gian nhờ hàng đợi bền vững (Kafka); consumer offset giúp đọc tiếp đúng chỗ; kho tự chủ, không phụ thuộc trung tâm cho nghiệp vụ hằng ngày.
