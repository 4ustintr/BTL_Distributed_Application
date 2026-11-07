<!-- File sinh tự động từ Google Docs bằng scripts/sync_docs.py. Không sửa tay: sửa Google Docs rồi chạy lại script. -->

# Giải pháp phân tán


## 1\. Tại sao hệ thống cần phân tán?

* Kho nằm ở 3 vùng cách xa nhau: kho Bắc, Trung, Nam cách nhau hàng trăm km. Nếu mọi thao tác nhập, xuất, quét mã đều gọi về một máy chủ ở HQ thì độ trễ cao và phụ thuộc hoàn toàn vào đường truyền.  
* Kho phải tự hoạt động được: mất mạng hoặc HQ gặp sự cố thì kho vẫn phải nhập, xuất hàng. Với hệ tập trung, một điểm lỗi làm cả 3 kho dừng.  
* Dữ liệu vốn thuộc về từng kho: tồn kho và phiếu xuất nhập của kho nào do kho đó tạo ra và dùng nhiều nhất. Để dữ liệu ở gần nơi sử dụng thì hợp lý hơn dồn hết về một chỗ.  
* Giảm tải và dễ mở rộng: mỗi kho xử lý giao dịch của mình nên HQ không thành nút thắt cổ chai. Mở thêm kho thì chỉ cần thêm node, không phải nâng cấp máy chủ trung tâm.  
* Nhưng HQ vẫn cần nhìn toàn cảnh: HQ phải biết tồn kho cả nước để điều phối hàng giữa các kho. Vì vậy không thể tách các kho thành hệ thống rời rạc, mà phải là một hệ phân tán có phối hợp.

## 2\. Hệ thống phân tán ở phần nào?

| Phần | Phân tán như thế nào |
| :---- | :---- |
| Dữ liệu nghiệp vụ | Tồn kho, sổ nhập–xuất, phiếu điều chuyển được phân mảnh ngang theo kho: mỗi kho giữ phần dữ liệu của mình ở pg\_dc1, pg\_dc2, pg\_dc3 |
| Dữ liệu danh mục | Kho, sản phẩm, nhóm hàng được nhân bản: bản gốc ở HQ, bản sao chỉ đọc ở cả 3 kho |
| Dữ liệu tổng hợp | HQ giữ bản chụp tồn kho và bảng tổng hợp phiếu điều chuyển, được cập nhật từ sự kiện của các kho |
| Xử lý | Mỗi kho có Node API riêng xử lý nghiệp vụ tại chỗ; HQ có HQ API cho điều phối, ghi hộ và báo cáo |
| Giao dịch liên kho | Điều chuyển hàng trải qua nhiều node: HQ ra lệnh, kho xuất hàng, kho nhận xác nhận |

Phần không phân tán: danh mục chỉ được ghi tại HQ, nhờ đó không xảy ra xung đột khi ghi.

## 3\. Giải pháp phân tán nhóm sử dụng

Kiến trúc: nhiều node tự chủ có một trung tâm điều phối (hub-and-spoke). Mỗi node DC là một hệ thống hoàn chỉnh (WMS, API, CSDL), còn HQ đóng vai trò điều phối và tổng hợp.

| Vấn đề | Giải pháp | Công nghệ |
| :---- | :---- | :---- |
| Chia dữ liệu | Phân mảnh ngang chính theo dc\_id cho inventory, stock\_movement, transfer\_order; phân mảnh ngang dẫn xuất cho transfer\_item theo transfer\_order | PostgreSQL ở mỗi kho, CHECK ràng buộc mảnh |
| Đồng bộ danh mục HQ → kho | Nhân bản một nguồn ghi (single-leader), bất đồng bộ; bản sao ở kho chỉ đọc | Logical replication (PUBLICATION / SUBSCRIPTION) |
| Đồng bộ kho → HQ | Hướng sự kiện, publish–subscribe, nhất quán cuối cùng | Transactional outbox, Debezium CDC, Kafka, CDC Consumer |
| Truy vấn xuyên node | Truy vấn phân tán, trong suốt vị trí; mỗi kho một câu truy vấn riêng để một kho chết không làm hỏng kết quả của kho khác | postgres\_fdw, view v\_inventory\_all |
| Điều chuyển liên kho | Saga dạng điều phối (orchestration), HQ là bên điều phối; giao dịch bù khi hủy trước lúc xuất; bước xuất hàng là bước then chốt (pivot), sau bước này chỉ đi tiếp | Transfer Notice / Receipt Confirmation qua API, transfer\_id dạng UUID, transfer\_code từ sequence ở HQ |
| Điều khiển đồng thời trong một kho | Giữ chỗ (reservation) và khóa lạc quan theo version | UPDATE có điều kiện, trigger tăng version |
| Thứ tự sự kiện | Sự kiện của cùng một đối tượng đi cùng một partition Kafka; Consumer chỉ áp dụng sự kiện có version mới hơn | Kafka key \= aggregate\_id, cột last\_version |
| Phát hiện lỗi | Heartbeat và timeout; HQ và WMS cùng xác nhận kho chết trước khi chuyển sang ghi hộ | Health Monitor, GET /health, bảng dc\_status |
| Ghi hộ khi kho sập | Lưu tạm rồi chuyển tiếp (store-and-forward, tương tự hinted handoff của Amazon Dynamo); kho phục hồi ở trạng thái RECOVERING | Bảng pending\_txn, Replay Worker, cột request\_id UNIQUE |
| Chịu lỗi | Kafka giữ sự kiện khi HQ chết; consumer idempotent nên giao lại không bị tính trùng; kho vẫn chạy khi mất kết nối | Bảng processed\_event, outbox ghi cùng transaction |

Đánh đổi nhóm chấp nhận: chọn tính sẵn sàng và tự chủ của kho thay vì nhất quán tức thời. Số liệu ở HQ có thể trễ vài giây so với kho; khi cần số chính xác thì HQ truy vấn trực tiếp qua FDW.

Theo cách nhìn PACELC: khi có phân vùng mạng (P), nhóm chọn sẵn sàng (A) thay vì nhất quán (C); khi mạng bình thường (E), nhóm chọn độ trễ thấp (L), tức kho ghi cục bộ và không chờ HQ, thay vì nhất quán tức thời (C).

## 4\. Đối chiếu kiến thức môn học

Bảng dưới cho biết mỗi khái niệm của môn được dùng ở thành phần nào và được chứng minh ở kịch bản nào.

| Khái niệm | Thể hiện trong hệ thống | Chứng minh ở đâu |
| :---- | :---- | :---- |
| Phân mảnh ngang chính | inventory, stock\_movement, transfer\_order chia theo dc\_id | Thiết kế CSDL 2.7–2.9; Kịch bản 2 đọc từng mảnh |
| Phân mảnh ngang dẫn xuất | transfer\_item đi theo transfer\_order | Thiết kế CSDL 3.5 |
| Tính đúng của phân mảnh: đầy đủ, tái thiết, tách biệt | View v\_inventory\_all tái thiết tồn kho; transfer\_order cố ý không tách biệt | Thiết kế CSDL 3.5 |
| Nhân bản một nguồn ghi | Danh mục từ HQ xuống 3 kho | Logical replication; Kịch bản 3 bước 7 |
| Nhân bản dẫn xuất, nhất quán cuối cùng | inventory\_snapshot ở HQ | Kịch bản 2 |
| Trong suốt vị trí, trong suốt phân mảnh | postgres\_fdw, view v\_inventory\_all | Kịch bản 2 bước 5 |
| Giao dịch phân tán theo Saga: điều phối, giao dịch bù, bước then chốt | Chuyển hàng giữa các kho | Kịch bản 1 bước 1–5 |
| Hạn chế của 2PC: giữ khóa qua mạng diện rộng, bị chặn khi bên điều phối chết | Lý do chọn Saga | Mục 5 bên dưới |
| Điều khiển đồng thời lạc quan | inventory.version | Kịch bản 1 bước 2 |
| Gửi ít nhất một lần \+ xử lý idempotent \= hiệu quả đúng một lần | processed\_event, request\_id | Kịch bản 3 bước 12 |
| Vấn đề ghi kép và transactional outbox | outbox\_event ghi cùng transaction | Kịch bản 1, Dashboard cập nhật theo từng bước |
| Thứ tự sự kiện, đồng hồ logic | version theo từng dòng thay cho thời gian thực | Thiết kế CSDL 3.4 |
| Lỗi một phần, suy giảm từ từ | Một kho chết, phần còn lại vẫn phục vụ | Kịch bản 2 |
| Phát hiện lỗi bằng timeout; không phân biệt được node chết và node chậm | Health Monitor; docker pause so với docker kill | Kịch bản 2 bước 2, Kịch bản 3 bước 2 |
| CAP và PACELC | Chọn sẵn sàng khi kho không truy cập được | Kịch bản 3 |
| Hinted handoff, store-and-forward | pending\_txn và Replay Worker | Kịch bản 3 |
| Phục hồi dựa trên log, tính bền vững | WAL, replication slot, offset của Kafka | Kịch bản 3 bước 7–8 |
| Trạng thái toàn cục, snapshot Chandy–Lamport | Lý do chỉ kiểm tra bất biến ở thời điểm tĩnh | Đoạn mở đầu thẻ Kịch bản demo |

## 5\. Các phương án đã cân nhắc

Với mỗi vấn đề chính, nhóm đã so sánh ít nhất một phương án khác trước khi chọn.

| Vấn đề | Phương án khác | Vì sao không chọn |
| :---- | :---- | :---- |
| Điều chuyển liên kho | 2PC (PREPARE TRANSACTION của PostgreSQL) | Hai kho giữ khóa suốt lượt trao đổi qua mạng; HQ chết giữa pha 1 và pha 2 thì cả hai kho bị chặn |
| Điều chuyển liên kho | Saga dạng biên đạo (choreography), kho gọi thẳng nhau | Không nơi nào nắm trạng thái cả phiếu, khó theo dõi và khó dựng transfer\_summary; trong khi HQ vốn phải điều phối hàng |
| Đồng bộ kho → HQ | Node API vừa ghi CSDL kho vừa gọi HQ (ghi kép) | Một bên thành công, một bên lỗi là lệch vĩnh viễn; outbox đưa hai việc vào cùng một transaction |
| Đồng bộ kho → HQ | Tác vụ quét bảng outbox định kỳ (polling) | Tăng tải lên CSDL kho, độ trễ phụ thuộc chu kỳ quét; CDC đọc WAL nên gần thời gian thực |
| Đồng bộ kho → HQ | Logical replication thẳng bảng inventory lên HQ | Làm được, nhưng HQ chỉ nhận thay đổi dòng, không có sự kiện nghiệp vụ và request\_id để khép vòng pending\_txn, khó thêm bên tiêu thụ khác |
| Dashboard | Chỉ đọc qua FDW | Chậm hơn, và kho chết thì mất luôn số của kho đó |
| Danh mục | Cho mỗi kho tự sửa danh mục (nhiều nguồn ghi) | Phải giải quyết xung đột ghi; danh mục ít thay đổi nên một nguồn ghi là đủ |
| Kho sập | Từ chối mọi giao dịch của kho sập (chọn nhất quán) | Kho đứng im suốt thời gian sửa máy, trong khi xe hàng không chờ được |
| Đồng thời trong một kho | Khóa bi quan SELECT … FOR UPDATE | Cũng đúng; nhóm chọn khóa lạc quan vì giao dịch ngắn, ít xung đột và cho thấy rõ việc kiểm tra version |
