# Hệ thống quản lý mạng lưới kho phân tán

## Bài tập lớn môn Phát triển ứng dụng phân tán · Trung tâm điều hành (HQ) và ba kho Bắc, Trung, Nam

Đề tài xây dựng hệ thống quản lý mạng lưới kho phân phối gồm một trung tâm điều hành (HQ) và ba kho ở miền Bắc, miền Trung, miền Nam. Mỗi kho tự lưu và xử lý dữ liệu của mình; trung tâm tổng hợp số liệu cả nước và điều phối hàng giữa các kho.

## 1\. Bài toán

Một công ty logistics có ba kho lớn đặt cách nhau hàng trăm km. Mỗi ngày các kho nhập hàng từ nhà cung cấp, xuất hàng cho đại lý và chuyển hàng cho nhau. Trụ sở cần biết tồn kho cả nước để quyết định chuyển hàng từ đâu sang đâu.

Có hai cách làm đơn giản, nhưng cả hai đều có vấn đề:

* Dồn một CSDL duy nhất ở trụ sở: kho ở xa thao tác chậm; mất mạng hoặc máy chủ trụ sở hỏng thì cả ba kho cùng dừng.  
* Mỗi kho một hệ thống riêng, không nối với nhau: trụ sở không thấy toàn cảnh, danh mục hàng mỗi nơi một kiểu, chuyển hàng giữa hai kho dễ lệch số.

## 2\. Ý tưởng: kho tự chủ, trung tâm nhìn toàn cảnh

* Mỗi kho có CSDL riêng và xử lý giao dịch ngay tại chỗ, nên thao tác nhanh và vẫn chạy được khi mất kết nối với trung tâm.  
* Mọi thay đổi ở kho được gửi dần về trung tâm. Trung tâm luôn có bản tổng hợp tồn kho cả nước, chỉ trễ vài giây so với kho.  
* Danh mục sản phẩm chỉ được sửa ở trung tâm, rồi tự động phát xuống cả ba kho, nên mọi nơi dùng chung một bộ mã hàng.  
* Chuyển hàng giữa hai kho do trung tâm điều phối theo từng bước có xác nhận: ra lệnh, kho gửi xuất hàng, kho nhận xác nhận đã nhận. Trước khi kho gửi xuất hàng, bước nào lỗi thì hủy phiếu và trả lại phần hàng đã giữ chỗ. Khi kho gửi đã xuất hàng thì phiếu không hủy được nữa mà chỉ đi tiếp tới lúc kho nhận xác nhận; muốn trả hàng thì tạo một phiếu điều chuyển theo chiều ngược lại.

Có thể hình dung giống một chuỗi chi nhánh: mỗi chi nhánh tự ghi sổ của mình, còn trụ sở nhận báo cáo liên tục và chỉ đạo khi cần chuyển hàng.

## 3\. Hệ thống gồm những gì

Hệ thống có tám thành phần chính:

| Thành phần | Đặt ở đâu | Làm gì |
| :---- | :---- | :---- |
| WMS | Mỗi kho | Màn hình cho nhân viên kho nhập, xuất, tra cứu hàng; tự chuyển sang gửi giao dịch lên trung tâm khi không gọi được Node API của kho |
| Node API và pg\_dc | Mỗi kho | Xử lý nghiệp vụ và lưu dữ liệu của riêng kho đó |
| HQ API và hq\_db | Trung tâm | Điều phối chuyển hàng, lưu danh mục gốc và bản tổng hợp |
| Dashboard | Trung tâm | Xem tồn kho cả nước và tình trạng từng kho |
| Debezium và Kafka | Trung tâm | Đọc sự kiện từ bảng outbox của từng kho và chuyển lên trung tâm |
| CDC Consumer | Trung tâm | Ghi các thay đổi nhận được vào bản tổng hợp |
| Health Monitor | Trung tâm, chạy nền trong HQ API | Hỏi thăm từng kho mỗi 2 giây, xác định kho đang ONLINE, OFFLINE hay RECOVERING |
| Replay Worker | Trung tâm, chạy nền trong HQ API | Đẩy lại giao dịch nhận hộ về kho khi kho sống lại, theo đúng thứ tự |

Khi demo, máy A chạy phần trung tâm, máy B chạy ba kho; mọi trao đổi giữa trung tâm và kho đều đi qua mạng thật giữa hai máy. Tên container, cổng và địa chỉ của từng thành phần được cố định trong thẻ Đặc tả kỹ thuật.

## 4\. Dữ liệu được phân tán thế nào

* Chia nhỏ (phân mảnh): tồn kho, phiếu chuyển hàng và sổ nhập–xuất của kho nào thì nằm ở CSDL của kho đó.  
* Sao chép (nhân bản): danh mục có bản gốc ở trung tâm và bản sao ở ba kho; tồn kho có bản gốc ở kho và bản tổng hợp ở trung tâm.  
* Đọc chéo: khi cần số chính xác ngay lúc đó, trung tâm đọc thẳng từ CSDL của từng kho.

## 5\. Dữ liệu đi lại giữa kho và trung tâm

Kho và trung tâm trao đổi dữ liệu qua bốn đường:

| Đường đi | Chiều | Mang gì | Công nghệ |
| :---- | :---- | :---- | :---- |
| CDC | Kho → trung tâm | Thay đổi tồn kho (sự kiện phiếu chỉ để quan sát) | Outbox, Debezium, Kafka |
| Logical replication | Trung tâm → kho | Danh mục kho, sản phẩm, nhóm hàng | PostgreSQL |
| Truy vấn trực tiếp | Trung tâm đọc kho | Số tức thời để đối soát | postgres\_fdw |
| Gọi API | Hai chiều | Lệnh chuyển hàng, xác nhận, kiểm tra kho còn sống, đẩy lại giao dịch | HTTP (FastAPI) |

## 6\. Khi có sự cố

* Mất mạng giữa trung tâm và kho: kho vẫn nhập, xuất bình thường; trung tâm hiện số cũ kèm thời điểm cập nhật; có mạng lại thì tự cập nhật đủ.  
* Một kho sập: hai kho còn lại không bị ảnh hưởng; trung tâm vẫn xem số của kho đó qua bản tổng hợp. Giao dịch của kho sập được gửi lên trung tâm lưu tạm, kho hoạt động lại thì đẩy về.  
* Trung tâm sập: ba kho vẫn nhập, xuất bình thường nhưng không tạo được phiếu điều chuyển mới. Sự kiện từ kho nằm chờ trong Kafka; khi trung tâm chạy lại, CDC Consumer đọc tiếp từ vị trí đã dừng; phiếu điều chuyển đang dở lúc HQ sập được xử lý thủ công (xem mục 8).  
* Kho vừa khởi động lại: kho tạm dừng nhận giao dịch mới (trạng thái RECOVERING) cho tới khi trung tâm đẩy hết các giao dịch nhận hộ, để giao dịch được áp dụng đúng thứ tự. Nếu trung tâm không chạy, quản trị viên chuyển kho về bình thường bằng một lệnh gọi tay (xem mục 8).  
* Gửi trùng hoặc có người sửa tay vào CSDL: mỗi giao dịch có mã riêng nên gửi lại không bị ghi hai lần; trung tâm đối soát với kho (nút Đối soát trên Dashboard) và lấy số ở kho làm chuẩn.

Nhóm chấp nhận một đánh đổi: ưu tiên để kho luôn hoạt động, đổi lại số ở trung tâm có thể trễ vài giây so với kho. Đây là lựa chọn "sẵn sàng thay vì nhất quán tức thời" theo định lý CAP.

## 7\. Demo gồm 3 kịch bản

1. Chuyển hàng giữa các kho: trung tâm điều phối chuyển 50 thùng từ kho Bắc sang kho Nam, có tranh chấp khi hai người cùng xuất hàng và có hủy phiếu trước khi xuất hàng.  
2. Kho sập, trung tâm xem qua bản tổng hợp: tắt hẳn kho Trung, hai kho kia vẫn chạy, trung tâm vẫn báo cáo được tồn kho cả nước.  
3. Kho sập, giao dịch đi qua trung tâm: kho Trung vẫn nhập, xuất bằng cách gửi lên trung tâm; khi kho hoạt động lại, giao dịch được đẩy về đủ và không bị trùng.

Chi tiết xem ở các thẻ Thiết kế CSDL, Đặc tả kỹ thuật, Kịch bản demo, Kiểm thử và đánh giá, HLD.

## 8\. Giới hạn và hướng phát triển

Nhóm tự nêu các giới hạn dưới đây để làm rõ phạm vi của thiết kế; mỗi giới hạn đi kèm hướng khắc phục nếu phát triển tiếp.

| Giới hạn | Ảnh hưởng | Hướng phát triển |
| :---- | :---- | :---- |
| HQ là điểm lỗi duy nhất cho điều phối và ghi hộ | HQ chết thì không tạo được phiếu điều chuyển và không ghi hộ được cho kho sập | Nhân bản hq\_db (streaming replication kèm công cụ chuyển đổi dự phòng như Patroni), chạy nhiều bản HQ API sau load balancer |
| Kafka chỉ có 1 broker, replication factor 1 | Broker chết thì CDC dừng; sự kiện vẫn nằm trong replication slot của kho, chưa mất | 3 broker, replication factor 3, min.insync.replicas \= 2 |
| Phân vùng mạng làm hai nơi cùng nhận ghi cho một kho | Có thể xuất vượt; chỉ phát hiện lúc đẩy lại (REJECTED) | Hạn mức ghi hộ, hoặc lease kèm fencing token để kho tự dừng ghi khi mất liên lạc với HQ |
| Không hỗ trợ xuất thiếu, nhận thiếu | Chênh lệch thực tế khi vận chuyển không ghi được | Thêm trạng thái PARTIALLY\_RECEIVED và phiếu điều chỉnh chênh lệch |
| Danh mục mới được tạo khi một kho đang sập | Lệnh đẩy lại có thể nhắc tới SKU mà kho chưa nhận được | Kho ở trạng thái RECOVERING chờ subscription bắt kịp (pg\_stat\_subscription) trước khi nhận replay |
| Đồng hồ giữa hai máy | Số đo độ trễ sai nếu hai máy lệch giờ | NTP, hoặc đo trên cùng một máy |
| Bảo mật tối giản | Một khóa nội bộ dùng chung, chưa có TLS; pg\_hba cho phép mọi địa chỉ (bắt buộc scram-sha-256), vì Docker Desktop đi qua NAT | Giới hạn pg\_hba theo HOST\_A\_IP, HOST\_B\_IP khi chạy Docker trên Linux không qua NAT; mTLS giữa các service, khóa riêng cho từng kho, TLS cho kết nối PostgreSQL |
| Replication slot giữ WAL khi connector hoặc subscriber dừng lâu | Có thể đầy đĩa | Đã giới hạn bằng max\_slot\_wal\_keep\_size; thêm cảnh báo theo dõi dung lượng đĩa |
| Demo chỉ có 1 SKU | Chưa đánh giá với dữ liệu lớn | Sinh nhiều SKU và đo lại độ trễ, thông lượng |

Ngoài các giới hạn trên, bản nộp chủ động lược bỏ một số cơ chế để vừa sức thời gian, và xem đây là hướng phát triển: tác vụ Saga Recovery tự gửi lại Transfer Notice và tự hủy phiếu quá hạn; cập nhật trạng thái phiếu qua CDC song song với HTTP; kho tự chuyển ONLINE khi HQ chết; khóa dòng khi HQ ghi hộ nhiều lệnh cùng lúc; trigger tự sinh dòng sổ và sự kiện cho thay đổi sửa tay. Các cơ chế này không ảnh hưởng tới 3 kịch bản demo; chi tiết ở thẻ Hạn chế và hướng phát triển.
