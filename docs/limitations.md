<!-- File sinh tự động từ Google Docs bằng scripts/sync_docs.py. Không sửa tay: sửa Google Docs rồi chạy lại script. -->

# Hạn chế và hướng phát triển


Thẻ này gom các phần nhóm chủ động lược bỏ để bản nộp vừa sức, cùng các giới hạn còn lại của kiến trúc. Mọi mục dưới đây không ảnh hưởng tới 3 kịch bản demo; chúng là hướng phát triển khi muốn đưa hệ thống gần với vận hành thật hơn.

## 1\. Các cơ chế đã lược bỏ trong bản nộp

Mỗi cơ chế dưới đây đã được thiết kế trong quá trình làm đề tài; bảng ghi rõ vì sao chưa làm, ảnh hưởng hiện tại và cách bổ sung.

| Cơ chế | Vì sao lược bỏ | Ảnh hưởng hiện tại | Cách phát triển |
| :---- | :---- | :---- | :---- |
| Saga Recovery chạy nền (tự gửi lại Transfer Notice, tự hủy phiếu quá hạn) | Cần theo dõi xác nhận từng kho và một tác vụ định kỳ; demo luôn có người thao tác | Phiếu dở khi HQ sập hoặc khi lệnh gọi xuống kho lỗi phải hủy tay bằng nút Hủy phiếu | Thêm cột source\_ack\_at, dest\_ack\_at; tác vụ mỗi 30 giây gửi lại notice còn thiếu và hủy phiếu CREATED quá 10 phút |
| Cập nhật trạng thái phiếu qua CDC song song với HTTP | Hai đường ghi cần quy tắc thứ hạng và chống ghi trùng lịch sử | Callback lỗi thì nhân viên phải bấm lại thao tác | Consumer đọc outbox.event.TRANSFER, cập nhật transfer\_summary theo thứ hạng trạng thái; ghi transfer\_event chỉ khi UPDATE đổi đúng 1 dòng |
| Kho đích hỏi lại HQ trước khi nhận hàng | HQ đã đồng bộ bằng /sync-status ngay sau khi kho nguồn xuất | Nếu /sync-status lỗi, kho đích phải chờ HQ đồng bộ lại mới nhận được hàng | Node API gọi GET /transfers/{id} trên HQ khi bản INBOUND chưa ở trạng thái cuối |
| Kho tự chuyển ONLINE khi HQ chết | Cần quy tắc an toàn khi chưa biết còn giao dịch nhận hộ hay không | Khởi động kho lúc HQ chết thì phải gọi tay /recovery/complete | Kho gọi HQ GET /health trong lúc RECOVERING; 30 giây không gọi được thì tự ONLINE và ghi cảnh báo |
| Khóa dòng khi HQ ghi hộ | Demo chỉ có một người thao tác | Hai lệnh xuất ghi hộ cùng lúc có thể cùng vượt kiểm tra tạm tính; lệnh thừa bị REJECTED khi đẩy lại | SELECT … FOR UPDATE trên inventory\_snapshot, hoặc pg\_advisory\_xact\_lock(dc\_id, product\_id) khi chưa có dòng |
| Trigger ghi nhận sửa tay vào CSDL | Cần thêm trigger AFTER và biến phiên app.source | Sửa tay chỉ phát hiện được khi đối soát | Trigger AFTER INSERT OR UPDATE ghi dòng ADJUST (ref\_type DIRECT) và sự kiện InventoryChangedDirect khi app.source khác 'api' |
| Đánh dấu đã giao khi replay | Gửi lại đã an toàn nhờ request\_id | Replay Worker gửi lặp vài lần trong lúc chờ CDC; không sai số | Thêm cột delivered\_at, chỉ gửi dòng chưa giao |
| Đối soát định kỳ tự động | Nút Đối soát trên Dashboard đã đủ cho demo | Lệch số chỉ phát hiện khi bấm Đối soát | Tác vụ 60 giây so snapshot với số đọc qua FDW, ghi đè snapshot khi lệch (Đặc tả kỹ thuật, F10.k) |
| Test và số đo tùy chọn | Ưu tiên 10 test bắt buộc phủ 3 kịch bản | Một số tình huống biên chưa được kiểm chứng tự động | Bổ sung TC04, 06, 08, 09, 11, 13, 17, 18, 19 và số đo M3–M6 vào make test |

## 2\. Giới hạn của kiến trúc hiện tại

Ngoài các cơ chế trên, kiến trúc còn những giới hạn chung, đã phân tích kỹ ở thẻ Tổng quan, mục 8:

* HQ là điểm lỗi duy nhất cho điều phối và ghi hộ.  
* Kafka chỉ có 1 broker, replication factor 1\.  
* Phân vùng mạng có thể làm hai nơi cùng nhận ghi cho một kho; chỉ phát hiện lúc đẩy lại (REJECTED).  
* Không hỗ trợ xuất thiếu, nhận thiếu.  
* Bảo mật tối giản: một khóa nội bộ dùng chung, chưa có TLS.

## 3\. Thứ tự phát triển đề xuất

1. Khóa dòng khi ghi hộ và trigger ghi nhận sửa tay: ít code, tăng độ đúng ngay.  
2. Saga Recovery và cập nhật trạng thái phiếu qua CDC: phiếu không còn kẹt khi HQ hoặc mạng lỗi.  
3. Kho tự chuyển ONLINE khi HQ chết và đối soát định kỳ: kho tự chủ hoàn toàn.  
4. Nhân bản HQ (streaming replication kèm Patroni) và Kafka 3 broker: bỏ điểm lỗi duy nhất.  
5. Lease kèm fencing token cho ghi hộ, mTLS giữa các service, hỗ trợ xuất và nhận thiếu: đưa hệ thống gần với vận hành thật.
