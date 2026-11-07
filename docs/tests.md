<!-- File sinh tự động từ Google Docs bằng scripts/sync_docs.py. Không sửa tay: sửa Google Docs rồi chạy lại script. -->

# Kiểm thử và đánh giá


Thẻ này là chuẩn nghiệm thu chung: mọi thành viên viết code phải cho ra đúng các kết quả dưới đây, và một chức năng chỉ coi là xong khi test tương ứng chạy xanh. Khi đầu ra thực tế khác bảng, sửa code chứ không sửa bảng, trừ khi cả nhóm thống nhất và cập nhật đồng thời thẻ Đặc tả kỹ thuật.

## 1\. Quy ước chung để tránh xung đột khi làm nhóm

* Nguồn chuẩn khi có mâu thuẫn: thẻ Đặc tả kỹ thuật, rồi Thiết kế CSDL, rồi Kịch bản demo. Không ai tự đổi tên cột, mã lỗi, tên sự kiện hay endpoint khi chưa cập nhật thẻ Đặc tả kỹ thuật và báo cả nhóm.  
* Một repository với cấu trúc cố định: hq-api/, node-api/, consumer/, dashboard/, wms/, common/, db/ (migration đánh số 001\_, 002\_…), infra/ (compose, connector JSON), scripts/ (lệnh make, demo\_ids.env), tests/ (pytest).  
* Mọi thứ dùng chung đặt trong common/ (Python): model payload sự kiện, danh sách error\_code, hàm sinh request\_id và uuid5, client HTTP có thử lại. Không service nào tự định nghĩa lại.  
* Schema chỉ đổi bằng file migration mới trong db/; không sửa migration đã merge.  
* Mỗi chức năng một nhánh, merge qua pull request; pull request phải chạy make test xanh và có một người khác duyệt.  
* Script demo in dòng trạng thái theo định dạng ở mục 3 sau mỗi bước; test so khớp đúng định dạng và số liệu đó.

## 2\. Dữ liệu và mã cố định dùng chung

Mọi script demo và test dùng chung các mã dưới đây, nên log, Dashboard và kết quả test trùng khớp giữa các thành viên. transfer\_code\_seq bắt đầu lại từ 1 sau make reset, nên mã phiếu luôn giống nhau ở mỗi lần chạy.

| Tên | Giá trị | Dùng ở |
| :---- | :---- | :---- |
| SKU A | product\_id \= 1, sku \= SKU-A | Mọi kịch bản và test |
| Tồn ban đầu | Bắc 100, Trung 40, Nam 10 (nhập bằng REQ\_SEED\_1, REQ\_SEED\_2, REQ\_SEED\_3) | make seed |
| Phiếu 1 | TR-2026-000001, Bắc → Nam, 50 thùng | KB1 bước 1–4, TC05 |
| Phiếu 2 | TR-2026-000002, Bắc → Trung, 10 thùng | KB1 bước 5, TC07 |
| Hai lệnh tranh chấp | REQ\_RACE\_1, REQ\_RACE\_2: mỗi lệnh xuất 30 thùng ở kho Bắc | KB1 bước 2, TC03, TC04 (tùy chọn) |
| Xuất cho đại lý ở kho Trung | REQ\_KB2\_OUT: xuất 5 thùng | KB2 bước 1 |
| Ghi hộ ở kho Trung | REQ\_KB3\_IN (nhập 30), REQ\_KB3\_OUT (xuất 10), REQ\_KB3\_OUT\_BIG (xuất 100\) | KB3 bước 3–5, make replay-again |
| Giá trị của các REQ\_\* | UUID cố định, khai báo một lần trong scripts/demo\_ids.env và tests/fixtures.py | Script demo, test, Dashboard |

## 3\. Kết quả chuẩn theo từng bước demo

Sau mỗi bước, script demo in một dòng trạng thái theo đúng định dạng dưới đây, lấy từ GET /invariant và GET /dashboard/inventory. Test tự động so khớp với cùng các số trong bảng.

```text
[KB1-2] BAC=70/20 TRUNG=40/40 NAM=10/10 TRANSIT=0 SHIPPED_OUT=30 RECEIVED_IN=150 DIFF=0 [KB2-5] BAC=20/20 TRUNG=35/35(offline) NAM=60/60 TRANSIT=0 SHIPPED_OUT=NA RECEIVED_IN=NA DIFF=NA
```

| Bước | Bắc | Trung | Nam | Đi đường | Đã xuất | Đã nhập | Phản hồi chính |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| KB1-0 | 100/100 | 40/40 | 10/10 | 0 | 0 | 150 | make reset xong |
| KB1-1 | 100/50 | 40/40 | 10/10 | 0 | 0 | 150 | POST /transfers 201 |
| KB1-2 | 70/20 | 40/40 | 10/10 | 0 | 30 | 150 | 200 và 422 INSUFFICIENT\_STOCK |
| KB1-3 | 20/20 | 40/40 | 10/10 | 50 | 30 | 150 | ship 200 |
| KB1-4 | 20/20 | 40/40 | 60/60 | 0 | 30 | 150 | receive 200 |
| KB1-5a | 20/10 | 40/40 | 60/60 | 0 | 30 | 150 | POST /transfers 201 (phiếu 2\) |
| KB1-5b | 20/20 | 40/40 | 60/60 | 0 | 30 | 150 | decline 200, phiếu 2 CANCELLED |
| KB2-1 | 20/20 | 35/35 | 60/60 | 0 | 35 | 150 | outbound 200 |
| KB2-5 | 20/20 | 35/35 (offline) | 60/60 | 0 | không tính | không tính | /invariant 503; Trung source=offline |
| KB3-6 | 20/20 | 55/55 (tạm tính) | 60/60 | 0 | không tính | không tính | failover 200, 200, 422 |
| KB3-11 | 20/20 | 55/55 | 60/60 | 0 | 45 | 180 | recovery/complete 200 |
| KB3-12 | 20/20 | 55/55 | 60/60 | 0 | 45 | 180 | replay 200, duplicate=true |

Quy ước: mỗi ô kho ghi tồn/khả dụng, với khả dụng \= qty\_on\_hand − qty\_reserved. Đi đường, Đã xuất, Đã nhập lấy từ GET /invariant. Bất kỳ bước nào DIFF khác 0 là lỗi: dừng demo và kiểm tra log.

## 4\. Bộ test case

Mỗi test case là một hàm pytest đặt tên test\_tcXX\_\<tên\_ngắn\> trong thư mục tests/, dùng dữ liệu và mã cố định ở mục 2, và tự gọi make reset trước khi chạy (trừ khi ghi khác). Cột Kết quả mong đợi là điều kiện assert; cột Thành phần cho biết service nào phải làm đúng để test xanh, dùng khi chia việc. Test có ghi (tùy chọn) chỉ làm khi còn thời gian; 10 test còn lại là bắt buộc để demo. make test chạy 10 test bắt buộc, make test-all chạy tất cả.

| ID | Kiểm tra | Cách chạy | Kết quả mong đợi (assert) | Thành phần |
| :---- | :---- | :---- | :---- | :---- |
| TC01 | Nhập, xuất cơ bản | /inbound 10 rồi /outbound 5 ở kho Nam | Cả hai 200; tồn Nam 10 → 20 → 15; 2 dòng stock\_movement; snapshot HQ \= 15 trong 5 giây | node-api, consumer |
| TC02 | Gửi trùng request\_id | /outbound 5 hai lần cùng request\_id ở kho Nam | Lần 1: 200, duplicate=false; lần 2: 200, duplicate=true; tồn chỉ giảm 5; chỉ 1 dòng sổ | node-api |
| TC03 | Xuất đồng thời có khóa | Tạo phiếu 1 (giữ chỗ 50\) rồi make race | Đúng 1 lệnh 200, 1 lệnh 422 INSUFFICIENT\_STOCK; Bắc 70/20; DIFF \= 0 | node-api |
| TC04 (tùy chọn) | Xuất đồng thời không khóa | make race-unsafe trên dữ liệu vừa reset | Cả 2 lệnh 200; DIFF \= 30 (chứng minh lost update) | node-api |
| TC05 | Điều chuyển trọn vẹn | Tạo phiếu 1, ship, receive (không chạy race) | RECEIVED ở transfer\_summary và cả 2 transfer\_order; Bắc 50/50, Nam 60/60, DIFF \= 0 | hq-api, node-api |
| TC06 (tùy chọn) | Từ chối sau khi đã xuất | Tạo phiếu, ship, rồi decline | decline trả 409 INVALID\_STATE; phiếu vẫn SHIPPED ở cả 3 nơi | hq-api |
| TC07 | Từ chối trước khi xuất | Tạo phiếu Bắc → Trung 10 thùng, rồi decline | 200; CANCELLED ở cả 3 nơi, cancel\_reason \= DECLINED; qty\_reserved Bắc về 0 | hq-api, node-api |
| TC08 (tùy chọn) | Gửi lại Transfer Notice | Gọi /transfers/notice 2 lần cùng transfer\_code | Lần 2 duplicate=true; qty\_reserved không tăng gấp đôi | node-api |
| TC09 (tùy chọn) | Callback lỗi rồi bấm lại | docker pause hq-api lúc kho Bắc bấm Xuất phiếu 1, unpause rồi bấm Xuất lại | Lần 1 trả 503; lần 2 trả 200 duplicate và HQ chuyển SHIPPED; tồn Bắc chỉ giảm một lần | hq-api |
| TC10 | Consumer nhận lại sự kiện | Reset offset của hq-consumer về earliest, chạy lại Consumer | inventory\_snapshot không đổi; số dòng processed\_event không đổi | consumer |
| TC11 (tùy chọn) | Sự kiện đến sai thứ tự | Gửi vào topic một sự kiện có version nhỏ hơn last\_version | inventory\_snapshot không đổi | consumer |
| TC12 | FDW khi kho chết | make kill-dc2, gọi GET /dashboard/inventory?mode=realtime | Phản hồi dưới 1 giây; Bắc, Nam source=realtime; Trung source=offline | hq-api |
| TC13 (tùy chọn) | FDW khi kho treo | make pause-dc2, gọi chế độ realtime | Phản hồi 3–5 giây; Trung source=offline; Bắc, Nam có số | hq-api |
| TC14 | Ghi hộ và đẩy lại | kill-dc2; failover nhập 30, xuất 10; start-dc2 | Trung 60/60 (40 \+ 30 − 10); 2 pending\_txn APPLIED; dc\_status ONLINE | hq-api, node-api, consumer |
| TC15 | Failover khi kho còn sống | Gọi /failover cho kho đang ONLINE | 409 DC\_NOT\_OFFLINE; không có dòng pending\_txn mới | hq-api |
| TC16 | Replay bị từ chối | Dừng hq-consumer; kho Trung xuất 30 (thật còn 10, snapshot vẫn 40); kill-dc2; failover xuất 30; start-dc2; bật lại hq-consumer | pending\_txn REJECTED, error\_msg chứa INSUFFICIENT\_STOCK; Trung còn 10 | hq-api, node-api |
| TC17 (tùy chọn) | Kho khởi động khi HQ chết | docker stop hq-api, khởi động lại node-api-dc1 | /health trả mode RECOVERING; gọi tay POST /recovery/complete thì chuyển ONLINE | node-api |
| TC18 (tùy chọn) | Đối soát phát hiện sửa tay | UPDATE inventory bằng psql, rồi bấm Đối soát trên Dashboard | Dashboard báo lệch giữa snapshot và số đọc qua FDW; version của dòng tăng 1 | db, hq-api |
| TC19 (tùy chọn) | HQ sập, sự kiện không mất | Kịch bản dự phòng ở thẻ Kịch bản demo, chạy sau KB3 | Consumer lag về 0; snapshot Bắc \= 15 | consumer |

## 5\. Số đo và cách đo

| Mã | Số đo | Cách đo | Ghi kết quả |
| :---- | :---- | :---- | :---- |
| M1 | Độ trễ CDC: từ lúc commit ở kho tới lúc snapshot cập nhật ở HQ | make bench-cdc gửi 200 lệnh xuất 1 thùng, cách nhau 100 ms; tính synced\_at − occurred\_at | p50, p95, max (ms) |
| M2 | Thời gian phát hiện kho chết | Ghi thời điểm docker kill và updated\_at của dc\_status; lặp 5 lần | Trung bình, max (giây); dự kiến 4–6 giây |
| M3 (tùy chọn) | Thời gian WMS chuyển sang ghi hộ | Từ docker kill tới lệnh failover đầu tiên được HQ chấp nhận, theo log WMS | Giây |
| M4 | Thời gian phục hồi | Từ docker start tới dc\_status ONLINE, với số giao dịch chờ N \= 2, 20, 200 | Giây theo từng N |
| M5 (tùy chọn) | Độ trễ đọc của Dashboard | 50 lần cho mỗi chế độ: snapshot, realtime khi 3 kho sống, realtime khi một kho treo | p50, p95 (ms) |
| M6 (tùy chọn) | Thông lượng xuất tại một kho | 10 client song song gửi lệnh xuất 1 thùng trong 30 giây | Lệnh thành công mỗi giây, tỉ lệ xung đột version |

M1 cần đồng hồ hai máy đồng bộ bằng NTP. Nếu không làm được, đặt kho và HQ trên cùng một máy khi đo M1 và ghi rõ điều này trong báo cáo.

Khi trình bày, vẽ biểu đồ phân phối cho M1 và biểu đồ theo N cho M4. Mỗi số đo đi kèm một câu nối với thiết kế, ví dụ: M2 xấp xỉ 3 × 2 giây đúng như tham số heartbeat; rút ngắn chu kỳ thì phát hiện nhanh hơn nhưng dễ báo nhầm khi mạng chỉ chậm.

## 6\. Tiêu chí hoàn thành

* Một chức năng chỉ được merge khi các test liên quan ở mục 4 chạy xanh và không làm đỏ test khác.  
* Trước buổi demo, make test chạy xanh hai lần liên tiếp trên đúng hai máy demo.  
* Chạy thử trọn kịch bản từ make reset tới bước KB3-12 ít nhất một lần; mọi dòng trạng thái khớp bảng mục 3\.  
* Số đo M1, M2 (bắt buộc) và M4 (nên có) ở mục 5 được ghi vào báo cáo kèm ngày đo và cấu hình máy.
