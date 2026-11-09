# scripts/measure/

Script đo cho các số đo bắt buộc ở docs/tests.md mục 5. Kết quả ghi vào báo cáo kèm ngày đo và cấu
hình máy (docs/tests.md mục 6).

| Mã | Số đo | Cách đo (docs/tests.md mục 5) | Ghi kết quả | Lệnh make | Chủ sở hữu |
| :---- | :---- | :---- | :---- | :---- | :---- |
| M1 | Độ trễ CDC: từ lúc commit ở kho tới lúc snapshot cập nhật ở HQ | Gửi 200 lệnh xuất 1 thùng, cách nhau 100 ms; tính synced\_at − occurred\_at | p50, p95, max (ms) | make bench-cdc | Ngọc Chung |
| M2 | Thời gian phát hiện kho chết | Ghi thời điểm docker kill và updated\_at của dc\_status; lặp 5 lần | Trung bình, max (giây); dự kiến 4–6 giây | make measure-failover (scripts/measure/m2\_m4.py) | Nhật Khánh |
| M4 | Thời gian phục hồi | Từ docker start tới dc\_status ONLINE, với số giao dịch chờ N \= 2, 20, 200 | Giây theo từng N | make measure-failover (scripts/measure/m2\_m4.py) | Nhật Khánh |

Ghi chú:

* M1 cần đồng hồ hai máy đồng bộ bằng NTP; nếu không, đặt kho và HQ trên cùng một máy khi đo và ghi
  rõ trong báo cáo (docs/tests.md mục 5).
* Tên file script M1 do Ngọc Chung đặt khi viết; cập nhật bảng trên cùng lúc.
* make measure-failover hiện là TODO; Nhật Khánh nối lệnh này vào scripts/measure/m2\_m4.py.
* Script dùng mã cố định trong scripts/demo\_ids.env và thư viện trong danh sách cho phép (CLAUDE.md).
* README này do Minh Danh quản lý; script đo thuộc chủ sở hữu ghi trong bảng.
