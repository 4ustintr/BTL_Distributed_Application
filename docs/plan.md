# Phân công và kế hoạch

Thẻ này là cam kết làm việc của nhóm trong 7 ngày: ai làm gì, mỗi ngày phải xong gì, và các quy tắc bắt buộc khi dùng AI sinh code. Mỗi người sở hữu trọn một mảng gồm lý thuyết, code, test và phần trình bày khi demo. Mọi thành viên đọc thẻ này cùng thẻ Đặc tả kỹ thuật trước khi viết dòng code đầu tiên.

## 1\. Phân vai

| Vai trò | Thành viên | Code phụ trách | Lý thuyết cần nắm | Test, số đo, phần trình bày |
| :---- | :---- | :---- | :---- | :---- |
| 1\. Hạ tầng và tích hợp | Minh Danh | infra/ (compose 2 máy, Kafka, Kafka Connect, 3 connector Debezium), db/ (migration, role, logical replication, FDW), common/, scripts/ (make; trừ script đo trong scripts/measure/), docs/, tests/conftest.py, hai file main.py, CI | Phân mảnh và tính đúng của phân mảnh; nhân bản một nguồn ghi; trong suốt vị trí; WAL và replication slot | Điều khiển máy khi demo; trình bày tổng quan, kiến trúc, CAP và PACELC; tích hợp và chạy make test mỗi tối |
| 2\. Giao dịch cục bộ và đồng thời | Tuấn Minh | node-api/routers/inventory.py, node-api/services/stock.py, node-api/recovery.py (F10.a, /replay, RECOVERING); wms/ (giao diện) | ACID, mức cô lập; lost update; khóa lạc quan và bi quan; idempotency; transactional outbox | TC01, TC02, TC03 (TC04); thuyết minh Kịch bản 1 bước 0–2 |
| 3\. Saga điều chuyển | Minh Hiếu | hq-api/routers/transfers.py (F10.f, g, nút Hủy phiếu); node-api/routers/transfers.py (F10.b–e) | Giao dịch phân tán; 2PC và vì sao không dùng; Saga orchestration; giao dịch bù; bước then chốt | TC05, TC07 (TC06, TC08, TC09); thuyết minh Kịch bản 1 bước 3–5 |
| 4\. CDC và đọc phân tán | Ngọc Chung | consumer/; hq-api/routers/dashboard.py, hq-api/routers/invariant.py; dashboard/; script đo M1 trong scripts/measure/ | Kiến trúc hướng sự kiện; at-least-once kết hợp idempotent; thứ tự sự kiện và đồng hồ logic; nhất quán cuối cùng; Chandy–Lamport | TC10, TC12 (TC11, TC13); số đo M1; thuyết minh Kịch bản 2 |
| 5\. Chịu lỗi và phục hồi | Nhật Khánh | hq-api/workers/health.py, hq-api/workers/replay.py, hq-api/routers/failover.py; wms/js/client.js (chuyển chế độ NORMAL, FAILOVER, WAITING); scripts/measure/m2\_m4.py | Phát hiện lỗi bằng timeout; lỗi sập và lỗi treo; hinted handoff, store-and-forward; CAP và PACELC; phục hồi dựa trên log | TC14, TC15, TC16 (TC17); số đo M2, M4; thuyết minh Kịch bản 3 |

Quy tắc ranh giới: Tuấn Minh sở hữu mọi lệnh SQL thay đổi inventory, stock\_movement, outbox\_event, đóng gói thành các hàm trong node-api/services/stock.py (reserve, release, consume, add\_stock, write\_outbox); Minh Hiếu gọi các hàm này, không tự viết SQL trừ kho. HQ API và Node API mỗi bên có một file main.py do trưởng nhóm tạo; mỗi người chỉ thêm router hoặc worker của mình và đăng ký bằng một dòng include\_router, nên không ai phải sửa cùng một file với người khác.

## 2\. Kế hoạch 7 ngày

Một tuần chỉ đủ khi cả 5 người làm gần như toàn thời gian và tích hợp mỗi ngày. Mỗi ngày kết thúc bằng một mốc chạy được trên hai máy thật; trễ mốc thì báo ngay trong nhóm và cắt theo mục 6, không dời mốc sang hôm sau. Không ai chờ ai: phần chưa có thì dùng stub đúng định dạng F6, F7.

| Ngày | Việc chính | Mốc nghiệm thu cuối ngày | Chịu trách nhiệm mốc |
| :---- | :---- | :---- | :---- |
| 1 | Trưởng nhóm dựng repo khung, docs/, CLAUDE.md, common/, migration, compose 2 máy, CODEOWNERS, CI. Bốn người còn lại đọc thẻ Đặc tả kỹ thuật, Thiết kế CSDL, viết test của mình và bắt đầu code trên PostgreSQL cục bộ hoặc stub | make up chạy trên 2 máy; repo có docs/, CLAUDE.md, common/; mỗi người đã có test của mình trong tests/ (đang đỏ) | Minh Danh |
| 2 | Minh Danh: logical replication, FDW, 3 connector. Tuấn Minh: inbound, outbound, outbox, chống trùng. Ngọc Chung: Consumer với Kafka thật. Minh Hiếu và Nhật Khánh: luồng của mình chạy với stub | TC01, TC02 xanh trên 1 máy (HOST\_A\_IP \= HOST\_B\_IP); trên 2 máy: nhập ở kho, snapshot HQ đổi số | Tuấn Minh, Ngọc Chung |
| 3 | Tuấn Minh: khóa lạc quan, race, chế độ RECOVERING, /replay. Minh Hiếu: Saga hai phía với kho thật. Ngọc Chung: /dashboard/inventory, /invariant. Nhật Khánh: Health Monitor, dc\_status, endpoint failover | TC03, TC05, TC07 xanh; Kịch bản 1 chạy trọn từ make reset, khớp bảng kết quả chuẩn | Minh Hiếu |
| 4 | Ngọc Chung: Dashboard (HTML/JS), chế độ realtime và offline. Tuấn Minh: WMS (HTML/JS). Nhật Khánh: pending\_txn, Replay Worker, /recovery/complete. Minh Hiếu: nút Hủy phiếu và test của mình | TC10, TC12 xanh; Kịch bản 2 chạy trọn | Ngọc Chung |
| 5 | Nhật Khánh ghép Kịch bản 3 với kho thật và chế độ failover của WMS. Mọi người sửa lỗi tích hợp của phần mình. Trưởng nhóm viết script demo in dòng trạng thái theo thẻ Kiểm thử, mục 3 | TC14, TC15, TC16 xanh; Kịch bản 3 chạy trọn, check-invariant ra 180 | Nhật Khánh |
| 6 | Đóng băng tính năng từ trưa, chỉ còn sửa lỗi. Đo M1, M2. Chạy make test hai lần liên tiếp. Tập demo lần 1 và quay video dự phòng | make test (10 test bắt buộc, gồm test tích hợp) xanh hai lần liên tiếp trên 1 máy (HOST\_A\_IP \= HOST\_B\_IP); trên 2 máy demo, script demo chạy trọn 3 kịch bản hai lần liên tiếp, dòng trạng thái khớp bảng kết quả chuẩn; có video demo dự phòng | Trưởng nhóm |
| 7 | Tập demo lần 2 và 3 theo đúng phân vai mục 5\. Mỗi người viết phần báo cáo của mình và chuẩn bị 3 câu hỏi xoáy về phần đó. Dán hình ERD, HLD, sơ đồ tuần tự vào tài liệu | Sẵn sàng bảo vệ | Cả nhóm |

## 3\. Chính sách dùng AI sinh code

AI chỉ sinh code đúng khi cả nhóm cho nó cùng một nguồn sự thật và cùng một khuôn làm việc. Các quy tắc dưới đây là bắt buộc; pull request vi phạm sẽ không được merge, dù code chạy được.

### 3.1. Một nguồn sự thật trong repo

* Thư mục docs/ trong repo (docs/spec.md, docs/db.md, docs/demo.md, docs/tests.md và các file còn lại) là nguồn sự thật duy nhất cho cả người và AI.  
* Muốn đổi hợp đồng (tên cột, endpoint, mã lỗi, payload sự kiện, tham số): đề xuất cho trưởng nhóm; trưởng nhóm sửa docs/ bằng lệnh /doc-change và merge trước, rồi mới sửa code. Không ai sửa code trước rồi "sẽ cập nhật tài liệu sau".  
* Mọi hằng số dùng chung nằm trong common/: tên topic, mã lỗi, trạng thái, tham số F11, model Pydantic cho request, response và payload sự kiện. Code không viết chuỗi trực tiếp như "PENDING" hay "outbox.event.INVENTORY".

### 3.2. File CLAUDE.md ở gốc repo

```markdown
# Quy tắc cho AI trong repo BTL kho phân tán

## Nguồn sự thật
- docs/spec.md, docs/db.md, docs/tests.md, docs/demo.md là chuẩn.
  Mâu thuẫn thì theo thứ tự: spec.md > db.md > demo.md.
- Không tự đặt tên bảng, cột, endpoint, mã lỗi, sự kiện, topic.
  Thiếu thông tin thì dừng và hỏi, không đoán.
- Không thêm thư viện ngoài danh sách spec F1.

## Phạm vi sửa
- Chỉ sửa file trong phạm vi ghi ở prompt.
  Cần đổi chỗ khác thì dừng lại và ghi đề xuất.
- Không sửa db/migrations đã có; đổi schema = file migration mới.
- Không sửa common/ và docs/; cần thêm thì đề xuất.

## Quy tắc nghiệp vụ không được vi phạm
- Mọi lệnh ghi ở kho chạy trong một transaction, bắt đầu bằng
  SET LOCAL app.source = 'api', đúng thứ tự bước của spec F10.
- Kiểm tra trùng request_id TRƯỚC khi trừ kho;
  ON CONFLICT chỉ là chốt chặn cuối.
- Ghi outbox_event trong cùng transaction với thay đổi tồn kho.
  Không gọi HTTP bên trong transaction.
- Không tự tăng version (trigger làm việc đó).
  UPDATE inventory luôn có điều kiện version và khả dụng.
- Trạng thái chỉ đi tới: UPDATE ... WHERE status = trạng thái liền trước.
- Consumer: processed_event và cập nhật snapshot trong một transaction;
  chỉ áp dụng khi version > last_version;
  commit offset Kafka sau khi commit CSDL.
- Không dùng thời gian thực để sắp thứ tự sự kiện.

## Cách làm
- Viết hoặc cập nhật test trong tests/ trước, đúng ID và assert
  ở docs/tests.md, rồi mới viết code.
- Dùng hằng số và model trong common/, không viết chuỗi trực tiếp.
- Lỗi trả theo dạng {error_code, message, details} của spec F5.
- Gọi service khác bằng client có timeout và thử lại trong common/.
- Python 3.11, FastAPI, asyncpg, httpx; có type hint; định dạng bằng ruff.
- Kết thúc mỗi việc: chạy pytest phần liên quan và báo kết quả.
```

Mỗi thư mục service có thêm một CLAUDE.md ngắn ghi phạm vi của thư mục đó. Ví dụ node-api/CLAUDE.md liệt kê các mục F6, F10.a–e và các hàm dùng chung trong services/stock.py; consumer/CLAUDE.md trỏ tới Thiết kế CSDL mục 3.4 và F8.

### 3.3. Cách viết prompt

* Mỗi prompt một mục nhỏ của spec và ghi rõ số mục, ví dụ: "Viết POST /outbound theo spec F6 và F10.a, đúng thứ tự các bước, chỉ sửa node-api/routers/inventory.py và node-api/services/stock.py".  
* Luôn trỏ AI tới docs/spec.md và docs/db.md; không tự tóm tắt lại spec theo trí nhớ trong prompt.  
* Yêu cầu AI liệt kê giả định trước khi viết code; giả định nào không có trong spec thì hỏi trưởng nhóm, không để AI tự quyết.  
* Không prompt kiểu "viết cả Node API" hay "làm cho chạy được": prompt càng rộng, AI càng tự chế tên và luồng.  
* Sau khi AI viết xong, yêu cầu nó tự đối chiếu với checklist ở mục 3.5 và chỉ ra dòng code ứng với từng ý. Mẫu prompt dùng chung:

```text
Bối cảnh: đọc CLAUDE.md, docs/spec.md mục <Fx>, docs/db.md mục <2.x>.
Việc: viết <endpoint hoặc hàm> theo đúng <F10.x>, giữ đúng thứ tự các bước.
Phạm vi: chỉ sửa <danh sách file>. Cần sửa file khác thì dừng và đề xuất.
Test: viết trước test <TCxx> theo docs/tests.md, chạy cho tới khi xanh.
Trước khi code: liệt kê giả định và các điểm spec chưa rõ.
Sau khi code: đối chiếu checklist mục 3.5, chỉ ra dòng code ứng với từng ý.
```

### 3.4. Quy trình Git

* Nhánh master được bảo vệ, không ai push thẳng. Mọi thay đổi đi qua pull request; tên nhánh theo dạng \<tên\>/\<vai\>/\<việc\>, ví dụ danh/infra/base, tuanminh/kho/outbound, hieu/saga/ship, chung/cdc/consumer, khanh/failover/replay.  
* File CODEOWNERS gán chủ cho từng thư mục theo bảng mục 1; pull request đụng thư mục của người khác cần người đó duyệt. db/, common/, docs/ chỉ trưởng nhóm merge.  
* Pull request nhỏ: một mục spec, khoảng dưới 400 dòng. Mô tả ghi rõ mục spec (ví dụ F10.c) và ID test liên quan.  
* Rebase theo master trước khi mở pull request; xung đột ở file của người khác thì hỏi chủ file, không để AI tự giải quyết.  
* CI (GitHub Actions) chạy ruff và các test không cần hai máy cho mọi pull request; CI đỏ thì không merge.

### 3.5. Checklist duyệt pull request

Người duyệt là một người khác mảng. Mỗi ý không đạt là một yêu cầu sửa; đây cũng là những lỗi AI hay mắc nhất trong đề tài này.

- [ ] Đúng tên bảng, cột, endpoint, mã lỗi, sự kiện, topic như docs/; không có chuỗi viết trực tiếp thay cho hằng số trong common/.  
- [ ] Thứ tự các bước trong transaction đúng F10; kiểm tra trùng request\_id chạy trước bước trừ kho.  
- [ ] outbox\_event được ghi cùng transaction với thay đổi nghiệp vụ; không có lệnh gọi HTTP nào nằm bên trong transaction.  
- [ ] UPDATE inventory có điều kiện version và điều kiện khả dụng; code không tự tăng version.  
- [ ] Cập nhật trạng thái phiếu có điều kiện trạng thái liền trước; không có lệnh UPDATE status không điều kiện.  
- [ ] Consumer kiểm tra processed\_event và last\_version, commit offset Kafka sau khi commit CSDL.  
- [ ] Lệnh gọi giữa các service dùng client có timeout và thử lại trong common/, không tự viết vòng lặp thử lại riêng.  
- [ ] Có test tương ứng ở docs/tests.md, đặt đúng tên test\_tcXX\_…, và test xanh.

## 4\. Phân vai khi demo

* Minh Danh điều khiển máy (make reset, docker kill, docker start, restart connector), mở đầu bằng tổng quan và kiến trúc, trả lời các câu hỏi chung về CAP, PACELC và vì sao chọn sẵn sàng.  
* Tuấn Minh và Minh Hiếu thuyết minh Kịch bản 1, Ngọc Chung thuyết minh Kịch bản 2, Nhật Khánh thuyết minh Kịch bản 3\. Thầy hỏi sâu phần nào thì chủ phần đó trả lời.  
* Mạng: dùng router hoặc hotspot riêng, không dùng Wi-Fi của trường. Video demo quay ở ngày 6 là phương án dự phòng nếu môi trường lỗi giữa buổi.
