# CLAUDE.md – Quy tắc cho AI trong repo BTL kho phân tán

Đọc file này ở đầu mọi phiên làm việc. Các quy tắc dưới đây là bắt buộc.

## Dự án
Hệ thống quản lý kho phân tán: trung tâm HQ (máy A) và 3 kho Bắc, Trung, Nam (máy B).
Mỗi kho có PostgreSQL riêng; kho → HQ bằng outbox + Debezium + Kafka; danh mục HQ → kho bằng
logical replication; HQ đọc kho bằng postgres_fdw; chuyển hàng theo Saga do HQ điều phối;
kho sập thì WMS ghi hộ qua HQ (pending_txn) rồi Replay Worker đẩy lại.

## Nguồn sự thật
- `docs/spec.md` (Đặc tả kỹ thuật, F1–F12) là chuẩn cao nhất, rồi `docs/db.md`, rồi `docs/demo.md`.
- `docs/tests.md`: kết quả phải đạt (bảng kết quả chuẩn, test case, số đo).
- `docs/overview.md`, `docs/solution.md`: bối cảnh. `docs/plan.md`: phân vai, quy trình làm việc.
- Không tự đặt tên bảng, cột, endpoint, mã lỗi, tên sự kiện, topic, biến môi trường.
  Spec thiếu hoặc mâu thuẫn thì DỪNG và hỏi, không đoán.
- Không thêm thư viện ngoài danh sách: fastapi, uvicorn, asyncpg, httpx, pydantic (v2),
  confluent-kafka; test: pytest, pytest-asyncio; lint: ruff.
- Giao diện (dashboard/, wms/): HTML, CSS, JavaScript thuần, dùng fetch. Không framework, không npm,
  không bước build.

## Bản đồ repo và chủ sở hữu
| Thư mục / file | Chủ | Mục spec chính |
|---|---|---|
| infra/, db/, common/, scripts/ (trừ script đo trong scripts/measure/), docs/, tests/conftest.py, hq-api/main.py, node-api/main.py | Minh Danh | F1–F5, F11, F12, db.md |
| node-api/routers/inventory.py, node-api/services/, node-api/recovery.py, wms/ (giao diện) | Tuấn Minh | F6, F10.a |
| hq-api/routers/transfers.py, node-api/routers/transfers.py | Minh Hiếu | F6, F7, F10.b–g |
| consumer/ (kể cả consumer/main.py), hq-api/routers/dashboard.py, hq-api/routers/invariant.py, dashboard/, script đo M1 trong scripts/measure/ | Ngọc Chung | F7, F8, db.md 2.4, 3.4; tests.md mục 5 (M1) |
| hq-api/workers/, hq-api/routers/failover.py, wms/js/client.js, scripts/measure/m2_m4.py | Nhật Khánh | F7, F9, F10.h–j; tests.md mục 5 (M2, M4) |

## Phạm vi sửa
- Chỉ sửa file trong phạm vi ghi ở prompt. Cần sửa chỗ khác thì dừng và ghi đề xuất.
- Không sửa `db/migrations/` đã merge; đổi schema = file migration mới, đánh số tiếp theo.
- Không sửa `common/` và `docs/`; cần thêm thì đề xuất cho Minh Danh.
- `docs/` là bản gốc; chỉ sửa khi prompt yêu cầu rõ, bằng lệnh `/doc-change`; mọi thay đổi phải
  nhất quán giữa các file `docs/`.

## Quy tắc nghiệp vụ không được vi phạm
- Mọi lệnh ghi ở kho chạy trong một transaction, bắt đầu bằng `SET LOCAL app.source = 'api'`,
  đúng thứ tự bước của spec F10.
- Kiểm tra trùng `request_id` TRƯỚC khi trừ kho; `ON CONFLICT` chỉ là chốt chặn cuối.
- Ghi `outbox_event` trong cùng transaction với thay đổi tồn kho. Không gọi HTTP bên trong transaction.
- Không tự tăng `version` (trigger làm việc đó). `UPDATE inventory` luôn có điều kiện version và khả dụng.
- Trạng thái chỉ đi tới: `UPDATE ... WHERE status = <trạng thái liền trước>`.
- Chỉ HQ API ghi `transfer_summary`; Consumer chỉ xử lý sự kiện tồn kho.
- Consumer: `processed_event` + cập nhật snapshot trong một transaction; chỉ áp dụng khi
  version > last_version; commit offset Kafka sau khi commit CSDL.
- Không dùng thời gian thực để sắp thứ tự sự kiện.

## Cách làm
- Quy trình 4 bước, dùng lệnh dự án: `/plan-task` → `/test-first` → `/implement` → `/self-review`.
- Test đặt trong `tests/<service>/`. Tiền tố `test_tcXX_` chỉ dành cho test hiện thực đúng một TC
  trong `docs/tests.md` (`test_tcXX_<ten_ngan>.py`); unit test đặt tên theo thành phần
  (`test_health_monitor.py`…).
  Test cần container thật: `@pytest.mark.integration` (chỉ chạy khi HQ và kho cùng một máy,
  HOST_A_IP = HOST_B_IP; khác thì bị skip). Test tùy chọn: `@pytest.mark.optional`.
- Dùng mã cố định trong `scripts/demo_ids.env` cho dữ liệu test và demo.
- Dùng hằng số, model, lỗi và HTTP client trong `common/`; không viết chuỗi trực tiếp như "PENDING".
- Lỗi trả theo dạng `{error_code, message, details}` của spec F5.
- Python 3.11, async, có type hint, định dạng bằng ruff (line-length 100).
- Kết thúc mỗi việc: chạy pytest phần liên quan, báo kết quả, liệt kê giả định còn mở.

Bản báo cáo để nộp (không phải nguồn sự thật): https://docs.google.com/document/d/19rYYpcJDy31dYiV4I0HSz2jq6s84ijCzqlsqEDfcHc0/edit
