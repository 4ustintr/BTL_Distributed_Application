# common/

Thư viện dùng chung cho mọi service Python.

Chủ sở hữu: Minh Danh. Mục spec cần đọc: F5, F6, F7, F8, F9, F11.

- constants.py: tên topic, trạng thái, error_code, tham số F11 (đúng tên trong spec).
- models.py: Pydantic v2 cho request/response F6, F7 và payload sự kiện F8.
- errors.py: exception → {error_code, message, details} và HTTP status theo F5.
- http.py: client httpx async có timeout, thử lại, gắn X-Internal-Key (F5).
- ids.py: sinh request_id; uuid5 tất định cho dòng sổ điều chuyển (db.md 2.8).
- Không chứa logic nghiệp vụ. Người khác cần thêm gì thì đề xuất, không tự sửa.

Quy tắc chung xem CLAUDE.md ở gốc repo.
