# db/

Migration SQL cho hq_db và 3 CSDL kho.

Chủ sở hữu: Minh Danh. Mục spec cần đọc: db.md toàn bộ, F3.

- migrations/001_hq.sql, 002_dc.sql, ... đánh số tăng dần; không sửa file đã merge.
- Đúng tên bảng, cột, kiểu, ràng buộc ở docs/db.md; role và quyền theo F3.
- CHECK mảnh dùng biến dc_id cho từng kho (pg_dc1 = 1, ...).

Quy tắc chung xem CLAUDE.md ở gốc repo.
