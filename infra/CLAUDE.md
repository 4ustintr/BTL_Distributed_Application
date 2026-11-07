# infra/

Docker Compose cho 2 máy, cấu hình PostgreSQL, Kafka, Kafka Connect.

Chủ sở hữu: Minh Danh. Mục spec cần đọc: F1, F2, F3, F4.

- compose.hq.yml (máy A), compose.dc.yml (máy B): tên container và cổng đúng F2.
- postgres/: postgresql.conf, pg_hba.conf theo F3. connectors/: dc1..dc3-outbox.json theo F4.
- Dashboard và WMS chạy bằng nginx phục vụ file tĩnh; config.js sinh từ biến môi trường.

Quy tắc chung xem CLAUDE.md ở gốc repo.
