# consumer/

CDC Consumer: đọc Kafka, cập nhật inventory_snapshot ở hq_db.

Chủ sở hữu: Ngọc Chung. Mục spec cần đọc: F8, db.md 3.4.

- Theo đúng db.md mục 3.4: processed_event, so version, đánh dấu pending_txn APPLIED, commit offset sau khi commit CSDL.
- Chỉ đọc topic outbox.event.INVENTORY; sự kiện lỗi định dạng đẩy sang outbox.event.DLQ.

Quy tắc chung xem CLAUDE.md ở gốc repo.
