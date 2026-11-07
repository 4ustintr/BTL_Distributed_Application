# hq-api/

HQ API (FastAPI) chạy ở máy A, cổng 8000.

Chủ sở hữu: Minh Danh (main.py), Minh Hiếu, Ngọc Chung, Nhật Khánh. Mục spec cần đọc: F5, F7, F9, F10.f–j.

- main.py (Minh Danh): tạo app, pool asyncpg, CORS cho cổng 3000–3003, include_router, khởi động worker.
- routers/transfers.py (Minh Hiếu): POST /transfers, callback shipped/received/decline, nút Hủy phiếu — F7, F10.f, F10.g.
- routers/dashboard.py, routers/invariant.py (Ngọc Chung): /dashboard/inventory, /dc-status, /invariant — F7, db.md 2.4.
- routers/failover.py, workers/health.py, workers/replay.py (Nhật Khánh): F7, F9, F10.h, F10.i, F10.j.
- Mỗi người chỉ thêm router/worker của mình và đăng ký bằng một dòng trong main.py.

Quy tắc chung xem CLAUDE.md ở gốc repo.
