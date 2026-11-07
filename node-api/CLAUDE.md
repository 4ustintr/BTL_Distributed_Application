# node-api/

Node API (FastAPI) của mỗi kho, cổng 8001–8003, DC_ID từ biến môi trường.

Chủ sở hữu: Minh Danh (main.py), Tuấn Minh, Minh Hiếu. Mục spec cần đọc: F5, F6, F10.a–e.

- main.py (Minh Danh): tạo app, pool asyncpg, CORS, include_router, endpoint GET /health (F6).
- routers/inventory.py, services/stock.py, recovery.py (Tuấn Minh): /inventory, /inbound, /outbound, /replay, /recovery/complete — F6, F10.a.
- recovery.py chỉ đổi app.state.mode (RECOVERING/ONLINE); KHÔNG tạo endpoint /health (đã có trong main.py).
- services/stock.py là nơi DUY NHẤT chứa SQL thay đổi inventory, stock_movement, outbox_event (reserve, release, consume, add_stock, write_outbox).
- routers/transfers.py (Minh Hiếu): notice, ship, receive, decline, cancel, sync-status — F6, F10.b–e; chỉ gọi hàm của services/stock.py.

Quy tắc chung xem CLAUDE.md ở gốc repo.
