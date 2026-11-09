# tests/

Test pytest theo docs/tests.md.

Chủ sở hữu: Minh Danh (conftest.py); mỗi người sở hữu test của mình. Mục spec cần đọc: docs/tests.md.

- Mỗi service một thư mục: tests/common, tests/node_api, tests/hq_api, tests/consumer.
- Mỗi thư mục có conftest.py tự thêm đường dẫn service vào sys.path; chạy pytest theo từng thư mục để tránh trùng tên module (routers/ có ở cả hai API).
- Test cần container thật: @pytest.mark.integration; conftest.py skip khi HOST_A_IP khác HOST_B_IP. Test tùy chọn: @pytest.mark.optional.
- Dùng mã cố định trong scripts/demo_ids.env.

Quy tắc chung xem CLAUDE.md ở gốc repo.
