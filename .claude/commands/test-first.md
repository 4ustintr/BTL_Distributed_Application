Theo kế hoạch đã duyệt, viết test trước cho: $ARGUMENTS

- Đặt trong tests/<service>/, tên test_tcXX_<ten_ngan>.py đúng ID ở docs/tests.md.
- Assert đúng các điều kiện ở cột "Kết quả mong đợi" của docs/tests.md; dùng mã trong scripts/demo_ids.env.
- Test cần container thật đánh dấu @pytest.mark.integration; test tùy chọn đánh dấu @pytest.mark.optional.
- Chưa viết code nghiệp vụ. Chạy test và xác nhận test đang đỏ vì thiếu chức năng (không phải lỗi cú pháp).
