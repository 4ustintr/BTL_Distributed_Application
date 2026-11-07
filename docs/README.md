# docs/ – Bản gốc của tài liệu thiết kế

Từ ngày chốt, **các file trong thư mục này là nguồn sự thật duy nhất** của dự án.
Google Docs chỉ còn là bản báo cáo để nộp, được cập nhật lại từ đây trước ngày nộp.

| File | Nội dung |
|---|---|
| spec.md | Đặc tả kỹ thuật F1–F12 (chuẩn cao nhất) |
| db.md | Thiết kế CSDL |
| demo.md | Kịch bản demo |
| tests.md | Kiểm thử và đánh giá: bảng kết quả chuẩn, test case, số đo |
| overview.md | Tổng quan |
| solution.md | Giải pháp phân tán, đối chiếu lý thuyết |
| plan.md | Phân công, kế hoạch, chính sách dùng AI |
| limitations.md | Hạn chế và hướng phát triển |
| hld.md, hld.png | Kiến trúc tổng thể |
| references.md | Tài liệu tham khảo |

## Quy tắc sửa
- Chỉ trưởng nhóm sửa thư mục này (CODEOWNERS). Thành viên cần đổi thì đề xuất.
- Sửa bằng lệnh `/doc-change <nội dung>` trong Claude Code để mọi chỗ liên quan được sửa cùng lúc.
- Thay đổi hợp đồng (tên, API, mã lỗi, sự kiện, tham số): commit và merge `docs/` TRƯỚC, báo nhóm `git pull`,
  rồi mới sửa code.
- Thứ tự ưu tiên khi mâu thuẫn: spec.md > db.md > demo.md.
