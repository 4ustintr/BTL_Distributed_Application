# docs/

Bản Markdown của tài liệu thiết kế, để AI đọc. Bản gốc cho người đọc: https://docs.google.com/document/d/19rYYpcJDy31dYiV4I0HSz2jq6s84ijCzqlsqEDfcHc0/edit

| File | Thẻ trong Google Docs |
|---|---|
| overview.md | Tổng quan |
| solution.md | Giải pháp phân tán |
| db.md | Thiết kế CSDL |
| spec.md | Đặc tả kỹ thuật (chuẩn cao nhất) |
| demo.md | Kịch bản demo |
| tests.md | Kiểm thử và đánh giá |
| hld.md, hld.png | HLD |
| limitations.md | Hạn chế và hướng phát triển |
| plan.md | Phân công và kế hoạch |
| references.md | Tài liệu tham khảo |

Cập nhật: sửa Google Docs → File → Tải xuống → Markdown (.md) → chạy
`python scripts/sync_docs.py <file.md>` → mở pull request. Không sửa tay các file trong thư mục này.

Lưu ý: khi xuất Markdown, khối code mất xuống dòng (các dòng nối thành một). Nội dung vẫn đủ để đọc;
khi viết code, viết lại đúng cú pháp, đặc biệt các dòng chú thích SQL bắt đầu bằng `--`.
