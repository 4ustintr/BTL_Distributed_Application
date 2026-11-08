Duyệt pull request số $ARGUMENTS. Không sửa code, không push, không approve trên GitHub.

1. gh pr view $ARGUMENTS --json title,author,headRefName,body,files; rồi gh pr checkout $ARGUMENTS.
2. Phạm vi: tên nhánh có đúng <tên>/<vai>/<việc> không; liệt kê mọi file sửa nằm ngoài phạm vi của tác giả
   theo bảng "Bản đồ repo và chủ sở hữu" trong CLAUDE.md.
3. Conflict: git fetch origin; git merge --no-commit --no-ff origin/master. Có conflict thì liệt kê file và đoạn bị trùng,
   rồi git merge --abort. Không có thì cũng git merge --abort.
4. Đúng spec: đọc git diff origin/master...HEAD, đối chiếu docs/spec.md, docs/db.md, docs/tests.md theo các mục spec
   ghi trong mô tả pull request:
   - mọi tên bảng, cột, endpoint, mã lỗi, sự kiện, topic đều có trong docs/;
   - thứ tự bước đúng F10.x;
   - từng ý trong checklist của .github/pull_request_template.md: ĐẠT / KHÔNG ĐẠT, kèm file:dòng làm bằng chứng.
5. Test: source .venv/Scripts/activate; ruff check .; pytest -m "not integration" cho thư mục test liên quan.
   Nếu stack đang chạy, chạy thêm test integration liên quan.
6. Kết luận: APPROVE hoặc REQUEST CHANGES. Liệt kê yêu cầu sửa cụ thể (file:dòng, sai gì, sửa thế nào),
   và viết sẵn nội dung comment review bằng tiếng Việt để tôi dán lên GitHub.
Cuối cùng git checkout master.
