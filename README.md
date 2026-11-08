# BTL Phát triển ứng dụng phân tán – Hệ thống quản lý kho phân tán

Trung tâm HQ (máy A) điều phối và tổng hợp; 3 kho Bắc, Trung, Nam (máy B) tự chủ, mỗi kho một PostgreSQL.
Tài liệu thiết kế gốc nằm trong `docs/` (nguồn sự thật duy nhất). Google Docs chỉ là bản báo cáo để nộp:
https://docs.google.com/document/d/19rYYpcJDy31dYiV4I0HSz2jq6s84ijCzqlsqEDfcHc0/edit

## Bắt đầu
1. Cài Git, Docker, Python 3.11, VS Code và extension Claude Code.
2. `python -m venv .venv && source .venv/bin/activate && pip install -r requirements-dev.txt`
3. Sao chép `.env.example` thành `.env`, điền IP hai máy.
4. Đọc `CLAUDE.md`, `docs/plan.md` (phân vai), rồi các mục spec của vai mình.

## Quy trình làm việc
- Nhánh `<tên>/<vai>/<việc>` (ví dụ `tuanminh/kho/outbound`), pull request vào `master`, cần 1 người duyệt và CI xanh.
- Với AI: `/plan-task` → `/test-first` → `/implement` → `/self-review` (lệnh trong `.claude/commands/`).
- Hạn merge 20 giờ mỗi ngày; 20–22 giờ trưởng nhóm tích hợp trên hai máy.

## Cấu trúc
```
common/    thư viện dùng chung        db/        migration SQL
infra/     compose 2 máy, connector    hq-api/    HQ API (máy A)
node-api/  Node API (máy B)            consumer/  CDC Consumer
dashboard/ giao diện HQ                wms/       giao diện kho
scripts/   demo, seed                  tests/     pytest theo docs/tests.md
docs/      tài liệu thiết kế gốc
```
