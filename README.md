# BTL Phát triển ứng dụng phân tán – Hệ thống quản lý kho phân tán

Trung tâm HQ (máy A) điều phối và tổng hợp; 3 kho Bắc, Trung, Nam (máy B) tự chủ, mỗi kho một PostgreSQL.
Tài liệu thiết kế gốc nằm trong `docs/` (nguồn sự thật duy nhất). Google Docs chỉ là bản báo cáo để nộp:
https://docs.google.com/document/d/19rYYpcJDy31dYiV4I0HSz2jq6s84ijCzqlsqEDfcHc0/edit

## Bắt đầu (làm 1 lần)
Dùng terminal **Git Bash** (không dùng PowerShell), Docker Desktop đang chạy.

```bash
git clone https://github.com/4ustintr/BTL_Distributed_Application.git
cd BTL_Distributed_Application
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements-dev.txt
cp .env.example .env      # điền HOST_A_IP, HOST_B_IP = IP Wi-Fi máy mình (ipconfig); mật khẩu chỉ chữ và số
make reset                # dựng toàn bộ hệ thống; lỗi thì xem infra/README.md
```

Đọc trước khi code: `CLAUDE.md`, `docs/plan.md` (vai của mình), các mục spec của vai mình trong `docs/spec.md`.
`docs/` là bản gốc của tài liệu; Google Docs chỉ là bản báo cáo, không dùng để code.

## Quy trình làm một việc

```bash
# 1. Lấy code mới nhất
git checkout master && git pull

# 2. Tạo nhánh theo dạng <tên>/<vai>/<việc>
git checkout -b khanh/failover/health
```

3. Code với Claude Code, mỗi việc một phiên chat mới, đi đủ 4 lệnh:
   `/plan-task <việc>` → duyệt kế hoạch → `/test-first` → `/implement` → `/self-review`.
   Không dùng Claude thì lấy mẫu prompt trong `.claude/commands/` dán vào AI của mình.

```bash
# 4. Gộp master mới vào nhánh, gỡ conflict trên máy mình trước
git fetch origin && git merge origin/master

# 5. Kiểm tra, commit, push
ruff check . && make test-ci
git add -A && git commit -m "feat(hq-api): add health monitor"
git push -u origin khanh/failover/health
```

6. Mở pull request vào `master`, điền đủ mẫu (mục spec, test, checklist). Trưởng nhóm duyệt và merge.

| Tên | Vai | Tiền tố nhánh |
|---|---|---|
| Minh Danh | Hạ tầng, tích hợp | `danh/infra/` |
| Tuấn Minh | Giao dịch tại kho | `tuanminh/kho/` |
| Minh Hiếu | Saga điều chuyển | `hieu/saga/` |
| Ngọc Chung | CDC, Dashboard | `chung/cdc/` |
| Nhật Khánh | Chịu lỗi, phục hồi | `khanh/failover/` |

**Commit message:** `<type>(<scope>): <mô tả tiếng Anh, viết thường>`.
Type: `feat`, `fix`, `docs`, `test`, `refactor`, `ci`, `build`, `chore`.
Scope: `common`, `db`, `infra`, `hq-api`, `node-api`, `consumer`, `dashboard`, `wms`, `tests`, `docs`.

## Quy tắc bắt buộc
- Chỉ sửa file thuộc phần mình (bảng "Bản đồ repo và chủ sở hữu" trong `CLAUDE.md`).
- Không tự đặt tên bảng, cột, endpoint, mã lỗi, sự kiện ngoài `docs/`; dùng hằng số trong `common/`.
- Spec chưa rõ thì hỏi trưởng nhóm, không để AI tự đoán. Muốn đổi spec thì đề xuất, trưởng nhóm sửa `docs/` trước.
- Viết test trước rồi mới code; test xanh mới mở pull request.

## Nhịp hằng ngày
- **Trước 20h:** merge pull request trong ngày.
- **20–22h:** trưởng nhóm duyệt bằng `/review-pr`, ghép code, chạy `make reset` và `make test`, báo kết quả vào nhóm.
- Vướng quá 1 giờ thì báo ngay trong nhóm.

## Cấu trúc
```
common/    thư viện dùng chung        db/        migration SQL
infra/     compose 2 máy, connector    hq-api/    HQ API (máy A)
node-api/  Node API (máy B)            consumer/  CDC Consumer
dashboard/ giao diện HQ                wms/       giao diện kho
scripts/   demo, seed                  tests/     pytest theo docs/tests.md
docs/      tài liệu thiết kế gốc
```
