"""Tách file Markdown xuất từ Google Docs thành các file trong docs/.

Cách dùng: python scripts/sync_docs.py <file_xuat_tu_google_docs.md>
Google Docs: File -> Tải xuống -> Markdown (.md). Kiểm tra file tải về có đủ mọi thẻ.
"""
import re
import sys
from pathlib import Path

SECTIONS = {
    "Hệ thống quản lý mạng lưới kho phân tán": "overview.md",
    "Phân công và kế hoạch": "plan.md",
    "Giải pháp phân tán": "solution.md",
    "Thiết kế cơ sở dữ liệu": "db.md",
    "Đặc tả kỹ thuật": "spec.md",
    "Kịch bản demo": "demo.md",
    "Kiểm thử và đánh giá": "tests.md",
    "HLD": "hld.md",
    "Hạn chế và hướng phát triển": "limitations.md",
    "Tài liệu tham khảo": "references.md",
}


def code_tables_to_fences(text: str) -> str:
    """Bảng 1 cột (khối code trong Google Docs) -> khối ``` để AI đọc dễ hơn."""
    lines = text.split("\n")
    out, i = [], 0
    while i < len(lines):
        ln = lines[i]
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if ln.startswith("| ") and ln.rstrip().endswith(" |") and re.fullmatch(r"\|\s*:?-+:?\s*\|", nxt.strip()):
            body = ln.strip()[1:-1].strip()
            body = re.sub(r"\\(.)", r"\1", body)
            out += ["```text", body, "```"]
            i += 2
            continue
        out.append(ln)
        i += 1
    return "\n".join(out)


def strip_images(text: str) -> str:
    """Bỏ ảnh nhúng base64 (ERD, HLD dán trong Google Docs); ảnh để riêng trong docs/."""
    text = re.sub(r"(?m)^\[image\d+\]: <data:[^>]*>\s*$", "", text)
    return re.sub(r"!\[[^\]]*\]\[image\d+\]", "(xem hình trong thư mục docs/)", text)


def main(src: str) -> None:
    text = strip_images(Path(src).read_text(encoding="utf-8"))
    parts = re.split(r"(?m)^# (.+)$", text)
    docs = Path(__file__).resolve().parent.parent / "docs"
    docs.mkdir(exist_ok=True)
    found = []
    for title, body in zip(parts[1::2], parts[2::2]):
        name = SECTIONS.get(title.strip())
        if not name:
            continue
        content = f"# {title.strip()}\n{code_tables_to_fences(body)}".rstrip() + "\n"
        header = ("<!-- File sinh tự động từ Google Docs bằng scripts/sync_docs.py. "
                  "Không sửa tay: sửa Google Docs rồi chạy lại script. -->\n\n")
        (docs / name).write_text(header + content, encoding="utf-8")
        found.append(name)
    missing = sorted(set(SECTIONS.values()) - set(found))
    print("Đã ghi:", ", ".join(found))
    if missing:
        print("THIẾU (kiểm tra file xuất có đủ thẻ không):", ", ".join(missing))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
