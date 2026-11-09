export const DC_NAMES = Object.freeze({1: "Bắc", 2: "Trung", 3: "Nam"});
export const STATUS_NAMES = Object.freeze({
  ONLINE: "Trực tuyến", OFFLINE: "Mất kết nối", RECOVERING: "Đang phục hồi",
  CREATED: "Đã tạo", SHIPPED: "Đang vận chuyển", RECEIVED: "Đã nhận",
  CANCELLED: "Đã hủy", PENDING: "Đang chờ", APPLIED: "Đã đồng bộ", REJECTED: "Bị từ chối",
});
export const formatQty = (value) => Number(value).toLocaleString("vi-VN");
export function totals(items) {
  return items.reduce((sum, item) => ({
    stock: sum.stock + item.qty_on_hand,
    available: sum.available + item.qty_on_hand - item.qty_reserved,
    transit: sum.transit + item.qty_in_transit,
  }), {stock: 0, available: 0, transit: 0});
}
export function sourceLabel(item) {
  const stamp = item.as_of ? new Date(item.as_of).toLocaleTimeString("vi-VN") : "chưa có bản cập nhật";
  switch (item.source) {
    case "realtime": return `Trực tiếp · ${stamp}`;
    case "offline": return `Offline · Dữ liệu lúc ${stamp}`;
    case "provisional": return `Tạm tính · Bản gốc lúc ${stamp}`;
    default: return `Bản tổng hợp · Cập nhật lúc ${stamp}`;
  }
}
export function validateTransfer(from, to, qty) {
  if (!(from in DC_NAMES) || !(to in DC_NAMES)) return "Vui lòng chọn kho hợp lệ.";
  if (from === to) return "Kho nguồn và kho đích phải khác nhau.";
  if (!Number.isSafeInteger(qty) || qty <= 0) return "Số thùng phải là số nguyên dương.";
  return "";
}
