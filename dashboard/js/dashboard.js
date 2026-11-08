import {DC_NAMES, STATUS_NAMES, formatQty, totals, sourceLabel, validateTransfer} from "./domain.js";

const $ = (id) => document.getElementById(id);
const root = window.CONFIG?.HQ_URL?.replace(/\/$/, "");
const state = {inventory: null, statuses: [], busy: false, holdUntil: 0, transfersLoaded: false, pendingLoaded: false};

async function api(path, options = {}) {
  if (!root) throw new Error("Chưa có địa chỉ kết nối trung tâm.");
  const response = await fetch(`${root}${path}`, {
    ...options, headers: {"Content-Type": "application/json", ...options.headers},
    signal: AbortSignal.timeout(options.method === "POST" ? 120000 : 8000), cache: "no-store",
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.message || `Không tải được dữ liệu (${response.status}).`);
  return data;
}
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function badge(status) {
  // Status affects presentation only; values from the server are never parsed as HTML.
  return el("span", `badge ${status}`, STATUS_NAMES[status] || status);
}
function notice(text) {
  $("notice").textContent = text;
  $("notice").hidden = !text;
}
function renderInventory(data) {
  state.inventory = data;
  const sum = totals(data.items);
  for (const [id, key] of [["total-stock", "stock"], ["total-available", "available"], ["total-transit", "transit"]]) {
    $(id).textContent = formatQty(sum[key]);
  }
  $("warehouses").replaceChildren(...data.items.map((item) => {
    const status = state.statuses.find((row) => row.dc_id === item.dc_id);
    const card = el("article", "warehouse"), top = el("div", "warehouse-top");
    const name = el("div", "warehouse-name", `Kho ${DC_NAMES[item.dc_id] || item.dc_id}`);
    name.append(el("span", "warehouse-code", `DC-0${item.dc_id} / VIỆT NAM`));
    top.append(name, badge(status?.status || "Chưa rõ"));
    const value = el("div", "stock-value");
    value.append(el("strong", "", formatQty(item.qty_on_hand)), el("span", "", "thùng tồn"));
    const stats = el("div", "warehouse-stats");
    for (const [label, qty] of [["Khả dụng", item.qty_on_hand - item.qty_reserved], ["Giữ chỗ", item.qty_reserved], ["Đang về", item.qty_in_transit]]) {
      const list = el("dl");
      list.append(el("dt", "", label), el("dd", "", formatQty(qty)));
      stats.append(list);
    }
    const source = el("div", `warehouse-source ${item.source}`, sourceLabel(item));
    if (status?.status === "OFFLINE") {
      source.append(document.createTextNode(` · Offline từ ${new Date(status.updated_at).toLocaleTimeString("vi-VN")}`));
    }
    card.append(top, value, stats, source);
    return card;
  }));
}
function setRows(id, rows, columns, message) {
  if (rows.length) $(id).replaceChildren(...rows);
  else {
    const row = el("tr"), cell = el("td", "empty", message);
    cell.colSpan = columns;
    row.append(cell);
    $(id).replaceChildren(row);
  }
}
function cells(row, values) {
  for (const value of values) row.append(el("td", "", value));
}
async function loadTransfers() {
  try {
    const data = await api("/transfers");
    if (!Array.isArray(data)) throw new Error("Chưa tải được danh sách phiếu.");
    const rows = data.map((transfer) => {
      const row = el("tr");
      const quantity = transfer.items.filter((item) => item.product_id === 1).reduce((sum, item) => sum + item.qty, 0);
      cells(row, [transfer.transfer_code, `${DC_NAMES[transfer.from_dc_id]} → ${DC_NAMES[transfer.to_dc_id]}`, formatQty(quantity)]);
      const status = el("td"), action = el("td");
      status.append(badge(transfer.status));
      if (transfer.status === "CREATED") {
        const button = el("button", "row-action", "Hủy phiếu");
        button.addEventListener("click", () => cancelTransfer(transfer, button));
        action.append(button);
      } else action.textContent = "—";
      row.append(status, action);
      return row;
    });
    setRows("transfer-rows", rows, 5, "Chưa có phiếu điều chuyển.");
    state.transfersLoaded = true;
    $("transfer-message").textContent = "";
  } catch (error) {
    if (!state.transfersLoaded) setRows("transfer-rows", [], 5, "Chưa tải được danh sách phiếu.");
    $("transfer-message").textContent = `Danh sách phiếu chưa được cập nhật. ${error.message}`;
  }
}
async function loadPending() {
  const selected = $("pending-dc").value;
  try {
    const data = await api(`/pending-txn${selected ? `?dc_id=${selected}` : ""}`);
    if (selected !== $("pending-dc").value) return;
    const rows = data.map((txn) => {
      const row = el("tr");
      cells(row, [txn.txn_id, DC_NAMES[txn.dc_id], txn.txn_type === "INBOUND" ? "Nhập" : "Xuất", formatQty(txn.qty)]);
      const status = el("td");
      status.append(badge(txn.status));
      row.append(status, el("td", "", txn.error_msg || "—"));
      return row;
    });
    setRows("pending-rows", rows, 6, "Không có giao dịch ghi hộ.");
    state.pendingLoaded = true;
    $("pending-message").textContent = "";
  } catch (error) {
    if (!state.pendingLoaded) setRows("pending-rows", [], 6, "Chưa tải được giao dịch ghi hộ.");
    $("pending-message").textContent = `Giao dịch ghi hộ chưa được cập nhật. ${error.message}`;
  }
}
async function refresh(manual = false) {
  if (state.busy || (!manual && Date.now() < state.holdUntil)) return;
  state.busy = true;
  $("refresh").disabled = true;
  $("reconcile").disabled = true;
  try {
    const [inventory, statuses] = await Promise.allSettled([
      api("/dashboard/inventory?mode=snapshot&product_id=1"), api("/dc-status"),
    ]);
    state.statuses = statuses.status === "fulfilled" ? statuses.value : [];
    const online = state.statuses.filter((row) => row.status === "ONLINE").length;
    $("online-count").textContent = statuses.status === "fulfilled" ? `${online} / 3` : "—";
    $("status-caption").textContent = statuses.status === "rejected" ? "Chưa xác nhận được trạng thái" : online === 3 ? "Toàn bộ kho đang trực tuyến" : "Một số kho cần theo dõi";
    if (inventory.status === "rejected") throw inventory.reason;
    renderInventory(inventory.value);
    $("read-mode").textContent = "Bản tổng hợp";
    $("connection").className = "connection";
    $("connection").textContent = "HQ đã kết nối";
    $("last-refresh").textContent = `Làm mới ${new Date().toLocaleTimeString("vi-VN")}`;
    notice(statuses.status === "rejected" ? "Đã tải tồn kho; chưa xác nhận được trạng thái các kho." : "");
    await Promise.allSettled([loadTransfers(), loadPending()]);
  } catch (error) {
    if (!state.inventory) $("warehouses").replaceChildren(el("p", "empty", "Chưa tải được tồn kho."));
    notice(`Không thể cập nhật từ trung tâm. ${error.message} Số đang hiển thị là lần tải thành công gần nhất.`);
    $("connection").className = "connection error";
    $("connection").textContent = "Mất kết nối HQ";
  } finally {
    state.busy = false;
    $("refresh").disabled = false;
    $("reconcile").disabled = false;
  }
}
$("reconcile").addEventListener("click", async () => {
  if (state.busy) return;
  state.busy = true;
  $("reconcile").disabled = true;
  try {
    const data = await api("/dashboard/inventory?mode=realtime&product_id=1");
    const differences = data.items.filter((item) => {
      const old = state.inventory?.items.find((row) => row.dc_id === item.dc_id);
      return item.source === "realtime" && old?.source === "snapshot" && (old.qty_on_hand !== item.qty_on_hand || old.qty_reserved !== item.qty_reserved);
    });
    renderInventory(data);
    $("read-mode").textContent = "Kết quả đối soát · giữ 15 giây";
    state.holdUntil = Date.now() + 15000;
    notice(differences.length ? `Có chênh lệch so với bản tổng hợp tại kho ${differences.map((item) => DC_NAMES[item.dc_id]).join(", ")}. Cần kiểm tra lại sau khi đồng bộ.` : "");
  } catch (error) { notice(error.message); }
  finally { state.busy = false; $("reconcile").disabled = false; }
});
$("check-invariant").addEventListener("click", async () => {
  $("check-invariant").disabled = true;
  try {
    const data = await api("/invariant?product_id=1");
    $("invariant-result").className = data.diff === 0 ? "success-text" : "warning-text";
    $("invariant-result").textContent = data.diff === 0 ? "Cân bằng hàng hóa · Chênh lệch 0" : `Cần đối soát · Chênh lệch ${formatQty(data.diff)} thùng`;
    $("invariant-details").textContent = `Tồn ${data.stock} + Đi đường ${data.in_transit} + Đã xuất ${data.shipped_out} − Đã nhập ${data.received_in} = ${data.diff}`;
  } catch (error) {
    $("invariant-result").className = "warning-text";
    $("invariant-result").textContent = `Chưa kiểm tra được. ${error.message}`;
    $("invariant-details").textContent = "Cần truy cập đủ ba kho để kiểm tra bảo toàn hàng.";
  } finally { $("check-invariant").disabled = false; }
});
async function cancelTransfer(transfer, button) {
  if (!window.confirm(`Hủy phiếu ${transfer.transfer_code}? Hàng giữ chỗ sẽ được hoàn lại nếu kho nguồn chưa xuất.`)) return;
  button.disabled = true;
  try {
    await api(`/transfers/${encodeURIComponent(transfer.transfer_id)}/decline`, {method: "POST", body: JSON.stringify({reason: "TIMEOUT"})});
    await loadTransfers();
  } catch (error) { notice(error.message); }
  finally { button.disabled = false; }
}
$("new-transfer").addEventListener("click", () => { $("form-error").textContent = ""; $("transfer-dialog").showModal(); });
$("close-dialog").addEventListener("click", () => $("transfer-dialog").close());
$("transfer-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const from = Number($("from-dc").value), to = Number($("to-dc").value), qty = Number($("transfer-qty").value);
  const error = validateTransfer(from, to, qty);
  if (error) { $("form-error").textContent = error; return; }
  $("submit-transfer").disabled = true;
  try {
    await api("/transfers", {method: "POST", body: JSON.stringify({from_dc_id: from, to_dc_id: to, items: [{product_id: 1, qty}]})});
    $("transfer-dialog").close();
    await refresh(true);
  } catch (failure) { $("form-error").textContent = failure.message; }
  finally { $("submit-transfer").disabled = false; }
});
$("refresh").addEventListener("click", () => refresh(true));
$("pending-dc").addEventListener("change", loadPending);
await refresh();
setInterval(() => { if (!document.hidden && !$("transfer-dialog").open) refresh(); }, 2000);

