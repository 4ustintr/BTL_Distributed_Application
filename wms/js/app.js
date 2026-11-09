// Giao diện WMS của một kho (F6, F7): tra cứu tồn, nhập, xuất, xuất/nhận/từ chối phiếu điều chuyển.
// Logic thuần ở logic.js, lệnh gọi mạng ở api.js. Máy trạng thái NORMAL/FAILOVER/WAITING (F9) và
// chuyển sang /failover của HQ do js/client.js (Nhật Khánh) đảm nhiệm, không làm ở đây.
(function () {
  "use strict";

  const L = window.WmsLogic;
  const config = window.CONFIG;
  const dcId = Number(config.DC_ID);
  const api = window.WmsApi.create(config);
  const ids = new L.RequestIds();
  const REFRESH_MS = 3000;

  const $ = (id) => document.getElementById(id);
  const el = {
    dcName: $("dc-name"), mode: $("mode"), refresh: $("refresh"), notice: $("notice"),
    stockRows: $("stock-rows"), form: $("stock-form"), productId: $("product-id"),
    qty: $("qty"), createdBy: $("created-by"), result: $("result"),
    toShip: $("to-ship"), toReceive: $("to-receive"), toDecline: $("to-decline"),
    stockButtons: Array.from(document.querySelectorAll("#stock-form button")),
  };

  let busy = false;
  el.dcName.textContent = `${L.dcName(dcId)} (DC ${dcId})`;
  el.createdBy.value = `wms-dc${dcId}`;
  document.title = `WMS kho ${L.dcName(dcId)}`;

  function showResult(text, kind) {
    el.result.textContent = text;
    el.result.className = `result ${kind || ""}`.trim();
  }

  // Hai nguồn thông báo (chế độ kho, HQ) cập nhật độc lập nên giữ riêng rồi ghép lại
  const notices = { mode: "", hq: "" };
  function showNotice(source, text) {
    notices[source] = text || "";
    const all = [notices.mode, notices.hq].filter(Boolean).join(" ");
    el.notice.textContent = all;
    el.notice.hidden = !all;
  }

  function setBusy(value) {
    busy = value;
    el.stockButtons.forEach((b) => { b.disabled = value || b.dataset.locked === "1"; });
    document.querySelectorAll(".transfers button").forEach((b) => { b.disabled = value; });
  }

  // ---- Trạng thái kho -----------------------------------------------------------------------
  async function refreshHealth() {
    const res = await api.health();
    const mode = res.body && res.body.mode;
    el.mode.className = "badge";
    if (res.status === 0) {
      el.mode.textContent = "mất kết nối";
      el.mode.classList.add("badge-down");
    } else if (mode === "ONLINE") {
      el.mode.textContent = "ONLINE";
      el.mode.classList.add("badge-online");
    } else if (mode === "RECOVERING") {
      el.mode.textContent = "đang đồng bộ";
      el.mode.classList.add("badge-recovering");
    } else {
      el.mode.textContent = "chưa rõ";
      el.mode.classList.add("badge-unknown");
    }
    // F6: khi RECOVERING kho từ chối nhập, xuất từ WMS (503 DC_RECOVERING), nên khóa nút
    const locked = mode === "RECOVERING";
    el.stockButtons.forEach((b) => {
      b.dataset.locked = locked ? "1" : "0";
      b.disabled = busy || locked;
    });
    showNotice(
      "mode",
      locked ? "Kho đang đồng bộ: tạm khóa nhập, xuất cho tới khi HQ xác nhận xong." : "",
    );
  }

  // ---- Tồn kho ------------------------------------------------------------------------------
  function cell(text) {
    const td = document.createElement("td");
    td.textContent = String(text);
    return td;
  }

  function renderInventory(items) {
    el.stockRows.replaceChildren();
    if (!items.length) {
      const tr = document.createElement("tr");
      const td = cell("Kho chưa có hàng.");
      td.colSpan = 5;
      tr.append(td);
      el.stockRows.append(tr);
      return;
    }
    for (const item of items) {
      const tr = document.createElement("tr");
      tr.append(
        cell(item.product_id), cell(item.qty_on_hand), cell(item.qty_reserved),
        cell(L.availableOf(item)), cell(item.version),
      );
      el.stockRows.append(tr);
    }
  }

  async function refreshInventory() {
    const res = await api.inventory();
    if (res.ok) renderInventory(res.body.items);
    else if (res.status === 0) renderInventory([]);
  }

  // ---- Nhập, xuất ---------------------------------------------------------------------------
  async function submitStock(kind) {
    const fields = {
      productId: el.productId.value, qty: el.qty.value, createdBy: el.createdBy.value,
    };
    const errors = L.validateStockForm(fields);
    if (errors.length) return showResult(errors.join(" "), "error");

    const key = L.stockKey(kind, fields);
    const wasRetry = ids.pending.has(key);
    const { path, body } = L.buildStockRequest(kind, fields, ids.idFor(key));
    const verb = kind === "inbound" ? "Nhập" : "Xuất";
    setBusy(true);
    showResult(wasRetry ? `${verb} hàng: đang gửi lại cùng mã lệnh…` : `${verb} hàng: đang gửi…`);
    const res = await api.sendStock(path, body);
    setBusy(false);

    if (res.ok) {
      ids.settle(key);
      const dup = res.body.duplicate ? " (lệnh trùng, đã được áp dụng từ trước)" : "";
      showResult(
        `${verb} thành công${dup}. Tồn hiện tại ${res.body.qty_on_hand} (version ${res.body.version}).`,
        "ok",
      );
      await Promise.all([refreshInventory(), refreshHealth()]);
      return;
    }
    let text = L.describeError(res.status, res.body);
    if (L.isRetryable(res.status)) {
      text += " Bấm lại cùng thao tác để gửi lại (giữ nguyên mã lệnh, không bị trừ hai lần).";
    } else {
      ids.settle(key); // lỗi dứt khoát: thao tác kế tiếp là thao tác mới
    }
    showResult(text, "error");
    refreshHealth();
  }

  // ---- Phiếu điều chuyển --------------------------------------------------------------------
  function renderTransfers(list, target, actions) {
    target.replaceChildren();
    if (!list.length) {
      const li = document.createElement("li");
      li.className = "empty";
      li.textContent = "Không có phiếu.";
      target.append(li);
      return;
    }
    for (const t of list) {
      const li = document.createElement("li");
      const label = document.createElement("span");
      label.textContent = L.transferLabel(t);
      const box = document.createElement("span");
      box.className = "actions";
      for (const a of actions) {
        const b = document.createElement("button");
        b.type = "button";
        b.textContent = a.text;
        if (a.className) b.className = a.className;
        b.addEventListener("click", () => runTransferAction(t, a));
        box.append(b);
      }
      li.append(label, box);
      target.append(li);
    }
  }

  async function refreshTransfers() {
    const [created, shipped] = await Promise.all([api.transfers("CREATED"), api.transfers("SHIPPED")]);
    if (!created.ok || !shipped.ok) {
      showNotice("hq", "Không tải được danh sách phiếu từ HQ. Nhập, xuất tại kho vẫn dùng được.");
      renderTransfers([], el.toShip, []);
      renderTransfers([], el.toReceive, []);
      renderTransfers([], el.toDecline, []);
      return;
    }
    showNotice("hq", "");
    const listOf = (res) => (Array.isArray(res.body) ? res.body : (res.body && res.body.items) || []);
    const split = L.splitTransfers([...listOf(created), ...listOf(shipped)], dcId);
    renderTransfers(split.toShip, el.toShip, [{ text: "Xuất hàng", call: "ship" }]);
    renderTransfers(split.toReceive, el.toReceive, [{ text: "Nhận hàng", call: "receive" }]);
    renderTransfers(split.toDecline, el.toDecline, [
      { text: "Từ chối", call: "decline", className: "danger", confirm: true },
    ]);
  }

  async function runTransferAction(t, action) {
    const who = el.createdBy.value.trim();
    if (action.call !== "decline" && !who) return showResult("Nhập tên người thao tác.", "error");
    if (action.confirm && !window.confirm(`Từ chối phiếu ${t.transfer_code}?`)) return;
    setBusy(true);
    showResult(`${action.text} ${t.transfer_code}: đang gửi…`);
    const res = await api[action.call](t.transfer_id, who);
    setBusy(false);
    if (res.ok) {
      showResult(`${action.text} ${t.transfer_code} thành công.`, "ok");
    } else {
      let text = L.describeError(res.status, res.body);
      if (L.isRetryable(res.status)) text += " Bấm lại thao tác; kho sẽ nhận ra lệnh trùng.";
      showResult(text, "error");
    }
    await Promise.all([refreshTransfers(), refreshInventory(), refreshHealth()]);
  }

  // ---- Khởi động ----------------------------------------------------------------------------
  el.form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (busy) return;
    const kind = event.submitter && event.submitter.dataset.kind;
    if (kind) submitStock(kind);
  });

  async function refreshAll() {
    if (busy) return;
    await Promise.all([refreshHealth(), refreshInventory(), refreshTransfers()]);
  }

  el.refresh.addEventListener("click", refreshAll);
  refreshAll();
  setInterval(refreshAll, REFRESH_MS);
})();
