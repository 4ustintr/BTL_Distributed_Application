// Lớp gọi API của WMS: chỉ dùng fetch. Mọi hàm trả {status, ok, body}; lỗi mạng hoặc quá thời gian
// trả status 0 (không ném lỗi), để giao diện quyết định có gửi lại hay không (F5, F9).
// sendStock là điểm để js/client.js (Nhật Khánh) chuyển sang /failover của HQ khi kho không trả lời.
(function (root) {
  "use strict";

  const TIMEOUT_MS = 5000;

  async function request(base, method, path, body) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    try {
      const res = await fetch(base + path, {
        method,
        headers: body === undefined ? {} : { "Content-Type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: ctrl.signal,
      });
      let parsed = null;
      try {
        parsed = await res.json();
      } catch (_) {
        parsed = null;
      }
      return { status: res.status, ok: res.ok, body: parsed };
    } catch (_) {
      return { status: 0, ok: false, body: null };
    } finally {
      clearTimeout(timer);
    }
  }

  function create(config) {
    const node = (method, path, body) => request(config.NODE_URL, method, path, body);
    const hq = (method, path, body) => request(config.HQ_URL, method, path, body);
    return {
      health: () => node("GET", "/health"),
      inventory: () => node("GET", "/inventory"),
      // F6: {request_id, product_id, qty, ref_type, created_by}; path do WmsLogic.buildStockRequest
      sendStock: (path, body) => node("POST", path, body),
      // F7: danh sách phiếu ở HQ, lọc theo trạng thái
      transfers: (status) => hq("GET", `/transfers?status=${encodeURIComponent(status)}`),
      // F6: ship ở kho nguồn, receive ở kho đích (cùng kho của WMS này), decline chuyển tiếp lên HQ
      ship: (id, createdBy) => node("POST", `/transfers/${id}/ship`, { created_by: createdBy }),
      receive: (id, createdBy) =>
        node("POST", `/transfers/${id}/receive`, { created_by: createdBy }),
      decline: (id) => node("POST", `/transfers/${id}/decline`, { reason: "DECLINED" }),
    };
  }

  root.WmsApi = { create };
})(typeof self !== "undefined" ? self : this);
