// Logic thuần của WMS: không đụng DOM, không gọi mạng, để test bằng node --test (tests/wms).
// Trình duyệt dùng qua window.WmsLogic; Node dùng qua require.
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.WmsLogic = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  // F6: /inbound ghi ref_type RECEIPT, /outbound ghi ref_type ISSUE
  const STOCK_KINDS = {
    inbound: { path: "/inbound", refType: "RECEIPT" },
    outbound: { path: "/outbound", refType: "ISSUE" },
  };
  const CREATED_BY_MAX = 50; // db.md 2.8: created_by VARCHAR(50)
  const DC_NAMES = { 1: "Bắc", 2: "Trung", 3: "Nam" }; // F12

  // crypto.randomUUID chỉ có ở ngữ cảnh an toàn (https, localhost); WMS chạy qua http://IP
  // nên dựng UUID v4 từ getRandomValues.
  function uuidv4() {
    const bytes = new Uint8Array(16);
    const c = typeof globalThis !== "undefined" ? globalThis.crypto : undefined;
    if (c && c.getRandomValues) c.getRandomValues(bytes);
    else for (let i = 0; i < 16; i++) bytes[i] = Math.floor(Math.random() * 256);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const h = Array.from(bytes, (b) => b.toString(16).padStart(2, "0"));
    return `${h.slice(0, 4).join("")}-${h.slice(4, 6).join("")}-${h.slice(6, 8).join("")}-` +
      `${h.slice(8, 10).join("")}-${h.slice(10).join("")}`;
  }

  function validateStockForm({ productId, qty, createdBy }) {
    const errors = [];
    if (!/^\d+$/.test(String(productId).trim())) errors.push("Mã hàng phải là số nguyên.");
    if (!/^[1-9]\d*$/.test(String(qty).trim())) errors.push("Số lượng phải là số nguyên dương.");
    const who = String(createdBy).trim();
    if (!who) errors.push("Nhập tên người thao tác.");
    else if (who.length > CREATED_BY_MAX) {
      errors.push(`Tên người thao tác tối đa ${CREATED_BY_MAX} ký tự.`);
    }
    return errors;
  }

  function buildStockRequest(kind, { productId, qty, createdBy }, requestId) {
    const spec = STOCK_KINDS[kind];
    return {
      path: spec.path,
      body: {
        request_id: requestId,
        product_id: Number(productId),
        qty: Number(qty),
        ref_type: spec.refType,
        created_by: String(createdBy).trim(),
      },
    };
  }

  // F9: WMS sinh request_id một lần cho mỗi thao tác của người dùng và giữ nguyên khi gửi lại.
  // Cùng thao tác (cùng loại, mã hàng, số lượng, người thao tác) chưa xong thì dùng lại mã cũ.
  function stockKey(kind, { productId, qty, createdBy }) {
    return [kind, String(productId).trim(), String(qty).trim(), String(createdBy).trim()].join("|");
  }

  class RequestIds {
    constructor(generate = uuidv4) {
      this.generate = generate;
      this.pending = new Map();
    }
    idFor(key) {
      if (!this.pending.has(key)) this.pending.set(key, this.generate());
      return this.pending.get(key);
    }
    settle(key) {
      this.pending.delete(key);
    }
  }

  // F5: chỉ lỗi mạng (status 0) hoặc quá thời gian/không sẵn sàng mới gửi lại cùng request_id.
  function isRetryable(status) {
    return status === 0 || status === 503 || status === 504;
  }

  function describeError(status, body) {
    if (status === 0) return "Không kết nối được tới máy chủ. Bấm gửi lại để thử tiếp.";
    const code = body && body.error_code;
    const d = (body && body.details) || {};
    switch (code) {
      case "INSUFFICIENT_STOCK":
        return `Không đủ hàng khả dụng (cần ${d.requested}, còn ${d.available}).`;
      case "VERSION_CONFLICT":
        return "Có thao tác khác đang sửa tồn kho này. Hãy bấm lại.";
      case "DC_RECOVERING":
        return "Kho đang đồng bộ, chưa nhận giao dịch. Thử lại sau ít phút.";
      case "DC_UNREACHABLE":
        return "Không gọi được HQ. Bấm lại thao tác; kho sẽ nhận ra lệnh trùng.";
      case "INVALID_STATE":
        return "Thao tác không hợp lệ với trạng thái hiện tại của phiếu.";
      case "VALIDATION_ERROR":
        return "Dữ liệu không hợp lệ" + (body.message ? `: ${body.message}` : ".");
      case "NOT_FOUND":
        return "Không tìm thấy dữ liệu yêu cầu.";
      case "UNAUTHORIZED":
        return "Sai khóa nội bộ.";
      case "UPSTREAM_TIMEOUT":
        return "Quá thời gian chờ. Bấm lại để thử tiếp.";
      default:
        return `Lỗi ${status}.`;
    }
  }

  const availableOf = (item) => item.qty_on_hand - item.qty_reserved;
  const totalQty = (t) => (t.items || []).reduce((sum, i) => sum + i.qty, 0);
  const dcName = (id) => DC_NAMES[id] || `kho ${id}`;

  function transferLabel(t) {
    return `${t.transfer_code}: ${dcName(t.from_dc_id)} → ${dcName(t.to_dc_id)}, ` +
      `${totalQty(t)} thùng`;
  }

  // Phiếu của kho này: cần xuất (kho nguồn, CREATED), cần nhận (kho đích, SHIPPED),
  // có thể từ chối (kho đích, CREATED: hàng chưa rời kho nguồn, F10.g).
  function splitTransfers(transfers, dcId) {
    const seen = new Set();
    const unique = transfers.filter((t) => !seen.has(t.transfer_id) && seen.add(t.transfer_id));
    return {
      toShip: unique.filter((t) => t.from_dc_id === dcId && t.status === "CREATED"),
      toReceive: unique.filter((t) => t.to_dc_id === dcId && t.status === "SHIPPED"),
      toDecline: unique.filter((t) => t.to_dc_id === dcId && t.status === "CREATED"),
    };
  }

  return {
    STOCK_KINDS, uuidv4, validateStockForm, buildStockRequest, stockKey, RequestIds,
    isRetryable, describeError, availableOf, totalQty, transferLabel, splitTransfers, dcName,
  };
});
