// Test logic thuần của WMS (wms/js/logic.js): node --test, không cần npm (make test-js).
const test = require("node:test");
const assert = require("node:assert/strict");
const L = require("../../wms/js/logic.js");

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

test("uuidv4 sinh UUID v4 hợp lệ kể cả khi không có crypto.randomUUID", () => {
  const ids = new Set();
  for (let i = 0; i < 50; i++) {
    const id = L.uuidv4();
    assert.match(id, UUID_RE);
    ids.add(id);
  }
  assert.equal(ids.size, 50);
});

test("validateStockForm: qty phải là số nguyên dương, product_id và người thao tác bắt buộc", () => {
  assert.deepEqual(L.validateStockForm({ productId: "1", qty: "5", createdBy: "an" }), []);
  assert.equal(L.validateStockForm({ productId: "1", qty: "0", createdBy: "an" }).length, 1);
  assert.equal(L.validateStockForm({ productId: "1", qty: "-2", createdBy: "an" }).length, 1);
  assert.equal(L.validateStockForm({ productId: "1", qty: "1.5", createdBy: "an" }).length, 1);
  assert.equal(L.validateStockForm({ productId: "", qty: "5", createdBy: "an" }).length, 1);
  assert.equal(L.validateStockForm({ productId: "1", qty: "5", createdBy: "  " }).length, 1);
  assert.equal(
    L.validateStockForm({ productId: "1", qty: "5", createdBy: "x".repeat(51) }).length, 1,
  ); // created_by VARCHAR(50)
});

test("buildStockRequest: đúng endpoint, ref_type và body theo F6", () => {
  const fields = { productId: "1", qty: "10", createdBy: "an" };
  const inb = L.buildStockRequest("inbound", fields, "id-1");
  assert.equal(inb.path, "/inbound");
  assert.deepEqual(inb.body, {
    request_id: "id-1", product_id: 1, qty: 10, ref_type: "RECEIPT", created_by: "an",
  });
  const out = L.buildStockRequest("outbound", { ...fields, qty: "5" }, "id-2");
  assert.equal(out.path, "/outbound");
  assert.equal(out.body.ref_type, "ISSUE");
  assert.equal(out.body.qty, 5);
});

test("RequestIds giữ nguyên request_id khi gửi lại, đổi sau khi xong (F9)", () => {
  let n = 0;
  const ids = new L.RequestIds(() => `id-${++n}`);
  const fields = { productId: "1", qty: "5", createdBy: "an" };
  const key = L.stockKey("outbound", fields);
  assert.equal(ids.idFor(key), "id-1");
  assert.equal(ids.idFor(key), "id-1"); // gửi lại cùng thao tác
  assert.notEqual(ids.idFor(L.stockKey("outbound", { ...fields, qty: "6" })), "id-1");
  assert.notEqual(ids.idFor(L.stockKey("inbound", fields)), "id-1"); // nhập khác xuất
  ids.settle(key);
  assert.equal(ids.idFor(key), "id-4"); // thao tác mới sau khi đã xong
});

test("isRetryable: lỗi mạng, 503, 504 gửi lại được; 4xx và 422 thì không", () => {
  for (const s of [0, 503, 504]) assert.equal(L.isRetryable(s), true, String(s));
  for (const s of [200, 400, 404, 409, 422]) assert.equal(L.isRetryable(s), false, String(s));
});

test("describeError: thông báo tiếng Việt theo error_code F5", () => {
  const msg = (status, code, details) => L.describeError(status, { error_code: code, details });
  assert.match(msg(422, "INSUFFICIENT_STOCK", { requested: 30, available: 20 }), /30.*20/);
  assert.match(msg(503, "DC_RECOVERING"), /đồng bộ/);
  assert.match(msg(409, "VERSION_CONFLICT"), /bấm lại/i);
  assert.match(msg(409, "INVALID_STATE"), /trạng thái/);
  assert.match(msg(503, "DC_UNREACHABLE"), /HQ/);
  assert.match(msg(400, "VALIDATION_ERROR"), /không hợp lệ/i);
  assert.match(L.describeError(0, null), /kết nối/);
  assert.match(L.describeError(500, null), /500/);
});

test("availableOf = qty_on_hand - qty_reserved", () => {
  assert.equal(L.availableOf({ qty_on_hand: 100, qty_reserved: 50 }), 50);
});

test("splitTransfers: phiếu cần xuất, nhận, từ chối của kho này", () => {
  const t = (id, from, to, status) => ({
    transfer_id: id, transfer_code: id, from_dc_id: from, to_dc_id: to, status, items: [],
  });
  const list = [
    t("a", 1, 3, "CREATED"), // Bắc xuất cho Nam
    t("b", 1, 3, "SHIPPED"),
    t("c", 2, 1, "CREATED"), // Trung gửi Bắc, chưa xuất
    t("d", 2, 1, "SHIPPED"),
    t("e", 1, 2, "RECEIVED"),
    t("f", 3, 2, "CANCELLED"),
    t("a", 1, 3, "CREATED"), // trùng
  ];
  const bac = L.splitTransfers(list, 1);
  assert.deepEqual(bac.toShip.map((x) => x.transfer_id), ["a"]);
  assert.deepEqual(bac.toReceive.map((x) => x.transfer_id), ["d"]);
  assert.deepEqual(bac.toDecline.map((x) => x.transfer_id), ["c"]);
  const nam = L.splitTransfers(list, 3);
  assert.deepEqual(nam.toShip, []);
  assert.deepEqual(nam.toReceive.map((x) => x.transfer_id), ["b"]);
  assert.deepEqual(nam.toDecline.map((x) => x.transfer_id), ["a"]);
});

test("transferLabel và totalQty", () => {
  const t = {
    transfer_code: "TR-2026-000001", from_dc_id: 1, to_dc_id: 3,
    items: [{ product_id: 1, qty: 50 }, { product_id: 2, qty: 5 }],
  };
  assert.equal(L.totalQty(t), 55);
  assert.match(L.transferLabel(t), /TR-2026-000001.*Bắc.*Nam.*55/);
});
