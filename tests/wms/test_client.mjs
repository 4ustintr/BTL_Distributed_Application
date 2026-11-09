// wms/js/client.js – chế độ WMS theo F9 (Nhật Khánh). Chạy: make test-js
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import * as client from "../../wms/js/client.js";
import {createWmsClient, Message, Mode, newRequestId, nextMode} from "../../wms/js/client.js";

const CONFIG = {DC_ID: 2, NODE_URL: "http://b:8002", HQ_URL: "http://a:8000"};
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

const json = (status, body) => new Response(JSON.stringify(body), {status});
const health = (mode) => json(200, {dc_id: 2, mode, db: "ok"});
const down = () => Promise.reject(new TypeError("Failed to fetch"));

// Mạng giả: route(url, init) trả Response hoặc Promise bị từ chối
function fakeClient(route) {
  const calls = [];
  const client = createWmsClient(CONFIG, {
    fetch: async (url, init = {}) => {
      calls.push({url, body: init.body ? JSON.parse(init.body) : undefined});
      return route(url, init, calls);
    },
    sleep: async () => {},
    log: () => {},
  });
  return {client, calls};
}

test("bảng chuyển chế độ F9", () => {
  const fail = {ok: false};
  const online = {ok: true, mode: "ONLINE"};
  const recovering = {ok: true, mode: "RECOVERING"};
  assert.deepEqual(nextMode(Mode.NORMAL, 0, fail), {mode: Mode.NORMAL, missed: 1});
  assert.deepEqual(nextMode(Mode.NORMAL, 1, fail), {mode: Mode.FAILOVER, missed: 2});
  assert.deepEqual(nextMode(Mode.FAILOVER, 0, recovering), {mode: Mode.WAITING, missed: 0});
  assert.deepEqual(nextMode(Mode.FAILOVER, 3, online), {mode: Mode.NORMAL, missed: 0});
  assert.deepEqual(nextMode(Mode.WAITING, 0, online), {mode: Mode.NORMAL, missed: 0});
  assert.deepEqual(nextMode(Mode.NORMAL, 1, online), {mode: Mode.NORMAL, missed: 0});
  assert.deepEqual(nextMode(Mode.NORMAL, 0, recovering), {mode: Mode.WAITING, missed: 0});
  assert.deepEqual(nextMode(Mode.WAITING, 0, fail), {mode: Mode.WAITING, missed: 1});
  assert.deepEqual(nextMode(Mode.WAITING, 1, fail), {mode: Mode.FAILOVER, missed: 2});
  assert.deepEqual(nextMode(Mode.WAITING, 1, recovering), {mode: Mode.WAITING, missed: 0});
});

test("hằng số khớp common/constants.py (F11, F5)", () => {
  const py = readFileSync(new URL("../../common/constants.py", import.meta.url), "utf8");
  for (const name of ["WMS_HEALTH_INTERVAL_S", "WMS_HEALTH_TIMEOUT_S", "WMS_FAILOVER_MISSED", "HTTP_TIMEOUT_S"]) {
    const m = py.match(new RegExp(`^${name} = ([0-9.]+)`, "m"));
    assert.ok(m, `không thấy ${name} trong common/constants.py`);
    assert.equal(client[name], Number(m[1]), name);
  }
});

test("NORMAL mà kho báo RECOVERING thì WAITING; WAITING mà kho chết 2 lần thì FAILOVER", async () => {
  let nodeState = "RECOVERING";
  const {client: wms} = fakeClient(() => (nodeState === "DOWN" ? down() : health(nodeState)));
  const seen = [];
  wms.onModeChange((m) => seen.push(m));
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.WAITING);
  nodeState = "DOWN";
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.WAITING);
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.FAILOVER);
  assert.deepEqual(seen, [Mode.WAITING, Mode.FAILOVER]);
});

test("request_id là UUID v4, kể cả khi không có crypto.randomUUID (http không bảo mật)", () => {
  const noRandomUuid = {getRandomValues: (a) => globalThis.crypto.getRandomValues(a)};
  const id = newRequestId(noRandomUuid);
  assert.match(id, UUID_V4);
  assert.notEqual(id, newRequestId(noRandomUuid));
});

test("2 lần health lỗi thì FAILOVER; kho báo RECOVERING thì WAITING; ONLINE thì NORMAL", async () => {
  let nodeState = "DOWN";
  const {client} = fakeClient(() => (nodeState === "DOWN" ? down() : health(nodeState)));
  const seen = [];
  client.onModeChange((m) => seen.push(m));
  await client.checkHealth();
  assert.equal(client.getMode(), Mode.NORMAL);
  await client.checkHealth();
  assert.equal(client.getMode(), Mode.FAILOVER);
  nodeState = "RECOVERING";
  await client.checkHealth();
  assert.equal(client.getMode(), Mode.WAITING);
  nodeState = "ONLINE";
  await client.checkHealth();
  assert.deepEqual(seen, [Mode.FAILOVER, Mode.WAITING, Mode.NORMAL]);
});

test("health trả 503 (CSDL kho lỗi) tính là lỗi", async () => {
  const {client} = fakeClient(() => json(503, {dc_id: 2, mode: "ONLINE", db: "error"}));
  await client.checkHealth();
  await client.checkHealth();
  assert.equal(client.getMode(), Mode.FAILOVER);
});

test("NORMAL: gửi Node API đúng body F6", async () => {
  const {client, calls} = fakeClient(() => json(200, {qty_on_hand: 35, version: 7, duplicate: false}));
  const op = client.newOperation("outbound", {product_id: 1, qty: 5, created_by: "nv"});
  const r = await client.submit(op);
  assert.equal(r.via, "NODE");
  assert.equal(r.status, 200);
  assert.deepEqual(calls[0], {
    url: "http://b:8002/outbound",
    body: {request_id: op.request_id, product_id: 1, qty: 5, ref_type: "ISSUE", created_by: "nv"},
  });
});

test("FAILOVER: gặp 409 DC_NOT_OFFLINE thì báo và gửi lại cùng request_id tới HQ", async () => {
  let failoverCalls = 0;
  const {client, calls} = fakeClient((url) => {
    if (url.endsWith("/health")) return down();
    failoverCalls += 1;
    if (failoverCalls === 1) return json(409, {error_code: "DC_NOT_OFFLINE", message: "", details: {}});
    return json(200, {txn_id: "x", status: "PENDING", provisional_qty: 65});
  });
  await client.checkHealth();
  await client.checkHealth();
  const op = client.newOperation("inbound", {product_id: 1, qty: 30, created_by: "nv"});
  const statuses = [];
  const r = await client.submit(op, {onStatus: (m) => statuses.push(m)});
  assert.equal(r.via, "HQ");
  assert.equal(r.body.provisional_qty, 65);
  assert.deepEqual(statuses, [Message.CONFIRMING]);
  const posted = calls.filter((c) => c.url === "http://a:8000/failover/dc/2/inbound");
  assert.equal(posted.length, 2);
  assert.deepEqual(posted[0].body, {request_id: op.request_id, product_id: 1, qty: 30, created_by: "nv"});
  assert.equal(posted[1].body.request_id, op.request_id);
});

test("kho chết giữa lúc gửi: chuyển sang HQ mà giữ nguyên request_id", async () => {
  const {client, calls} = fakeClient((url) => {
    if (url.startsWith("http://b:8002")) return down();
    return json(200, {txn_id: "x", status: "PENDING", provisional_qty: 55});
  });
  const op = client.newOperation("outbound", {product_id: 1, qty: 10, created_by: "nv"});
  const pending = client.submit(op);
  await client.checkHealth();
  await client.checkHealth(); // vòng health song song đưa WMS sang FAILOVER
  const r = await pending;
  assert.equal(r.via, "HQ");
  const ids = calls.filter((c) => c.body).map((c) => c.body.request_id);
  assert.ok(ids.length >= 2);
  assert.ok(ids.every((id) => id === op.request_id));
});

test("WAITING: không gửi lệnh nhập, xuất", async () => {
  let state = "DOWN";
  const {client, calls} = fakeClient(() => (state === "DOWN" ? down() : health(state)));
  await client.checkHealth();
  await client.checkHealth();
  state = "RECOVERING";
  await client.checkHealth();
  const before = calls.length;
  const r = await client.submit(client.newOperation("inbound", {product_id: 1, qty: 1, created_by: "nv"}));
  assert.deepEqual(r, {ok: false, reason: Mode.WAITING, message: Message.WAITING});
  assert.equal(calls.length, before);
});

test("lỗi nghiệp vụ (422) trả nguyên cho giao diện, không gửi lại", async () => {
  const {client, calls} = fakeClient(() => json(422, {error_code: "INSUFFICIENT_STOCK", message: "", details: {}}));
  const r = await client.submit(client.newOperation("outbound", {product_id: 1, qty: 99, created_by: "nv"}));
  assert.equal(r.status, 422);
  assert.equal(r.body.error_code, "INSUFFICIENT_STOCK");
  assert.equal(calls.length, 1);
});
