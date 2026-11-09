// wms/js/client.js – chế độ WMS theo F9 (Nhật Khánh). Chạy: make test-js
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import client from "../../wms/js/client.js";

const {createWmsClient, Message, Mode, newRequestId, nextMode} = client;
const CONFIG = {DC_ID: 2, NODE_URL: "http://b:8002", HQ_URL: "http://a:8000"};
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

const json = (status, body) => new Response(JSON.stringify(body), {status});
const health = (mode) => json(200, {dc_id: 2, mode, db: "ok"});
const down = () => Promise.reject(new TypeError("Failed to fetch"));
const stockBody = (kind, qty) => ({
  request_id: "2cb5d179-358d-5b52-8c39-0f1f4a1c167e",
  product_id: 1,
  qty,
  ref_type: kind === "inbound" ? "RECEIPT" : "ISSUE",
  created_by: "nv",
});

// Mạng giả: route(url, init, calls) trả Response hoặc Promise bị từ chối
function fakeClient(route) {
  const calls = [];
  const wms = createWmsClient(CONFIG, {
    fetch: async (url, init = {}) => {
      calls.push({url, body: init.body ? JSON.parse(init.body) : undefined});
      return route(url, init, calls);
    },
    sleep: async () => {},
    log: () => {},
  });
  return {wms, calls};
}

async function toFailover(wms) {
  await wms.checkHealth();
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.FAILOVER);
}

// ---------------------------------------------------------------------------------------------
// Bảng F9, hằng số, request_id
// ---------------------------------------------------------------------------------------------
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
  for (const name of [
    "WMS_HEALTH_INTERVAL_S", "WMS_HEALTH_TIMEOUT_S", "WMS_FAILOVER_MISSED",
    "HTTP_TIMEOUT_S", "HTTP_MAX_RETRIES", "HTTP_RETRY_INTERVAL_S",
  ]) {
    const m = py.match(new RegExp(`^${name} = ([0-9.]+)`, "m"));
    assert.ok(m, `không thấy ${name} trong common/constants.py`);
    assert.equal(client[name], Number(m[1]), name);
  }
});

test("nạp được bằng <script> thường: gán window.WmsClient", () => {
  const src = readFileSync(new URL("../../wms/js/client.js", import.meta.url), "utf8");
  const self = {};
  new Function("self", "module", src)(self, undefined);
  assert.equal(typeof self.WmsClient.createWmsClient, "function");
});

test("request_id là UUID v4, kể cả khi không có crypto.randomUUID (http không bảo mật)", () => {
  const noRandomUuid = {getRandomValues: (a) => globalThis.crypto.getRandomValues(a)};
  const id = newRequestId(noRandomUuid);
  assert.match(id, UUID_V4);
  assert.notEqual(id, newRequestId(noRandomUuid));
});

// ---------------------------------------------------------------------------------------------
// Health check -> chế độ
// ---------------------------------------------------------------------------------------------
test("2 lần health lỗi thì FAILOVER; kho báo RECOVERING thì WAITING; ONLINE thì NORMAL", async () => {
  let nodeState = "DOWN";
  const {wms} = fakeClient(() => (nodeState === "DOWN" ? down() : health(nodeState)));
  const seen = [];
  wms.onModeChange((m) => seen.push(m));
  await toFailover(wms);
  nodeState = "RECOVERING";
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.WAITING);
  nodeState = "ONLINE";
  await wms.checkHealth();
  assert.deepEqual(seen, [Mode.FAILOVER, Mode.WAITING, Mode.NORMAL]);
});

test("NORMAL mà kho báo RECOVERING thì WAITING; WAITING mà kho chết 2 lần thì FAILOVER", async () => {
  let nodeState = "RECOVERING";
  const {wms} = fakeClient(() => (nodeState === "DOWN" ? down() : health(nodeState)));
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.WAITING);
  nodeState = "DOWN";
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.WAITING);
  await wms.checkHealth();
  assert.equal(wms.getMode(), Mode.FAILOVER);
});

test("health trả 503 (CSDL kho lỗi) tính là lỗi", async () => {
  const {wms} = fakeClient(() => json(503, {dc_id: 2, mode: "ONLINE", db: "error"}));
  await toFailover(wms);
});

// ---------------------------------------------------------------------------------------------
// sendStock: cùng chữ ký và quy ước kết quả với api.sendStock của giao diện
// ---------------------------------------------------------------------------------------------
test("NORMAL: gửi Node API nguyên body F6", async () => {
  const {wms, calls} = fakeClient(() => json(200, {qty_on_hand: 35, version: 7, duplicate: false}));
  const r = await wms.sendStock("/outbound", stockBody("outbound", 5));
  assert.deepEqual([r.status, r.ok, r.via, r.body.qty_on_hand], [200, true, "NODE", 35]);
  assert.deepEqual(calls, [{url: "http://b:8002/outbound", body: stockBody("outbound", 5)}]);
});

test("FAILOVER: body F7 (bỏ ref_type); 409 DC_NOT_OFFLINE thì báo và gửi lại cùng request_id", async () => {
  let failoverCalls = 0;
  const {wms, calls} = fakeClient((url) => {
    if (url.endsWith("/health")) return down();
    failoverCalls += 1;
    if (failoverCalls === 1) return json(409, {error_code: "DC_NOT_OFFLINE", message: "", details: {}});
    return json(200, {txn_id: "x", status: "PENDING", provisional_qty: 65, duplicate: false});
  });
  await toFailover(wms);
  const statuses = [];
  const r = await wms.sendStock("/inbound", stockBody("inbound", 30), {onStatus: (m) => statuses.push(m)});
  assert.deepEqual([r.status, r.via, r.body.provisional_qty], [200, "HQ", 65]);
  assert.deepEqual(statuses, [Message.CONFIRMING]);
  const posted = calls.filter((c) => c.url === "http://a:8000/failover/dc/2/inbound");
  const {ref_type, ...f7} = stockBody("inbound", 30);
  assert.equal(ref_type, "RECEIPT");
  assert.deepEqual(posted.map((c) => c.body), [f7, f7]);
});

test("kho chết giữa lúc gửi: status 0; bấm lại sau khi FAILOVER thì đi HQ với đúng request_id", async () => {
  const {wms, calls} = fakeClient((url) =>
    url.startsWith("http://b:8002") ? down() : json(200, {txn_id: "x", status: "PENDING", provisional_qty: 55}));
  const body = stockBody("outbound", 10);
  const first = await wms.sendStock("/outbound", body);
  assert.deepEqual([first.status, first.ok, first.via], [0, false, "NODE"]);
  await toFailover(wms);
  const again = await wms.sendStock("/outbound", body);
  assert.deepEqual([again.status, again.via], [200, "HQ"]);
  const ids = calls.filter((c) => c.body).map((c) => c.body.request_id);
  assert.deepEqual(ids, [body.request_id, body.request_id]);
});

test("WAITING: không gửi, trả 503 DC_RECOVERING", async () => {
  const {wms, calls} = fakeClient(() => health("RECOVERING"));
  await wms.checkHealth();
  const before = calls.length;
  const r = await wms.sendStock("/inbound", stockBody("inbound", 1));
  assert.deepEqual([r.status, r.body.error_code, r.via], [503, "DC_RECOVERING", "WMS"]);
  assert.equal(calls.length, before);
});

test("lỗi nghiệp vụ (422) trả nguyên cho giao diện, không gửi lại", async () => {
  const {wms, calls} = fakeClient(() =>
    json(422, {error_code: "INSUFFICIENT_STOCK", message: "", details: {requested: 99, available: 10}}));
  const r = await wms.sendStock("/outbound", stockBody("outbound", 99));
  assert.deepEqual([r.status, r.body.details.requested], [422, 99]);
  assert.equal(calls.length, 1);
  await assert.rejects(() => wms.sendStock("/replay", stockBody("outbound", 1)));
});

// ---------------------------------------------------------------------------------------------
// transferAction: ship, receive, decline
// ---------------------------------------------------------------------------------------------
test("ship/receive/decline chờ lâu hơn 18 giây của F5 (5 × 2 + 4 × 2)", () => {
  assert.equal(client.CALLBACK_RETRY_BUDGET_S, 18);
  assert.ok(client.TRANSFER_ACTION_TIMEOUT_S > 18);
});

test("transferAction: đường dẫn và body F6 cho ship, receive, decline", async () => {
  const {wms, calls} = fakeClient(() => json(200, {status: "SHIPPED", duplicate: false}));
  for (const action of ["ship", "receive", "decline"]) {
    const r = await wms.transferAction("t-1", action, {created_by: "nv"});
    assert.deepEqual([r.status, r.via], [200, "NODE"]);
  }
  assert.deepEqual(calls, [
    {url: "http://b:8002/transfers/t-1/ship", body: {created_by: "nv"}},
    {url: "http://b:8002/transfers/t-1/receive", body: {created_by: "nv"}},
    {url: "http://b:8002/transfers/t-1/decline", body: {reason: "DECLINED"}},
  ]);
  await assert.rejects(() => wms.transferAction("t-1", "cancel", {created_by: "nv"}));
});

test("transferAction: 503 DC_UNREACHABLE trả nguyên; lỗi mạng status 0; không tự gửi lại", async () => {
  let reply = () => json(503, {error_code: "DC_UNREACHABLE", message: "", details: {}});
  const {wms, calls} = fakeClient(() => reply());
  const r = await wms.transferAction("t-1", "receive", {created_by: "nv"});
  assert.deepEqual([r.status, r.body.error_code], [503, "DC_UNREACHABLE"]);
  reply = down;
  const again = await wms.transferAction("t-1", "decline");
  assert.deepEqual([again.status, again.ok], [0, false]);
  assert.equal(calls.length, 2);
});

test("transferAction thật sự chờ tới TRANSFER_ACTION_TIMEOUT_S, không cắt ở 2 giây", async () => {
  const realSetTimeout = globalThis.setTimeout;
  const delays = [];
  globalThis.setTimeout = (fn, ms) => { delays.push(ms); return realSetTimeout(() => {}, 0); };
  try {
    const wms = createWmsClient(CONFIG, {fetch: async () => json(200, {status: "RECEIVED"}), sleep: async () => {}, log: () => {}});
    await wms.transferAction("t-1", "decline");
  } finally {
    globalThis.setTimeout = realSetTimeout;
  }
  assert.deepEqual(delays, [client.TRANSFER_ACTION_TIMEOUT_S * 1000]);
});
