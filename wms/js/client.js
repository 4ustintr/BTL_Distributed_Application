// wms/js/client.js – chế độ NORMAL, FAILOVER, WAITING của WMS (spec F9, F11) – Nhật Khánh.
//
// Giao diện (Tuấn Minh) dùng:
//   import {createWmsClient, Mode} from "./js/client.js";
//   const wms = createWmsClient(window.CONFIG);    // {DC_ID, NODE_URL, HQ_URL} từ config.js
//   wms.onModeChange((mode) => { ...khóa/mở nút nhập, xuất khi mode === Mode.WAITING... });
//   wms.start();
//   const op = wms.newOperation("outbound", {product_id: 1, qty: 10, created_by: "nv"});
//   const result = await wms.submit(op, {onStatus: (msg) => hienThongBao(msg)});
//   // result = {ok, status, body, via: "NODE" | "HQ"} hoặc {ok: false, reason, message}
//
// request_id sinh MỘT lần trong newOperation và giữ nguyên mọi lần gửi lại, kể cả khi đổi từ
// Node API sang HQ (F9). Bấm lại sau khi lỗi thì truyền lại đúng op cũ.

export const Mode = Object.freeze({NORMAL: "NORMAL", FAILOVER: "FAILOVER", WAITING: "WAITING"});
const NodeMode = Object.freeze({ONLINE: "ONLINE", RECOVERING: "RECOVERING"}); // F6 GET /health
const RefType = Object.freeze({inbound: "RECEIPT", outbound: "ISSUE"}); // F6 /inbound, /outbound
const ERR_DC_NOT_OFFLINE = "DC_NOT_OFFLINE"; // F5, F7

// Tham số F11, F5. JS không import được common/constants.py nên khai báo lại ĐÚNG TÊN và giá trị;
// tests/wms/test_client.mjs đọc common/constants.py để bảo đảm hai bên không lệch nhau.
export const WMS_HEALTH_INTERVAL_S = 2.0; // F11: chu kỳ WMS kiểm tra Node API
export const WMS_HEALTH_TIMEOUT_S = 1.0; // F11: timeout GET /health của WMS tới Node API
export const WMS_FAILOVER_MISSED = 2; // F11, F9: 2 lần lỗi liên tiếp -> FAILOVER
export const HTTP_TIMEOUT_S = 2.0; // F5, F11: timeout lệnh gọi
export const RESEND_MS = 2000; // F9: gặp 409 DC_NOT_OFFLINE thì gửi lại sau 2 giây

const HEALTH_INTERVAL_MS = WMS_HEALTH_INTERVAL_S * 1000;
const HEALTH_TIMEOUT_MS = WMS_HEALTH_TIMEOUT_S * 1000;
const REQUEST_TIMEOUT_MS = HTTP_TIMEOUT_S * 1000;

export const Message = Object.freeze({
  CONFIRMING: "Đang xác nhận trạng thái kho, sẽ tự gửi lại sau 2 giây",
  WAITING: "Kho đang đồng bộ, tạm khóa nhập, xuất",
  NODE_UNREACHABLE: "Không gọi được kho, đang thử lại",
  HQ_UNREACHABLE: "Không gọi được trung tâm, đang thử lại",
});

// ---------------------------------------------------------------------------
// Bảng chuyển chế độ F9 (hàm thuần)
// health: {ok: false} hoặc {ok: true, mode: "ONLINE" | "RECOVERING"}
// ---------------------------------------------------------------------------
export function nextMode(mode, missed, health) {
  if (!health.ok) {
    const count = missed + 1;
    // NORMAL -> FAILOVER, WAITING -> FAILOVER: 2 lần GET /health thất bại liên tiếp
    const canFailover = mode === Mode.NORMAL || mode === Mode.WAITING;
    if (canFailover && count >= WMS_FAILOVER_MISSED) return {mode: Mode.FAILOVER, missed: count};
    return {mode, missed: count};
  }
  // FAILOVER, WAITING -> NORMAL: Node API báo ONLINE
  if (health.mode === NodeMode.ONLINE) return {mode: Mode.NORMAL, missed: 0};
  // NORMAL -> WAITING, FAILOVER -> WAITING: Node API báo RECOVERING
  if (health.mode === NodeMode.RECOVERING) return {mode: Mode.WAITING, missed: 0};
  return {mode, missed: 0};
}

// ---------------------------------------------------------------------------
// request_id: UUID v4. crypto.randomUUID() chỉ có trong secure context (https, localhost),
// còn WMS chạy ở http://HOST_B_IP:300x nên tự ghép từ crypto.getRandomValues.
// ---------------------------------------------------------------------------
export function newRequestId(cryptoImpl = globalThis.crypto) {
  if (typeof cryptoImpl.randomUUID === "function") return cryptoImpl.randomUUID();
  const b = cryptoImpl.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40; // phiên bản 4
  b[8] = (b[8] & 0x3f) | 0x80; // biến thể RFC 4122
  const hex = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export async function fetchWithTimeout(fetchImpl, url, init, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetchImpl(url, {...init, signal: controller.signal});
  } finally {
    clearTimeout(timer);
  }
}

async function readJson(resp) {
  try {
    return await resp.json();
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------
export function createWmsClient(config, deps = {}) {
  const fetchImpl = deps.fetch ?? globalThis.fetch.bind(globalThis);
  const sleep = deps.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  const log = deps.log ?? ((...args) => console.info("[WMS]", new Date().toISOString(), ...args));
  const dcId = config.DC_ID;
  const nodeUrl = config.NODE_URL;
  const hqUrl = config.HQ_URL;

  let mode = Mode.NORMAL;
  let missed = 0;
  let timer = null;
  const listeners = new Set();

  function setMode(next) {
    if (next === mode) return;
    log(`chế độ ${mode} -> ${next}`); // log dùng để đo M3
    mode = next;
    for (const cb of listeners) cb(mode);
  }

  async function probeNode() {
    try {
      const resp = await fetchWithTimeout(fetchImpl, `${nodeUrl}/health`, {}, HEALTH_TIMEOUT_MS);
      if (resp.status !== 200) return {ok: false}; // 503: CSDL kho lỗi (F6)
      const body = await readJson(resp);
      if (!body || !Object.values(NodeMode).includes(body.mode)) return {ok: false};
      return {ok: true, mode: body.mode};
    } catch {
      return {ok: false};
    }
  }

  async function checkHealth() {
    const result = nextMode(mode, missed, await probeNode());
    missed = result.missed;
    setMode(result.mode);
    return mode;
  }

  function schedule() {
    timer = setTimeout(async () => {
      await checkHealth();
      if (timer !== null) schedule();
    }, HEALTH_INTERVAL_MS);
  }

  function start() {
    if (timer !== null) return;
    timer = 0;
    checkHealth().finally(() => {
      if (timer !== null) schedule();
    });
  }

  function stop() {
    if (timer) clearTimeout(timer);
    timer = null;
  }

  function newOperation(kind, {product_id, qty, created_by}) {
    if (!(kind in RefType)) throw new Error(`kind phải là inbound hoặc outbound: ${kind}`);
    return Object.freeze({kind, request_id: newRequestId(), product_id, qty, created_by});
  }

  async function postJson(url, body) {
    const resp = await fetchWithTimeout(
      fetchImpl,
      url,
      {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body)},
      REQUEST_TIMEOUT_MS,
    );
    return {status: resp.status, ok: resp.ok, body: await readJson(resp)};
  }

  function toNode(op) {
    // F6: {"request_id", "product_id", "qty", "ref_type", "created_by"}
    return postJson(`${nodeUrl}/${op.kind}`, {
      request_id: op.request_id,
      product_id: op.product_id,
      qty: op.qty,
      ref_type: RefType[op.kind],
      created_by: op.created_by,
    });
  }

  function toHq(op) {
    // F7: POST /failover/dc/{dc_id}/inbound|outbound {"request_id", "product_id", "qty", "created_by"}
    return postJson(`${hqUrl}/failover/dc/${dcId}/${op.kind}`, {
      request_id: op.request_id,
      product_id: op.product_id,
      qty: op.qty,
      created_by: op.created_by,
    });
  }

  // Gửi một thao tác theo chế độ hiện tại; tự gửi lại (cùng request_id) khi lỗi mạng hoặc
  // HQ trả 409 DC_NOT_OFFLINE, cho tới khi có phản hồi khác hoặc signal bị hủy.
  async function submit(op, {onStatus = () => {}, signal} = {}) {
    for (;;) {
      if (signal?.aborted) return {ok: false, reason: "CANCELLED", message: "Đã hủy"};
      if (mode === Mode.WAITING) return {ok: false, reason: Mode.WAITING, message: Message.WAITING};
      const via = mode === Mode.FAILOVER ? "HQ" : "NODE";
      let result;
      try {
        result = via === "HQ" ? await toHq(op) : await toNode(op);
      } catch {
        onStatus(via === "HQ" ? Message.HQ_UNREACHABLE : Message.NODE_UNREACHABLE);
        await sleep(RESEND_MS);
        continue;
      }
      if (via === "HQ" && result.status === 409 && result.body?.error_code === ERR_DC_NOT_OFFLINE) {
        onStatus(Message.CONFIRMING);
        await sleep(RESEND_MS);
        continue;
      }
      if (via === "HQ" && result.ok) log(`ghi hộ được HQ nhận: ${op.kind} ${op.request_id}`); // M3
      return {...result, via};
    }
  }

  return {
    start,
    stop,
    checkHealth,
    newOperation,
    submit,
    getMode: () => mode,
    onModeChange(cb) {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
  };
}
