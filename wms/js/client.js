// wms/js/client.js – chế độ NORMAL, FAILOVER, WAITING của WMS (spec F9, F11) – Nhật Khánh.
// Trình duyệt dùng qua window.WmsClient (nạp bằng <script> trước app.js); Node dùng qua require.
//
// Giao diện (Tuấn Minh) dùng:
//   const wms = WmsClient.createWmsClient(window.CONFIG);   // {DC_ID, NODE_URL, HQ_URL}
//   wms.onModeChange((mode) => { ...khóa nút nhập, xuất khi mode === WmsClient.Mode.WAITING... });
//   wms.start();                                            // health check 2 giây một lần (F11)
//   const res = await wms.sendStock(path, body, {onStatus});  // thay cho api.sendStock
//   //  -> {status, ok, body, via: "NODE" | "HQ" | "WMS"}; lỗi mạng: status 0, bấm lại cùng body
//   const r = await wms.transferAction(id, "ship" | "receive" | "decline", {created_by});
//   //  -> {status, ok, body, via: "NODE"}; chờ tới 20 giây (callback F5 của kho tối đa 18 giây)
//
// request_id do giao diện sinh một lần cho mỗi thao tác và giữ nguyên khi gửi lại; sendStock dùng
// đúng request_id trong body, kể cả khi đổi từ Node API sang HQ (F9).
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.WmsClient = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const Mode = Object.freeze({NORMAL: "NORMAL", FAILOVER: "FAILOVER", WAITING: "WAITING"});
  const NodeMode = Object.freeze({ONLINE: "ONLINE", RECOVERING: "RECOVERING"}); // F6 /health
  const RefType = Object.freeze({inbound: "RECEIPT", outbound: "ISSUE"}); // F6 /inbound, /outbound
  const CancelReason = Object.freeze({DECLINED: "DECLINED"}); // F10.g: kho đích từ chối
  const ErrorCode = Object.freeze({DC_NOT_OFFLINE: "DC_NOT_OFFLINE", DC_RECOVERING: "DC_RECOVERING"});

  // Tham số F11, F5. JS không import được common/constants.py nên khai báo lại ĐÚNG TÊN và giá trị;
  // tests/wms/test_client.mjs đọc common/constants.py để bảo đảm hai bên không lệch nhau.
  const WMS_HEALTH_INTERVAL_S = 2.0; // F11: chu kỳ WMS kiểm tra Node API
  const WMS_HEALTH_TIMEOUT_S = 1.0; // F11: timeout GET /health của WMS tới Node API
  const WMS_FAILOVER_MISSED = 2; // F11, F9: 2 lần lỗi liên tiếp -> FAILOVER
  const HTTP_TIMEOUT_S = 2.0; // F5, F11: timeout lệnh gọi
  const HTTP_MAX_RETRIES = 4; // F5, F11: thử lại tối đa sau lần gọi đầu (tổng 5 lần gọi)
  const HTTP_RETRY_INTERVAL_S = 2.0; // F5, F11: khoảng cách giữa hai lần thử
  const RESEND_MS = 2000; // F9: gặp 409 DC_NOT_OFFLINE thì gửi lại sau 2 giây

  // /ship, /receive, /decline (F6): kho gọi tiếp lên HQ theo F5 (callback /shipped, /received, hoặc
  // chuyển tiếp /decline), lâu nhất 5 × 2 + 4 × 2 = 18 giây. WMS phải chờ lâu hơn thế để nhận được
  // kết quả thật (vd. 503 DC_UNREACHABLE) thay vì tự báo hết giờ; cộng một HTTP_TIMEOUT_S cho kho.
  const CALLBACK_RETRY_BUDGET_S =
    (HTTP_MAX_RETRIES + 1) * HTTP_TIMEOUT_S + HTTP_MAX_RETRIES * HTTP_RETRY_INTERVAL_S;
  const TRANSFER_ACTION_TIMEOUT_S = CALLBACK_RETRY_BUDGET_S + HTTP_TIMEOUT_S;

  const HEALTH_INTERVAL_MS = WMS_HEALTH_INTERVAL_S * 1000;
  const HEALTH_TIMEOUT_MS = WMS_HEALTH_TIMEOUT_S * 1000;
  const REQUEST_TIMEOUT_MS = HTTP_TIMEOUT_S * 1000;
  const TRANSFER_ACTION_TIMEOUT_MS = TRANSFER_ACTION_TIMEOUT_S * 1000;

  const Message = Object.freeze({
    CONFIRMING: "Đang xác nhận trạng thái kho, sẽ tự gửi lại sau 2 giây",
    WAITING: "Kho đang đồng bộ, tạm khóa nhập, xuất",
  });

  // -------------------------------------------------------------------------------------------
  // Bảng chuyển chế độ F9 (hàm thuần)
  // health: {ok: false} hoặc {ok: true, mode: "ONLINE" | "RECOVERING"}
  // -------------------------------------------------------------------------------------------
  function nextMode(mode, missed, health) {
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

  // -------------------------------------------------------------------------------------------
  // request_id: UUID v4. crypto.randomUUID() chỉ có trong secure context (https, localhost),
  // còn WMS chạy ở http://HOST_B_IP:300x nên tự ghép từ crypto.getRandomValues.
  // -------------------------------------------------------------------------------------------
  function newRequestId(cryptoImpl) {
    const c = cryptoImpl || globalThis.crypto;
    if (typeof c.randomUUID === "function") return c.randomUUID();
    const b = c.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40; // phiên bản 4
    b[8] = (b[8] & 0x3f) | 0x80; // biến thể RFC 4122
    const hex = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  async function fetchWithTimeout(fetchImpl, url, init, timeoutMs) {
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
    } catch (_) {
      return null;
    }
  }

  // "/inbound" -> "inbound"; đường dẫn khác -> null
  function stockKind(path) {
    const kind = String(path).replace(/^\//, "");
    return kind in RefType ? kind : null;
  }

  // -------------------------------------------------------------------------------------------
  // Client
  // -------------------------------------------------------------------------------------------
  function createWmsClient(config, deps) {
    const d = deps || {};
    const fetchImpl = d.fetch || globalThis.fetch.bind(globalThis);
    const sleep = d.sleep || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    const log = d.log || ((...args) => console.info("[WMS]", new Date().toISOString(), ...args));
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
      } catch (_) {
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

    // POST JSON; lỗi mạng hoặc quá thời gian -> status 0 (quy ước của wms/js/api.js)
    async function postJson(url, body, timeoutMs) {
      try {
        const resp = await fetchWithTimeout(
          fetchImpl,
          url,
          {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body)},
          timeoutMs,
        );
        return {status: resp.status, ok: resp.ok, body: await readJson(resp)};
      } catch (_) {
        return {status: 0, ok: false, body: null};
      }
    }

    // Nhập, xuất theo chế độ hiện tại (F9). path = "/inbound" | "/outbound", body theo F6
    // {request_id, product_id, qty, ref_type, created_by}, như WmsLogic.buildStockRequest.
    // - NORMAL: gửi Node API. FAILOVER: gửi HQ /failover/dc/{DC_ID}/… (body F7, bỏ ref_type);
    //   HQ trả 409 DC_NOT_OFFLINE thì báo onStatus và tự gửi lại sau 2 giây, cùng request_id.
    // - WAITING: không gửi; trả 503 DC_RECOVERING như kho sẽ trả (via "WMS").
    // - Lỗi mạng: trả status 0, giao diện cho bấm lại cùng body; lúc đó WMS có thể đã chuyển
    //   FAILOVER và lệnh đi HQ với đúng request_id cũ.
    async function sendStock(path, body, opts) {
      const onStatus = (opts && opts.onStatus) || (() => {});
      const kind = stockKind(path);
      if (kind === null) throw new Error(`path phải là /inbound hoặc /outbound: ${path}`);
      for (;;) {
        if (mode === Mode.WAITING) {
          const err = {error_code: ErrorCode.DC_RECOVERING, message: Message.WAITING, details: {}};
          return {status: 503, ok: false, body: err, via: "WMS"};
        }
        if (mode !== Mode.FAILOVER) {
          return {...(await postJson(`${nodeUrl}/${kind}`, body, REQUEST_TIMEOUT_MS)), via: "NODE"};
        }
        const res = await postJson(
          `${hqUrl}/failover/dc/${dcId}/${kind}`,
          {
            request_id: body.request_id,
            product_id: body.product_id,
            qty: body.qty,
            created_by: body.created_by,
          },
          REQUEST_TIMEOUT_MS,
        );
        if (res.status === 409 && res.body && res.body.error_code === ErrorCode.DC_NOT_OFFLINE) {
          onStatus(Message.CONFIRMING);
          await sleep(RESEND_MS);
          continue;
        }
        if (res.ok) log(`ghi hộ được HQ nhận: ${kind} ${body.request_id}`); // M3
        return {...res, via: "HQ"};
      }
    }

    // Xuất / nhận / từ chối phiếu điều chuyển ở kho (F6 POST /transfers/{id}/ship, /receive
    // {"created_by"}; /decline {"reason": "DECLINED"}, F10.g). Gửi một lần, chờ tới
    // TRANSFER_ACTION_TIMEOUT_S; không tự gửi lại: lỗi mạng (status 0) hoặc 503 DC_UNREACHABLE thì
    // nhân viên bấm lại, kho nhận ra lệnh trùng. Không có đường ghi hộ cho điều chuyển.
    async function transferAction(transferId, action, fields) {
      const f = fields || {};
      let body;
      if (action === "ship" || action === "receive") body = {created_by: f.created_by};
      else if (action === "decline") body = {reason: CancelReason.DECLINED};
      else throw new Error(`action phải là ship, receive hoặc decline: ${action}`);
      const url = `${nodeUrl}/transfers/${encodeURIComponent(transferId)}/${action}`;
      return {...(await postJson(url, body, TRANSFER_ACTION_TIMEOUT_MS)), via: "NODE"};
    }

    return {
      start,
      stop,
      checkHealth,
      sendStock,
      transferAction,
      getMode: () => mode,
      onModeChange(cb) {
        listeners.add(cb);
        return () => listeners.delete(cb);
      },
    };
  }

  return {
    Mode, Message, nextMode, newRequestId, fetchWithTimeout, createWmsClient,
    WMS_HEALTH_INTERVAL_S, WMS_HEALTH_TIMEOUT_S, WMS_FAILOVER_MISSED, HTTP_TIMEOUT_S,
    HTTP_MAX_RETRIES, HTTP_RETRY_INTERVAL_S, RESEND_MS, CALLBACK_RETRY_BUDGET_S,
    TRANSFER_ACTION_TIMEOUT_S,
  };
});
