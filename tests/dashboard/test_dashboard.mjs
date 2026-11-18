import test from "node:test";
import assert from "node:assert/strict";
import {totals, sourceLabel, validateTransfer, failureMessage, asSentence} from "../../dashboard/js/domain.js";
import {requestHeaders} from "../../dashboard/js/http.js";

test("network failures are shown in Vietnamese, not the browser's English text", () => {
  assert.equal(failureMessage(new TypeError("Failed to fetch")), "Không gọi được HQ API.");
  const timeout = new Error("signal timed out");
  timeout.name = "TimeoutError";
  assert.equal(failureMessage(timeout), "HQ API không trả lời kịp.");
  const aborted = new Error("aborted");
  aborted.name = "AbortError";
  assert.equal(failureMessage(aborted), "HQ API không trả lời kịp.");
  assert.equal(failureMessage(new Error("Kho đang mất kết nối")), "Kho đang mất kết nối.");
});
test("messages joined into a notice always end with a full stop", () => {
  assert.equal(asSentence("Không đủ hàng khả dụng"), "Không đủ hàng khả dụng.");
  assert.equal(asSentence("Đã xong."), "Đã xong.");
  assert.equal(asSentence("Có lỗi?"), "Có lỗi?");
  assert.equal(asSentence(""), "");
});

test("GET polling avoids Content-Type preflight; JSON writes carry the header", () => {
  assert.deepEqual(requestHeaders(), {});
  assert.deepEqual(requestHeaders({method: "GET"}), {});
  assert.deepEqual(requestHeaders({body: "{}"}), {"Content-Type": "application/json"});
  assert.deepEqual(requestHeaders({body: "{}", headers: {"content-type": "text/plain"}}), {
    "content-type": "text/plain",
  });
});

test("provisional inventory is not added a second time on the client", () => {
  assert.deepEqual(totals([
    {qty_on_hand: 20, qty_reserved: 10, qty_in_transit: 0},
    {qty_on_hand: 55, qty_reserved: 0, qty_in_transit: 0},
    {qty_on_hand: 60, qty_reserved: 0, qty_in_transit: 50},
  ]), {stock: 135, available: 125, transit: 50});
});
test("every data source has a visible freshness label", () => {
  for (const [source, label] of [["snapshot", "Bản tổng hợp"], ["realtime", "Trực tiếp"], ["offline", "Offline"], ["provisional", "Tạm tính"]]) {
    assert.ok(sourceLabel({source, as_of: null}).startsWith(label));
  }
});
test("transfer input rejects equal DCs, fractional, zero and invalid quantities", () => {
  assert.equal(validateTransfer(1, 3, 50), "");
  for (const args of [[1, 1, 50], [1, 3, 0], [1, 3, -1], [1, 3, 1.5], [1, 3, NaN], [4, 3, 10]]) {
    assert.notEqual(validateTransfer(...args), "");
  }
});

