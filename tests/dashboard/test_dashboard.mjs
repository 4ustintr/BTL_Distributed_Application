import test from "node:test";
import assert from "node:assert/strict";
import {totals, sourceLabel, validateTransfer} from "../../dashboard/js/domain.js";

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

