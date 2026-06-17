from __future__ import annotations

import unittest

from grid_optimizer.hybrid_maker_taker_runner import execute_hybrid_roundtrip


class _FakeExchange:
    """注入到 execute_hybrid_roundtrip 的可控交易所桩。

    用脚本化的 query 状态序列驱动 Maker 单成交时序,记录所有动作以便断言。
    """

    def __init__(self, *, query_script: dict[str, list[dict]], taker_result: dict | None = None,
                 maker_reject_sides: set[str] | None = None):
        # query_script: {coid: [状态1, 状态2, ...]} 按轮询次序逐个返回(末值重复)
        self.query_script = query_script
        self.taker_result = taker_result or {}
        self.maker_reject_sides = maker_reject_sides or set()
        self.placed_makers: list[dict] = []
        self.placed_takers: list[dict] = []
        self.canceled: list[str] = []
        self._query_idx: dict[str, int] = {}
        self._coid_seq = 0

    def place_maker(self, *, side, price, qty):
        if side in self.maker_reject_sides:
            raise RuntimeError("Order would immediately match and take (code=-2010)")
        self._coid_seq += 1
        coid = f"{side.lower()}-{self._coid_seq}"
        self.placed_makers.append({"side": side, "price": price, "qty": qty, "coid": coid})
        return coid

    def query(self, *, coid):
        seq = self.query_script.get(coid, [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}])
        i = self._query_idx.get(coid, 0)
        self._query_idx[coid] = min(i + 1, len(seq) - 1)
        return seq[i]

    def cancel(self, *, coid):
        self.canceled.append(coid)

    def place_taker(self, *, side, qty):
        self.placed_takers.append({"side": side, "qty": qty})
        return self.taker_result


_BOOK = {"bid": 100.0, "ask": 100.1, "mid": 100.05, "spread_bps": 10.0}


def _run(fx, **overrides):
    kwargs = dict(
        symbol="XAUTUSDT", notional=1000.0, book=_BOOK, qty_step=0.001, tick_size=0.1,
        poll_max_cycles=3,
        place_maker=fx.place_maker, query=fx.query, cancel=fx.cancel, place_taker=fx.place_taker,
    )
    kwargs.update(overrides)
    return execute_hybrid_roundtrip(**kwargs)


class HybridRoundtripTests(unittest.TestCase):

    def test_buy_leg_fills_first_cancels_sell_and_taker_sells(self):
        # qty ≈ 1000/100.1 ≈ 9.99 → round to 9.99
        fx = _FakeExchange(
            query_script={
                "buy-1": [{"status": "FILLED", "executedQty": 9.99, "cummulativeQuoteQty": 999.0}],
                "sell-2": [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}],
            },
            taker_result={"executedQty": 9.99, "cummulativeQuoteQty": 999.5},
        )
        r = _run(fx)
        self.assertTrue(r["ok"])
        # 撤掉了未成交的 SELL maker
        self.assertIn("sell-2", fx.canceled)
        # taker 反向 SELL 平掉买到的量
        self.assertEqual(len(fx.placed_takers), 1)
        self.assertEqual(fx.placed_takers[0]["side"], "SELL")
        # volume = maker_buy_quote + taker_sell_quote
        self.assertAlmostEqual(r["volume"], 999.0 + 999.5, places=2)
        self.assertAlmostEqual(r["residual_base"], 0.0, places=6)
        self.assertEqual(r["maker_quote"], 999.0)
        self.assertEqual(r["taker_quote"], 999.5)

    def test_sell_leg_fills_first_cancels_buy_and_taker_buys(self):
        fx = _FakeExchange(
            query_script={
                "buy-1": [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}],
                "sell-2": [{"status": "FILLED", "executedQty": 9.99, "cummulativeQuoteQty": 999.9}],
            },
            taker_result={"executedQty": 9.99, "cummulativeQuoteQty": 999.4},
        )
        r = _run(fx)
        self.assertTrue(r["ok"])
        self.assertIn("buy-1", fx.canceled)
        self.assertEqual(fx.placed_takers[0]["side"], "BUY")
        self.assertAlmostEqual(r["volume"], 999.9 + 999.4, places=2)
        self.assertAlmostEqual(r["residual_base"], 0.0, places=6)

    def test_neither_leg_fills_cancels_both_no_taker(self):
        fx = _FakeExchange(
            query_script={
                "buy-1": [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}],
                "sell-2": [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}],
            },
        )
        r = _run(fx, poll_max_cycles=2)
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "maker_unfilled")
        # 双边都撤
        self.assertIn("buy-1", fx.canceled)
        self.assertIn("sell-2", fx.canceled)
        # 没下任何 taker(死盘不退化)
        self.assertEqual(len(fx.placed_takers), 0)

    def test_maker_rejected_immediate_match_returns_rejected_no_single_side(self):
        # 卖边 LIMIT_MAKER 会立即成交被拒(-2010)
        fx = _FakeExchange(
            query_script={"buy-1": [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}]},
            maker_reject_sides={"SELL"},
        )
        r = _run(fx)
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "maker_rejected")
        # 不留单边:已挂的另一边被撤,无 taker
        self.assertEqual(len(fx.placed_takers), 0)
        # buy maker 已挂出 → 应被撤掉
        if fx.placed_makers:
            self.assertTrue(set(m["coid"] for m in fx.placed_makers) <= set(fx.canceled))

    def test_taker_partial_fill_reports_residual(self):
        # buy 成交 9.99,taker 卖只成交 9.0 → 残留 0.99
        fx = _FakeExchange(
            query_script={
                "buy-1": [{"status": "FILLED", "executedQty": 9.99, "cummulativeQuoteQty": 999.0}],
                "sell-2": [{"status": "NEW", "executedQty": 0.0, "cummulativeQuoteQty": 0.0}],
            },
            taker_result={"executedQty": 9.0, "cummulativeQuoteQty": 900.0},
        )
        r = _run(fx)
        self.assertTrue(r["ok"])
        self.assertAlmostEqual(r["residual_base"], 9.99 - 9.0, places=6)

    def test_dry_run_simulates_without_calling_callbacks(self):
        calls = {"placed": 0}

        def boom_place(**_):
            calls["placed"] += 1
            raise AssertionError("dry_run 不应真实下单")

        r = execute_hybrid_roundtrip(
            symbol="XAUTUSDT", notional=1000.0, book=_BOOK, qty_step=0.001, tick_size=0.1,
            poll_max_cycles=3, dry_run=True,
            place_maker=boom_place, query=boom_place, cancel=boom_place, place_taker=boom_place,
        )
        self.assertTrue(r["ok"])
        self.assertTrue(r.get("dry_run"))
        self.assertEqual(calls["placed"], 0)
        # dry-run 估算 volume = maker腿(贴bid成交) + taker腿(吃bid) 两腿名义
        self.assertGreater(r["volume"], 0.0)
        self.assertAlmostEqual(r["residual_base"], 0.0, places=6)


if __name__ == "__main__":
    unittest.main()
