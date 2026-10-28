import json
import tempfile
import unittest
from pathlib import Path

from backtest_cascade import Backtest, Candle, MINUTE, load_candles, signal


RULE = {"step": "0.001", "min": 0.001, "max": 100000, "min_notional": 0}


def bars(count=33):
    return [Candle(i * MINUTE, 100, 100, 100, 100, 1) for i in range(count)]


class BacktestTests(unittest.TestCase):
    def test_signal_uses_only_preceding_completed_bars(self):
        data = bars()
        data[29] = Candle(29 * MINUTE, 100, 102, 100, 102, 10)
        self.assertEqual(signal(data, 30), "buy")
        data[30] = Candle(30 * MINUTE, 50, 1000, 1, 50, 100000)
        self.assertEqual(signal(data, 30), "buy")
        self.assertIsNone(signal(data, 29))

    def test_entry_at_next_open_and_end_close_includes_fees(self):
        data = bars(31)
        data[29] = Candle(29 * MINUTE, 100, 102, 100, 102, 10)
        data[30] = Candle(30 * MINUTE, 103, 103, 103, 103, 1)
        engine = Backtest({"BTCUSDT": data}, {"BTCUSDT": RULE})
        report = engine.run(30 * MINUTE)
        self.assertEqual(report["closed_trades"], 1)
        trade = engine.trades[0]
        self.assertEqual(trade["entry_price"], 103)
        self.assertEqual(trade["reason"], "END_OF_TEST")
        self.assertAlmostEqual(trade["gross_pnl_usdt"], 0)
        self.assertAlmostEqual(report["final_balance_usdt"], 100 - trade["fees_usdt"])

    def test_ambiguous_bar_stops_first(self):
        engine = Backtest({"BTCUSDT": bars()}, {"BTCUSDT": RULE}, fee_rate=0)
        engine.enter("BTCUSDT", "buy", 100, 100, 0)
        engine.intrabar_exit("BTCUSDT", Candle(MINUTE, 100, 103, 98, 100, 1))
        self.assertEqual(engine.trades[0]["reason"], "SL_BOTH")
        self.assertAlmostEqual(engine.balance, 61.75)
        self.assertEqual(engine.ambiguous, 1)

    def test_short_gap_uses_worse_open_not_stop(self):
        engine = Backtest({"BTCUSDT": bars()}, {"BTCUSDT": RULE}, fee_rate=0)
        engine.enter("BTCUSDT", "sell", 100, 100, 0)
        engine.gap_exit("BTCUSDT", Candle(MINUTE, 103, 103, 103, 103, 1))
        self.assertAlmostEqual(engine.balance, -27.5)
        self.assertEqual(engine.trades[0]["reason"], "SL_GAP")

    def test_intrabar_profit_cannot_finance_another_open(self):
        a, b = bars(32), bars(32)
        a[29] = Candle(29 * MINUTE, 100, 102, 100, 102, 10)
        a[30] = Candle(30 * MINUTE, 102, 102, 102, 102, 1)
        a[31] = Candle(31 * MINUTE, 102, 110, 102, 104, 1)
        b[30] = Candle(30 * MINUTE, 100, 102, 100, 102, 10)
        b[31] = Candle(31 * MINUTE, 102, 102, 102, 102, 1)
        engine = Backtest({"BTCUSDT": a, "ETHUSDT": b},
                          {"BTCUSDT": RULE, "ETHUSDT": RULE}, fee_rate=0)
        engine.run(30 * MINUTE)
        eth = next(t for t in engine.trades if t["symbol"] == "ETHUSDT")
        self.assertLessEqual(eth["margin_usdt"], 12.751)

    def test_oversized_entry_does_not_change_balance(self):
        tight = {**RULE, "min_notional": 5000}
        engine = Backtest({"BTCUSDT": bars()}, {"BTCUSDT": tight})
        engine.enter("BTCUSDT", "buy", 100, 100, 0)
        self.assertEqual(engine.balance, 100)
        self.assertEqual(engine.skipped, 1)
        self.assertFalse(engine.positions)

    def test_missing_duplicate_and_invalid_data_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "candles.json"
            for rows in (
                [[0, 100, 100, 100, 100, 1]],
                [[0, 100, 100, 100, 100, 1]] * 2,
                [[0, 100, 99, 100, 100, 1], [MINUTE, 100, 100, 100, 100, 1]],
            ):
                path.write_text(json.dumps(rows))
                with self.assertRaises(ValueError):
                    load_candles(path, 0, 2 * MINUTE)

    def test_shared_wallet_and_accounting_reconcile(self):
        engine = Backtest({"BTCUSDT": bars(), "ETHUSDT": bars()},
                          {"BTCUSDT": RULE, "ETHUSDT": RULE})
        engine.enter("BTCUSDT", "buy", 100, 100, 0)
        engine.enter("ETHUSDT", "sell", 100, 100, 0)
        self.assertGreaterEqual(engine.free, 0)
        self.assertLessEqual(sum(p["margin"] for p in engine.positions.values()), engine.balance)
        engine.close("BTCUSDT", 102, MINUTE, "TP")
        engine.close("ETHUSDT", 99, MINUTE, "TP")
        self.assertAlmostEqual(engine.balance, 100 + sum(t["net_pnl_usdt"] for t in engine.trades))


if __name__ == "__main__":
    unittest.main()
