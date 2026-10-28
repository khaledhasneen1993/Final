import json
import gzip
import tempfile
import unittest
from pathlib import Path

import numpy as np

from backtest_dynamic import DynamicBacktest, cascade_signal, ranked_universe
from prepare_dynamic_history import DAY, DTYPE, HOUR, MINUTE, hourly_candidates

RULE = {"step": "0.001", "min": 0.001, "max": 1e9, "min_notional": 0}


def bars(count=1460):
    data = np.zeros(count, dtype=DTYPE).view(np.recarray)
    data.timestamp = np.arange(count) * MINUTE
    for name in ("open", "high", "low", "close"):
        data[name] = 100
    data.volume = 1
    data.quote_volume = 20_000
    return data


class HistoricalDynamicTests(unittest.TestCase):
    def test_hourly_snapshot_restores_without_network(self):
        rows = [[i * HOUR, 1_000_000] for i in range(26)]
        with tempfile.TemporaryDirectory() as folder:
            packed = gzip.compress(json.dumps({"AUSDT": rows}).encode())
            (Path(folder) / "hourly.json.gz").write_bytes(packed)
            selected, excluded = hourly_candidates(folder, DAY, 26 * HOUR, top=1)
            self.assertEqual(selected, ["AUSDT"])
            self.assertFalse(excluded)
            self.assertEqual(json.loads((Path(folder) / "hourly/AUSDT.json").read_text()), rows)

    def test_quote_ranking_ignores_current_and_future_minutes(self):
        a = np.ones(1460)
        b = np.full(1460, 2.0)
        a[1440:] = 1e12
        prefixes = {s: np.concatenate(([0.0], np.cumsum(q))) for s, q in {"A": a, "B": b}.items()}
        ranked = ranked_universe(prefixes, 1440, top=1, minimum=0)
        self.assertEqual(ranked[0][0], "B")
        self.assertEqual(ranked[0][1], 2880)

    def test_hourly_pruning_preserves_every_true_top_member(self):
        rng = np.random.default_rng(17)
        minute_volumes = {f"S{i}": rng.uniform(1, 1000, 26 * 60) * (i + 1) for i in range(7)}
        minute_volumes["IMPOSSIBLE"] = np.zeros(26 * 60)
        with tempfile.TemporaryDirectory() as folder:
            hourly = Path(folder) / "hourly"
            hourly.mkdir()
            for symbol, values in minute_volumes.items():
                rows = [[i * HOUR, float(v)] for i, v in enumerate(values.reshape(26, 60).sum(axis=1))]
                (hourly / f"{symbol}.json").write_text(json.dumps(rows))
            selected, excluded = hourly_candidates(folder, DAY, 26 * HOUR, top=3, minimum=100)
            self.assertFalse(excluded)
            self.assertNotIn("IMPOSSIBLE", selected)
            prefixes = {s: np.concatenate(([0.0], np.cumsum(v))) for s, v in minute_volumes.items()}
            for index in range(1440, 26 * 60, 10):
                actual = {s for s, _ in ranked_universe(prefixes, index, top=3, minimum=100)}
                self.assertTrue(actual <= set(selected))

    def test_signal_uses_closed_candle_and_has_no_synthetic_book_factor(self):
        data = bars()
        data.close[1439] = 102
        data.volume[1439] = 10
        result = cascade_signal(data, 1440)
        self.assertEqual(result["side"], "buy")
        self.assertAlmostEqual(result["strength"], 0.2)
        data.close[1440:] = 1e6
        self.assertAlmostEqual(cascade_signal(data, 1440)["strength"], 0.2)

    def test_scan_grid_and_position_cap(self):
        data = {s: bars() for s in ("A", "B", "C")}
        for i, values in enumerate(data.values(), 2):
            price = 100 + i
            values.close[1439] = price
            values.volume[1439] = 10
            for name in ("open", "high", "low", "close"):
                values[name][1440:] = price
        engine = DynamicBacktest(data, {s: RULE for s in data}, fee_rate=0)
        report = engine.run(DAY)
        self.assertEqual(report["scans"], 2)
        self.assertEqual(report["peak_open_positions"], 2)
        self.assertEqual({t["symbol"] for t in engine.trades}, {"B", "C"})
        self.assertTrue(all(t["reason"] == "END_OF_TEST" for t in engine.trades))

    def test_exits_continue_after_symbol_leaves_universe(self):
        a, b = bars(1441), bars(1441)
        a.quote_volume[:] = 0
        a.high[1440] = a.close[1440] = 102
        engine = DynamicBacktest({"A": a, "B": b}, {"A": RULE, "B": RULE}, fee_rate=0)
        engine.enter("A", "buy", 100, 100, DAY - MINUTE)
        engine.run(DAY)
        self.assertEqual([s for s, _ in engine.scans[0]["symbols"]], ["B"])
        self.assertEqual(engine.trades[0]["reason"], "TP")
        self.assertAlmostEqual(engine.balance, 176.5)

    def test_missing_24h_warmup_is_rejected(self):
        engine = DynamicBacktest({"A": bars()}, {"A": RULE})
        with self.assertRaises(ValueError):
            engine.run(30 * MINUTE)


if __name__ == "__main__":
    unittest.main()
