import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import binance_cascade_hunter_dynamic as dynamic
from binance_cascade_hunter_dynamic_paper import DynamicPaperHunter
from binance_cascade_hunter_paper import PaperCascadeHunter
from test_paper import PublicData


class DynamicPublicData(PublicData):
    def __init__(self):
        super().__init__()
        self.tickers = {s: {"quoteVolume": 20_000_000 + i * 1_000_000}
                        for i, s in enumerate(("BTC/USDT:USDT", "ETH/USDT:USDT",
                                               "SOL/USDT:USDT"))}

    async def fetch_tickers(self):
        return self.tickers


class DynamicTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "dynamic.json"
        self.data = DynamicPublicData()
        self.bot = DynamicPaperHunter(self.path, fee_rate=0, market_data=self.data)

    async def test_universe_uses_volume_threshold_sort_and_cap(self):
        self.data.tickers = {f"S{i:02}/USDT:USDT": {"quoteVolume": 15_000_000 + i}
                             for i in range(45)}
        self.data.tickers.update({"LOW/USDT:USDT": {"quoteVolume": 14_999_999},
                                  "BTC/USDT": {"quoteVolume": 1e12},
                                  "BTC/USD:BTC": {"quoteVolume": 1e12}})
        top = await self.bot.get_top_symbols()
        self.assertEqual(len(top), 40)
        self.assertEqual(top[0], "S44/USDT:USDT")
        self.assertEqual(top[-1], "S05/USDT:USDT")
        self.data.tickers = {"NEW/USDT:USDT": {"quoteVolume": 30_000_000}}
        self.assertEqual(await self.bot.get_top_symbols(), ["NEW/USDT:USDT"])

    async def test_strength_order_and_two_position_limit(self):
        strengths = {"BTC/USDT:USDT": 1, "ETH/USDT:USDT": 3, "SOL/USDT:USDT": 2}

        async def detect(symbol):
            return {"symbol": symbol, "long": True, "short": False,
                    "strength": strengths[symbol], "price": 100}

        self.bot.detect_cascade = detect
        await asyncio.wait_for(self.bot.scan_and_trade(), timeout=2)
        self.assertEqual(list(self.bot.positions), ["ETH/USDT:USDT", "SOL/USDT:USDT"])
        self.assertLessEqual(self.bot.exchange.used, 100)
        self.assertFalse(await self.bot.enter("BTC/USDT:USDT", "buy", 100))
        self.assertEqual(len(self.bot.exchange.trades), 2)
        await self.bot.scan_and_trade()
        self.assertEqual(len(self.bot.exchange.trades), 2)

    async def test_detect_inherits_full_book_signal_and_strength(self):
        self.assertIs(DynamicPaperHunter.detect_cascade, dynamic.CascadeHunter.detect_cascade)
        result = await self.bot.detect_cascade("BTC/USDT:USDT")
        self.assertTrue(result["long"])
        self.assertAlmostEqual(result["strength"], 0.02 * 10 * 0.8)

    async def test_detection_concurrency_capped_at_eight(self):
        self.data.tickers = {f"S{i}/USDT:USDT": {"quoteVolume": 20_000_000}
                             for i in range(20)}
        active = peak = calls = 0

        async def detect(symbol):
            nonlocal active, peak, calls
            active += 1
            calls += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return None

        self.bot.detect_cascade = detect
        await self.bot.scan_and_trade()
        self.assertEqual(calls, 20)
        self.assertEqual(peak, 8)

    async def test_watcher_closes_position_outside_scan_universe(self):
        await self.bot.enter("ETH/USDT:USDT", "buy", 100)
        self.data.tickers = {"BTC/USDT:USDT": {"quoteVolume": 30_000_000}}
        self.data.price = 102
        watcher = asyncio.create_task(self.bot.exit_watcher())
        try:
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertFalse(self.bot.positions)
            self.assertAlmostEqual(self.bot.exchange.balance, 185)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    async def test_dynamic_state_resume_reset_and_separation(self):
        await self.bot.enter("ETH/USDT:USDT", "sell", 100)
        restored = DynamicPaperHunter(self.path, fee_rate=0, market_data=self.data)
        self.assertEqual(restored.positions, self.bot.positions)
        self.assertEqual(restored.exchange.positions, self.bot.exchange.positions)
        with self.assertRaises(ValueError):
            PaperCascadeHunter(self.path, fee_rate=0, market_data=self.data)
        with patch.object(dynamic, "MAX_CONCURRENT_POSITIONS", 3):
            with self.assertRaises(ValueError):
                DynamicPaperHunter(self.path, fee_rate=0, market_data=self.data)
        reset = DynamicPaperHunter(self.path, fee_rate=0, reset=True, market_data=self.data)
        self.assertEqual(reset.exchange.balance, 100)
        self.assertFalse(reset.positions)

    async def test_dynamic_leverage_drives_margin_independently(self):
        with patch.object(dynamic, "LEVERAGE", 25):
            bot = DynamicPaperHunter(self.path, fee_rate=0, market_data=self.data)
            await bot.enter("ETH/USDT:USDT", "buy", 100)
            self.assertAlmostEqual(bot.exchange.used, 85)
            self.assertAlmostEqual(bot.positions["ETH/USDT:USDT"]["amount"], 21.25)

    async def test_cancel_saves_and_closes_scanner_and_watcher(self):
        task = asyncio.create_task(self.bot.start())
        for _ in range(20):
            await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(self.data.closed)
        self.assertTrue(self.path.exists())


if __name__ == "__main__":
    unittest.main()
