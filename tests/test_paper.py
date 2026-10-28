import asyncio
import tempfile
import unittest
from pathlib import Path

import binance_cascade_hunter as strategy
from binance_cascade_hunter_paper import PaperCascadeHunter, PaperExchange


class PublicData:
    """No trading, balance or leverage endpoints exist on this fake client."""

    def __init__(self):
        self.price = 100.0
        self.closed = False

    async def load_markets(self):
        return {}

    async def fetch_ticker(self, symbol):
        await asyncio.sleep(0)
        return {"last": self.price}

    async def fetch_order_book(self, symbol, limit):
        return {"bids": [[100, 9]], "asks": [[100, 1]]}

    async def fetch_ohlcv(self, symbol, timeframe, limit):
        rows = [[i, 100, 100, 100, 100, 1] for i in range(limit)]
        rows[-1] = [limit, 100, 102, 100, 102, 10]
        return rows

    def amount_to_precision(self, symbol, amount):
        return str(int(amount * 1000) / 1000)

    def market(self, symbol):
        return {"linear": True, "swap": True, "settle": "USDT",
                "limits": {"amount": {"min": 0.001}, "cost": {"min": 5}}}

    async def close(self):
        self.closed = True


class PaperTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.json"
        self.data = PublicData()
        self.bot = PaperCascadeHunter(self.path, fee_rate=0, market_data=self.data)

    async def test_original_signal_is_inherited(self):
        self.assertIs(PaperCascadeHunter.detect_cascade, strategy.CascadeHunter.detect_cascade)
        signal = await self.bot.detect_cascade("BTC/USDT:USDT")
        self.assertTrue(signal["long"])
        self.assertFalse(signal["short"])
        self.assertAlmostEqual(signal["ret"], 0.02)

    async def test_long_profit_and_equity(self):
        await self.bot.enter("BTC/USDT:USDT", "buy", 100)
        self.assertAlmostEqual(self.bot.exchange.used, 85)
        self.assertAlmostEqual(self.bot.exchange.free, 15)
        self.data.price = 102
        await self.bot.exchange.fetch_ticker("BTC/USDT:USDT")
        self.assertAlmostEqual(self.bot.exchange.equity, 185)
        await self.bot.manage_exits("BTC/USDT:USDT")
        self.assertAlmostEqual(self.bot.exchange.balance, 185)
        self.assertFalse(self.bot.positions)
        self.assertFalse(self.bot.exchange.positions)

    async def test_short_stop_loss(self):
        await self.bot.enter("ETH/USDT:USDT", "sell", 100)
        self.data.price = 101
        await self.bot.manage_exits("ETH/USDT:USDT")
        self.assertAlmostEqual(self.bot.exchange.balance, 57.5)
        self.assertFalse(self.bot.positions)

    async def test_short_profit(self):
        await self.bot.enter("ETH/USDT:USDT", "sell", 100)
        self.data.price = 98
        await self.bot.manage_exits("ETH/USDT:USDT")
        self.assertAlmostEqual(self.bot.exchange.balance, 185)

    async def test_fees_on_both_fills(self):
        account = PaperExchange(self.data, fee_rate=0.0005)
        await account.set_leverage(50, "BTC/USDT:USDT")
        await account.create_order("BTC/USDT:USDT", "market", "buy", 42.5)
        self.assertAlmostEqual(account.balance, 100 - 2.125)
        self.data.price = 102
        await account.create_order("BTC/USDT:USDT", "market", "sell", 42.5,
                                   {"reduceOnly": True})
        self.assertAlmostEqual(account.balance, 100 - 2.125 + 85 - 2.1675)

    async def test_concurrent_entries_reserve_shared_margin(self):
        await asyncio.gather(*(self.bot.enter(s, "buy", 100) for s in strategy.SYMBOLS))
        self.assertEqual(len(self.bot.positions), 3)
        self.assertLessEqual(self.bot.exchange.used, 100)
        self.assertGreaterEqual(self.bot.exchange.free, 0)
        self.assertLess(self.bot.positions[strategy.SYMBOLS[1]]["amount"], 42.5)

    async def test_insufficient_margin_and_invalid_reduce_only(self):
        account = self.bot.exchange
        with self.assertRaises(ValueError):
            await account.create_order("BTC/USDT:USDT", "market", "buy", 51)
        with self.assertRaises(ValueError):
            await account.create_order("BTC/USDT:USDT", "market", "sell", 1,
                                       {"reduceOnly": True})
        self.assertEqual(account.balance, 100)
        self.assertEqual(account.trades, [])

    async def test_resume_and_reset(self):
        await self.bot.enter("BTC/USDT:USDT", "buy", 100)
        restored = PaperCascadeHunter(self.path, fee_rate=0, market_data=self.data)
        self.assertEqual(restored.positions, self.bot.positions)
        self.assertEqual(restored.exchange.trades, self.bot.exchange.trades)
        self.assertEqual(restored.last_trade, self.bot.last_trade)
        with self.assertRaises(ValueError):
            PaperCascadeHunter(self.path, fee_rate=0.0005, market_data=self.data)
        reset = PaperCascadeHunter(self.path, fee_rate=0, reset=True, market_data=self.data)
        self.assertEqual(reset.exchange.balance, 100)
        self.assertFalse(reset.positions)

    async def test_cancel_saves_and_closes_public_client(self):
        task = asyncio.create_task(self.bot.start())
        for _ in range(10):
            await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(self.data.closed)
        self.assertTrue(self.path.exists())


if __name__ == "__main__":
    unittest.main()
