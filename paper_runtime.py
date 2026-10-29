"""Continuous paper runtime for the approved V2 strategy."""

import asyncio
from pathlib import Path

import ccxt.async_support as ccxt

import binance_cascade_hunter_dynamic_v2 as dynamic
from paper_account import PaperState, PaperExchange
from continuous_scanner import ContinuousScanner


class DynamicPaperExchange(PaperExchange):
    async def fetch_tickers(self):
        return await self.market_data.fetch_tickers()


class DynamicPaperHunter(dynamic.CascadeHunter, PaperState):
    def __init__(self, state_path="paper_state_dynamic_v2.json", fee_rate=0.0005,
                 reset=False, market_data=None, strategy_config=dynamic):
        self.strategy = strategy_config
        # Neither real-trading initializer is invoked. No keys are accepted or loaded.
        public = market_data if market_data is not None else ccxt.binanceusdm({
            "enableRateLimit": True, "options": {"defaultType": "future"},
        })
        self.exchange = DynamicPaperExchange(public, fee_rate, leverage=self.strategy.LEVERAGE)
        self.positions = {}
        self.last_trade = {}
        self.lock = asyncio.Lock()
        self.state_path = Path(state_path)
        if self.state_path.exists() and not reset:
            self.restore()
        self.scanner = ContinuousScanner(self, config=self.strategy)

    def settings(self):
        return {"mode": "dynamic_scan", **{name: getattr(self.strategy, name) for name in (
            "LEVERAGE", "MAX_POSITION_PCT", "CASCADE_THRESHOLD", "VOLUME_SPIKE",
            "TAKE_PROFIT_PCT", "STOP_LOSS_PCT", "COOLDOWN_SEC", "SCAN_INTERVAL",
            "MAX_SYMBOLS_TO_SCAN", "MAX_CONCURRENT_POSITIONS", "MIN_QUOTE_VOLUME",
        )}}

    async def enter(self, symbol, side, price):
        # scan_and_trade already owns the lock; reacquiring it here would deadlock.
        result = await self.strategy.CascadeHunter.enter(self, symbol, side, float(price))
        self.save()
        return result

    async def manage_exits(self):
        await self.strategy.CascadeHunter.manage_exits(self)
        self.save()

    async def scan_loop(self):
        await self.scanner.run()

    async def start(self):
        tasks = []
        try:
            await self.exchange.load_markets()
            self.save()
            dynamic.log.info("CONTINUOUS PAPER ONLY | initial=100 USDT | wallet=%.4f | state=%s",
                             self.exchange.balance, self.state_path)
            tasks = [asyncio.create_task(self.scan_loop()),
                     asyncio.create_task(self.exit_watcher()),
                     asyncio.create_task(self.report())]
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                self.save()
            finally:
                await self.exchange.close()

