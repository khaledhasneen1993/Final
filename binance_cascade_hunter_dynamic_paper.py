"""Dynamic top-40 scanner using a persistent 100 USDT paper account."""

import argparse
import asyncio
import math
from pathlib import Path

import ccxt.async_support as ccxt

import binance_cascade_hunter_dynamic as dynamic
from binance_cascade_hunter_paper import PaperCascadeHunter, PaperExchange
from continuous_scanner import ContinuousScanner


class DynamicPaperExchange(PaperExchange):
    async def fetch_tickers(self):
        return await self.market_data.fetch_tickers()


class DynamicPaperHunter(dynamic.CascadeHunter, PaperCascadeHunter):
    def __init__(self, state_path="paper_state_dynamic.json", fee_rate=0.0005,
                 reset=False, market_data=None):
        # Neither real-trading initializer is invoked. No keys are accepted or loaded.
        public = market_data if market_data is not None else ccxt.binanceusdm({
            "enableRateLimit": True, "options": {"defaultType": "future"},
        })
        self.exchange = DynamicPaperExchange(public, fee_rate, leverage=dynamic.LEVERAGE)
        self.positions = {}
        self.last_trade = {}
        self.lock = asyncio.Lock()
        self.state_path = Path(state_path)
        if self.state_path.exists() and not reset:
            self.restore()
        self.scanner = ContinuousScanner(self)

    def settings(self):
        return {"mode": "dynamic_scan", **{name: getattr(dynamic, name) for name in (
            "LEVERAGE", "MAX_POSITION_PCT", "CASCADE_THRESHOLD", "VOLUME_SPIKE",
            "TAKE_PROFIT_PCT", "STOP_LOSS_PCT", "COOLDOWN_SEC", "SCAN_INTERVAL",
            "MAX_SYMBOLS_TO_SCAN", "MAX_CONCURRENT_POSITIONS", "MIN_QUOTE_VOLUME",
        )}}

    async def enter(self, symbol, side, price):
        # scan_and_trade already owns the lock; reacquiring it here would deadlock.
        result = await dynamic.CascadeHunter.enter(self, symbol, side, float(price))
        self.save()
        return result

    async def manage_exits(self):
        await dynamic.CascadeHunter.manage_exits(self)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="paper_state_dynamic.json")
    parser.add_argument("--reset", action="store_true", help="Replace state with a fresh $100 account")
    parser.add_argument("--fee-rate", type=float, default=0.0005,
                        help="Assumed fee per fill; default 0.0005 (0.05%%)")
    args = parser.parse_args()
    if not math.isfinite(args.fee_rate) or not 0 <= args.fee_rate < 0.01:
        parser.error("--fee-rate must be finite and in [0, 0.01)")
    bot = DynamicPaperHunter(args.state, args.fee_rate, args.reset)
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        dynamic.log.info("Dynamic paper account saved; stopped")


if __name__ == "__main__":
    main()
