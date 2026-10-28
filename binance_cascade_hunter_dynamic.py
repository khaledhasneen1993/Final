# language: Python 3.11+, file: binance_cascade_hunter.py
# target: Binance USDT-M Futures — dynamic scan every 10 min
# deps: pip install ccxt pandas numpy aiohttp

import ccxt.async_support as ccxt
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("CASCADE")

# === CONFIG ===
API_KEY = "YOUR_BINANCE_API_KEY"
API_SECRET = "YOUR_BINANCE_API_SECRET"

LEVERAGE = 50
MAX_POSITION_PCT = 0.85          # 85% من الهامش المتاح
CASCADE_THRESHOLD = 0.012        # 1.2%
VOLUME_SPIKE = 3.5
TAKE_PROFIT_PCT = 0.018
STOP_LOSS_PCT = 0.009
COOLDOWN_SEC = 8

SCAN_INTERVAL = 600              # 10 دقائق
MAX_SYMBOLS_TO_SCAN = 40         # أعلى 40 عملة سيولة
MAX_CONCURRENT_POSITIONS = 2     # أقصى عدد مراكز مفتوحة في نفس الوقت
MIN_QUOTE_VOLUME = 15_000_000    # حجم تداول 24 ساعة أدنى (USDT)

class CascadeHunter:
    def __init__(self):
        self.exchange = ccxt.binanceusdm({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "future"}
        })
        self.last_trade = {}
        self.positions = {}
        self.lock = asyncio.Lock()

    async def set_leverage(self, symbol):
        try:
            await self.exchange.set_leverage(LEVERAGE, symbol)
        except Exception as e:
            log.warning(f"Leverage set failed {symbol}: {e}")

    async def get_top_symbols(self):
        """يجيب أعلى العملات سيولة (USDT-M linear)"""
        try:
            tickers = await self.exchange.fetch_tickers()
            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith(":USDT"):
                    continue
                if "/USDT:USDT" not in symbol:
                    continue
                quote_vol = float(t.get("quoteVolume") or 0)
                if quote_vol < MIN_QUOTE_VOLUME:
                    continue
                candidates.append((symbol, quote_vol))

            candidates.sort(key=lambda x: x[1], reverse=True)
            top = [s for s, _ in candidates[:MAX_SYMBOLS_TO_SCAN]]
            log.info(f"Scan universe: {len(top)} symbols")
            return top
        except Exception as e:
            log.error(f"Failed to fetch top symbols: {e}")
            return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

    async def get_orderbook_imbalance(self, symbol, depth=20):
        try:
            ob = await self.exchange.fetch_order_book(symbol, limit=depth)
            bid_vol = sum(x[1] for x in ob["bids"][:depth])
            ask_vol = sum(x[1] for x in ob["asks"][:depth])
            if bid_vol + ask_vol == 0:
                return 0.0
            return (bid_vol - ask_vol) / (bid_vol + ask_vol)
        except Exception:
            return 0.0

    async def detect_cascade(self, symbol):
        try:
            ohlcv_1m = await self.exchange.fetch_ohlcv(symbol, "1m", limit=30)
            df1 = pd.DataFrame(ohlcv_1m, columns=["ts", "o", "h", "l", "c", "v"])

            ret_1m = (df1["c"].iloc[-1] - df1["c"].iloc[-3]) / df1["c"].iloc[-3]
            vol_avg = df1["v"].iloc[-15:-3].mean()
            vol_now = df1["v"].iloc[-1]
            vol_spike = vol_now / vol_avg if vol_avg > 0 else 0

            imb = await self.get_orderbook_imbalance(symbol)

            long_sig = ret_1m > CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb > 0.35
            short_sig = ret_1m < -CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb < -0.35

            strength = abs(ret_1m) * vol_spike * abs(imb)

            return {
                "long": long_sig,
                "short": short_sig,
                "ret": ret_1m,
                "vol_spike": vol_spike,
                "imb": imb,
                "price": float(df1["c"].iloc[-1]),
                "strength": strength,
                "symbol": symbol
            }
        except Exception as e:
            log.debug(f"Detect failed {symbol}: {e}")
            return None

    async def size_position(self, symbol):
        bal = await self.exchange.fetch_balance()
        usdt = float(bal["USDT"]["free"])
        price = (await self.exchange.fetch_ticker(symbol))["last"]
        notional = usdt * MAX_POSITION_PCT * LEVERAGE
        amount = notional / price
        amount = float(self.exchange.amount_to_precision(symbol, amount))
        return amount

    async def enter(self, symbol, side, price):
        now = datetime.now(timezone.utc).timestamp()
        if symbol in self.last_trade and now - self.last_trade[symbol] < COOLDOWN_SEC:
            return False
        if len(self.positions) >= MAX_CONCURRENT_POSITIONS:
            return False

        amount = await self.size_position(symbol)
        if amount <= 0:
            return False

        try:
            await self.set_leverage(symbol)
            await self.exchange.create_order(
                symbol, "market", side, amount,
                params={"reduceOnly": False}
            )
            self.last_trade[symbol] = now
            self.positions[symbol] = {
                "side": side,
                "entry": price,
                "amount": amount,
                "tp": price * (1 + TAKE_PROFIT_PCT) if side == "buy" else price * (1 - TAKE_PROFIT_PCT),
                "sl": price * (1 - STOP_LOSS_PCT) if side == "buy" else price * (1 + STOP_LOSS_PCT)
            }
            log.info(f"ENTER {side.upper()} {symbol} size={amount:.4f} @ {price:.4f}")
            return True
        except Exception as e:
            log.error(f"Entry failed {symbol}: {e}")
            return False

    async def manage_exits(self):
        to_remove = []
        for symbol, pos in list(self.positions.items()):
            try:
                ticker = await self.exchange.fetch_ticker(symbol)
                price = ticker["last"]

                hit_tp = (pos["side"] == "buy" and price >= pos["tp"]) or \
                         (pos["side"] == "sell" and price <= pos["tp"])
                hit_sl = (pos["side"] == "buy" and price <= pos["sl"]) or \
                         (pos["side"] == "sell" and price >= pos["sl"])

                if hit_tp or hit_sl:
                    close_side = "sell" if pos["side"] == "buy" else "buy"
                    await self.exchange.create_order(
                        symbol, "market", close_side, pos["amount"],
                        params={"reduceOnly": True}
                    )
                    reason = "TP" if hit_tp else "SL"
                    log.info(f"EXIT {reason} {symbol} @ {price:.4f}")
                    to_remove.append(symbol)
            except Exception as e:
                log.error(f"Exit check failed {symbol}: {e}")

        for s in to_remove:
            del self.positions[s]

    async def scan_and_trade(self):
        """المسح الرئيسي كل 10 دقائق"""
        log.info("=== Starting market scan ===")
        symbols = await self.get_top_symbols()

        signals = []
        # نفحص العملات بشكل متوازي مع حد معقول
        sem = asyncio.Semaphore(8)

        async def check(sym):
            async with sem:
                return await self.detect_cascade(sym)

        results = await asyncio.gather(*[check(s) for s in symbols])
        for res in results:
            if res and (res["long"] or res["short"]):
                signals.append(res)

        if not signals:
            log.info("No cascade signals found this scan")
            return

        # رتب حسب القوة
        signals.sort(key=lambda x: x["strength"], reverse=True)
        log.info(f"Found {len(signals)} signals. Top: {signals[0]['symbol']} strength={signals[0]['strength']:.3f}")

        async with self.lock:
            await self.manage_exits()

            for sig in signals:
                if len(self.positions) >= MAX_CONCURRENT_POSITIONS:
                    break
                if sig["symbol"] in self.positions:
                    continue

                side = "buy" if sig["long"] else "sell"
                await self.enter(sig["symbol"], side, sig["price"])

    async def exit_watcher(self):
        """مراقبة الخروج بشكل مستمر بين المسحات"""
        while True:
            try:
                async with self.lock:
                    await self.manage_exits()
                await asyncio.sleep(8)
            except Exception as e:
                log.error(f"Exit watcher error: {e}")
                await asyncio.sleep(15)

    async def start(self):
        # نبدأ مهام الخروج المستمرة
        asyncio.create_task(self.exit_watcher())

        while True:
            try:
                await self.scan_and_trade()
            except Exception as e:
                log.error(f"Scan cycle error: {e}")
            log.info(f"Next scan in {SCAN_INTERVAL} seconds...")
            await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    bot = CascadeHunter()
    asyncio.run(bot.start())
