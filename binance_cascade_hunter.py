# language: Python 3.11+, file: binance_cascade_hunter.py
# target: Binance USDT-M Futures, high-leverage aggressive
# deps: pip install ccxt pandas numpy ta websockets aiohttp

import ccxt.async_support as ccxt
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("CASCADE")

# === CONFIG — عدّل هنا ===
API_KEY = "YOUR_BINANCE_API_KEY"
API_SECRET = "YOUR_BINANCE_API_SECRET"
SYMBOLS = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]  # أضف المزيد
LEVERAGE = 50          # ارفع لـ 75-100 لو حسابك يسمح (خطر تصفية عالي)
MAX_POSITION_PCT = 0.85  # 85% من الهامش المتاح في صفقة واحدة
CASCADE_THRESHOLD = 0.012  # 1.2% حركة سريعة = إشارة cascade
VOLUME_SPIKE = 3.5     # حجم أكبر من المتوسط × 3.5
TAKE_PROFIT_PCT = 0.018
STOP_LOSS_PCT = 0.009
COOLDOWN_SEC = 8

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

    async def set_leverage(self, symbol):
        try:
            await self.exchange.set_leverage(LEVERAGE, symbol)
            log.info(f"Leverage {LEVERAGE}x set on {symbol}")
        except Exception as e:
            log.warning(f"Leverage set failed {symbol}: {e}")

    async def get_orderbook_imbalance(self, symbol, depth=20):
        ob = await self.exchange.fetch_order_book(symbol, limit=depth)
        bid_vol = sum([x[1] for x in ob["bids"][:depth]])
        ask_vol = sum([x[1] for x in ob["asks"][:depth]])
        if bid_vol + ask_vol == 0:
            return 0.0
        return (bid_vol - ask_vol) / (bid_vol + ask_vol)

    async def detect_cascade(self, symbol):
        # OHLCV 1m + 5m للسرعة
        ohlcv_1m = await self.exchange.fetch_ohlcv(symbol, "1m", limit=30)
        ohlcv_5m = await self.exchange.fetch_ohlcv(symbol, "5m", limit=12)
        df1 = pd.DataFrame(ohlcv_1m, columns=["ts", "o", "h", "l", "c", "v"])
        df5 = pd.DataFrame(ohlcv_5m, columns=["ts", "o", "h", "l", "c", "v"])

        # حركة سريعة + حجم
        ret_1m = (df1["c"].iloc[-1] - df1["c"].iloc[-3]) / df1["c"].iloc[-3]
        vol_avg = df1["v"].iloc[-15:-3].mean()
        vol_now = df1["v"].iloc[-1]
        vol_spike = vol_now / vol_avg if vol_avg > 0 else 0

        # Orderbook pressure
        imb = await self.get_orderbook_imbalance(symbol)

        # Cascade signal: حركة حادة + حجم + imbalance قوي
        long_sig = ret_1m > CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb > 0.35
        short_sig = ret_1m < -CASCADE_THRESHOLD and vol_spike > VOLUME_SPIKE and imb < -0.35

        return {
            "long": long_sig,
            "short": short_sig,
            "ret": ret_1m,
            "vol_spike": vol_spike,
            "imb": imb,
            "price": df1["c"].iloc[-1]
        }

    async def size_position(self, symbol, side):
        bal = await self.exchange.fetch_balance()
        usdt = float(bal["USDT"]["free"])
        price = (await self.exchange.fetch_ticker(symbol))["last"]
        # حجم عدواني
        notional = usdt * MAX_POSITION_PCT * LEVERAGE
        amount = notional / price
        # تقريب حسب precision
        markets = await self.exchange.load_markets()
        amount = float(self.exchange.amount_to_precision(symbol, amount))
        return amount

    async def enter(self, symbol, side, price):
        now = datetime.now(timezone.utc).timestamp()
        if symbol in self.last_trade and now - self.last_trade[symbol] < COOLDOWN_SEC:
            return
        amount = await self.size_position(symbol, side)
        if amount <= 0:
            return

        try:
            order = await self.exchange.create_order(
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
            log.info(f"ENTER {side.upper()} {symbol} size={amount:.4f} @ {price:.2f}")
        except Exception as e:
            log.error(f"Entry failed {symbol}: {e}")

    async def manage_exits(self, symbol):
        if symbol not in self.positions:
            return
        pos = self.positions[symbol]
        ticker = await self.exchange.fetch_ticker(symbol)
        price = ticker["last"]

        hit_tp = (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
        hit_sl = (pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"])

        if hit_tp or hit_sl:
            close_side = "sell" if pos["side"] == "buy" else "buy"
            try:
                await self.exchange.create_order(
                    symbol, "market", close_side, pos["amount"],
                    params={"reduceOnly": True}
                )
                reason = "TP" if hit_tp else "SL"
                log.info(f"EXIT {reason} {symbol} @ {price:.2f}")
                del self.positions[symbol]
            except Exception as e:
                log.error(f"Exit failed: {e}")

    async def run_symbol(self, symbol):
        await self.set_leverage(symbol)
        while True:
            try:
                sig = await self.detect_cascade(symbol)
                await self.manage_exits(symbol)

                if symbol not in self.positions:
                    if sig["long"]:
                        await self.enter(symbol, "buy", sig["price"])
                    elif sig["short"]:
                        await self.enter(symbol, "sell", sig["price"])

                await asyncio.sleep(1.2)
            except Exception as e:
                log.error(f"Loop error {symbol}: {e}")
                await asyncio.sleep(3)

    async def start(self):
        tasks = [self.run_symbol(s) for s in SYMBOLS]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    bot = CascadeHunter()
    asyncio.run(bot.start())
