"""Approximate closed-candle backtest. Never submits exchange orders."""

import argparse
import csv
import gzip
import hashlib
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import binance_cascade_hunter as strategy

MINUTE = 60_000
WARMUP = 30
API = "https://fapi.binance.com/fapi/v1/"


def iso(timestamp):
    return datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat()


def date_ms(value):
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def get_json(endpoint, parameters=None):
    url = API + endpoint + ("?" + urlencode(parameters) if parameters else "")
    for attempt in range(3):
        try:
            with urlopen(url, timeout=20) as response:
                return json.load(response)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)


def download(symbol, start, end, path):
    rows = []
    cursor = start
    while cursor < end:
        page = get_json("klines", {"symbol": symbol, "interval": "1m", "startTime": cursor,
                                   "endTime": end - 1, "limit": 1500})
        if not isinstance(page, list) or not page or int(page[-1][0]) < cursor:
            raise ValueError(f"Incomplete data for {symbol} at {iso(cursor)}")
        rows.extend(page)
        cursor = int(page[-1][0]) + MINUTE
        time.sleep(0.15)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


@dataclass(frozen=True)
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def load_candles(path, start, end):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    candles = []
    for row in raw:
        timestamp = int(row[0])
        if not start <= timestamp < end:
            continue
        o, h, l, c, v = map(float, row[1:6])
        if (timestamp % MINUTE or not all(math.isfinite(x) for x in (o, h, l, c, v))
                or min(o, h, l, c) <= 0 or v < 0 or l > min(o, c)
                or h < max(o, c) or l > h):
            raise ValueError(f"Invalid candle in {path} at {timestamp}")
        candles.append(Candle(timestamp, o, h, l, c, v))
    expected = list(range(start, end, MINUTE))
    if [c.timestamp for c in candles] != expected:
        raise ValueError(f"Missing, duplicate or unsorted one-minute candles: {path}")
    return candles


def signal(candles, index):
    """At bar index, only candles through index-1 are available."""
    if index < WARMUP:
        return None
    ret = candles[index - 1].close / candles[index - 3].close - 1
    average = sum(c.volume for c in candles[index - 15:index - 3]) / 12
    spike = candles[index - 1].volume / average if average > 0 else 0
    if spike <= strategy.VOLUME_SPIKE:
        return None
    if ret > strategy.CASCADE_THRESHOLD:
        return "buy"
    if ret < -strategy.CASCADE_THRESHOLD:
        return "sell"
    return None


def market_rules(info):
    rules = {}
    for item in info["symbols"]:
        filters = {f["filterType"]: f for f in item["filters"]}
        lot = filters.get("MARKET_LOT_SIZE", filters.get("LOT_SIZE", {}))
        notional = filters.get("MIN_NOTIONAL", {})
        rules[item["symbol"]] = {"step": lot.get("stepSize", "0"),
                                 "min": float(lot.get("minQty", 0)),
                                 "max": float(lot.get("maxQty", "inf")),
                                 "min_notional": float(notional.get("notional", 0))}
    return rules


def rounded_amount(amount, rule):
    step = Decimal(rule["step"])
    if step <= 0:
        raise ValueError("Missing positive market quantity step")
    return float((Decimal(str(amount)) / step).to_integral_value(rounding=ROUND_DOWN) * step)


class Backtest:
    def __init__(self, candles, rules, fee_rate=0.0005):
        self.candles = candles
        self.symbols = list(candles)
        self.rules = rules
        self.fee_rate = fee_rate
        self.balance = 100.0
        self.positions = {}
        self.last_entry = {}
        self.trades = []
        self.equity_curve = []
        self.signals = {s: 0 for s in self.symbols}
        self.skipped = 0
        self.ambiguous = 0

    @property
    def free(self):
        return max(0.0, self.balance - sum(p["margin"] for p in self.positions.values()))

    def enter(self, symbol, side, price, reference, timestamp):
        if timestamp - self.last_entry.get(symbol, -10**20) < strategy.COOLDOWN_SEC * 1000:
            return
        rule = self.rules[symbol]
        amount = rounded_amount(self.free * strategy.MAX_POSITION_PCT * strategy.LEVERAGE / price, rule)
        notional = amount * price
        margin = notional / strategy.LEVERAGE
        fee = notional * self.fee_rate
        if (amount <= 0 or amount < rule["min"] or amount > rule["max"]
                or notional < rule["min_notional"] or margin + fee > self.free):
            self.skipped += 1
            return
        self.balance -= fee
        self.last_entry[symbol] = timestamp
        self.positions[symbol] = {
            "side": side, "entry": price, "amount": amount, "margin": margin,
            "entry_fee": fee, "entry_time": timestamp,
            "tp": reference * (1 + strategy.TAKE_PROFIT_PCT if side == "buy"
                               else 1 - strategy.TAKE_PROFIT_PCT),
            "sl": reference * (1 - strategy.STOP_LOSS_PCT if side == "buy"
                               else 1 + strategy.STOP_LOSS_PCT),
        }

    def close(self, symbol, price, timestamp, reason):
        p = self.positions.pop(symbol)
        gross = (price - p["entry"]) * p["amount"] * (1 if p["side"] == "buy" else -1)
        fee = price * p["amount"] * self.fee_rate
        self.balance += gross - fee
        self.trades.append({"symbol": symbol, "side": p["side"],
                            "entry_utc": iso(p["entry_time"]), "exit_bar_utc": iso(timestamp),
                            "entry_price": p["entry"], "exit_price": price,
                            "amount": p["amount"], "margin_usdt": p["margin"],
                            "reason": reason, "gross_pnl_usdt": gross,
                            "fees_usdt": p["entry_fee"] + fee,
                            "net_pnl_usdt": gross - p["entry_fee"] - fee,
                            "balance_usdt": self.balance})

    def gap_exit(self, symbol, bar):
        p = self.positions[symbol]
        long = p["side"] == "buy"
        if (long and bar.open <= p["sl"]) or (not long and bar.open >= p["sl"]):
            self.close(symbol, bar.open, bar.timestamp, "SL_GAP")
            return True
        if (long and bar.open >= p["tp"]) or (not long and bar.open <= p["tp"]):
            self.close(symbol, bar.open, bar.timestamp, "TP_GAP")
            return True
        return False

    def intrabar_exit(self, symbol, bar):
        p = self.positions[symbol]
        long = p["side"] == "buy"
        stop = bar.low <= p["sl"] if long else bar.high >= p["sl"]
        target = bar.high >= p["tp"] if long else bar.low <= p["tp"]
        if stop and target:
            self.ambiguous += 1
        if stop:
            self.close(symbol, p["sl"], bar.timestamp, "SL_BOTH" if target else "SL")
        elif target:
            self.close(symbol, p["tp"], bar.timestamp, "TP")

    def mark_equity(self, index):
        equity = self.balance + sum(
            (self.candles[s][index].close - p["entry"]) * p["amount"]
            * (1 if p["side"] == "buy" else -1) for s, p in self.positions.items()
        )
        self.equity_curve.append({"utc": iso(self.candles[self.symbols[0]][index].timestamp + MINUTE),
                                  "equity_usdt": equity, "wallet_usdt": self.balance,
                                  "open_positions": len(self.positions)})

    def run(self, start):
        timeline = [c.timestamp for c in self.candles[self.symbols[0]]]
        for symbol in self.symbols:
            if [c.timestamp for c in self.candles[symbol]] != timeline:
                raise ValueError("Symbols must have identical complete timelines")
        for i, timestamp in enumerate(timeline):
            if timestamp < start:
                continue
            # Only opening prices are known here. Intrabar proceeds cannot fund earlier entries.
            exited = set()
            for s in self.symbols:
                if s in self.positions and self.gap_exit(s, self.candles[s][i]):
                    exited.add(s)
            for s in self.symbols:
                side = signal(self.candles[s], i)
                if side:
                    self.signals[s] += 1
                if side and s not in self.positions and s not in exited:
                    bar = self.candles[s][i]
                    self.enter(s, side, bar.open, self.candles[s][i - 1].close, timestamp)
            # All entries are settled before observing this minute's high/low.
            for s in self.symbols:
                if s in self.positions:
                    bar = self.candles[s][i]
                    if not self.gap_exit(s, bar):
                        self.intrabar_exit(s, bar)
            self.mark_equity(i)
        for s in list(self.positions):
            bar = self.candles[s][-1]
            self.close(s, bar.close, bar.timestamp, "END_OF_TEST")
        if self.equity_curve:
            self.equity_curve[-1].update(equity_usdt=self.balance, wallet_usdt=self.balance,
                                         open_positions=0)
        return self.summary()

    def summary(self):
        peak = 100.0
        max_dd = 0.0
        for point in self.equity_curve:
            peak = max(peak, point["equity_usdt"])
            max_dd = max(max_dd, (peak - point["equity_usdt"]) / peak * 100)
        wins = sum(t["net_pnl_usdt"] > 0 for t in self.trades)
        return {"initial_balance_usdt": 100.0, "final_balance_usdt": self.balance,
                "net_pnl_usdt": self.balance - 100, "return_pct": self.balance - 100,
                "closed_trades": len(self.trades), "wins": wins,
                "losses": sum(t["net_pnl_usdt"] < 0 for t in self.trades),
                "win_rate_pct": wins / len(self.trades) * 100 if self.trades else 0,
                "fees_usdt": sum(t["fees_usdt"] for t in self.trades),
                "max_close_equity_drawdown_pct": max_dd,
                "signals": self.signals, "skipped_entries": self.skipped,
                "ambiguous_stop_first_bars": self.ambiguous,
                "by_symbol": {s: {"trades": sum(t["symbol"] == s for t in self.trades),
                                  "net_pnl_usdt": sum(t["net_pnl_usdt"] for t in self.trades
                                                      if t["symbol"] == s)} for s in self.symbols}}


def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--end", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Exclusive UTC end date, YYYY-MM-DD")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path, default=Path("backtest_results"))
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--offline", action="store_true", help="Require complete cached data")
    args = parser.parse_args()
    if not 1 <= args.days <= 365 or not math.isfinite(args.fee_rate) or not 0 <= args.fee_rate < 0.01:
        parser.error("days must be 1..365 and fee-rate must be finite in [0, 0.01)")
    end = date_ms(args.end)
    if end > int(datetime.now(timezone.utc).timestamp() * 1000):
        parser.error("End date must not be in the future")
    start = end - args.days * 86_400_000
    warmup_start = start - WARMUP * MINUTE
    args.data_dir.mkdir(parents=True, exist_ok=True)
    info_path = args.data_dir / "exchange_info.json"
    if not info_path.exists():
        if args.offline:
            parser.error(f"Missing {info_path}")
        info = get_json("exchangeInfo")
        info = {"retrieved_utc": datetime.now(timezone.utc).isoformat(),
                "source": "Binance USD-M exchangeInfo (current filters, not historical)",
                "symbols": info["symbols"]}
        info_path.write_text(json.dumps(info), encoding="utf-8")
    info = json.loads(info_path.read_text(encoding="utf-8"))
    symbols = [s.split("/")[0] + "USDT" for s in strategy.SYMBOLS]
    candles, hashes = {}, {}
    for symbol in symbols:
        path = args.data_dir / f"{symbol}_1m.json"
        compressed = path.with_suffix(".json.gz")
        if not path.exists() and compressed.exists():
            path.write_bytes(gzip.decompress(compressed.read_bytes()))
        if path.exists():
            try:
                candles[symbol] = load_candles(path, warmup_start, end)
            except ValueError:
                if args.offline:
                    raise
        if symbol not in candles:
            if args.offline:
                parser.error(f"Missing or incomplete data: {path}")
            download(symbol, warmup_start, end, path)
            candles[symbol] = load_candles(path, warmup_start, end)
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    bot = Backtest(candles, market_rules(info), args.fee_rate)
    report = bot.run(start)
    report.update(start_utc=iso(start), end_utc_exclusive=iso(end), days=args.days,
                  symbols=symbols, candles_per_symbol=len(next(iter(candles.values()))),
                  warmup_candles=WARMUP, fee_rate=args.fee_rate, data_sha256=hashes,
                  filter_metadata=info.get("retrieved_utc"),
                  settings={k: getattr(strategy, k) for k in (
                      "LEVERAGE", "MAX_POSITION_PCT", "CASCADE_THRESHOLD", "VOLUME_SPIKE",
                      "TAKE_PROFIT_PCT", "STOP_LOSS_PCT", "COOLDOWN_SEC")},
                  assumptions=["Order-book filter disabled; not the original full strategy",
                               "Closed one-minute signals; next-minute open entry",
                               "Stops/targets referenced to signal close, as in original",
                               "OHLC idealized exits; stop first if both touched; gaps at open",
                               "Shared $100 wallet; symbol priority BTC, ETH, SOL",
                               "No reentry in same bar after exit; force close at end",
                               "Current quantity filters, not historical filters",
                               "No funding, slippage, liquidation or maintenance-margin model",
                               "Drawdown sampled at minute close; intrabar losses may be larger"])
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False),
                                             encoding="utf-8")
    fields = ["symbol", "side", "entry_utc", "exit_bar_utc", "entry_price", "exit_price",
              "amount", "margin_usdt", "reason", "gross_pnl_usdt", "fees_usdt",
              "net_pnl_usdt", "balance_usdt"]
    write_csv(args.output / "trades.csv", bot.trades, fields)
    write_csv(args.output / "equity.csv", bot.equity_curve,
              ["utc", "equity_usdt", "wallet_usdt", "open_positions"])
    print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
