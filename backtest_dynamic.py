"""Minute-resolution historical test of the dynamic scanner, without order books."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

import numpy as np

import binance_cascade_hunter_dynamic as config
from backtest_cascade import Backtest, date_ms, iso, market_rules, write_csv
from prepare_dynamic_history import DAY, MINUTE, hourly_candidates


def ranked_universe(prefixes, index, top=40, minimum=15_000_000):
    volumes = [(s, float(p[index] - p[index - 1440])) for s, p in prefixes.items()]
    volumes = [(s, v) for s, v in volumes if v >= minimum]
    return sorted(volumes, key=lambda x: (-x[1], x[0]))[:top]


def cascade_signal(bars, index):
    prior = bars[index - 30:index]
    if len(prior) < 30 or not np.all(np.isfinite(prior["close"])):
        return None
    ret = float(bars[index - 1].close / bars[index - 3].close - 1)
    average = float(np.mean(bars["volume"][index - 15:index - 3]))
    spike = float(bars[index - 1].volume / average) if average > 0 else 0
    if abs(ret) <= config.CASCADE_THRESHOLD or spike <= config.VOLUME_SPIKE:
        return None
    return {"side": "buy" if ret > 0 else "sell", "strength": abs(ret) * spike,
            "reference": float(bars[index - 1].close)}


class DynamicBacktest(Backtest):
    def __init__(self, candles, rules, fee_rate=0.0005):
        super().__init__(candles, rules, fee_rate, config=config)
        self.prefixes = {s: np.concatenate(([0.0], np.cumsum(np.nan_to_num(b["quote_volume"]))))
                         for s, b in candles.items()}
        self.scans = []
        self.peak_positions = 0

    def run(self, start):
        timeline = self.candles[self.symbols[0]]["timestamp"]
        for s, bars in self.candles.items():
            if not np.array_equal(timeline, bars["timestamp"]):
                raise ValueError(f"Unaligned minute grid: {s}")
        first = int(np.searchsorted(timeline, start))
        if first < 1440:
            raise ValueError("A full 24-hour quote-volume warmup is required")
        for index in range(first, len(timeline)):
            timestamp = int(timeline[index])
            exited = set()
            for symbol in list(self.positions):
                bar = self.candles[symbol][index]
                if not np.isfinite(bar.open):
                    previous = self.candles[symbol][index - 1]
                    self.close(symbol, float(previous.close), int(previous.timestamp), "DATA_END")
                    exited.add(symbol)
                elif self.gap_exit(symbol, bar):
                    exited.add(symbol)
            if (timestamp - start) % (config.SCAN_INTERVAL * 1000) == 0:
                universe = ranked_universe(self.prefixes, index, config.MAX_SYMBOLS_TO_SCAN,
                                           config.MIN_QUOTE_VOLUME)
                self.scans.append({"utc": iso(timestamp), "symbols": universe})
                signals = []
                for symbol, _ in universe:
                    bar = self.candles[symbol][index]
                    if not np.isfinite(bar.open):
                        continue
                    result = cascade_signal(self.candles[symbol], index)
                    if result:
                        self.signals[symbol] += 1
                        signals.append((symbol, result))
                signals.sort(key=lambda pair: pair[1]["strength"], reverse=True)
                for symbol, sig in signals:
                    if len(self.positions) >= config.MAX_CONCURRENT_POSITIONS:
                        break
                    if symbol in self.positions or symbol in exited:
                        continue
                    bar = self.candles[symbol][index]
                    self.enter(symbol, sig["side"], float(bar.open), sig["reference"], timestamp)
                self.peak_positions = max(self.peak_positions, len(self.positions))
            for symbol in list(self.positions):
                bar = self.candles[symbol][index]
                if not self.gap_exit(symbol, bar):
                    self.intrabar_exit(symbol, bar)
            self.mark_equity(index)
        for symbol in list(self.positions):
            bar = self.candles[symbol][-1]
            self.close(symbol, float(bar.close), int(bar.timestamp), "END_OF_TEST")
        if self.equity_curve:
            self.equity_curve[-1].update(equity_usdt=self.balance, wallet_usdt=self.balance,
                                        open_positions=0)
        result = self.summary()
        actual = sorted({s for scan in self.scans for s, _ in scan["symbols"]})
        result.update(scans=len(self.scans), symbols_ever_in_top40=actual,
                      symbols_ever_in_top40_count=len(actual), peak_open_positions=self.peak_positions)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--folder", type=Path, default=Path("data/dynamic60d"))
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--end", default="2026-10-28")
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--output", type=Path, default=Path("results/dynamic_2026-08-29_2026-10-28"))
    args = parser.parse_args()
    if not 1 <= args.days <= 365 or not np.isfinite(args.fee_rate) or not 0 <= args.fee_rate < 0.01:
        parser.error("days must be 1..365 and fee-rate finite in [0, 0.01)")
    end = date_ms(args.end)
    start = end - args.days * DAY
    info = json.loads((args.folder / "exchange_info.json").read_text())
    files = {p.stem for p in (args.folder / "hourly").glob("*.json")}
    fetch_errors = json.loads((args.folder / "hourly_fetch_errors.json").read_text())
    failed = {x["symbol"] for x in fetch_errors}
    catalog = {s["symbol"] for s in info["symbols"]}
    if catalog != files | failed or files & failed:
        raise ValueError("Hourly catalog coverage is incomplete or ambiguous")
    candidates, exclusions = hourly_candidates(args.folder, start, end,
                                               config.MAX_SYMBOLS_TO_SCAN, config.MIN_QUOTE_VOLUME)
    candles, hashes = {}, {}
    for symbol in candidates:
        path = args.folder / "minutes" / f"{symbol}.npz"
        with np.load(path) as saved:
            bars = saved["bars"].view(np.recarray)
        if (len(bars) != (args.days + 1) * 1440 or bars.timestamp[0] != start - DAY
                or bars.timestamp[-1] != end - MINUTE or np.any(np.diff(bars.timestamp) != MINUTE)):
            raise ValueError(f"Invalid minute grid: {symbol}")
        manifest = json.loads(path.with_suffix(".manifest.json").read_text())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != manifest["npz_sha256"]:
            raise ValueError(f"Source cache checksum changed: {symbol}")
        candles[symbol] = bars
        hashes[symbol] = digest
    engine = DynamicBacktest(candles, market_rules(info), args.fee_rate)
    report = engine.run(start)
    report.update(mode="dynamic_approximate_no_orderbook", days=args.days,
                  start_utc=iso(start), end_utc_exclusive=iso(end), fee_rate=args.fee_rate,
                  catalog_symbols=len(catalog), hourly_sources=len(files),
                  hourly_fetch_errors=fetch_errors, excluded_hourly=exclusions,
                  candidate_symbols=len(candidates), minute_source_sha256=hashes,
                  settings={k: getattr(config, k) for k in (
                      "LEVERAGE", "MAX_POSITION_PCT", "CASCADE_THRESHOLD", "VOLUME_SPIKE",
                      "TAKE_PROFIT_PCT", "STOP_LOSS_PCT", "COOLDOWN_SEC", "SCAN_INTERVAL",
                      "MAX_SYMBOLS_TO_SCAN", "MAX_CONCURRENT_POSITIONS", "MIN_QUOTE_VOLUME")},
                  assumptions=["Current USDT perpetual catalog; survivorship bias remains possible",
                               "One catalog symbol with an API status error may be excluded; see errors",
                               "Minute-exact previous-24h quote volume; no current-day top-40 list",
                               "Hourly upper/lower bounds only prune provably ineligible symbols",
                               "Order-book filter disabled; strength = abs(return) * volume_spike",
                               "Completed minute signal; next open entry on fixed 10-minute scan grid",
                               "One-minute OHLC exit approximation, not an 8-second replay",
                               "Stop first if both touched; gap at open; no reentry in exit bar",
                               "Current quantity filters; assumed 50x availability",
                               "No liquidation, funding, slippage or maintenance-margin model"])
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    fields = ["symbol", "side", "entry_utc", "exit_bar_utc", "entry_price", "exit_price",
              "amount", "margin_usdt", "reason", "gross_pnl_usdt", "fees_usdt",
              "net_pnl_usdt", "balance_usdt"]
    write_csv(args.output / "trades.csv", engine.trades, fields)
    write_csv(args.output / "equity.csv", engine.equity_curve,
              ["utc", "equity_usdt", "wallet_usdt", "open_positions"])
    with gzip.open(args.output / "scans.jsonl.gz", "wt", encoding="utf-8") as stream:
        for scan in engine.scans:
            stream.write(json.dumps(scan) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k not in (
        "by_symbol", "minute_source_sha256", "signals", "symbols_ever_in_top40")}, indent=2))


if __name__ == "__main__":
    main()
