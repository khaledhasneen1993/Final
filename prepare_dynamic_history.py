"""Prepare public Binance minute archives for a historical dynamic universe.

Hourly quote-volume bounds prune symbols which cannot enter the top 40.
Final ranking MUST use completed minute volumes, never these bounds.
"""
import argparse
import calendar
import hashlib
import gzip
import io
import json
import os
import time
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import requests

HOUR = 3_600_000
MINUTE = 60_000
DAY = 86_400_000
DTYPE = [("timestamp", "i8"), ("open", "f8"), ("high", "f8"), ("low", "f8"),
         ("close", "f8"), ("volume", "f8"), ("quote_volume", "f8")]
BASE = "https://data.binance.vision/data/futures/um"
_THREAD = threading.local()


@lru_cache(maxsize=4)
def archive_checksums(folder):
    path = Path(folder) / "archive_checksums.json.gz"
    if path.exists():
        with gzip.open(path, "rt") as stream:
            return json.load(stream)
    return {}


def hourly_candidates(folder, start, end, top=40, minimum=15_000_000):
    """Conservative superset of possible minute-exact top-N members.

    At a non-hour boundary, 23 complete hours provide a lower volume bound;
    adding both boundary hours provides an upper bound. The Nth largest lower
    bound is no larger than the Nth largest true volume, so pruning is safe.
    Current/future boundary-hour totals are used ONLY to rule out impossible
    candidates, not as trading inputs or to rank the final universe.
    """
    origin = start - DAY
    n = (end - origin) // HOUR
    bundle = Path(folder) / "hourly.json.gz"
    if bundle.exists():
        with gzip.open(bundle, "rt") as stream:
            packed = json.load(stream)
        for symbol, rows in packed.items():
            if not symbol.replace("_", "").isalnum():
                raise ValueError("Invalid symbol in hourly snapshot")
            path = Path(folder) / "hourly" / f"{symbol}.json"
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(rows))
    symbols, volumes, exclusions = [], [], []
    for path in sorted((Path(folder) / "hourly").glob("*.json")):
        raw = json.loads(path.read_text())
        if not raw:
            exclusions.append({"symbol": path.stem, "reason": "no hourly data"})
            continue
        array = np.asarray(raw, dtype=float)
        if array.ndim != 2 or array.shape[1] != 2:
            exclusions.append({"symbol": path.stem, "reason": "invalid hourly shape"})
            continue
        times = array[:, 0].astype(np.int64)
        if (array.shape[1] != 2 or np.any(~np.isfinite(array)) or np.any(array[:, 1] < 0)
                or np.any(times % HOUR) or np.any(np.diff(times) != HOUR)):
            exclusions.append({"symbol": path.stem, "reason": "invalid or gapped hourly data"})
            continue
        values = np.zeros(n)
        idx = (times - origin) // HOUR
        inside = (idx >= 0) & (idx < n)
        values[idx[inside]] = array[inside, 1]
        symbols.append(path.stem)
        volumes.append(values)
    if not symbols:
        raise ValueError("No hourly source data")
    matrix = np.asarray(volumes)
    cumulative = np.column_stack((np.zeros(len(symbols)), np.cumsum(matrix, axis=1)))
    possible = np.zeros(len(symbols), dtype=bool)
    for h in range(24, n):
        exact = cumulative[:, h] - cumulative[:, h - 24]
        inner = cumulative[:, h] - cumulative[:, h - 23]
        upper = inner + matrix[:, h - 24] + matrix[:, h]
        k = min(top, len(symbols))
        boundary = max(minimum, np.partition(exact, -k)[-k])
        possible |= exact + max(1.0, abs(boundary) * 1e-5) >= boundary
        boundary = max(minimum, np.partition(inner, -k)[-k])
        possible |= upper + max(1.0, abs(boundary) * 1e-5) >= boundary
    return [s for s, keep in zip(symbols, possible) if keep], exclusions


def read_archive(symbol, stamp, monthly, cache):
    period = "monthly" if monthly else "daily"
    name = f"{symbol}-1m-{stamp}.zip"
    path = Path(cache) / symbol / name
    if path.exists():
        raw = path.read_bytes()
    else:
        url = f"{BASE}/{period}/klines/{symbol}/1m/{name}"
        for attempt in range(3):
            try:
                if not hasattr(_THREAD, "session"):
                    _THREAD.session = requests.Session()
                with _THREAD.session.get(url, timeout=40) as response:
                    if response.status_code == 404:
                        return None, None
                    response.raise_for_status()
                    raw = response.content
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f".{os.getpid()}.{threading.get_ident()}.tmp")
        temporary.write_bytes(raw)
        temporary.replace(path)
    digest = hashlib.sha256(raw).hexdigest()
    expected = archive_checksums(str(Path(cache).parent)).get(f"{period}/{name}")
    if expected is not None and expected != digest:
        raise ValueError(f"Archive checksum differs from recorded test: {name}")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = [x for x in archive.namelist() if x.endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"Unexpected archive members: {name}")
        with archive.open(members[0]) as stream:
            frame = pd.read_csv(stream, header=None, usecols=[0, 1, 2, 3, 4, 5, 7],
                                dtype=str)
    frame = frame.apply(pd.to_numeric, errors="coerce")
    # Futures archives may have a textual header. Any other nonnumeric row is invalid.
    if len(frame) and frame.iloc[0].isna().all():
        frame = frame.iloc[1:]
    if frame.isna().any().any():
        raise ValueError(f"Nonnumeric rows: {name}")
    return frame.to_numpy(dtype=float), {"file": f"{period}/{name}", "sha256": digest}


def prepare_symbol(symbol, folder, start, end):
    folder = Path(folder)
    output = folder / "minutes" / f"{symbol}.npz"
    origin = start - DAY
    rows = (end - origin) // MINUTE
    if output.exists():
        with np.load(output) as saved:
            bars = saved["bars"]
            if (len(bars) == rows and bars["timestamp"][0] == origin
                    and bars["timestamp"][-1] == end - MINUTE):
                return {"symbol": symbol, "cached": True}
    hours = json.loads((folder / "hourly" / f"{symbol}.json").read_text())
    if not hours:
        raise ValueError(f"No hourly data: {symbol}")
    active_start = max(origin, int(hours[0][0]))
    active_end = min(end, int(hours[-1][0]) + HOUR)
    bars = np.empty(rows, dtype=DTYPE).view(np.recarray)
    bars["timestamp"] = np.arange(origin, end, MINUTE)
    for name in bars.dtype.names[1:]:
        bars[name] = np.nan
    manifest = []
    first = datetime.fromtimestamp(active_start / 1000, timezone.utc)
    month = first.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while int(month.timestamp() * 1000) < active_end:
        year, number = month.year, month.month
        next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
        data, item = read_archive(symbol, month.strftime("%Y-%m"), True, folder / "archives")
        pieces = []
        if data is not None:
            pieces.append((data, item))
        else:
            for day in range(1, calendar.monthrange(year, number)[1] + 1):
                date = month.replace(day=day)
                t = int(date.timestamp() * 1000)
                if t + DAY <= active_start or t >= active_end:
                    continue
                data, item = read_archive(symbol, date.strftime("%Y-%m-%d"), False,
                                          folder / "archives")
                if data is None:
                    raise ValueError(f"Missing archive {symbol} {date.date()}")
                pieces.append((data, item))
        for data, item in pieces:
            timestamps = data[:, 0].astype(np.int64)
            if np.any(timestamps % MINUTE) or np.any(np.diff(timestamps) != MINUTE):
                raise ValueError(f"Invalid timestamps in {item['file']}")
            keep = (timestamps >= origin) & (timestamps < end)
            data = data[keep]
            indexes = (data[:, 0].astype(np.int64) - origin) // MINUTE
            if np.any(np.isfinite(bars["close"][indexes])):
                raise ValueError(f"Duplicate archive rows: {symbol}")
            for i, name in enumerate(bars.dtype.names[1:], start=1):
                bars[name][indexes] = data[:, i]
            manifest.append(item)
        month = next_month
    present = np.isfinite(bars["close"])
    indexes = np.flatnonzero(present)
    if not len(indexes) or np.any(np.diff(indexes) != 1):
        raise ValueError(f"Missing minute data inside active history: {symbol}")
    active = bars[present]
    if (np.any(active["low"] > np.minimum(active["open"], active["close"]))
            or np.any(active["high"] < np.maximum(active["open"], active["close"]))
            or any(np.any(~np.isfinite(active[k])) for k in active.dtype.names)
            or any(np.any(active[k] <= 0) for k in ("open", "high", "low", "close"))
            or np.any(active["volume"] < 0) or np.any(active["quote_volume"] < 0)):
        raise ValueError(f"Invalid minute prices or volumes: {symbol}")
    observed = np.nan_to_num(bars["quote_volume"]).reshape(-1, 60).sum(axis=1)
    for timestamp, quote in hours:
        index = (int(timestamp) - origin) // HOUR
        if 0 <= index < len(observed) and not np.isclose(observed[index], quote, rtol=1e-6, atol=0.05):
            raise ValueError(f"Hourly/minute quote volume mismatch: {symbol} at {timestamp}")
    output.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(output, bars=bars)
    meta = {"symbol": symbol, "present_minutes": int(present.sum()),
            "first_minute": int(bars["timestamp"][indexes[0]]),
            "last_minute": int(bars["timestamp"][indexes[-1]]), "archives": manifest,
            "npz_sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
    (output.with_suffix(".manifest.json")).write_text(json.dumps(meta, indent=2))
    return {"symbol": symbol, "present_minutes": int(present.sum()), "cached": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--folder", type=Path, default=Path("data/dynamic60d"))
    parser.add_argument("--end", default="2026-10-28")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()
    end = int(datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    start = end - args.days * DAY
    selected, exclusions = hourly_candidates(args.folder, start, end)
    (args.folder / "candidate_symbols.json").write_text(json.dumps(selected))
    (args.folder / "hourly_exclusions.json").write_text(json.dumps(exclusions))
    print(json.dumps({"possible_top40_symbols": len(selected), "excluded_hourly": len(exclusions)}), flush=True)
    errors = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(prepare_symbol, s, args.folder, start, end): s for s in selected}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                print(json.dumps(future.result()), flush=True)
            except Exception as error:
                errors.append({"symbol": symbol, "error": str(error)})
                print(json.dumps(errors[-1]), flush=True)
    (args.folder / "preparation_errors.json").write_text(json.dumps(errors, indent=2))
    if errors:
        raise SystemExit("Incomplete preparation; see preparation_errors.json")


if __name__ == "__main__":
    main()
