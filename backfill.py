#!/usr/bin/env python
"""One-time seed backfill — pull a date range of India bhavcopy (per-day parquet) and a US
window, then adjust. Run once to seed `data/` (or to rebuild from scratch); the nightly
`scrape.py` keeps it current incrementally.

  python backfill.py --start 2025-07-01 --end 2026-07-03
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import os
import time

import pandas as pd

import storage  # noqa: F401 (kept for parity; backfill writes local, scrape syncs)
from ingest import bhavcopy, corp_actions as ca, yahoo_us

ROOT = os.path.dirname(__file__)
DATA = os.path.join(ROOT, "data")
BC = os.path.join(DATA, "bhavcopy")


def daterange(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--delay", type=float, default=0.4)
    a = ap.parse_args()
    os.makedirs(BC, exist_ok=True)
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)

    # India
    sess = bhavcopy.new_session()
    got = 0
    for i, d in enumerate(daterange(start, end)):
        p = os.path.join(BC, f"IN_cash_{d.strftime('%Y%m%d')}.parquet")
        if os.path.exists(p):
            continue
        if i and i % 60 == 0:
            sess = bhavcopy.new_session()
        df = bhavcopy.fetch_day(d, session=sess)
        if df is not None and len(df):
            df.to_parquet(p, index=False)
            got += 1
            print(f"[IN] {d} {len(df)} rows", flush=True)
        time.sleep(a.delay)
    print(f"[IN] {got} new days", flush=True)

    # India adjust
    files = sorted(glob.glob(os.path.join(BC, "IN_cash_*.parquet")))
    raw = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    raw["date"] = pd.to_datetime(raw["date"])
    ohlcv = raw.sort_values(["symbol", "date"])[["market", "segment", "symbol", "date", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
    cand = ca.detect_candidates(ohlcv)
    yahoo = ca.fetch_yahoo_splits(sorted(cand["symbol"].unique()), suffix=".NS")
    confirmed, _ = ca.reconcile(cand, yahoo, market="IN")
    ca.apply_adjustment(ohlcv, confirmed).to_parquet(os.path.join(DATA, "ohlcv_adj.parquet"), index=False)
    print(f"[IN] adjusted, {len(confirmed)} corporate actions", flush=True)

    # US
    uni = yahoo_us.sp500_universe()
    us = yahoo_us.fetch_bars(uni["symbol"].tolist(), a.start, (end + dt.timedelta(days=1)).isoformat())
    cols = ["market", "segment", "symbol", "date", "open", "high", "low", "close", "volume"]
    us[cols].to_parquet(os.path.join(DATA, "us_ohlcv_raw.parquet"), index=False)
    uni.assign(market="US", segment="cash", currency="USD", exchange="US", active=True).to_parquet(os.path.join(DATA, "us_symbol_master.parquet"), index=False)
    ucand = ca.detect_candidates(us)
    uy = ca.fetch_yahoo_splits(sorted(ucand["symbol"].unique()), suffix="") if len(ucand) else {}
    uconf, _ = ca.reconcile(ucand, uy, market="US")
    ca.apply_adjustment(us, uconf).to_parquet(os.path.join(DATA, "us_ohlcv_adj.parquet"), index=False)
    print(f"[US] {us['symbol'].nunique()} symbols, {len(us):,} rows", flush=True)


if __name__ == "__main__":
    main()
