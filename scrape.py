#!/usr/bin/env python
"""Winnow data scraper — incremental daily EOD ingest for India (NSE bhavcopy) + US
(yfinance S&P 500 + ETFs), split/bonus-adjusted, published to R2. Data-only: no scan
engine here (that lives in the private app repo, which reads this store from R2).

Per run: pull store (R2→local) → fetch new trading days since last stored (both markets) →
re-adjust (split/bonus via Yahoo-confirmed corporate actions) → push store (local→R2).
Idempotent; a single post-US-close run covers both prior sessions.

  python scrape.py            # local data/
  R2_* env set → syncs to R2
"""
from __future__ import annotations

import datetime as dt
import glob
import os

import pandas as pd

import storage
from ingest import bhavcopy, corp_actions as ca, yahoo_us

ROOT = os.path.dirname(__file__)
DATA = os.path.join(ROOT, "data")
BC = os.path.join(DATA, "bhavcopy")


def hr(t):
    print("\n" + "=" * 68 + f"\n{t}\n" + "=" * 68, flush=True)


def _in_daypath(d):
    return os.path.join(BC, f"IN_cash_{d.strftime('%Y%m%d')}.parquet")


def _latest_in_date():
    files = sorted(glob.glob(os.path.join(BC, "IN_cash_*.parquet")))
    if not files:
        return None
    s = files[-1].split("_")[-1].split(".")[0]
    return dt.date(int(s[:4]), int(s[4:6]), int(s[6:]))


def update_india(today):
    os.makedirs(BC, exist_ok=True)
    last = _latest_in_date()
    if last is None:
        print("[IN] no store — run: python scrape.py --backfill (or backfill_bhavcopy.py) first")
        return 0
    sess = bhavcopy.new_session()
    d, new = last + dt.timedelta(days=1), 0
    while d <= today:
        if d.weekday() < 5 and not os.path.exists(_in_daypath(d)):
            df = bhavcopy.fetch_day(d, session=sess)
            if df is not None and len(df):
                df.to_parquet(_in_daypath(d), index=False)
                new += 1
                print(f"[IN] {d} → {len(df)} rows", flush=True)
            else:
                print(f"[IN] {d} → no trading data (holiday / not published yet)", flush=True)
        d += dt.timedelta(days=1)
    return new


def rebuild_india_adj():
    files = sorted(glob.glob(os.path.join(BC, "IN_cash_*.parquet")))
    raw = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw.sort_values(["symbol", "date"]).reset_index(drop=True)
    ohlcv = raw[["market", "segment", "symbol", "date", "open", "high", "low", "close", "volume"]].copy()
    cand = ca.detect_candidates(ohlcv)
    yahoo = ca.fetch_yahoo_splits(sorted(cand["symbol"].unique()), suffix=".NS")
    confirmed, _ = ca.reconcile(cand, yahoo, market="IN")
    ca.apply_adjustment(ohlcv, confirmed).to_parquet(os.path.join(DATA, "ohlcv_adj.parquet"), index=False)
    print(f"[IN] re-adjusted: {len(confirmed)} corporate actions", flush=True)


def update_us(today):
    rawpath = os.path.join(DATA, "us_ohlcv_raw.parquet")
    uni = yahoo_us.sp500_universe()
    start = (today - dt.timedelta(days=8)).isoformat()
    fresh = yahoo_us.fetch_bars(uni["symbol"].tolist(), start, (today + dt.timedelta(days=1)).isoformat())
    if fresh.empty:
        return 0
    fresh["date"] = pd.to_datetime(fresh["date"])
    cols = ["market", "segment", "symbol", "date", "open", "high", "low", "close", "volume"]
    if os.path.exists(rawpath):
        ex = pd.read_parquet(rawpath)
        ex["date"] = pd.to_datetime(ex["date"])
        prev = ex["date"].nunique()
        merged = pd.concat([ex[cols], fresh[cols]], ignore_index=True)
    else:
        prev = 0
        merged = fresh[cols].copy()
    merged = (merged.drop_duplicates(["market", "segment", "symbol", "date"], keep="last")
              .sort_values(["symbol", "date"]).reset_index(drop=True))
    merged.to_parquet(rawpath, index=False)
    uni.assign(market="US", segment="cash", currency="USD", exchange="US", active=True) \
       .to_parquet(os.path.join(DATA, "us_symbol_master.parquet"), index=False)
    cand = ca.detect_candidates(merged)
    yahoo = ca.fetch_yahoo_splits(sorted(cand["symbol"].unique()), suffix="") if len(cand) else {}
    confirmed, _ = ca.reconcile(cand, yahoo, market="US")
    ca.apply_adjustment(merged, confirmed).to_parquet(os.path.join(DATA, "us_ohlcv_adj.parquet"), index=False)
    new = merged["date"].nunique() - prev
    print(f"[US] refreshed → {len(merged):,} rows, +{max(new,0)} new day(s)", flush=True)
    return max(new, 0)


def main():
    today = dt.date.today()
    hr(f"SCRAPE — {today} (UTC {dt.datetime.utcnow():%H:%M})")
    storage.pull_store()
    in_new = update_india(today)
    us_new = update_us(today)
    if in_new:
        rebuild_india_adj()
    changed = ([os.path.join(DATA, x) for x in
                ("ohlcv_adj.parquet", "us_ohlcv_raw.parquet", "us_ohlcv_adj.parquet", "us_symbol_master.parquet")]
               + glob.glob(os.path.join(BC, "IN_cash_*.parquet")))
    storage.push(changed)
    hr(f"DONE — IN +{in_new} day(s), US +{us_new} day(s)")


if __name__ == "__main__":
    main()
