"""NSE full-bhavcopy ingest — India cash-equity nightly source (handoff §6/§10).

Source: `sec_bhavdata_full_<DDMMYYYY>.csv` (unadjusted OHLCV + turnover + delivery). NSE
403s bare requests, so we prime cookies from the homepage and send a browser UA + Referer.
Columns → engine shapes are documented in `docs/data-sources.md`.

This adapter is deliberately dependency-light (`requests` + `pandas`). It returns tidy
DataFrames in the engine's table shapes; assembly/persistence is the caller's job
(`scripts/backfill_bhavcopy.py`).
"""
from __future__ import annotations

import datetime as dt
import io
import time
from typing import Optional

import pandas as pd
import requests

BASE = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
HOME = "https://www.nseindia.com/"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Cash-equity universe series (v1 decision — see DECISIONS.md):
#   EQ = mainboard equity (+ ETFs, which trade in EQ) · BE = book-entry / trade-to-trade equity.
# Excluded for v1: SME (SM/ST), govt securities (GS/GB), bonds/InvITs/MF/etc. Liquidity floor
# further trims dead names. Widen by editing this set — no engine change.
CASH_SERIES = {"EQ", "BE"}


def new_session() -> requests.Session:
    """A cookie-primed session. Reuse across many day-fetches; re-prime if NSE starts 401/403."""
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Referer": HOME + "all-reports",
                      "Accept": "text/csv,*/*", "Accept-Language": "en-US,en;q=0.9"})
    try:
        s.get(HOME, timeout=20)
    except requests.RequestException:
        pass
    return s


def fetch_day(day: dt.date, session: Optional[requests.Session] = None,
              series: Optional[set] = None, retries: int = 4) -> Optional[pd.DataFrame]:
    """Fetch + parse one trading day's full bhavcopy → ohlcv_raw-shaped rows (+ turnover,
    delivery, series). Returns None for a non-trading day (404) or a persistent failure.
    Respects 429/Retry-After."""
    s = session or new_session()
    url = BASE.format(ddmmyyyy=day.strftime("%d%m%Y"))
    for attempt in range(retries):
        try:
            r = s.get(url, timeout=30)
        except requests.RequestException:
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code == 200 and len(r.content) > 500 and b"SYMBOL" in r.content[:200]:
            return _parse(r.content, day, series or CASH_SERIES)
        if r.status_code in (403, 401):
            # cookies went stale — re-prime and retry
            s = new_session()
            time.sleep(1.0)
            continue
        if r.status_code == 404:
            return None  # holiday / no session that day
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5)) + 2 * attempt)
            continue
        time.sleep(1.0 + attempt)
    return None


def _parse(content: bytes, day: dt.date, series: set) -> Optional[pd.DataFrame]:
    df = pd.read_csv(io.BytesIO(content), skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    # VALIDATED INGRESS (handoff §15): NSE serves the PREVIOUS session's file for some
    # holidays (e.g. Republic Day) instead of a 404. Reject any file whose trade date
    # (DATE1) ≠ the requested day, so stale data never lands stamped with the wrong date.
    file_date = pd.to_datetime(str(df["DATE1"].iloc[0]).strip(), format="%d-%b-%Y", errors="coerce")
    if pd.isna(file_date) or file_date.date() != day:
        return None
    for c in ("SYMBOL", "SERIES"):
        df[c] = df[c].astype(str).str.strip()
    df = df[df["SERIES"].isin(series)].copy()
    num = lambda col: pd.to_numeric(df[col], errors="coerce")
    out = pd.DataFrame({
        "market": "IN",
        "segment": "cash",
        "symbol": df["SYMBOL"],
        "date": pd.Timestamp(day).date(),
        "open": num("OPEN_PRICE"),
        "high": num("HIGH_PRICE"),
        "low": num("LOW_PRICE"),
        "close": num("CLOSE_PRICE"),
        "volume": num("TTL_TRD_QNTY"),
        "turnover_lacs": num("TURNOVER_LACS"),   # ₹ lakhs — feeds liquidity, no compute
        "deliv_qty": pd.to_numeric(df.get("DELIV_QTY"), errors="coerce"),
        "deliv_per": pd.to_numeric(df.get("DELIV_PER"), errors="coerce"),
        "trades": num("NO_OF_TRADES"),
        "series": df["SERIES"],
    })
    # drop rows with no close (suspended/settlement rows) and non-positive prices
    out = out[out["close"].notna() & (out["close"] > 0)].reset_index(drop=True)
    return out
