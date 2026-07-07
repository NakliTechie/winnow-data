"""Corporate-action adjustment — turn unadjusted bhavcopy `ohlcv_raw` into `ohlcv_adj`.

Strategy (robust to noisy vendor data): the **bhavcopy discontinuity is the anchor**, Yahoo
is the **ratio source**, and the two must **agree**:

  1. Detect candidate ex-dates from bhavcopy: an overnight close/prev_close drop big enough
     to be a split/bonus (not normal volatility). These are OUR real price gaps.
  2. Pull Yahoo `.splits` for the candidate symbols (yfinance).
  3. Confirm each bhavcopy gap against a Yahoo split near that date with a matching ratio.
     - Yahoo split with no bhavcopy gap  → ignored (Yahoo emits phantom/duplicate splits,
       e.g. ROLEXRINGS shows 3× 10:1 where only one is real).
     - bhavcopy gap with no Yahoo split  → left UNADJUSTED + logged (likely a genuine crash,
       not a corporate action — never fabricate a split from a price drop).

Only confirmed actions produce an adjustment factor. Back-adjusted (latest bar unadjusted):
prices before an ex-date are scaled by 1/Π(ratio after date); volume scales inversely so
turnover is preserved. Split-only (matches Chartink's split/bonus adjustment); dividends are
intentionally NOT adjusted (they don't cause the false breakouts §6 warns about).
"""
from __future__ import annotations

import datetime as dt
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd


def detect_candidates(ohlcv: pd.DataFrame, thresh: float = 0.65) -> pd.DataFrame:
    """Overnight close-to-close drops below `thresh` — split/bonus suspects. `ohlcv` needs
    (symbol, date, close), sorted or not."""
    df = ohlcv.sort_values(["symbol", "date"]).copy()
    df["prev_close"] = df.groupby("symbol")["close"].shift(1)
    df["obs_ratio"] = df["close"] / df["prev_close"]
    cand = df[(df["obs_ratio"] < thresh) & df["prev_close"].notna()]
    return cand[["symbol", "date", "obs_ratio", "prev_close", "close"]].reset_index(drop=True)


def fetch_yahoo_splits(symbols: List[str], start: Optional[dt.date] = None,
                       suffix: str = ".NS", delay: float = 0.15) -> Dict[str, List[Tuple[dt.date, float]]]:
    """{symbol: [(ex_date, ratio), …]} from Yahoo via yfinance. Symbols not on Yahoo → []."""
    import yfinance as yf
    out: Dict[str, List[Tuple[dt.date, float]]] = {}
    for sym in symbols:
        try:
            sp = yf.Ticker(sym + suffix).splits
            events = []
            for d, r in sp.items():
                ex = pd.Timestamp(d).date()
                if start and ex < start:
                    continue
                if float(r) > 0:
                    events.append((ex, float(r)))
            out[sym] = events
        except Exception:
            out[sym] = []
        time.sleep(delay)
    return out


def reconcile(candidates: pd.DataFrame, yahoo: Dict[str, List[Tuple[dt.date, float]]],
              market: str = "IN", segment: str = "cash",
              tol: float = 0.06, day_window: int = 4) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Match each price-gap candidate to a Yahoo split (date within ±day_window, ratio agrees).
    Returns (confirmed corporate_actions, unmatched candidates). The ex-date used is OUR
    series' gap date; the ratio is Yahoo's clean value. Market-agnostic (IN or US)."""
    confirmed, unmatched = [], []
    for _, c in candidates.iterrows():
        gap = pd.Timestamp(c["date"]).date()
        best = None
        for ex_y, ratio_y in yahoo.get(c["symbol"], []):
            if abs((gap - ex_y).days) <= day_window and abs(c["obs_ratio"] - 1.0 / ratio_y) <= tol:
                best = ratio_y
                break
        if best is not None:
            confirmed.append({"market": market, "segment": segment, "symbol": c["symbol"],
                              "ex_date": gap, "ratio": best, "source": "yahoo"})
        else:
            unmatched.append({"symbol": c["symbol"], "date": gap,
                              "obs_ratio": round(float(c["obs_ratio"]), 4)})
    return pd.DataFrame(confirmed), pd.DataFrame(unmatched)


def apply_adjustment(ohlcv_raw: pd.DataFrame, corp_actions: pd.DataFrame) -> pd.DataFrame:
    """Build ohlcv_adj: back-adjust OHLC by factor=1/Π(ratio for ex_date > date); volume
    scales inversely (turnover preserved). Symbols with no action → factor 1.0 (identity)."""
    df = ohlcv_raw.sort_values(["symbol", "date"]).copy()
    df["date"] = pd.to_datetime(df["date"])
    df["_factor"] = 1.0          # price multiplier
    if len(corp_actions):
        acts = corp_actions.copy()
        acts["ex_date"] = pd.to_datetime(acts["ex_date"])
        by_symbol = {s: g for s, g in acts.groupby("symbol")}
        # per symbol, fold each action into the factor for the pre-ex-date rows
        parts = []
        for sym, g in df.groupby("symbol"):
            g = g.copy()
            for _, a in by_symbol.get(sym, pd.DataFrame()).iterrows():
                mask = g["date"] < a["ex_date"]
                g.loc[mask, "_factor"] = g.loc[mask, "_factor"] / a["ratio"]
            parts.append(g)
        df = pd.concat(parts, ignore_index=True)
    adj = pd.DataFrame({
        "market": df["market"], "segment": df["segment"], "symbol": df["symbol"],
        "date": df["date"].dt.date,
        "open": df["open"] * df["_factor"], "high": df["high"] * df["_factor"],
        "low": df["low"] * df["_factor"], "close": df["close"] * df["_factor"],
        "volume": df["volume"] / df["_factor"],
        "adj_factor": df["_factor"],
    })
    return adj.reset_index(drop=True)
