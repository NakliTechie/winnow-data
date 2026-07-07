"""US equity ingest via Yahoo (yfinance) — S&P 500 + major ETFs.

The handoff routes US *bulk* through Stooq, but Stooq now gates its CSV behind a JS
proof-of-work challenge; yfinance is the reliable adjusted source (the handoff's own US
gap-fill/adjusted feed) and is more than adequate at S&P-500 scale. Whole-market Stooq bulk
(≈14k symbols, needs a PoW solver) is a later production concern — see docs/data-sources.md.

Adjustment is handled the SAME way as India — split/bonus-only via `corp_actions` (detect a
price gap + confirm against Yahoo `.splits`), NOT Yahoo's dividend-inclusive Adj Close, so
US indicators match Chartink/TradingView (which don't dividend-adjust).
"""
from __future__ import annotations

import time
from typing import List

import pandas as pd

SP500_CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"

# a handful of big, liquid ETFs to exercise the is_etf path (handoff: US ETFs included).
ETFS = [("SPY", "SPDR S&P 500 ETF Trust", "Broad Market"),
        ("QQQ", "Invesco QQQ Trust", "Nasdaq 100"),
        ("IWM", "iShares Russell 2000 ETF", "Small Cap"),
        ("DIA", "SPDR Dow Jones Industrial", "Large Cap"),
        ("XLK", "Technology Select Sector SPDR", "Technology"),
        ("XLF", "Financial Select Sector SPDR", "Financials"),
        ("XLE", "Energy Select Sector SPDR", "Energy"),
        ("XLV", "Health Care Select Sector SPDR", "Health Care")]


def sp500_universe() -> pd.DataFrame:
    """S&P 500 constituents (symbol, name, sector, industry, is_etf) + major ETFs. Symbols
    normalized to Yahoo form (BRK.B → BRK-B)."""
    df = pd.read_csv(SP500_CSV)
    eq = pd.DataFrame({
        "symbol": df["Symbol"].str.replace(".", "-", regex=False),
        "name": df["Security"], "sector": df["GICS Sector"],
        "industry": df["GICS Sub-Industry"], "is_etf": False,
    })
    etf = pd.DataFrame([{"symbol": s, "name": n, "sector": "ETF", "industry": i, "is_etf": True}
                        for s, n, i in ETFS])
    return pd.concat([eq, etf], ignore_index=True).drop_duplicates("symbol").reset_index(drop=True)


def fetch_bars(tickers: List[str], start: str, end: str,
               batch: int = 100, delay: float = 1.0) -> pd.DataFrame:
    """Batch-download daily OHLCV via yfinance → long ohlcv_raw shape (market=US,
    segment=cash). `Adj Close` is retained for cross-check but adjustment is done via
    corp_actions (split-only). Missing/failed tickers are skipped."""
    import yfinance as yf
    frames = []
    for i in range(0, len(tickers), batch):
        chunk = tickers[i:i + batch]
        df = yf.download(chunk, start=start, end=end, auto_adjust=False,
                         group_by="ticker", progress=False, threads=True)
        for sym in chunk:
            try:
                sub = df[sym][["Open", "High", "Low", "Close", "Adj Close", "Volume"]].dropna(subset=["Close"])
            except (KeyError, TypeError):
                continue
            if sub.empty:
                continue
            frames.append(pd.DataFrame({
                "market": "US", "segment": "cash", "symbol": sym,
                "date": pd.to_datetime(sub.index).date,
                "open": sub["Open"].values, "high": sub["High"].values,
                "low": sub["Low"].values, "close": sub["Close"].values,
                "volume": sub["Volume"].values, "adj_close": sub["Adj Close"].values,
            }))
        time.sleep(delay)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
