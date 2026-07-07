# winnow-data

Independent, public **EOD market-data scraper** for [Winnow](https://winnow.chiragpatnaik.com) —
India (NSE) + US, split/bonus-adjusted, published nightly to R2. Data-only: no scan engine
here (that lives in the private app repo, which reads this store from R2).

Runs as a nightly GitHub Action (free minutes on public repos) so the scrape is independent
of the app's deploy cycle.

## What it does

Per nightly run (`scrape.py`), a single post-US-close pass covering both sessions:

1. **Pull** the store from R2 → local `data/`.
2. **India** — fetch new NSE full-bhavcopy trading days since the last stored date
   (`sec_bhavdata_full_<DDMMYYYY>.csv`), EQ+BE cash universe. **Validated ingress**: rejects
   NSE's stale holiday files (some holidays serve the *prior* session's file instead of 404 —
   we reject any file whose `DATE1` ≠ the requested date).
3. **US** — refresh a recent window for the S&P 500 + major ETFs via yfinance.
4. **Adjust** — split/bonus adjustment (`ohlcv_adj`): detect price discontinuities in our own
   series, **confirm against Yahoo `.splits`**, back-adjust (rejects Yahoo phantom splits *and*
   genuine crashes; dividends not adjusted, to match Chartink/TradingView).
5. **Push** the updated store back to R2.

Idempotent — re-running fetches only missing days.

## Store layout (R2)

```
bhavcopy/IN_cash_<YYYYMMDD>.parquet   # India, per trading day (unadjusted, as delivered)
ohlcv_adj.parquet                     # India, split/bonus-adjusted
us_ohlcv_raw.parquet                  # US bars (yfinance, already split-adjusted upstream)
us_ohlcv_adj.parquet                  # US, adjusted
us_symbol_master.parquet              # US universe + GICS sector/industry
```

Columns follow Winnow's `ohlcv_raw`/`ohlcv_adj` shape: `(market, segment, symbol, date,
open, high, low, close, volume[, adj_factor])`. `market` ∈ `IN`/`US`, `segment` = `cash`.

## Run it

```bash
pip install -r requirements.txt

# one-time seed (a date range)
python backfill.py --start 2025-07-01 --end 2026-07-03

# nightly incremental (local data/; set R2_* env to sync to R2)
python scrape.py
```

R2 sync activates when all four are set: `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY` (GitHub Actions → repo secrets). See the workflow.

## Sources

NSE full bhavcopy (India, official/free) · yfinance (US bars + corporate actions). No
API keys. No third-party retention. See the app repo for the full data-strategy notes.

## Universe note

US is currently S&P 500 + major ETFs (a solid liquid universe). Widen toward the full market
by swapping `ingest/yahoo_us.py::sp500_universe()` for the NASDAQ-Trader symbol directory.
