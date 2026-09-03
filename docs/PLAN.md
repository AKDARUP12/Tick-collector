# Plan: daily tick→backtest collector (4 strikes CE+PE, no overlap, backtest-ready)

## 1. What you asked
- Collect **both CE and PE**, only **4 strikes** around ATM (8 options + NIFTY spot = 9 instruments)
- Run **daily**, keep it **backtest-ready** for `nifty_backtest` (currently `nbt/schema.py:28,36` + `nbt/cache.py:129`)
- Existing dataset is **1-min OHLC only**, no volume/CVD/OI/orderflow; you want a **fresh large tick-level source** that can still feed the old engine
- Current `arrow_broker_orderflow/storage.py:1` appends to 4 CSVs tick-by-tick with no dedupe, no partitioning, double-counts HFT+DataStream, not idempotent → overlap

## 2. Diagnosis of existing dataset (`nifty_backtest/data/cache`)
- `nbt/cache.py:129` writes `index.parquet` (all dates deduped) + `options/expiry=YYYY-MM-DD/data.parquet` (one folder per expiry assignment). Manifest maps date→expiry. This is **expiry-assignment** logic (`nbt/normalise.py:246`) because vendor zips are per-expiry. See `CLAUDE.md:section6`.
- `nbt/schema.py:28,36` contract: timestamps are **tz-naive IST open times** 09:15-15:29 (375 candles), `OPTION_COLUMNS` = `timestamp, expiry, strike, side, open, high, low, close, volume, open_interest` — `normalise.py:175` currently drops OI to NaN, `volume` kept as float.
- Your future backtest can plug in a **second cache dir** via `nbt/datasets.py` (`dataset.yaml`) without touching `data/cache`. That’s the intended extension point, not mutating the vendor archive path.

## 3. Target storage — two layers, daily-partitioned, no global CSV growth
```
arrow_broker_orderflow/data/
  live/
    ticks/                  # Layer 0 — immutable tick lake, research-grade
      date=2026-08-27/
        token=26000-NIFTY.parquet
        token=XXXXX-NIFTY26000CE.parquet  (×8, zstd, pyarrow)
        _meta.json         # {expiry, strikes, tokens, collected: 09:14-15:35}
    minute/                 # Layer 1 — 1-min resampled, BACKTEST-READY
      index.parquet         # schema.INDEX_COLUMNS  (float64) — deduped per day
      options/
        expiry=2026-09-02/data.parquet  # schema.OPTION_COLUMNS + extras
      features/
        date=2026-08-27/features.parquet  # per-minute CVD, delta, imbalance, OI delta
    cache/                  # Layer 1 mirror in nbt/cache layout (for run_app dropdown)
      index.parquet
      options/expiry=.../data.parquet
      manifest.parquet
      dataset.yaml         # {label: live_ticks, mode: options}
```
- **No more** `data/nifty_ticks_YYYYMMDD.csv` global forever-files. Each day is a folder; rerunning a day **atomically replaces** it (`tmp/*.parquet` → `rename`), so no overlap.
- Tick parquet uses `pyarrow` (≈5-10 MB/day for 9 instruments × ~20k ticks). Minute parquet ~30 KB/day. Yearly ~1.2 GB ticks, ~10 MB minute — fits your 150 GB SSD (`CLAUDE.md:section7`).
- Writer dedupes on `(token, ts_ns)` in-memory before flush (1000 ticks or 5 s). HFT and DataStream feed the **same writer**; duplicates (same token at same ns) are dropped once.

## 4. Instrument selection (4 strikes × 2 sides)
- At **09:14 IST** (before `SESSION_START 09:15` in `nbt/normalise.py:25`), call `instruments.py:12` → `get_option_chain_symbols()` → nearest expiry (smallest date ≥ today), then `get_option_chain("NIFTY", Exchange.INDEX, count=4, expiry=nearest)`.
- Fetch NIFTY open (`client.get_quote(QuoteMode.OHLCV, "NIFTY", Exchange.INDEX)`) or wait for first tick; compute `ATM = round(open / 100) * 100`.
- Select **4 strike prices centered on ATM**: for even 4, take `[ATM-100, ATM, ATM+100, ATM+200]` (bias up — alternatives are symmetric ATM±100/200; make it a flag `STRIKE_POLICY=centered`). Keep only strikes divisible by 100 (`normalise.py:198` enforces this). Result = 8 legs + spot token 26000.
- Freeze mapping for the day in `_meta.json` — never retarget strikes intraday (prevents overlap/mixing expiries).

## 5. Live collection (09:14-15:35 IST)
- **One subscription path**: FO legs → `HFTDataStream` `subscribe_by_segment(EXCH_NSE_FO, tokens, latency=50, mode=full)`; NIFTY spot → `DataStream` `subscribe_market_data(DataMode.FULL, [26000])`. Both callbacks call `storage.on_tick(tick, meta_map)` → unified `storage._normalize_tick()` (paise→rupees, bids/asks). Previous `collector.py:1` subscribed to both streams for all tokens → double-counted ticks; fixed by routing each token to one stream.
- CVD: `delta = aggressor * ltq` where `aggressor = +1 if ltp>mid else -1 if ltp<mid else sign(change_flag)` (`storage.py:_infer_aggressor`). `cvd[token] = cumsum(delta)` per instrument + global `cvd_all = sum(cvd)`. Stored per tick and **resampled per minute** (sum delta, cum at minute close, imbalance mean) in `features/`.
- Volume/OI: HFT `volume`/`oi`, `ltq`; DataStream `volume`. Keep raw `ltq` per tick; minute `volume = sum(ltq)`, `oi = last(oi)`, `oi_delta = last - first`.
- Auto-reconnect (`ArrowStreams` already does `max_reconnect_attempts=300`); on `on_connect` resubscribe. On gap (>5 s no tick) log warning.
- Graceful stop: `SIGINT/SIGTERM` → `disconnect_all()` → flush buffers → write `_meta.json` with counts.

## 6. Post-market resample (15:35, idempotent)
- Read `ticks/date=YYYY-MM-DD/*.parquet` → resample to 1-min IST open-time candles 09:15-15:29.
  - OHLC from `ltp` per minute (first/open, max/high, min/low, last/close)
  - `volume = sum(ltq)`, `open_interest = last(oi)` (or NaN if unavailable — keep compatible with `schema.py:46`)
  - `features`: `cvd_close`, `delta_sum`, `imbalance_mean`, `spread_mean`, `trade_count`
- Validate: `schema.validate_index()` / `validate_options()` → reject days with >5 missing minutes or impossible OHLC.
- Write minute parquet (append to `minute/index.parquet` and `minute/options/...`) with `drop_duplicates(subset=OPTION_KEY)`. If day already exists, **replace** its slice, not append — prevents overlap on rerun.
- Generate `dataset.yaml` so `nifty_backtest` sees a new dataset `live_ticks` in the Streamlit sidebar.

## 7. Daily operation
- `systemd` or cron:
  ```
  14 09 * * 1-5  /home/souvikk/projects/arrow_broker_orderflow/.venv/bin/python collector.py --date today >> logs/collector.log 2>&1
  35 15 * * 1-5  /home/souvikk/projects/arrow_broker_orderflow/.venv/bin/python tools/build_minute.py --date today
  ```
- Manual: `python run_collect.py --dry-run` (simulated ticks → same 2-layer output) and `python run_collect.py --date 2026-08-27 --gap-fill` (REST `candle_data` fallback if WS dropped).

## 8. Backtest compatibility
- Existing engine expects `cache.load_index()` / `load_options(date, strikes, sides)` — new `data/live/cache` satisfies it without changes because it uses identical `INDEX_COLUMNS`/`OPTION_COLUMNS` plus extra columns ignored by the engine.
- Indicator warm-up: minute data starts 09:15, so first 2 days of a new live cache per `CLAUDE.md:section10` are skipped automatically.
- Existing 2024-01-01 onwards vendor cache and live_ticks dataset are **swappable** via sidebar dataset selector — run sweeps side-by-side to measure whether volume/CVD helps (`CLAUDE.md:section8c` shows premium expensive at random minutes; CVD may be the filter).

## 9. Risks & choices needing your sign-off
- [ ] **Strike policy**: `ATM-100, ATM, ATM+100, ATM+200` vs symmetric `ATM-100, ATM, ATM+100, ATM-100`? (affects which 4 levels)
- [ ] **Keep CSV at all?** Proposed: **drop CSV**, use Parquet only (CSV 10× larger, breaks on commas). Keep a `tools/export_csv.py` if you need CSV exports.
- [ ] **CVD definition**: per-instrument standalone vs aggregated across 8 options? Propose both: `cvd_instrument` (per leg) + `cvd_total` (sum of 8 deltas) in features.
- [ ] **Where to write**: `arrow_broker_orderflow/data/live` (isolated) vs `nifty_backtest/data/live_cache` (directly visible to backtester) — propose former + symlink + `dataset.yaml`.

## 10. Implementation steps (after approval)
1. `storage.py` rewrite → `TickWriter` → Parquet writer + dedupe + minute resampler + `build_minute.py`
2. `collector.py` fix → single-stream routing + frozen daily strikes + atomic flush + `_meta.json`
3. `instruments.py` adjust → `count=4` + ATM logic + strike policy flag
4. `tools/verify.py` → schema validation + manifest generation + dataset.yaml
5. Migrate existing CSV dry-runs to new layout and delete `loader.py`/`main.py` stubs

Reply **approve / adjust** (e.g., “keep 5 strikes” or “need CSV too”) and I’ll implement.
