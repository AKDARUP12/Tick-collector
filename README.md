# Arrow Broker Orderflow — NIFTY Live Collector

Live tick-by-tick collection for **NIFTY spot + 4 strikes (8 option legs, nearest weekly expiry)**. Stores raw ticks, derived orderflow and CVD into a daily-partitioned **Parquet tick lake**, then resamples to backtest-ready 1-minute bars.

Built on the [`pyarrow-client`](https://docs.arrow.trade/) SDK.

## What you get

- **NIFTY spot** (`INDEX:NIFTY`, token 26000) + **4 strike levels** (ATM-100 / ATM / ATM+100 / ATM+200 → 8 legs CE+PE) for the nearest weekly expiry
- **HFT DataStream** (`wss://socket.arrow.trade`, zstd) for the options + **DataStream** (`wss://ds.arrow.trade`) for the index — single stream per instrument, no double counting, auto-reconnect
- Storage (`data/live/ticks/date=YYYY-MM-DD/`):
  - `token=<token>-<SYMBOL>.part=NNNNNN.parquet` — append-only flush segments (1,000 ticks / 5 s), atomic, crash-safe
  - `token=<token>-<SYMBOL>.parquet` — merged per-instrument file (after `tools/compact_day.py`)
  - `_meta.json` — session meta, per-token counts, final CVD
- Per-tick columns: normalized OHLC/volume/OI, best bid/ask, spread, mid, imbalance, aggressor, delta, **CVD** (per token + total), plus `ts` (exchange trade time) and `ts_recv` (receive time)
- Post-market layers:
  - `data/live/minute/` — 1-min OHLCV + per-minute CVD/imbalance features
  - `data/live/cache/` — mirror in `nbt/cache` layout + `dataset.yaml`, so `nifty_backtest` can select it as a dataset unchanged

## Correctness guarantees (storage.py)

- **Units**: the SDK documents all prices in paise on both feeds — converted unconditionally, no `>10000` guessing. Implausible converted values print a loud one-time warning.
- **Timestamps**: `ts` = exchange trade time (`ltt`) when present, else receive time; `ts_recv` always kept separately.
- **Dedup**: identity is (token, ts, ltp, ltq) with a bounded in-memory set — reconnect retransmits dedupe exactly.
- **CVD survives restarts**: `init_session` restores running CVD from the day's files.
- **No data loss on failed writes**: rows only leave the buffer after the write succeeds.
- **Timer flush**: saves every 5 s even when the feed is quiet.

## Quick start

```bash
# 1. setup
bash setup.sh
cp .env.example .env          # fill in ALL credentials from app.arrow.trade → Developer Apps

# 2. verify login (needs your public IP whitelisted — SEBI requirement)
source .venv/bin/activate
python auth.py                # should print "Successfully logged in"

# 3. rehearsal without market/credentials (simulated morning, self-checked)
python run_collect.py --dry-run

# 4. live (market hours 09:15–15:30 IST)
python run_collect.py         # Ctrl-C to stop

# 5. post-close: merge segments, build 1-min bars
python tools/compact_day.py --date today
python tools/build_minute.py --date today
```

Daily automation (server): cron/systemd — `run_collect.py` at 09:14, `compact_day.py` + `build_minute.py` at 15:35, weekdays.

## Inspecting the data

- `python tools/parquet_viewer.py` — local web UI to browse parquet: pick any folder
  from the sidebar (address box, up-level, or native folder dialog), view a file or a
  whole folder as a dataset, with schema + stats, CE/PE quick filter, search, sort,
  SQL filter, pagination, CSV export, and a live auto-refresh mode (`pip install duckdb` first)
- `python tools/view.py` — list instruments (segments merged automatically)
- `python tools/view.py 26000` — preview rows; add `--tail`, `--rows N`, `--cols ts,ltp,cvd`
- `python tools/view.py --dupes` — duplicate + volume-drop health check
- `python tools/view.py <date> --xlsx day.xlsx` — Excel export (one sheet per instrument)
- Prefer clicking? Run `NIFTY_Setup.exe` — installs **NIFTY Studio**, a windowed app
  (rehearse / start-stop live / after-close jobs / health check / Excel export) with
  Desktop + Start Menu shortcuts and a proper uninstaller. Console fans: `NIFTY_Viewer.exe`
  and `NIFTY_Collector.exe` (menu-driven, keep next to `data/`), plus
  `NIFTY_ParquetViewer.exe` — the web parquet viewer as a double-clickable exe
  (keep next to `data/`; opens the UI in your browser).

## Credentials

All from https://app.arrow.trade → Developer Apps → **kept only in `.env` (gitignored)**:

| key | description |
|-----|-------------|
| `ARROW_APP_ID` | app id |
| `ARROW_APP_SECRET` | 64-hex-char secret |
| `ARROW_TOTP_SECRET` | TOTP seed (base32) |
| `ARROW_USER_ID` | your UCC |
| `ARROW_PASSWORD` | your Arrow login password |

If a secret was ever exposed, rotate it at app.arrow.trade. Login fails with `invalid checksum`/400 if the password is wrong or your public IP is not whitelisted.

## Layout

```
arrow_broker_orderflow/
  .venv/               # venv (setup.sh)
  data/live/ticks/     # daily Parquet tick lake — gitignored
  data/live/minute/    # 1-min bars + features
  data/live/cache/     # nbt-compatible mirror + dataset.yaml
  logs/                # collector.log
  auth.py              # login (TOTP), credentials from .env only
  auth_sdk.py          # compat shim delegating to auth.py
  instruments.py       # nearest expiry + ATM±strikes resolution
  storage.py           # tick lake writer (segments, dedup, CVD, timer)
  collector.py         # live: HFT+DataStream → storage
  run_collect.py       # entry: live or --dry-run (self-checked simulation)
  tools/view.py        # inspect/export/health-check (also NIFTY_Viewer.exe)
  tools/compact_day.py # merge a day's segments into one file per token
  tools/build_minute.py# ticks → 1-min bars + features + backtest cache
  requirements.txt
```

`loader.py` / `main.py` are scaffold stubs — use `run_collect.py` / `collector.py`.

## Troubleshooting

- `invalid checksum` / `400` — wrong password/secret, or IP not whitelisted.
- No ticks after "Subscribing HFT…" — market closed (use `--dry-run`), or tokens unresolved (spot flows only on DataStream — both are subscribed).
- `WARNING token=… looks impossible after paise→rupees conversion` — feed units differ from the SDK docs for that instrument; investigate before using the data.
- Health check reports duplicates/volume drops — usually ticks saved around a reconnect; run `tools/compact_day.py` and re-check.
