#!/usr/bin/env python3
"""
Resample Parquet tick lake -> 1-min backtest-ready minute bars.

Input:  data/live/ticks/date=YYYY-MM-DD/token=*.parquet  (writer: storage.py)
Output: data/live/minute/index.parquet + options/expiry=YYYY-MM-DD/data.parquet + features/date=*.parquet
        data/live/cache/ (mirror in nbt/cache layout) + dataset.yaml  -> selectable in nifty_backtest

1-min candles are IST tz-naive open times 09:15-15:29 (375), session-filtered, deduped, schema-validated.
Idempotent: rerunning a date replaces its slice, no overlap.

Usage:
  python tools/build_minute.py --date 2026-08-27
  python tools/build_minute.py --all             # rebuild all dates
  python tools/build_minute.py --date today
"""
import argparse
import json
from datetime import datetime, time, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd

IST = ZoneInfo("Asia/Kolkata")
import sys
ROOT = Path(__file__).resolve().parent.parent
if getattr(sys, "frozen", False):  # PyInstaller exe: use the real .exe folder
    ROOT = Path(sys.executable).resolve().parent
sys.path.insert(0, str(ROOT))
try:
    from paths import data_root
except ImportError:
    def data_root():
        return ROOT / "data"


def _tick_root():
    return data_root() / "live" / "ticks"


def _minute_root():
    return data_root() / "live" / "minute"


def _cache_root():
    return data_root() / "live" / "cache"

SESSION_START = time(9, 15)
SESSION_END = time(15, 29)

def _session_filter(df: pd.DataFrame) -> pd.DataFrame:
    t = pd.to_datetime(df["ts"]).dt.time
    return df[(t >= SESSION_START) & (t <= SESSION_END)].copy()

def _resample_ohlc(g: pd.DataFrame) -> dict:
    g = g.sort_values("ts")
    return {
        "open": g["ltp"].iloc[0],
        "high": g["ltp"].max(),
        "low": g["ltp"].min(),
        "close": g["ltp"].iloc[-1],
        "volume": int(g["ltq"].sum()) if "ltq" in g else 0,
        "open_interest": int(g["oi"].iloc[-1]) if "oi" in g else 0,
        "cvd_close": int(g["cvd"].iloc[-1]) if "cvd" in g else 0,
        "delta_sum": int(g["delta"].sum()) if "delta" in g else 0,
        "imbalance_mean": float(g["imbalance"].mean()) if "imbalance" in g else 0.0,
        "spread_mean": float(g["spread"].mean()) if "spread" in g else 0.0,
        "trade_count": int(len(g)),
    }

def build_one(date_str: str):
    day_dir = _tick_root() / f"date={date_str}"
    if not day_dir.exists():
        print(f"no ticks for {date_str}: {day_dir} missing")
        return False
    parquet_files = list(day_dir.glob("token=*.parquet"))
    if not parquet_files:
        print(f"no token parquet for {date_str}")
        return False

    # Load all ticks for the day
    dfs = []
    for p in parquet_files:
        try:
            df = pd.read_parquet(p)
            dfs.append(df)
        except Exception as e:
            print(f"skip {p.name}: {e}")
    if not dfs:
        return False
    ticks = pd.concat(dfs, ignore_index=True)
    ticks["ts"] = pd.to_datetime(ticks["ts"])
    # Ensure IST naive (storage already writes IST naive)
    ticks = _session_filter(ticks)
    if ticks.empty:
        print(f"{date_str}: no ticks in session window")
        return False
    ticks = ticks.drop_duplicates(subset=["token", "ts", "ltp", "ltq"]).sort_values("ts")

    # Bucket to 1-min open time
    ticks["minute"] = ticks["ts"].dt.floor("min")

    # Normalize side column (storage uses option_type)
    if "side" not in ticks.columns and "option_type" in ticks.columns:
        ticks["side"] = ticks["option_type"]
    # Split spot vs options by side/strike
    is_spot = ticks["side"].isin(["SPOT", "", None])
    spot_ticks = ticks[is_spot]
    opt_ticks = ticks[~is_spot]
    # live sessions may store strike=0 (strikePrice arrives as "24000.00") - recover from symbol
    if not opt_ticks.empty and "symbol" in opt_ticks.columns:
        bad = opt_ticks["strike"].fillna(0) == 0
        if bad.any():
            rec = opt_ticks.loc[bad, "symbol"].str.extract(r"[CP](\d+)$", expand=False)
            opt_ticks.loc[bad, "strike"] = pd.to_numeric(rec, errors="coerce").fillna(0)
        opt_ticks["strike"] = pd.to_numeric(opt_ticks["strike"], errors="coerce").fillna(0).astype("int64")

    # --- Index minute ---
    index_rows = []
    if not spot_ticks.empty:
        for minute, g in spot_ticks.groupby("minute"):
            r = _resample_ohlc(g)
            index_rows.append({"timestamp": minute, **{k: r[k] for k in ["open","high","low","close"]}})
    index_df = pd.DataFrame(index_rows)
    if not index_df.empty:
        # Use schema contract: float64, sorted, deduped
        index_df["timestamp"] = pd.to_datetime(index_df["timestamp"])
        index_df = index_df.sort_values("timestamp").drop_duplicates("timestamp")
        # Validate basic
        try:
            # lazy import to avoid hard dep on nifty_backtest
            import sys; sys.path.insert(0, str(ROOT.parent / "nifty_backtest"))
            from nbt import schema as nbt_schema
            nbt_schema.validate_index(index_df[["timestamp","open","high","low","close"]].astype({"open":float,"high":float,"low":float,"close":float}))
            print(f"index {date_str}: {len(index_df)} 1-min candles OK")
        except Exception as e:
            print(f"index validate warn: {e}")
    else:
        print(f"{date_str}: no spot ticks -> empty index")

    # --- Options minute ---
    opt_rows = []
    features_rows = []
    expiry_val = None
    if not opt_ticks.empty:
        # expiry from meta or first row
        try:
            meta = json.loads((day_dir / "_meta.json").read_text())
            expiry_val = meta.get("session_meta", {}).get("expiry") or opt_ticks["expiry"].iloc[0]
        except:
            expiry_val = opt_ticks["expiry"].iloc[0]
        # expiry as date for partitioning
        try:
            expiry_date = pd.to_datetime(expiry_val, format="%d-%b-%Y").date() if "-" in str(expiry_val) else pd.to_datetime(expiry_val).date()
        except:
            expiry_date = pd.to_datetime(date_str).date()
        for (minute, strike, side), g in opt_ticks.groupby(["minute", "strike", "side"]):
            # side normalized to CE/PE
            side = "CE" if str(side).upper().startswith("C") else "PE" if str(side).upper().startswith("P") else str(side)
            r = _resample_ohlc(g)
            opt_rows.append({
                "timestamp": minute, "expiry": pd.to_datetime(expiry_date),
                "strike": int(strike), "side": side,
                "open": float(r["open"]), "high": float(r["high"]), "low": float(r["low"]), "close": float(r["close"]),
                "volume": float(r["volume"]), "open_interest": float(r["open_interest"]),
            })
            features_rows.append({
                "timestamp": minute, "strike": int(strike), "side": side,
                "cvd_close": r["cvd_close"], "delta_sum": r["delta_sum"],
                "imbalance_mean": r["imbalance_mean"], "spread_mean": r["spread_mean"],
                "volume": r["volume"], "oi": r["open_interest"], "trade_count": r["trade_count"],
                "expiry": pd.to_datetime(expiry_date),
            })
    else:
        expiry_date = pd.to_datetime(date_str).date()
        print(f"{date_str}: no option ticks")

    options_df = pd.DataFrame(opt_rows)
    features_df = pd.DataFrame(features_rows)

    # Validate options
    if not options_df.empty:
        options_df = options_df.sort_values(["timestamp","expiry","strike","side"]).drop_duplicates(["timestamp","expiry","strike","side"])
        try:
            import sys; sys.path.insert(0, str(ROOT.parent / "nifty_backtest"))
            from nbt import schema as nbt_schema
            nbt_schema.validate_options(options_df)
            print(f"options {date_str}: {len(options_df)} rows across {options_df['strike'].nunique()} strikes OK")
        except Exception as e:
            print(f"options validate warn: {e}")

    write_minute_layer(date_str, index_df, options_df, features_df, expiry_date)


def write_minute_layer(date_str: str, index_df, options_df, features_df=None, expiry_date=None,
                       index_name: str = "NIFTY", futures_df=None):
    """Shared minute-layer writer: 1-min bars + features + backtest cache mirror.
    Idempotent - replaces this date's slice everywhere, never appends duplicates.
    Used by tools/build_minute.py (live ticks) and tools/backfill.py (REST candles).
    NIFTY keeps the legacy top-level paths (backtest dataset.yaml points there);
    other indexes get their own subfolder: minute/<INDEX>/, cache/<INDEX>/.
    futures_df (optional) lands in futures/expiry=<date>/data.parquet."""
    mroot = _minute_root() if index_name == "NIFTY" else _minute_root() / index_name
    croot = _cache_root() if index_name == "NIFTY" else _cache_root() / index_name
    mroot.mkdir(parents=True, exist_ok=True)

    def upsert_index(new_df: pd.DataFrame):
        path = mroot / "index.parquet"
        if new_df.empty:
            return
        if path.exists():
            old = pd.read_parquet(path)
            old = old[old["timestamp"].dt.date != pd.to_datetime(date_str).date()]
            combined = pd.concat([old, new_df], ignore_index=True).sort_values("timestamp").drop_duplicates("timestamp")
        else:
            combined = new_df
        combined.to_parquet(path, index=False)
    upsert_index(index_df if not index_df.empty else pd.DataFrame())

    if features_df is not None and not features_df.empty:
        feat_dir = mroot / "features"
        feat_dir.mkdir(parents=True, exist_ok=True)
        features_df["timestamp"] = pd.to_datetime(features_df["timestamp"])
        features_df.to_parquet(feat_dir / f"date={date_str}.parquet", index=False)
        print(f"features {date_str}: {len(features_df)} minute-features")

    if futures_df is not None and not futures_df.empty:
        fut_exp = pd.to_datetime(futures_df["expiry"].iloc[0]).date()
        fut_dir = mroot / "futures" / f"expiry={fut_exp.isoformat()}"
        fut_dir.mkdir(parents=True, exist_ok=True)
        fout = fut_dir / "data.parquet"
        if fout.exists():
            old = pd.read_parquet(fout)
            old = old[old["timestamp"].dt.date != pd.to_datetime(date_str).date()]
            combined = pd.concat([old, futures_df], ignore_index=True)
        else:
            combined = futures_df
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        combined.to_parquet(fout, index=False)
        print(f"futures {date_str}: {len(combined)} rows ({futures_df['symbol'].iloc[0] if 'symbol' in futures_df else 'FUT'})")

    if not options_df.empty:
        minute_opt_dir = mroot / "options" / f"expiry={expiry_date.isoformat()}"
        minute_opt_dir.mkdir(parents=True, exist_ok=True)
        out = minute_opt_dir / "data.parquet"
        if out.exists():
            old = pd.read_parquet(out)
            old = old[old["timestamp"].dt.date != pd.to_datetime(date_str).date()]
            combined = pd.concat([old, options_df], ignore_index=True)
        else:
            combined = options_df
        combined = combined.sort_values(["timestamp","strike","side"]).reset_index(drop=True)
        combined.to_parquet(out, index=False)

    # --- Mirror to nbt/cache layout for backtest dropdown (NIFTY only) ---
    if index_name == "NIFTY":
        _cache_root().mkdir(parents=True, exist_ok=True)
        idx_path = _cache_root() / "index.parquet"
        if not index_df.empty:
            if idx_path.exists():
                old = pd.read_parquet(idx_path)
                old = old[old["timestamp"].dt.date != pd.to_datetime(date_str).date()]
                combined = pd.concat([old, index_df], ignore_index=True)
            else:
                combined = index_df
            combined = combined.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
            combined.to_parquet(idx_path, index=False)
        if not options_df.empty:
            opt_cache_dir = _cache_root() / "options" / f"expiry={expiry_date.isoformat()}"
            opt_cache_dir.mkdir(parents=True, exist_ok=True)
            out = opt_cache_dir / "data.parquet"
            if out.exists():
                old = pd.read_parquet(out)
                old = old[old["timestamp"].dt.date != pd.to_datetime(date_str).date()]
                combined = pd.concat([old, options_df], ignore_index=True)
            else:
                combined = options_df
            combined = combined.sort_values(["timestamp","expiry","strike","side"]).reset_index(drop=True)
            combined.to_parquet(out, index=False)
            _write_manifest()
            _write_dataset_yaml()
    print(f"built {index_name} {date_str}: index {len(index_df)} | options {len(options_df)} | features {len(features_df) if features_df is not None else 0} -> {mroot}")

def _write_manifest():
    # date -> expiry -> rows
    import pandas as pd
    rows = []
    for p in (_cache_root() / "options").glob("expiry=*/data.parquet"):
        try:
            df = pd.read_parquet(p)
            expiry = p.parent.name.split("=")[1]
            for d, g in df.groupby(df["timestamp"].dt.date):
                rows.append({"date": pd.to_datetime(d).date(), "expiry": pd.to_datetime(expiry).date(), "expiry_kind": "weekly", "option_rows": len(g)})
        except: pass
    if rows:
        pd.DataFrame(rows).sort_values("date").to_parquet(_cache_root() / "manifest.parquet", index=False)

def _write_dataset_yaml():
    (_cache_root() / "dataset.yaml").write_text(
        "name: live_ticks\n"
        "label: live_ticks - NIFTY 4-strike CVD (tick->1min)\n"
        "mode: option\n"
        "instrument: NIFTY\n"
        f"ingested_at: '{datetime.now().isoformat()}'\n"
        "description: Live 4-strike (8 legs+spot) tick lake -> 1-min OHLCV+OI+CVD, deduped, session-filtered\n"
    )

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD or 'today'")
    ap.add_argument("--all", action="store_true", help="rebuild all dates in ticks/")
    args = ap.parse_args()
    if args.all:
        for d in sorted(_tick_root().glob("date=*")):
            build_one(d.name.split("=")[1])
    elif args.date:
        ds = datetime.now(IST).date().isoformat() if args.date == "today" else args.date
        build_one(ds)
    else:
        ap.print_help()
