#!/usr/bin/env python3
"""Backfill PAST days from Arrow's historical REST API into the minute layer.

The live collector only captures days while it runs. This tool downloads
historical 1-minute candles (index + NIFTY options, with OI) for a date range
and writes them into the SAME minute/cache layers as tools/build_minute.py -
so backfilled days appear in the GUI day dropdown, Excel export, and the
backtest dataset, side by side with live days.

What backfill CANNOT provide: tick-level orderflow (aggressor/CVD). Candles
have no buyer/seller information, so backfilled days have OHLCV+OI but no
CVD features. Historical TICKS are simply not offered by the API.

Usage:
  python tools/backfill.py --from 2026-08-17 --to 2026-08-28
  python tools/backfill.py --date 2026-08-28
"""
import argparse
import sys
import time as _time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if getattr(sys, "frozen", False):  # PyInstaller exe: use the real .exe folder
    ROOT = Path(sys.executable).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
try:
    from paths import data_root
except ImportError:
    def data_root():
        return ROOT / "data"

IST = pd.Timedelta(hours=5, minutes=30)
BASE = "https://historical-api.arrow.trade/candle"
NUM_STRIKES = 4
STRIDE = 100  # strike step used for selection

# Index registry - tokens/exchanges verified against the historical API.
# NOTE: Arrow uses its OWN index token numbering: BANKNIFTY is 26009 here
# (26012 is a different index on their API) and SENSEX lives on bse/999001.
# Futures contracts are found in the instrument master by F-suffix naming
# (e.g. NIFTY29SEP26F); fut_ex is the candle exchange for them.
INDEXES = {
    "NIFTY":     {"index_token": "26000",  "index_ex": "nse", "opt_ex": "nfo", "chain": "NIFTY",     "stride": 100,
                  "fut_sym": "NIFTY",     "fut_ex": "nfo", "fut_mkt": ("NSE", "FO")},
    "BANKNIFTY": {"index_token": "26009",  "index_ex": "nse", "opt_ex": "nfo", "chain": "BANKNIFTY", "stride": 100,
                  "fut_sym": "BANKNIFTY", "fut_ex": "nfo", "fut_mkt": ("NSE", "FO")},
    "SENSEX":    {"index_token": "999001", "index_ex": "bse", "opt_ex": "bfo", "chain": "SENSEX",    "stride": 100,
                  "fut_sym": "SENSEX",    "fut_ex": "bfo", "fut_mkt": ("BSE", "FO")},
}

_INSTR_CACHE = None  # parsed futures per symbol, refreshed daily


def _futures_contracts(client, index_name: str):
    """{expiry_date: (symbol, token)} for the index's futures, via the instrument
    master (25 MB download, cached on disk for a day)."""
    global _INSTR_CACHE
    cfg = INDEXES[index_name]
    exchange, segment = cfg["fut_mkt"]
    import csv as _csv
    cache_file = data_root() / "_instruments.csv"
    if cache_file.exists() and (_time.time() - cache_file.stat().st_mtime) > 86400:
        cache_file.unlink()
    if _INSTR_CACHE is None or index_name not in _INSTR_CACHE:
        if not cache_file.exists():
            print("    downloading instrument master (~25 MB, cached for a day)...")
            raw = client.get_instruments()
            cache_file.write_bytes(raw)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        rows = _csv.DictReader(open(cache_file, encoding="utf-8"))
        fut_cache = _INSTR_CACHE or {}
        con = {}
        for r in rows:
            if (r["Exchange"], r["Segment"]) != (exchange, segment):
                continue
            sym = r["TradingSymbol"]
            if not sym.upper().startswith(cfg["fut_sym"]) or not sym.upper().endswith("F"):
                continue
            rest = sym[len(cfg["fut_sym"]):-1].upper()  # e.g. '29SEP26'
            if not (len(rest) == 7 and rest[:2].isdigit()):
                continue  # rejects NIFTYNXT50-style names; futures are NIFTY<dd><MMM><yy>F
            try:
                exp = pd.to_datetime(r["Expiry"], format="%d-%b-%Y").date()
            except ValueError:
                continue
            con[exp] = (sym, r["Token"])
        fut_cache[index_name] = con
        _INSTR_CACHE = fut_cache
    return _INSTR_CACHE[index_name]


def _candles(client, exchange: str, token: int, day: date, oi: bool = False):
    """1-min candles for one instrument for one day. Returns list of rows or []."""
    from_iso = f"{day.isoformat()}T09:15:00"
    to_iso = f"{day.isoformat()}T15:30:00"
    url = f"{BASE}/{exchange}/{token}/min"
    params = {"from": from_iso, "to": to_iso}
    if oi:
        params["oi"] = 1
    r = client.req_session.get(url, params=params,
                               headers={"appID": client.app_id, "token": client.get_token()},
                               timeout=30)
    if r.status_code != 200:
        print(f"    {exchange}/{token}: HTTP {r.status_code} {r.text[:80]}")
        return []
    rows = r.json()
    if not isinstance(rows, list):
        print(f"    {exchange}/{token}: unexpected response {str(rows)[:80]}")
        return []
    _time.sleep(0.12)  # be gentle with the rate limit
    out = []
    for c in rows:
        ts = pd.to_datetime(c[0]).tz_convert("Asia/Kolkata").tz_localize(None)
        o, h, l, cl, v = (c[1] / 100, c[2] / 100, c[3] / 100, c[4] / 100, c[5])
        rec = {"timestamp": ts, "open": o, "high": h, "low": l, "close": cl, "volume": float(v)}
        if oi and len(c) >= 7:
            rec["open_interest"] = float(c[6])
        out.append(rec)
    return out


def _nearest_expiry_for(chain_expiries, day: date):
    """First expiry date >= `day` that is still resolvable in the chain list."""
    for e in chain_expiries:
        try:
            d = pd.to_datetime(e, format="%d-%b-%Y").date()
        except ValueError:
            d = pd.to_datetime(e).date()
        if d >= day:
            return d, e
    return None, None


def backfill_day(client, day: date, index_name: str = "NIFTY", want_options: bool = True) -> bool:
    cfg = INDEXES[index_name]
    stride = cfg["stride"]
    print(f"--- {index_name} {day} ---")
    # Index
    idx_rows = _candles(client, cfg["index_ex"], cfg["index_token"], day)
    index_df = pd.DataFrame(idx_rows)
    if index_df.empty:
        print(f"    no index candles (weekend/holiday?) - skipping day")
        return False
    open_px = index_df["open"].iloc[0]
    atm = int(round(open_px / stride) * stride)
    print(f"    index: {len(index_df)} candles, open {open_px:.2f} -> ATM {atm}")

    options_df = pd.DataFrame()
    expiry_date = pd.to_datetime(day).date()
    if want_options:
        try:
            chains = client.get_option_chain_symbols()
            expiries = chains.get("indices", {}).get(f"INDEX:{cfg['chain']}") or []
            expiry_date, expiry_str = _nearest_expiry_for(expiries, day)
            if expiry_str is None:
                print("    options: expiry for this date no longer listed - index only")
            else:
                from pyarrow_client import Exchange
                legs = []
                for chain_ex in ("INDEX", "BSE"):  # NSE indexes resolve via INDEX, SENSEX via BSE
                    try:
                        # count=60: enough strike coverage to always bracket ATM
                        legs = client.get_option_chain(cfg["chain"], getattr(Exchange, chain_ex),
                                                       count=60, expiry=expiry_str)
                        if legs:
                            break
                    except Exception:
                        legs = []
                want = set()
                for s in range(1, NUM_STRIKES + 1):
                    want.add(round((atm - stride * s) / stride) * stride)
                    want.add(round((atm + stride * (s - 1)) / stride) * stride)
                legs = [l for l in legs if round(float(l.get("strikePrice", 0)) / stride) * stride in want]
                # keep NUM_STRIKES strikes nearest ATM, both sides
                by_strike = {}
                for l in legs:
                    by_strike.setdefault(float(l["strikePrice"]), []).append(l)
                chosen = sorted(by_strike, key=lambda s: abs(s - atm))[:NUM_STRIKES]
                rows = []
                for strike in chosen:
                    for leg in by_strike.get(strike, []):
                        side = str(leg.get("optionType", "")).upper()
                        token = int(leg.get("token", 0) or 0)
                        if not token:
                            continue
                        cr = _candles(client, cfg["opt_ex"], token, day, oi=True)
                        for r in cr:
                            rows.append({"expiry": pd.to_datetime(expiry_date),
                                         "strike": int(strike), "side": side, **r})
                        print(f"    {leg.get('symbol')}: {len(cr)} candles")
                options_df = pd.DataFrame(rows)
        except Exception as e:
            print(f"    options failed ({e}) - writing index only")
            options_df = pd.DataFrame()

    if not options_df.empty:
        options_df = options_df.sort_values(["timestamp", "strike", "side"]).reset_index(drop=True)

    # Futures: nearest contract whose expiry is >= the day being backfilled
    futures_df = pd.DataFrame()
    try:
        contracts = _futures_contracts(client, index_name)
        fut_exp, fut = _nearest_expiry_for([e.isoformat() for e in sorted(contracts)], day)
        if fut:
            fut_exp = pd.to_datetime(fut_exp).date()
            fut_sym, fut_token = contracts[fut_exp]
            fr = _candles(client, cfg["fut_ex"], int(fut_token), day, oi=True)
            futures_df = pd.DataFrame([{"expiry": pd.to_datetime(fut_exp), "symbol": fut_sym, **r} for r in fr])
            print(f"    {fut_sym}: {len(fr)} candles")
        else:
            print("    futures: no contract with expiry >= this day")
    except Exception as e:
        print(f"    futures failed ({e}) - continuing without")

    from build_minute import write_minute_layer
    write_minute_layer(day.isoformat(), index_df, options_df, None, expiry_date,
                       index_name=index_name, futures_df=futures_df)
    return True


def run(d1: date, d2: date, index_name: str = "NIFTY", want_options: bool = True):
    if index_name not in INDEXES:
        print(f"ERROR: unknown index {index_name!r}. Supported: {', '.join(INDEXES)}")
        return
    client = None
    for attempt in (1, 2, 3):  # Arrow's REST occasionally 502s - retry login
        try:
            from auth import create_client
            client, _ = create_client()
            break
        except Exception as e:
            print(f"login attempt {attempt}/3 failed: {str(e)[:120]}")
            if attempt < 3:
                _time.sleep(5)
    if client is None:
        print("ERROR: login failed 3 times - Arrow's server may be down. Try again in a few minutes.")
        return
    print(f"Backfilling {index_name} {d1} .. {d2} (login OK)")
    day = d1
    ok = 0
    while day <= d2:
        if backfill_day(client, day, index_name, want_options):
            ok += 1
        day += timedelta(days=1)
    print(f"\nbackfill complete: {ok} day(s) written into the minute layer")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d1", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--to", dest="d2", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--index", default="NIFTY", choices=sorted(INDEXES) + ["SENSEX"],
                    help="index to download (SENSEX unsupported by the historical API)")
    ap.add_argument("--index-only", action="store_true", help="skip options (index candles only)")
    a = ap.parse_args()
    run(date.fromisoformat(a.d1), date.fromisoformat(a.d2), a.index,
        want_options=not a.index_only)
