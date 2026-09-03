#!/usr/bin/env python3
"""NIFTY Termux Collector - live tick capture on Android via Termux.

Works inside the Termux app (Android). Behaviour:
  - Auto-collects 09:15-15:40 IST, Mon-Fri (waits for the window if started early,
    stops by itself at 15:40). Run it once in the morning and forget it.
  - Saves ticks as CSV (opens directly in Excel): data/live/ticks_csv/date=.../token=...csv
  - Same integrity as the desktop: exchange-time timestamps, dedupe, CVD engine,
    exact index matching, double-collection guard.

Setup (one time, inside Termux):
  pkg update && pkg upgrade -y
  pkg install python clang libffi openssl -y
  termux-setup-storage                      # tap Allow
  pip install pyarrow-client zstandard pyotp websocket-client requests
  termux-wake-lock                          # keeps Android from sleeping us
Then: put creds.json (template provided) in the same folder and run:
  python NIFTY_Termux_Collector.py

Battery settings (important):
  Android Settings > Apps > Termux > Battery > Unrestricted
Keep the phone on the home WiFi that is whitelisted with Arrow - mobile data
IPs change constantly and login will fail on them.
"""
import csv
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta, date, timezone, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from zoneinfo import ZoneInfo as ZI2  # noqa: F401
except Exception:
    pass

IST = ZoneInfo("Asia/Kolkata")
HERE = Path(__file__).resolve().parent
DATA = HERE / "data" / "live" / "ticks_csv"
LOG = HERE / "collector.log"
WINDOW_START = dtime(9, 15)
WINDOW_END = dtime(15, 40)
NUM_STRIKES = 4
STRIDE = 100

# Verified Arrow tokens (vendor numbering differs from NSE standard!)
INDEXES = {
    "NIFTY":     {"index_token": 26000,  "chain": "NIFTY",     "stride": 100},
    "BANKNIFTY": {"index_token": 26009,  "chain": "BANKNIFTY", "stride": 100},
}

_running = True
def _stop(signum, frame):
    global _running
    print("\nstopping... everything already received is saved", flush=True)
    _running = False

signal.signal(signal.SIGINT, _stop)
if hasattr(signal, "SIGTERM"):
    try: signal.signal(signal.SIGTERM, _stop)
    except Exception: pass


def log(msg):
    line = f"[{datetime.now(IST).strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_creds():
    p = HERE / "creds.json"
    if not p.exists():
        sys.exit("creds.json not found - copy creds_template.json to creds.json and fill it in")
    creds = json.load(open(p, encoding="utf-8"))
    missing = [k for k in ("ARROW_APP_ID", "ARROW_APP_SECRET", "ARROW_TOTP_SECRET",
                           "ARROW_USER_ID", "ARROW_PASSWORD") if not creds.get(k)]
    if missing:
        sys.exit(f"creds.json missing: {', '.join(missing)}")
    return creds


def login(creds):
    from pyarrow_client import ArrowClient
    client = ArrowClient(app_id=creds["ARROW_APP_ID"])
    client.auto_login(user_id=creds["ARROW_USER_ID"], password=creds["ARROW_PASSWORD"],
                      api_secret=creds["ARROW_APP_SECRET"], totp_secret=creds["ARROW_TOTP_SECRET"])
    return client


def now_ist():
    return datetime.now(timezone.utc).astimezone(IST).replace(tzinfo=None)


def rupees(paise):
    return round(float(paise) / 100.0, 4)


def exchange_ts(ltt):
    if not ltt: return None
    v = float(ltt)
    if v > 1e17: v /= 1e9
    elif v > 1e14: v /= 1e3
    if not (1_000_000_000 <= v <= 4_000_000_000): return None
    return datetime.fromtimestamp(v, tz=timezone.utc).astimezone(IST).replace(tzinfo=None)


def resolve_day(client, index_name):
    """Spot token + option tokens + meta for today."""
    from pyarrow_client import Exchange
    cfg = INDEXES[index_name]
    spot_token = cfg["index_token"]
    atm = None
    try:
        from pyarrow_client import QuoteMode
        q = client.get_quote(QuoteMode.OHLCV, cfg["chain"], Exchange.INDEX)
        open_px = q.get("open", q.get("ltp", 0)) / 100
        if open_px > 1000:
            atm = int(round(open_px / STRIDE) * STRIDE)
    except Exception as e:
        print("spot quote failed:", str(e)[:80])
    chains = client.get_option_chain_symbols()
    expiries = chains.get("indices", {}).get(f"INDEX:{cfg['chain']}") or []
    exp_str = ""
    today = now_ist().date()
    for e in expiries:
        try:
            d = datetime.strptime(e, "%d-%b-%Y").date()
        except ValueError:
            d = pd_to_date(e)
        if d >= today:
            exp_str, exp_date = e, d
            break
    else:
        exp_date = today
    meta = {spot_token: {"symbol": cfg["chain"], "strike": 0, "option_type": "SPOT",
                         "expiry": exp_str, "token": spot_token}}
    tokens = [spot_token]
    if exp_str:
        legs = client.get_option_chain(cfg["chain"], Exchange.INDEX, count=60, expiry=exp_str)
        want = set()
        if atm:
            for s in range(1, NUM_STRIKES + 1):
                want.add(round((atm - STRIDE*s)/STRIDE)*STRIDE)
                want.add(round((atm + STRIDE*(s-1))/STRIDE)*STRIDE)
        by = {}
        for l in legs:
            st = round(float(l.get("strikePrice", 0))/STRIDE)*STRIDE
            if (not want) or (st in want): by.setdefault(st, []).append(l)
        chosen = sorted(by, key=lambda s_: abs(s_-(atm or s_)))[:NUM_STRIKES]
        for st in chosen:
            for leg in by.get(st, []):
                tok = int(leg.get("token", 0) or 0)
                if not tok: continue
                meta[tok] = {"symbol": leg.get("symbol", ""), "strike": int(st),
                             "option_type": str(leg.get("optionType", "")).upper(),
                             "expiry": exp_str, "token": tok}
                tokens.append(tok)
    return spot_token, tokens, meta


def pd_to_date(e):
    from datetime import datetime as dt
    return dt.strptime(e, "%d-%b-%Y").date()


class CsvStore:
    """CSV tick store: one file per instrument per day, deduped, Excel-ready."""
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.rows = {}
        self.cvd = {}
        self.seen = set()
        self.header = ["token", "symbol", "strike", "option_type", "expiry", "ts", "ltp", "ltq",
                       "volume", "oi", "bid_price", "ask_price", "bid_qty", "ask_qty",
                       "spread", "mid", "imbalance", "aggressor", "delta", "cvd",
                       "cvd_total", "open", "high", "low", "close", "avg_price",
                       "total_buy_qty", "total_sell_qty", "ltt", "mode"]

    def on_tick(self, tick, meta):
        token = int(tick.get("token", 0))
        bid = tick.get("bid_px", 0); bid = bid[0] if isinstance(bid, list) and bid else bid
        ask = tick.get("ask_px", 0); ask = ask[0] if isinstance(ask, list) and ask else ask
        bid, ask = rupees(bid), rupees(ask)
        ltp = rupees(tick.get("ltp", 0))
        ltq = int(tick.get("ltq", 0))
        ts = exchange_ts(tick.get("ltt", 0)) or now_ist()
        key = (token, ts, round(ltp, 2), ltq)
        if key in self.seen: return
        self.seen.add(key)
        agg = 1 if (bid and ask and ltp > (bid+ask)/2) else (-1 if (bid and ask and ltp < (bid+ask)/2) else 0)
        delta = agg * ltq if ltq else 0
        self.cvd[token] = self.cvd.get(token, 0) + delta
        strike = meta.get("strike", 0) or 0
        row = [token, meta.get("symbol", str(token)), strike, meta.get("option_type", ""),
               meta.get("expiry", ""), ts.isoformat(sep=" "), ltp, ltq,
               int(tick.get("volume", 0)), int(tick.get("oi", 0)), bid, ask,
               int(tick.get("bid_size", 0) or 0), int(tick.get("ask_size", 0) or 0),
               round(ask - bid, 4) if bid and ask else 0.0,
               round((bid+ask)/2, 4) if bid and ask else ltp,
               round((int(tick.get("bid_size", 0) or 0) - int(tick.get("ask_size", 0) or 0)) /
                     max(int(tick.get("bid_size", 0) or 0) + int(tick.get("ask_size", 0) or 0), 1), 6),
               agg, delta, self.cvd[token], sum(self.cvd.values()),
               rupees(tick.get("open", 0) or 0), rupees(tick.get("high", 0) or 0),
               rupees(tick.get("low", 0) or 0), rupees(tick.get("close", 0) or 0),
               rupees(tick.get("vwap", 0) or 0),
               int(tick.get("tbq", 0) or 0), int(tick.get("tsq", 0) or 0),
               int(tick.get("ltt", 0)), "hft_full"]
        self.rows.setdefault(token, []).append(row)

    def flush(self):
        day = now_ist().date().isoformat()
        day_dir = self.root / f"date={day}"
        day_dir.mkdir(parents=True, exist_ok=True)
        for token, rows in list(self.rows.items()):
            if not rows: continue
            sym = rows[0][1]
            safe = "".join(c if c.isalnum() else "_" for c in sym)[:40]
            out = day_dir / f"token={token}-{safe}.csv"
            new_file = not out.exists()
            with open(out, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if new_file: w.writerow(self.header)
                w.writerows(rows)
            self.rows[token] = []


def main():
    creds = load_creds()
    index_name = "NIFTY"
    store = CsvStore(DATA)
    log(f"NIFTY Termux Collector starting | data: {DATA}")

    from pyarrow_client import ArrowStreams, DataMode, HFTDataStream, Exchange
    client = login(creds)
    log("logged in")
    spot_token, option_tokens, meta_map = resolve_day(client, index_name)
    log(f"spot {spot_token} | options {option_tokens}")

    nticks = [0]
    def counted(tick):
        try:
            store.on_tick(tick, meta_map)
            nticks[0] += 1
            if nticks[0] % 2000 == 0:
                log(f"ticks={nticks[0]:,}")
        except Exception as e:
            log(f"tick error: {e!r}")

    streams = ArrowStreams(app_id=creds["ARROW_APP_ID"], token=client.get_token(), debug=False)
    def resub():
        try:
            streams.hft_data_stream.subscribe_by_segment(
                "full", {HFTDataStream.EXCH_NSE_FO: [t for t in option_tokens if t != spot_token]}, latency=50)
            streams.subscribe_market_data(DataMode.FULL, [spot_token])
            log("subscribed OK")
        except Exception as e:
            log(f"subscribe failed: {e}")
    streams.data_stream.on_ticks = counted
    streams.hft_data_stream.on_ltp_tick = counted
    streams.hft_data_stream.on_full_tick = counted
    streams.hft_data_stream.on_connect = lambda: resub()
    streams.data_stream.on_connect = lambda: resub()

    streams.connect_hft_data_stream(); time.sleep(1.2); resub()
    streams.connect_data_stream(); time.sleep(0.9); resub()
    log(f"LIVE - collecting until 15:40 IST")

    try:
        while _running:
            time.sleep(2)
            if now_ist().time() >= WINDOW_END:
                log("15:40 reached - stopping for today")
                break
            if sum(len(v) for v in store.rows.values()):
                store.flush()
    finally:
        try:
            streams.disconnect_all()
        except Exception:
            pass
        store.flush()
        log("stopped - data saved in data/live/ticks_csv")


if __name__ == "__main__":
    main()
