"""Live tick ingestion: NIFTY spot + options -> Parquet tick lake."""
import os
import time
import signal
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from pyarrow_client import ArrowStreams, DataMode, Exchange, HFTDataStream, QuoteMode
from auth import create_client
from instruments import get_nifty_spot_token, get_nearest_nifty_options
from storage import init_session, on_tick, start as storage_start, close as storage_close, flush as storage_flush

try:
    from paths import data_root
except ImportError:
    from storage import data_root

load_dotenv()
IST = ZoneInfo("Asia/Kolkata")

NUM_STRIKES = int(os.getenv("ARROW_STRIKES", "4"))
DATA_MODE = os.getenv("ARROW_MODE", "full")
USE_HFT = os.getenv("ARROW_USE_HFT", "1") == "1"

_running = True


def _handle_sig(signum, frame):
    global _running
    print("\nTermination signal received — stopping collector...")
    _running = False


if threading.current_thread() is threading.main_thread():
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)


def main(stop_event: threading.Event | None = None, interactive: bool = False, on_heartbeat=None):
    global _running
    _running = True
    print(f"=== Arrow Live Collector | Strikes: {NUM_STRIKES} | HFT: {USE_HFT} | Mode: {DATA_MODE} ===")

    # 1. Login
    client, resp = create_client()
    token = client.get_token()
    app_id = os.getenv("ARROW_APP_ID", "")
    print(f"Authenticated: {resp.get('name', resp.get('userID', ''))}")

    # 2. Resolve Spot & ATM
    spot_name, spot_token = get_nifty_spot_token(client)
    atm = None
    try:
        q = client.get_quote(QuoteMode.OHLCV, "NIFTY", Exchange.INDEX)
        open_px = q.get("open", q.get("ltp", 0)) / 100.0
        if open_px > 1000:
            atm = int(round(open_px / 100.0) * 100)
            print(f"Spot open {open_px:.1f} => inferred ATM: {atm}")
    except Exception as e:
        print(f"Could not fetch spot quote: {e}")

    options = get_nearest_nifty_options(client, num_strikes=NUM_STRIKES, atm=atm, gap_bias="auto")
    expiry = options[0].get("expiry", "") if options else ""
    print(f"Option Expiry: {expiry}")

    meta_map = {spot_token: {"symbol": "NIFTY", "strike": "", "option_type": "SPOT", "expiry": ""}}
    option_tokens = []

    for leg in options:
        tok = int(leg.get("token", 0) or 0)
        sym = leg.get("symbol", "")
        strike = leg.get("strikePrice", "")
        otype = leg.get("optionType", "")
        if tok:
            meta_map[tok] = {"symbol": sym, "strike": strike, "option_type": otype, "expiry": expiry, "token": tok}
            option_tokens.append(tok)

    sess_date = datetime.now(timezone.utc).astimezone(IST).date().isoformat()
    init_session(sess_date, meta={
        "num_strikes": NUM_STRIKES,
        "atm": atm,
        "expiry": expiry,
        "spot_token": spot_token,
        "option_tokens": option_tokens,
        "symbols": {tok: meta_map[tok]["symbol"] for tok in option_tokens}
    })
    storage_start()

    # 3. Connection and Callbacks
    streams = ArrowStreams(appID=app_id, token=token, debug=False)

    def on_data_tick(tick):
        try:
            on_tick(tick, meta_map)
            on_data_tick.count = getattr(on_data_tick, "count", 0) + 1
            if on_data_tick.count % 500 == 0:
                ltp = getattr(tick, 'ltp', tick.get('ltp', 0) if isinstance(tick, dict) else 0)
                last_tok = tick.token if hasattr(tick, 'token') else (tick.get('token',0) if isinstance(tick, dict) else 0)
                print(f"[{time.strftime('%H:%M:%S')}] ticks={on_data_tick.count} last token={last_tok} ltp={ltp}")
        except Exception as e:
            print(f"Tick parse error: {e}")

    streams.data_stream.on_ticks = on_data_tick
    streams.hft_data_stream.on_ltp_tick = on_data_tick
    streams.hft_data_stream.on_full_tick = on_data_tick

    # Telegram /status + 15m zip daemon
    _tg_stop = threading.Event()
    _zip_stop = threading.Event()
    _sess_start_ts = time.monotonic()
    try:
        _tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        _tg_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if _tg_token:
            from tools.telegram_status import start_polling
            import storage as _storage
            def _status_fn():
                return {
                    "running": True,
                    "session_date": sess_date,
                    "ticks": getattr(on_data_tick, 'count', 0),
                    "cvd": dict(getattr(_storage, '_cvd', {})),
                    "files": len(list((data_root() / "live" / "ticks" / f"date={sess_date}").glob("token=*.parquet"))),
                    "uptime": f"{int((time.monotonic()-_sess_start_ts)//3600)}h{int((time.monotonic()-_sess_start_ts)%3600//60)}m",
                }
            start_polling(_tg_stop, _tg_token, _tg_chat, _status_fn)
            def _zip_loop():
                import pathlib, zipfile
                last_sent=set()
                while not _zip_stop.is_set() and _running and not (stop_event and stop_event.is_set()):
                    now = datetime.now(timezone.utc).astimezone(IST)
                    mins = now.minute % 15
                    secs_to_next = (15 - mins)*60 - now.second - now.microsecond/1e6
                    if secs_to_next <=0: secs_to_next+=900
                    waited=0
                    while waited<900 and not _zip_stop.is_set() and _running and not (stop_event and stop_event.is_set()):
                        time.sleep(min(1,900-waited))
                        waited+=1
                        if waited>=secs_to_next: break
                    if _zip_stop.is_set() or not _running or (stop_event and stop_event.is_set()): break
                    try:
                        day_d = data_root() / "live" / "ticks" / f"date={sess_date}"
                        if not day_d.exists(): continue
                        files = sorted(day_d.glob("token=*.parquet"))
                        new_files=[p for p in files if p not in last_sent]
                        if not new_files: continue
                        end = datetime.now(timezone.utc).astimezone(IST)
                        start = end - __import__('datetime').timedelta(minutes=15)
                        ts = f"{start.strftime('%H%M')}-{end.strftime('%H%M')}"
                        zip_name = f"ticks-{sess_date}-{ts}.zip"
                        with zipfile.ZipFile(zip_name,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
                            for p in new_files: z.write(p,arcname=f"date={sess_date}/{p.name}")
                            meta = day_d / "_meta.json"
                            if meta.exists(): z.write(meta,arcname=f"date={sess_date}/_meta.json")
                        sz=pathlib.Path(zip_name).stat().st_size
                        print(f"15m zip {zip_name} {len(new_files)} files {sz//1024}KB")
                        import requests
                        with open(zip_name,"rb") as f:
                            r=requests.post(f"https://api.telegram.org/bot{_tg_token}/sendDocument",data={"chat_id":_tg_chat,"caption":f"Ticks {sess_date} {ts} 15m {len(new_files)} files {getattr(on_data_tick,'count',0)} ticks"},files={"document":(zip_name,f)},timeout=120)
                            print(f"telegram 15m send {r.status_code} {r.text[:300]}")
                        last_sent.update(new_files)
                        try: pathlib.Path(zip_name).unlink()
                        except: pass
                    except Exception as e:
                        print(f"15m zip failed: {e}")
            import threading as _th
            _th.Thread(target=_zip_loop,daemon=True,name="telegram-15m").start()
    except Exception as e:
        print(f"telegram start failed: {e}")

    def _resub():
        print("Re-subscribing feeds...")
        try:
            if option_tokens:
                streams.hft_data_stream.subscribe_by_segment(
                    DATA_MODE if DATA_MODE in ("ltpc", "full") else "full",
                    {HFTDataStream.EXCH_NSE_FO: option_tokens},
                    latency=50
                )
            streams.subscribe_market_data(DataMode.FULL, [spot_token])
        except Exception as e:
            print(f"Resubscription failed: {e}")

    streams.data_stream.on_connect = lambda: (_resub(), print("DataStream connected"))[1]
    streams.hft_data_stream.on_connect = lambda: (_resub(), print("HFT connected"))[1]
    try:
        base_open = type(streams.hft_data_stream).__bases__[0]._on_open
        streams.hft_data_stream._on_open = lambda ws, _s=streams.hft_data_stream, _b=base_open: _b(_s, ws)
    except: pass

    if USE_HFT and option_tokens:
        streams.connect_hft_data_stream()
        time.sleep(1.0)
        streams.hft_data_stream.subscribe_by_segment(
            DATA_MODE if DATA_MODE in ("ltpc", "full") else "full",
            {HFTDataStream.EXCH_NSE_FO: option_tokens},
            latency=50
        )
    streams.connect_data_stream()
    time.sleep(1.0)
    streams.subscribe_market_data(DataMode.FULL, [spot_token])
    print(f"Streams active for NIFTY spot + {len(option_tokens)} options contracts")

    last_hb = time.monotonic()
    try:
        while _running and not (stop_event and stop_event.is_set()):
            time.sleep(1)
            if on_heartbeat and (time.monotonic() - last_hb >= 60):
                on_heartbeat()
                last_hb = time.monotonic()
    finally:
        try: _tg_stop.set()
        except: pass
        try: _zip_stop.set()
        except: pass
        print("Shutting down streams and writing final segments...")
        try: streams.disconnect_all()
        except: pass
        storage_close()
        print(f"Collector finished clean for {sess_date}")

if __name__ == "__main__":
    main()
