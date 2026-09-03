"""Live tick-by-tick: NIFTY spot + 4/5 strikes (nearest weekly) -> Parquet tick lake."""
import os
import time
import signal
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pyarrow_client import ArrowStreams, DataMode, Exchange
from pyarrow_client import HFTDataStream

from auth import create_client
from instruments import get_nifty_spot_token, get_nearest_nifty_options
from storage import init_session, on_tick, start as storage_start, close as storage_close
try:
    from paths import data_root
except ImportError:
    from storage import data_root

load_dotenv()
IST = ZoneInfo("Asia/Kolkata")

# 4 strikes is default (8 legs). Set ARROW_STRIKES=5 for symmetric ATM±200.
NUM_STRIKES = int(os.getenv("ARROW_STRIKES", "4"))
DATA_MODE = os.getenv("ARROW_MODE", "full")
USE_HFT = os.getenv("ARROW_USE_HFT", "1") == "1"

_running = True
def _handle_sig(signum, frame):
    global _running
    print("\nCtrl-C - shutting down...")
    _running = False
if threading.current_thread() is threading.main_thread():
    # signal registration is only legal in the main thread (GUI runs us in a worker)
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

def main(stop_event: "threading.Event | None" = None, interactive: bool = True):
    print("=== Arrow Live Collector (Parquet) ===")
    print(f"Strikes: {NUM_STRIKES} levels (~{NUM_STRIKES*2} legs) + NIFTY spot | hft={USE_HFT} mode={DATA_MODE}")

    # 1. Auth
    client, resp = create_client()
    token = client.get_token()
    app_id = os.getenv("ARROW_APP_ID", "")
    print(f"Logged in: {resp.get('name', resp.get('userID',''))} token={token[:20]}...")

    # 2. Resolve instruments - frozen for the day
    spot_name, spot_token = get_nifty_spot_token(client)
    # Infer ATM from spot open (fallback to chain median)
    atm = None
    try:
        from pyarrow_client import QuoteMode
        q = client.get_quote(QuoteMode.OHLCV, "NIFTY", Exchange.INDEX)
        # quotes in paise
        open_px = q.get("open", q.get("ltp", 0)) / 100
        if open_px > 1000:
            atm = int(round(open_px / 100) * 100)
            print(f"Spot open {open_px:.1f} => ATM {atm}")
    except Exception as e:
        print(f"spot quote failed: {e}")

    options = get_nearest_nifty_options(client, num_strikes=NUM_STRIKES, atm=atm, gap_bias="auto")
    # expiry frozen from first leg
    expiry = options[0].get("expiry", "") if options else ""
    print(f"Expiry frozen: {expiry}")

    meta_map = {}
    option_tokens = []
    # spot
    meta_map[spot_token] = {"symbol": "NIFTY", "strike": "", "option_type": "SPOT", "expiry": ""}
    # options
    for leg in options:
        tok = int(leg.get("token", 0) or 0)
        sym = leg.get("symbol", "")
        strike = leg.get("strikePrice", "")
        otype = leg.get("optionType", "")
        if tok:
            meta_map[tok] = {"symbol": sym, "strike": strike, "option_type": otype, "expiry": expiry, "token": tok}
            option_tokens.append(tok)
        else:
            print(f"warn: leg without token {sym} {strike}{otype} - skipping (no HFT token)")

    # Session init - atomic daily folder data/live/ticks/date=YYYY-MM-DD
    sess_date = datetime.now(timezone.utc).astimezone(IST).date().isoformat()

    # Guard: refuse to double-collect - if today's meta was written moments ago,
    # another collector process is probably still running (e.g. a lost background one).
    day_dir = data_root() / f"date={sess_date}"
    meta_path = day_dir / "_meta.json"
    if meta_path.exists():
        try:
            age = time.time() - meta_path.stat().st_mtime
            if age < 30:
                msg = (f"WARNING: {day_dir} was updated {age:.0f}s ago - another collector"
                       f" may still be running for this day. Check Task Manager first.")
                print(msg)
                if interactive:
                    if input("Start anyway and append to this day? (y/N): ").strip().lower() != "y":
                        print("aborted.")
                        return
                else:
                    print("start refused in non-interactive mode - stop the other collector first.")
                    return
        except Exception:
            pass

    init_session(sess_date, meta={"num_strikes": NUM_STRIKES, "atm": atm, "expiry": expiry, "spot_token": spot_token, "option_tokens": option_tokens, "symbols": {tok: meta_map[tok]["symbol"] for tok in option_tokens}})
    storage_start()  # timer flush every FLUSH_SECS even when the feed is quiet

    print(f"Subscribing: NIFTY {spot_token} + {len(option_tokens)} FO tokens")
    print(f"Tokens: {[spot_token] + option_tokens}")

    # 3. Streams - NO overlap: HFT for FO, DataStream for INDEX spot only
    streams = ArrowStreams(appID=app_id, token=token, debug=False)

    def on_data_tick(tick):
        try:
            on_tick(tick, meta_map)
            on_data_tick.count = getattr(on_data_tick, 'count', 0) + 1
            if on_data_tick.count % 500 == 0:
                ltp = getattr(tick, 'ltp', tick.get('ltp', 0) if isinstance(tick, dict) else 0)
                print(f"[{time.strftime('%H:%M:%S')}] ticks={on_data_tick.count} last token={getattr(tick,'token', tick.get('token',0))} ltp={ltp}")
        except Exception as e:
            import traceback
            print(f"tick error: {e}")
            traceback.print_exc()

    streams.data_stream.on_ticks = on_data_tick
    streams.hft_data_stream.on_ltp_tick = lambda t: on_data_tick(t)
    streams.hft_data_stream.on_full_tick = lambda t: on_data_tick(t)
    streams.hft_data_stream.on_response = lambda r: print(f"HFT {r.get('error_code')} ok={r.get('success_count')} err={r.get('error_count')} {r.get('error_msg','')[:60]}")
    streams.data_stream.on_disconnect = lambda: print("DataStream disconnected - will auto-reconnect")

    # Auto-resubscribe on reconnect - the library's internal replay can fail with
    # E_SYMBOL_NOT_FOUND, so resubscribe explicitly with the same working call.
    def _resub():
        print("Re-subscribing after reconnect...")
        try:
            if option_tokens:
                streams.hft_data_stream.subscribe_by_segment(DATA_MODE if DATA_MODE in ("ltpc","full") else "full", {HFTDataStream.EXCH_NSE_FO: option_tokens}, latency=50)
            streams.subscribe_market_data(DataMode.FULL, [spot_token])
        except Exception as e:
            print(f"resub failed: {e}")
    streams.data_stream.on_connect = lambda: (_resub(), print("DataStream connected"))[1]
    streams.hft_data_stream.on_connect = lambda: (_resub(), print("HFT connected"))[1]

    # Silence the SDK's internal HFT auto-resubscribe: it replays without the
    # exchange-segment mapping and the server rejects every symbol
    # (E_ALL_INVALID noise on every reconnect). Our on_connect resubscribe above
    # uses the working subscribe_by_segment call.
    try:
        _base_open = type(streams.hft_data_stream).__bases__[0]._on_open

        def _hft_open_no_replay(ws, _s=streams.hft_data_stream, _b=_base_open):
            _b(_s, ws)

        streams.hft_data_stream._on_open = _hft_open_no_replay
    except Exception:
        pass  # if patching fails the noise is harmless - our resubscribe still wins

    # Connect
    if USE_HFT and option_tokens:
        print("Connecting HFT (zstd)...")
        streams.connect_hft_data_stream()
        time.sleep(1.2)
        streams.hft_data_stream.subscribe_by_segment(DATA_MODE if DATA_MODE in ("ltpc","full") else "full", {HFTDataStream.EXCH_NSE_FO: option_tokens}, latency=50)
        print(f"HFT subscribed {len(option_tokens)} FO tokens latency=50ms")
    # Spot always via DataStream (INDEX not on HFT)
    print("Connecting DataStream for NIFTY spot...")
    streams.connect_data_stream()
    time.sleep(0.9)
    streams.subscribe_market_data(DataMode.FULL, [spot_token])
    print("Live - writing data/live/ticks/date=YYYY-MM-DD/*.part=NNNNNN.parquet (append-only, flush 1000t/5s). Ctrl-C to stop.")
    print("Post-close: python tools/build_minute.py --date", sess_date)

    try:
        while _running and not (stop_event is not None and stop_event.is_set()):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        print("Disconnecting...")
        try: streams.disconnect_all()
        except: pass
        storage_close()
        print(f"Done. Day {sess_date} in data/live/ticks/date={sess_date}/")

if __name__ == "__main__":
    main()
