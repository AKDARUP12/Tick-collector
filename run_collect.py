#!/usr/bin/env python3
"""Entry: live or --dry-run (writes the Parquet tick lake, not CSV)."""
import sys
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def dry_run():
    """Simulate a realistic morning: running volume, paise units, 09:15 clock."""
    print("Dry-run: simulating a realistic morning (running volume, paise units, 09:15 clock)")
    import storage
    from storage import init_session, on_tick, flush, close
    from pyarrow_client import MarketTick

    # 4 strikes = 8 legs + spot
    n_strikes = 4
    atm = 25000
    strikes = [atm - 100, atm, atm + 100, atm + 200][:n_strikes]
    meta_map = {26000: {"symbol": "NIFTY", "strike": "", "option_type": "SPOT", "expiry": "02-SEP-2026", "token": 26000}}
    tok = 10001
    for s in strikes:
        for side in ("CE", "PE"):
            meta_map[tok] = {"symbol": f"NIFTY{s}{side}", "strike": str(s), "option_type": side, "expiry": "02-SEP-2026", "token": tok}
            tok += 1

    date_str = datetime.now(timezone.utc).astimezone(IST).date().isoformat()
    init_session(date_str, meta={"num_strikes": n_strikes, "atm": atm, "expiry": "02-SEP-2026", "dry_run": True})

    # Simulated clock: starts 09:15:00 IST, advances 0.25 s per tick -> 1200 ticks = 5 minutes
    # Naive IST (tzinfo stripped) - same convention as storage._now_ist_naive / the backtest schema.
    clock = {"t": datetime.now(timezone.utc).astimezone(IST).replace(tzinfo=None)
             .replace(hour=9, minute=15, second=0, microsecond=0)}

    def fake_now():
        t = clock["t"]
        clock["t"] = t + timedelta(milliseconds=250)
        return t

    storage._now_ist_naive = fake_now  # ticks get staggered receive-times like real data

    # Per-instrument state, in PAISE (like the real feed; storage.py converts to rupees)
    state = {}
    for token in meta_map:
        spot = token == 26000
        px = 2500000 if spot else random.randint(12000, 22000)
        state[token] = {
            "px": px, "open": px, "high": px, "low": px,
            "vol": random.randint(1000, 20000),   # morning running volume - only grows
            "oi": random.randint(50000, 300000),
            "vwap_q": 1, "vwap_pq": px,
            "tbq": random.randint(1000, 5000),
            "spread": 50 if spot else 100,
        }

    for _ in range(1200):
        token = random.choice(list(meta_map))
        st = state[token]
        spot = token == 26000
        step = random.randint(50, 200) if spot else random.randint(20, 100)
        st["px"] += random.choice([-1, 0, 1]) * step
        if spot:
            st["px"] = min(max(st["px"], 2450000), 2550000)
        else:
            st["px"] = min(max(st["px"], 12000), 25000)

        bid = st["px"] - st["spread"]
        ask = st["px"] + st["spread"]
        buy = random.random() < 0.5          # trade hits the ask (buyer aggressive) or the bid (seller)
        ltp = ask if buy else bid
        ltq = random.choice([25, 50, 75, 100, 125, 150])

        st["vol"] += ltq                     # running counter: only ever up
        st["high"] = max(st["high"], ltp)
        st["low"] = min(st["low"], ltp)
        st["vwap_q"] += ltq
        st["vwap_pq"] += ltq * ltp
        st["oi"] = max(0, st["oi"] + random.choice([-75, 0, 0, 75]))
        st["tbq"] = min(9999, max(100, st["tbq"] + random.randint(-100, 100)))

        now = clock["t"]  # peek (on_tick advances it) - also serves as exchange trade time
        # SDK computes change_flag as ltp-vs-previous-close direction, not buy/sell side
        prev_close = st["open"]
        change_flag = 43 if ltp > prev_close else 45 if ltp < prev_close else 32
        net_change = round((ltp - prev_close) / prev_close * 100, 2) if prev_close else 0.0
        tick = MarketTick(
            token=token, ltp=ltp, ltq=ltq, volume=st["vol"], oi=st["oi"],
            open=st["open"], high=st["high"], low=st["low"], close=prev_close,
            total_buy_quantity=st["tbq"],
            total_sell_quantity=random.randint(1000, 5000),
            avg_price=st["vwap_pq"] // st["vwap_q"],
            time=int(now.replace(tzinfo=IST).timestamp() * 1000),
            ltt=int(now.replace(tzinfo=IST).timestamp() * 1000), mode="full",
            bids=[{"price": bid, "quantity": random.randint(50, 300), "orders": 5}],
            asks=[{"price": ask, "quantity": random.randint(50, 300), "orders": 5}],
            net_change=net_change,
            change_flag=change_flag,
        )
        on_tick(tick, meta_map)

    flush()
    close()

    # --- self-check: the rehearsal must behave like a real day ---
    import pandas as pd
    day_dir = Path("data/live/ticks") / f"date={date_str}"
    print("\nSelf-check (all 9 files should be OK):")
    all_ok = True
    for p in sorted(day_dir.glob("token=*.parquet")):
        df = pd.read_parquet(p)
        spot = p.name.startswith("token=26000")
        vol_ok = bool((df["volume"].diff().dropna() >= 0).all())
        px = df["ltp"].iloc[0]
        px_ok = (20000 < px < 30000) if spot else (100 < px < 300)
        both_sides = bool((df["aggressor"] == 1).any() and (df["aggressor"] == -1).any())
        ok = vol_ok and px_ok and both_sides
        all_ok &= ok
        print(f"  {'OK ' if ok else 'BAD'}  {p.name:<38} volume-only-up={vol_ok}  price-sane={px_ok} ({px:g})  buys+sell={both_sides}")
    print("\nSelf-check:", "PASSED - data behaves like a real day" if all_ok else "FAILED")

    print("\nBuilding 1-min bars...")
    import importlib
    try:
        build_one = importlib.import_module("build_minute").build_one
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parent / "tools"))
        build_one = importlib.import_module("build_minute").build_one
    try:
        build_one(date_str)
    except Exception as e:
        print(f"minute build failed: {e}")
    return all_ok


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(0 if dry_run() else 1)
    # --until HH:MM (IST) for GitHub Actions timed sessions:
    # e.g. --until 15:00  stops gracefully at 15:00 IST,
    #      --until 15:40  for the closing 40-min session.
    # Stays under the 6h job limit when split into two workflows.
    until = None
    for i, a in enumerate(sys.argv):
        if a == "--until" and i + 1 < len(sys.argv):
            until = sys.argv[i + 1]
        elif a.startswith("--until="):
            until = a.split("=", 1)[1]
    if until:
        import threading
        import time
        import collector

        # parse HH:MM IST
        try:
            hh, mm = map(int, until.split(":"))
            assert 0 <= hh < 24 and 0 <= mm < 60
        except Exception:
            print(f"invalid --until value {until!r}, expected HH:MM")
            sys.exit(2)

        def _now_ist():
            return datetime.now(timezone.utc).astimezone(IST)

        now_ist = _now_ist()
        target = now_ist.replace(hour=hh, minute=mm, second=0, microsecond=0)
        # if target already passed today, assume next day (handles manual re-runs)
        if target <= now_ist:
            # don't run 24h - just exit (market already closed)
            print(f"--until {until} IST already passed (now {_now_ist().strftime('%H:%M:%S IST')}), exiting.")
            sys.exit(0)
        secs = (target - now_ist).total_seconds()
        print(f"Timed session: now={now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} until={until} IST ({secs/60:.1f} min)")

        stop_event = threading.Event()

        def _watcher():
            while not stop_event.is_set():
                remaining = (target - _now_ist()).total_seconds()
                if remaining <= 0:
                    print(f"Reached --until {until} IST, stopping collector...")
                    stop_event.set()
                    break
                # sleep min(30s, remaining) for responsive shutdown
                time.sleep(min(30, max(1, remaining)))

        threading.Thread(target=_watcher, daemon=True).start()
        collector.main(stop_event=stop_event, interactive=False)
        sys.exit(0)

    import collector
    collector.main()
