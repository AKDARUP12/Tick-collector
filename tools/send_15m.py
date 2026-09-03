#!/usr/bin/env python3
"""Split tick lake into 15m zips and send to Telegram (tick-only, <50MB each).
Usage: python tools/send_15m.py --date 2026-09-03 [--dry-run]
Reads data/live/ticks/date=YYYY-MM-DD/*.parquet, buckets by 15m ts floor.
"""
import argparse, os, zipfile, tempfile
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
import requests

IST = ZoneInfo("Asia/Kolkata")
BOT = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
CHAT = os.getenv("TELEGRAM_CHAT_ID","").strip()

def tick_root():
    try:
        from paths import data_root
        return data_root() / "live" / "ticks"
    except:
        return Path("data/live/ticks")

def send_zip(zip_path, caption):
    if not BOT or not CHAT:
        print(f"skip send no BOT/CHAT {zip_path}")
        return False
    with open(zip_path, "rb") as f:
        r = requests.post(f"https://api.telegram.org/bot{BOT}/sendDocument",
                          data={"chat_id": CHAT, "caption": caption},
                          files={"document": (zip_path.name, f)}, timeout=120)
        print(f"send {zip_path.name} {r.status_code} {r.text[:400]}")
        return r.ok

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--dry-run", action="store_true", help="don't send, just build zips")
    args = ap.parse_args()
    day_dir = tick_root() / f"date={args.date}"
    if not day_dir.exists():
        print(f"no ticks for {args.date}: {day_dir} missing")
        return
    files = list(day_dir.glob("token=*.parquet"))
    if not files:
        print("no parquet"); return
    dfs=[]
    for p in files:
        try: dfs.append(pd.read_parquet(p))
        except Exception as e: print(f"skip {p}: {e}")
    if not dfs: return
    ticks = pd.concat(dfs, ignore_index=True)
    ticks["ts"] = pd.to_datetime(ticks["ts"])
    # bucket 15m
    ticks["bucket"] = ticks["ts"].dt.floor("15min")
    for bucket, g in ticks.groupby("bucket"):
        start = bucket.strftime("%H%M")
        end = (bucket + pd.Timedelta(minutes=15)).strftime("%H%M")
        label = f"{start}-{end}"
        tmpdir = Path(tempfile.mkdtemp())
        slice_path = tmpdir / f"ticks-{args.date}-{label}.parquet"
        g.drop(columns=["bucket"]).to_parquet(slice_path, index=False, compression="zstd")
        zip_name = Path(f"ticks-{args.date}-{label}.zip")
        with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            z.write(slice_path, arcname=f"date={args.date}/ticks-{label}.parquet")
            meta = day_dir / "_meta.json"
            if meta.exists():
                z.write(meta, arcname=f"date={args.date}/_meta.json")
        print(f"bucket {label} {len(g)} ticks -> {zip_name} {zip_name.stat().st_size//1024}KB")
        if not args.dry_run:
            send_zip(zip_name, f"Ticks {args.date} {label} 15m {len(g)} ticks")
        zip_name.unlink()
        # cleanup tmp
        try: slice_path.unlink(); tmpdir.rmdir()
        except: pass

if __name__=="__main__":
    main()
