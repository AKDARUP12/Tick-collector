#!/usr/bin/env python3
"""Merge a day's flush segments into one parquet per token (append-only lake -> tidy files).

The collector writes one small .part=NNNNNN.parquet segment per flush. After the
session (or anytime), this tool merges each token's segments into a single
token=<token>-<SYMBOL>.parquet - sorted, deduped on (token, ts, ltp, ltq) - and
deletes the segments. Idempotent: a day with no segments is left untouched.

Usage:
  python tools/compact_day.py --date 2026-08-29
  python tools/compact_day.py --date today
  python tools/compact_day.py --all
"""
import argparse
import os
import re
from datetime import datetime
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
PART_RE = re.compile(r"\.part=(\d+)\.parquet$")


def compact_one(date_str: str) -> None:
    day_dir = _tick_root() / f"date={date_str}"
    if not day_dir.exists():
        print(f"no tick folder for {date_str}")
        return

    groups: dict[str, list[Path]] = {}
    for p in day_dir.glob("token=*.parquet"):
        base = PART_RE.sub(".parquet", p.name)
        groups.setdefault(base, []).append(p)

    merged_n = 0
    for base, parts in sorted(groups.items()):
        segments = [p for p in parts if PART_RE.search(p.name)]
        if not segments:
            continue  # already compacted
        dfs = [pd.read_parquet(p) for p in parts]
        df = pd.concat(dfs, ignore_index=True)
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.sort_values("ts").drop_duplicates(subset=["token", "ts", "ltp", "ltq"], keep="last")
        df = df.reset_index(drop=True)
        out = day_dir / base
        tmp = out.with_name(out.name + ".tmp")
        df.to_parquet(tmp, index=False, compression="zstd")
        os.replace(tmp, out)
        for p in segments:
            p.unlink()
        merged_n += 1
        print(f"  {base:<44} {len(df):>7,} rows  (from {len(segments)} segment(s))")

    print(f"compacted {date_str}: {merged_n} instrument file(s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD or 'today'")
    ap.add_argument("--all", action="store_true", help="compact every date")
    args = ap.parse_args()
    if args.all:
        for d in sorted(_tick_root().glob("date=*")):
            compact_one(d.name.split("=", 1)[1])
    elif args.date:
        ds = datetime.now(IST).date().isoformat() if args.date == "today" else args.date
        compact_one(ds)
    else:
        ap.print_help()
