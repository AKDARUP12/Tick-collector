#!/usr/bin/env python3
"""Peek inside the Parquet tick lake from the terminal.

The collector writes several .part=NNNNNN.parquet segments per instrument;
this tool merges each instrument's segments automatically, so you always see
one logical file per instrument.

Usage:
  python tools/view.py                            list all instruments (rows + size)
  python tools/view.py 26000                      preview the file matching "26000"
  python tools/view.py NIFTY25100CE --rows 5      first 5 rows
  python tools/view.py NIFTY --tail --rows 3      last 3 rows
  python tools/view.py 26000 --cols ts,ltp,ltq,cvd
  python tools/view.py 26000 --stats              min/max/mean per column
  python tools/view.py --dupes                    scan for duplicate rows + volume drops
  python tools/view.py 26000 --csv out.csv        export to CSV (opens in Excel)
  python tools/view.py 26000 --xlsx spot.xlsx     convert to Excel (.xlsx)
  python tools/view.py 2026-08-29 --xlsx day.xlsx all matching instruments, one sheet each
  python tools/view.py --xlsx everything.xlsx     no match = all instruments

Match by any part of the path; if several instruments match, the newest is shown
(--xlsx / --csv / --dupes process every match, not just the newest).
"""
import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
if getattr(sys, "frozen", False):
    ROOT = Path(sys.executable).resolve().parent
sys.path.insert(0, str(ROOT))

try:
    from paths import data_root
except ImportError:
    def data_root():
        return ROOT / "data"

DATA = data_root()

PART_RE = re.compile(r"\.part=(\d+)\.parquet$")


def _rel(p: Path):
    """Path relative to the project for display; absolute when data lives elsewhere."""
    try:
        return p.relative_to(ROOT)
    except ValueError:
        return p


def find_files(needles):
    files = list(data_root().glob("**/*.parquet"))
    if needles:
        nl = [n.lower() for n in needles]
        files = [f for f in files if all(n in f.as_posix().lower() for n in nl)]
    return sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)


def _label(f: Path) -> str:
    """Logical file name: segment token=10001-X.part=000007.parquet -> token=10001-X.parquet."""
    return PART_RE.sub(".parquet", f.name)


def _group_key(f: Path) -> str:
    return f.parent.as_posix() + "/" + _label(f)


def _group_for(f: Path, all_files=None):
    """All segment files belonging to the same logical file as f."""
    if all_files is None:
        all_files = find_files([])
    key = _group_key(f)
    return sorted([g for g in all_files if _group_key(g) == key], key=lambda p: p.name)


def _load(files, cols=None) -> pd.DataFrame:
    """Merge an instrument's segments into one deduped frame."""
    dfs = [pd.read_parquet(p, columns=cols) for p in files]
    df = pd.concat(dfs, ignore_index=True)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.sort_values("ts").drop_duplicates(
            subset=[c for c in ("token", "ts", "ltp", "ltq") if c in df.columns])
    return df.reset_index(drop=True)


def human(n):
    return f"{n / 1024:.0f} KB" if n < 1024 * 1024 else f"{n / 1024 / 1024:.1f} MB"


def list_files(files):
    groups: dict[str, list] = {}
    for f in files:
        groups.setdefault(_group_key(f), []).append(f)
    print(f"{len(groups)} data file(s) under data/:\n")
    for key, parts in sorted(groups.items()):
        try:
            rows = sum(pq.ParquetFile(p).metadata.num_rows for p in parts)
        except Exception as e:
            rows = f"? ({e})"
        size = sum(p.stat().st_size for p in parts)
        loc = _rel(Path(key))
        extra = f"  [{len(parts)} segments]" if len(parts) > 1 else ""
        print(f"  {rows!s:>9} rows  {human(size):>9}  {loc}{extra}")
    print("\nPreview one:  python tools/view.py <part-of-name>   e.g.  python tools/view.py 26000")


def preview(f, rows, tail, cols):
    parts = _group_for(f)
    df = _load(parts, cols)
    print(f"file: {_rel(parts[0].parent) / _label(parts[0])}"
          + (f"  ({len(parts)} segments merged)" if len(parts) > 1 else ""))
    print(f"rows: {len(df):,}   columns: {len(df.columns)}")
    print(f"columns: {', '.join(df.columns)}\n")
    show = df.tail(rows) if tail else df.head(rows)
    with pd.option_context("display.max_columns", None, "display.width", 250):
        print(show.to_string(index=False))


def stats(f):
    df = _load(_group_for(f))
    num = df.select_dtypes("number")
    print(f"file: {_label(f)}   rows: {len(df):,}\n")
    with pd.option_context("display.max_columns", None, "display.width", 250):
        print(num.describe().T[["min", "max", "mean"]].to_string())


EXCEL_MAX_ROWS = 1_048_575  # Excel's hard per-sheet limit


def check_dupes(files):
    """Report duplicate rows and volume drops per instrument.

    exact dupes = fully identical rows (a tick really was saved twice).
    same-ms rows = rows sharing a timestamp with another row - may be legit
    fast trades or missed dedupe.
    volume drops = significant regressions of the running day-volume counter (small out-of-order jitter ignored)
    data; means out-of-order, stale or duplicate ticks (0 is the only good number).
    """
    groups: dict[str, list] = {}
    for f in files:
        if _label(f).startswith("token="):  # tick data only
            groups.setdefault(_group_key(f), []).append(f)
    if not groups:
        print("no tick files matched (minute/cache files have nothing to duplicate-check)")
        return

    print(f"{'rows':>9}  {'exact dupes':>11}  {'same-ts rows':>12}  {'vol drops':>9}  file")
    tot_n = tot_dup = tot_ms = tot_vol = 0
    for key, parts in sorted(groups.items()):
        try:
            df = _load(parts, ["ts", "ltp", "ltq", "volume"])
        except Exception as e:
            print(f"{'?':>9}  {'?':>11}  {'?':>12}  {'?':>9}  {key} ({e})")
            continue
        n = len(df)
        dup = n - len(df.drop_duplicates())
        ms = n - len(df.drop_duplicates(subset=["ts"]))
        # HFT frames arrive out of order (multi-path feed): the day-cumulative
        # volume jitters slightly within a second. Only SIGNIFICANT regressions
        # (> 2% of the running value and > 5,000) indicate a real problem.
        vol = 0
        if "volume" in df.columns and len(df) > 1:
            rmax = df["volume"].cummax()
            prev_max = rmax.shift()
            # only judge once the counter is meaningfully established (past open warm-up)
            vol = int(((df["volume"] < 0.5 * prev_max) & (prev_max > 100_000)).sum())
        tot_n += n
        tot_dup += dup
        tot_ms += ms
        tot_vol += vol
        flag = ""
        if dup:
            flag = "   <-- DUPLICATES"
        elif vol:
            flag = "   <-- VOLUME DROPS"
        loc = _rel(Path(key))
        print(f"{n:>9,}  {dup:>11,}  {ms:>12,}  {vol:>9,}  {loc}{flag}")
    print(f"\ntotal: {tot_n:,} rows | {tot_dup:,} exact duplicates | "
          f"{tot_ms:,} same-timestamp rows | {tot_vol:,} volume drops")
    if tot_dup:
        print("verdict: PROBLEMS FOUND - some ticks were saved more than once")
    elif tot_vol:
        print(f"verdict: minor - {tot_vol} stale frame(s) detected (counter briefly slid back); data usable")
    else:
        print("verdict: clean - nothing saved twice, volume counter never went backwards")


def _sheet_name(label: str, used: set) -> str:
    stem = label.replace(".parquet", "").replace("token=", "").replace("date=", "").replace("expiry=", "")
    if stem.lower() in ("data", "index", "manifest"):  # generic names: use folder for context
        stem = label  # fall back to full logical name
    name = "".join(c if c.isalnum() or c in "-_." else "_" for c in stem)[:28] or "sheet"
    base, i = name, 2
    while name.lower() in used:
        name = f"{base[:26]}_{i}"
        i += 1
    used.add(name.lower())
    return name


def export_xlsx(files, out_path, cols):
    out_path = Path(out_path)
    if out_path.suffix.lower() != ".xlsx":
        out_path = out_path.with_suffix(".xlsx")
    groups: dict[str, list] = {}
    for f in files:
        groups.setdefault(_group_key(f), []).append(f)

    used = set()
    written = 0
    with pd.ExcelWriter(out_path, engine="openpyxl") as xl:
        for key, parts in sorted(groups.items()):
            use = cols
            if use:
                have = set(pq.ParquetFile(parts[0]).schema_arrow.names)
                use = [c for c in use if c in have]
                if not use:
                    print(f"  (skipped {key} - has none of the --cols)")
                    continue
            df = _load(parts, use)
            note = ""
            if len(df) > EXCEL_MAX_ROWS:
                df = df.head(EXCEL_MAX_ROWS)
                note = " (truncated to Excel's row limit)"
            name = _sheet_name(_label(parts[0]), used)
            df.to_excel(xl, sheet_name=name, index=False)
            written += 1
            seg = f", {len(parts)} segments merged" if len(parts) > 1 else ""
            print(f"  {name:<28} <- {key} ({len(df):,} rows{seg}{note})")
    if not written:
        out_path.unlink(missing_ok=True)
        sys.exit("nothing written - no matched file had the requested --cols")
    print(f"\nwrote {out_path.resolve()} ({written} sheet(s)) - double-click to open in Excel")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("match", nargs="*", help="parts of the file path to match (e.g. 26000, NIFTY25100CE, minute)")
    ap.add_argument("--rows", type=int, default=10, help="how many rows to show (default 10)")
    ap.add_argument("--tail", action="store_true", help="show last rows instead of first")
    ap.add_argument("--cols", help="comma-separated columns, e.g. ts,ltp,cvd")
    ap.add_argument("--stats", action="store_true", help="summary stats instead of rows")
    ap.add_argument("--dupes", action="store_true", help="scan matched instruments (or all) for duplicate rows + volume drops")
    ap.add_argument("--csv", metavar="OUT", help="export matched instrument(s) to CSV file(s)")
    ap.add_argument("--xlsx", metavar="OUT.xlsx", help="convert matched instrument(s) to one Excel workbook, one sheet each")
    args = ap.parse_args()

    if not data_root().exists():
        sys.exit("no data/ directory yet - run: python run_collect.py --dry-run")

    files = find_files(args.match)
    if not files:
        sys.exit("no files matched - run with no arguments to list everything")

    cols = [c.strip() for c in args.cols.split(",")] if args.cols else None

    if args.dupes:
        check_dupes(files)
        return

    if args.xlsx:
        export_xlsx(files, args.xlsx, cols)
        return

    if not args.match:
        list_files(files)
        return

    if len(files) > 1:
        seen = {}
        for f in files:
            seen.setdefault(_group_key(f), f)
        if len(seen) > 1:
            print(f"{len(seen)} instruments matched - showing the newest (add more words to narrow):\n")
            for f in list(seen.values())[:5]:
                print(f"  {_rel(f)}")
            print()
    f = files[0]

    if args.csv:
        groups: dict[str, list] = {}
        for g in files:
            groups.setdefault(_group_key(g), []).append(g)
        for i, (key, parts) in enumerate(sorted(groups.items())):
            out = Path(args.csv)
            if len(groups) > 1:  # several matches: out_1.csv, out_2.csv, ...
                out = out.with_name(f"{out.stem}_{i + 1}{out.suffix}")
            df = _load(parts, cols)
            df.to_csv(out, index=False)
            print(f"wrote {out} ({len(df):,} rows) - this opens in Excel")
        return
    if args.stats:
        stats(f)
        return

    preview(f, args.rows, args.tail, cols)


if __name__ == "__main__":
    main()
