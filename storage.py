"""Parquet tick lake - append-only segments, exchange-time keyed, restart-safe.

Layout per day (data/live/ticks/date=YYYY-MM-DD/):
  token=<token>-<SYMBOL>.part=<NNNNNN>.parquet   one segment per flush
  token=<token>-<SYMBOL>.parquet                 merged file (tools/compact_day.py)
  _meta.json                                     session meta + per-token counts

Design:
- Units: the SDK documents ALL prices in paise on both feeds (sockets.py frame
  layouts), so conversion is /100 unconditionally - no >10000 guessing. Values
  that look impossible after conversion log a loud one-time warning.
- Timestamps: primary `ts` is the exchange trade time (ltt) when present;
  local receive time is kept separately in `ts_recv`. Dedup identity is
  (token, ts, ltp, ltq), and the `_seen` set is bounded.
- CVD: restored from the day's existing files in init_session, so a mid-day
  restart continues the running total instead of resetting to 0.
- Flush: append-only segment per flush (no read-merge-rewrite), tmp ->
  os.replace, and rows are only dropped from the buffer after the write
  succeeds - a failed write puts them back.
- A background timer flushes every FLUSH_SECS even when no ticks arrive.

SDK facts (pyarrow_client/sockets.py):
- HFT stream: prices in paise, dict ticks (bid_px/ask_px/bid_size/ask_size).
- DataStream: prices in paise; `change_flag` is ltp-vs-previous-close direction
  (43/45/32) - NOT a buy/sell aggressor hint, so it is never used as one.
- ltt: exchange trade time in epoch seconds (both feeds).
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

IST = ZoneInfo("Asia/Kolkata")
try:
    from paths import data_root
except ImportError:  # standalone use: resolve without the shared module
    APP_ROOT = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
    def data_root():
        custom = os.environ.get("NIFTY_DATA_DIR", "").strip()
        if custom and Path(custom).is_dir():
            return Path(custom)
        return APP_ROOT / "data"

FLUSH_TICKS = 1000
FLUSH_SECS = 5
SEEN_MAX = 200_000  # dedupe memory bound; beyond this oldest keys are forgotten

_cvd: Dict[int, int] = {}
_cvd_lock = threading.Lock()
_seen: set = set()
_seen_order: deque = deque()
_seen_lock = threading.Lock()

_buffers: Dict[int, List[dict]] = defaultdict(list)
_buf_lock = threading.Lock()
_last_flush = time.monotonic()

_session_meta: Dict = {}
_session_date: Optional[str] = None
_part_seq: Dict[int, int] = {}          # next segment number per token
_warned_tokens: set = set()             # sanity warning, once per token

_flush_thread: Optional[threading.Thread] = None
_flush_stop: Optional[threading.Event] = None

_PART_RE = re.compile(r"\.part=(\d+)\.parquet$")


def _now_ist_naive() -> datetime:
    return datetime.now(timezone.utc).astimezone(IST).replace(tzinfo=None)


def _rupees(paise) -> float:
    """SDK sends every price as integer paise - convert unconditionally."""
    return round(float(paise) / 100.0, 4)


def _warn_price_once(token: int, what: str, value: float) -> None:
    if token in _warned_tokens:
        return
    _warned_tokens.add(token)
    print(f"WARNING token={token}: {what}={value:g} looks impossible after paise"
          f"->rupees conversion - verify feed units for this instrument!")


def _exchange_ts(ltt) -> Optional[datetime]:
    """Exchange trade time -> naive IST. Normalizes s/ms/ns by magnitude."""
    if not ltt:
        return None
    ltt = float(ltt)
    if ltt > 1e17:
        ltt /= 1e9
    elif ltt > 1e14:
        ltt /= 1e3
    if not (1_000_000_000 <= ltt <= 4_000_000_000):  # plausible epoch seconds 2001-2096
        return None
    return datetime.fromtimestamp(ltt, tz=timezone.utc).astimezone(IST).replace(tzinfo=None)


def _infer_aggressor(ltp: float, bid: float, ask: float) -> int:
    """+1 buyer-initiated (trade at ask), -1 seller-initiated (at bid), 0 unknown."""
    if bid and ask:
        mid = (bid + ask) / 2
        if ltp > mid:
            return 1
        if ltp < mid:
            return -1
    return 0


def _normalize_tick(tick, meta: dict, ts_recv: datetime) -> dict:
    """Unified parquet row. All prices converted paise->rupees unconditionally."""
    if isinstance(tick, dict):  # HFT stream
        bid = tick.get('bid_px', 0)
        if isinstance(bid, list):
            bid = bid[0] if bid else 0
        ask = tick.get('ask_px', 0)
        if isinstance(ask, list):
            ask = ask[0] if ask else 0
        bid_qty = tick.get('bid_size', 0)
        if isinstance(bid_qty, list):
            bid_qty = bid_qty[0] if bid_qty else 0
        ask_qty = tick.get('ask_size', 0)
        if isinstance(ask_qty, list):
            ask_qty = ask_qty[0] if ask_qty else 0
        token = int(tick.get('token', meta.get('token', 0)))
        norm = {
            "token": token,
            "ltp": _rupees(tick.get('ltp', 0)),
            "ltq": int(tick.get('ltq', 0)),
            "volume": int(tick.get('volume', 0)),
            "oi": int(tick.get('oi', 0)),
            "open": _rupees(tick.get('open', 0) or 0),
            "high": _rupees(tick.get('high', 0) or 0),
            "low": _rupees(tick.get('low', 0) or 0),
            "close": _rupees(tick.get('close', 0) or 0),
            "bid_price": _rupees(bid), "ask_price": _rupees(ask),
            "bid_qty": int(bid_qty), "ask_qty": int(ask_qty),
            "total_buy_qty": int(tick.get('tbq', 0)), "total_sell_qty": int(tick.get('tsq', 0)),
            "avg_price": _rupees(tick.get('vwap', 0) or 0),
            "mode": "hft_full",
            "ltt": int(tick.get('ltt', 0)),
        }
    else:  # DataStream MarketTick
        bid = tick.bids[0]['price'] if tick.bids else 0
        ask = tick.asks[0]['price'] if tick.asks else 0
        token = int(tick.token)
        norm = {
            "token": token,
            "ltp": _rupees(tick.ltp),
            "ltq": int(tick.ltq),
            "volume": int(tick.volume),
            "oi": int(tick.oi),
            "open": _rupees(tick.open),
            "high": _rupees(tick.high),
            "low": _rupees(tick.low),
            "close": _rupees(tick.close),
            "bid_price": _rupees(bid), "ask_price": _rupees(ask),
            "bid_qty": int(tick.bids[0]['quantity'] if tick.bids else 0),
            "ask_qty": int(tick.asks[0]['quantity'] if tick.asks else 0),
            "total_buy_qty": int(tick.total_buy_quantity), "total_sell_qty": int(tick.total_sell_quantity),
            "avg_price": _rupees(tick.avg_price),
            "mode": str(getattr(tick, 'mode', 'full')),
            "ltt": int(getattr(tick, 'ltt', 0)),
        }

    is_spot = meta.get("option_type", "") in ("SPOT", "")

    def _sane(v: float) -> bool:
        if not v:
            return True
        return (1000 <= v <= 100_000) if is_spot else (0 < v <= 10_000)

    for what in ("ltp", "open", "high", "low"):
        if not _sane(norm[what]):
            _warn_price_once(token, what, norm[what])

    spread = (norm["ask_price"] - norm["bid_price"]) if norm["bid_price"] and norm["ask_price"] else 0.0
    mid = (norm["bid_price"] + norm["ask_price"]) / 2 if norm["bid_price"] and norm["ask_price"] else norm["ltp"]
    imb = (norm["bid_qty"] - norm["ask_qty"]) / (norm["bid_qty"] + norm["ask_qty"]) if (norm["bid_qty"] + norm["ask_qty"]) else 0.0
    aggressor = _infer_aggressor(norm["ltp"], norm["bid_price"], norm["ask_price"])
    delta = aggressor * norm["ltq"] if norm["ltq"] else 0
    with _cvd_lock:
        _cvd[token] = _cvd.get(token, 0) + delta
        cum = _cvd[token]
        cvd_total = sum(_cvd.values())

    ts = _exchange_ts(norm["ltt"]) or ts_recv
    norm.update({
        "ts": ts,
        "ts_recv": ts_recv,
        "spread": float(spread), "mid": float(mid), "imbalance": float(imb),
        "aggressor": int(aggressor), "delta": int(delta), "cvd": int(cum), "cvd_total": int(cvd_total),
        "symbol": meta.get("symbol", str(token)),
        "strike": (lambda sv: int(float(sv)) if sv else 0)(str(meta.get("strike", 0) or 0).strip() or "0"),
        "option_type": meta.get("option_type", ""),
        "expiry": meta.get("expiry", ""),
    })
    return norm


def _restore_day(day_dir: Path) -> None:
    """Continue CVD + segment numbering from a day's already-written files."""
    for p in sorted(day_dir.glob("token=*.parquet")):
        m = _PART_RE.search(p.name)
        try:
            token = int(p.name.split("token=")[1].split("-")[0])
        except (IndexError, ValueError):
            continue
        if m:
            _part_seq[token] = max(_part_seq.get(token, 0), int(m.group(1)))
        try:
            df = pq.read_table(p, columns=["ts", "cvd"]).to_pandas()
            if not df.empty:
                last = df.sort_values("ts").iloc[-1]
                with _cvd_lock:
                    if last["cvd"] and abs(int(last["cvd"])) > abs(_cvd.get(token, 0)):
                        _cvd[token] = int(last["cvd"])
        except Exception:
            pass  # unreadable old file: CVD for that token just starts fresh


def init_session(date_str: Optional[str] = None, meta: Optional[Dict] = None):
    global _session_date, _session_meta
    _session_date = date_str or _now_ist_naive().date().isoformat()
    _session_meta = meta or {}
    day_dir = data_root() / f"date={_session_date}"
    day_dir.mkdir(parents=True, exist_ok=True)
    with _seen_lock:
        _seen.clear()
        _seen_order.clear()
    with _cvd_lock:
        _cvd.clear()
    _part_seq.clear()
    _warned_tokens.clear()
    if any(day_dir.glob("token=*.parquet")):
        _restore_day(day_dir)
        with _cvd_lock:
            print(f"Tick lake session REOPENED: date={_session_date} - "
                  f"CVD continued from files for {len(_cvd)} token(s), "
                  f"segments resume at {max(_part_seq.values(), default=0)}")
    else:
        print(f"Tick lake session: date={_session_date} meta="
              f"{ {k: v for k, v in _session_meta.items() if k != 'tokens'} }")


def on_tick(tick, meta_map: Dict[int, dict]):
    """Buffered tick handler - call from DataStream/HFT callbacks."""
    global _last_flush
    if _session_date is None:
        init_session()
    token = tick.token if hasattr(tick, 'token') else tick.get('token', 0)
    meta = meta_map.get(token, {"symbol": str(token), "strike": "", "option_type": "", "expiry": ""})
    ts_recv = _now_ist_naive()
    norm = _normalize_tick(tick, meta, ts_recv)

    dedup_key = (norm["token"], norm["ts"], round(norm["ltp"], 2), norm["ltq"])
    with _seen_lock:
        if dedup_key in _seen:
            return
        _seen.add(dedup_key)
        _seen_order.append(dedup_key)
        while len(_seen_order) > SEEN_MAX:
            _seen.discard(_seen_order.popleft())

    with _buf_lock:
        _buffers[norm["token"]].append(norm)
        total_buf = sum(len(v) for v in _buffers.values())

    if total_buf >= FLUSH_TICKS:
        flush()


def _write_segment(day_dir: Path, token: int, rows: List[dict]) -> Path:
    df_new = pd.DataFrame(rows)
    df_new["ts"] = pd.to_datetime(df_new["ts"])
    df_new = df_new.sort_values("ts")
    symbol = rows[0].get("symbol", str(token))
    safe_sym = "".join(c if c.isalnum() else "_" for c in symbol)[:40]
    n = _part_seq.get(token, 0) + 1
    _part_seq[token] = n
    out_path = day_dir / f"token={token}-{safe_sym}.part={n:06d}.parquet"
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    df_new.to_parquet(tmp_path, index=False, compression="zstd")
    os.replace(tmp_path, out_path)
    return out_path


def flush():
    """Write buffered ticks as append-only segments. Failed writes go back to the buffer."""
    global _last_flush
    with _buf_lock:
        batches = {tok: rows for tok, rows in _buffers.items() if rows}
        for tok in batches:
            _buffers[tok] = []
    if not batches:
        _last_flush = time.monotonic()
        return

    date_str = _session_date or _now_ist_naive().date().isoformat()
    day_dir = data_root() / f"date={date_str}"
    day_dir.mkdir(parents=True, exist_ok=True)

    failed: Dict[int, List[dict]] = {}
    for token, rows in batches.items():
        try:
            _write_segment(day_dir, token, rows)
        except Exception as e:
            failed[token] = rows
            print(f"flush: write failed for token={token} ({len(rows)} rows kept in buffer): {e}")
    if failed:
        with _buf_lock:
            for tok, rows in failed.items():
                _buffers[tok] = rows + _buffers[tok]

    _last_flush = time.monotonic()
    _write_meta(date_str)


def _write_meta(date_str: str):
    day_dir = data_root() / f"date={date_str}"
    meta_path = day_dir / "_meta.json"
    counts: Dict[str, int] = {}
    for p in day_dir.glob("token=*.parquet"):
        try:
            key = _PART_RE.sub(".parquet", p.name)
            counts[key] = counts.get(key, 0) + pq.ParquetFile(p).metadata.num_rows
        except Exception:
            pass
    with _cvd_lock:
        cvd_snapshot = dict(_cvd)
    meta = {
        "date": date_str,
        "session_meta": _session_meta,
        "cvd": cvd_snapshot,
        "counts": counts,
        "updated_at": _now_ist_naive().isoformat(),
    }
    tmp = meta_path.with_suffix(".tmp.json")
    tmp.write_text(json.dumps(meta, indent=2, default=str))
    os.replace(tmp, meta_path)


def start():
    """Flush every FLUSH_SECS even when no ticks arrive (quiet market, dropped feed)."""
    global _flush_thread, _flush_stop
    if _flush_thread and _flush_thread.is_alive():
        return
    _flush_stop = threading.Event()

    def _loop():
        while not _flush_stop.wait(FLUSH_SECS):
            try:
                flush()
            except Exception as e:
                print(f"timer flush failed: {e}")

    _flush_thread = threading.Thread(target=_loop, daemon=True, name="storage-flush")
    _flush_thread.start()


def stop():
    global _flush_thread
    if _flush_stop:
        _flush_stop.set()
    if _flush_thread:
        _flush_thread.join(timeout=10)
    _flush_thread = None


def close():
    stop()
    flush()
    if _session_date:
        _write_meta(_session_date)
        n_files = len(list((data_root() / f"date={_session_date}").glob("token=*.parquet")))
        print(f"Tick lake closed: {data_root()}/date={_session_date} - {n_files} segment file(s)")


# Legacy aliases for collector compatibility
def init_writers(*a, **kw):  # no-op, kept for run_collect --dry-run that imported old storage
    if _session_date is None:
        init_session()


# Keep tick_writer-ish globals to not break old dry-run code that calls storage.tick_writer
tick_writer = orderflow_writer = cvd_writer = price_writer = None
