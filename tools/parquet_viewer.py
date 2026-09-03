#!/usr/bin/env python3
"""
parquet_viewer.py — local, read-only web viewer for Parquet files.

Usage:
    python tools/parquet_viewer.py [path] [--port 8787] [--no-browser]

    path    a .parquet file, or a directory (a directory is viewed as a dataset:
            every *.parquet under it, with Hive partition columns included).
            Default: <project>/data if it exists, else the current directory.
            The folder can also be changed any time from the sidebar (type a
            path, go up a level, or use the native folder-picker dialog).

Requires:   duckdb   (pip install duckdb)

Binds to 127.0.0.1 only, serves a single-page UI at http://127.0.0.1:<port>.
Read-only: only SELECTs against parquet files are ever executed.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import threading
import time
import webbrowser
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

try:
    import duckdb
except ImportError:
    sys.exit("parquet_viewer needs duckdb — install it with:  pip install duckdb")

VERSION = "1.8"
MAX_FILES = 1_000_000     # effectively unlimited; glob reads datasets of any size
MAX_TREE_FILES = 200_000  # sidebar tree cap
TREE_DEPTH = 8
NESTED_TYPES = ("LIST", "STRUCT", "MAP", "UNION", "ARRAY", "ROW")

ROOT = ""                 # default root, set in main()
CURRENT_ROOT = None       # folder currently browsed, changes via the UI
INITIAL = None            # file auto-selected on load, set in main()
LOCK = threading.Lock()   # serializes every duckdb call
_PICK_LOCK = threading.Lock()  # only one folder dialog at a time
CON = duckdb.connect()    # in-memory DB; files are queried in place


class ApiError(Exception):
    def __init__(self, status: int, msg: str):
        super().__init__(msg)
        self.status = status
        self.msg = msg


# --------------------------------------------------------------------------- helpers

def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _ident(name: str, allowed: set[str] | None = None) -> str:
    if allowed is not None and name not in allowed:
        raise ApiError(400, f"unknown column: {name}")
    return '"' + name.replace('"', '""') + '"'


def resolve_target(path: str):
    """Map a request path to ('file'|'dataset', [parquet files], display path)."""
    p = os.path.realpath(path)
    if os.path.isdir(p):
        files: list[str] = []
        for dirpath, dirnames, filenames in os.walk(p):
            dirnames.sort()
            for fn in sorted(filenames):
                if fn.lower().endswith(".parquet"):
                    files.append(os.path.join(dirpath, fn))
            if len(files) > MAX_FILES:
                raise ApiError(400, f"dataset has more than {MAX_FILES} parquet files — pick a narrower folder")
        if not files:
            raise ApiError(400, f"no .parquet files under {p}")
        return "dataset", files, p
    if os.path.isfile(p):
        if not p.lower().endswith(".parquet"):
            raise ApiError(400, f"not a .parquet file: {p}")
        return "file", [p], p
    raise ApiError(404, f"not found: {p}")


def _relation_sql(files: list[str], dataset: bool, folder: str | None = None, hive: bool = True) -> str:
    """Dataset mode reads via one recursive glob — no per-file list, so a day
    folder with tens of thousands of segment files works like a small one."""
    if dataset and folder:
        if any(c in folder for c in "*?[]"):
            raise ApiError(400, f"folder name contains glob characters: {folder}")
        g = folder.replace("\\", "/").rstrip("/") + "/**/*.parquet"
        return (f"read_parquet({_sql_str(g)}, union_by_name=true, "
                f"hive_partitioning={'true' if hive else 'false'})")
    lst = "[" + ", ".join(_sql_str(f) for f in files) + "]"
    return f"read_parquet({lst}, hive_partitioning=false)"


def working_rel(file_param: str):
    """Resolve a request target to a queryable relation.

    If a dataset's hive partitions are inconsistent (e.g. the folder mixes files
    at different partition depths), fall back to reading file contents only and
    report the dropped partition columns."""
    kind, files, display = resolve_target(file_param)
    folder = display if kind == "dataset" else None
    rel = _relation_sql(files, kind == "dataset", folder)
    hive_dropped = False
    if kind == "dataset":
        try:
            schema_cols(rel)
        except Exception as exc:
            if "hive" not in str(exc).lower():
                raise
            rel = _relation_sql(files, True, folder, hive=False)
            hive_dropped = True
    return rel, kind, files, display, hive_dropped


def _q_all(sql: str, params: list | None = None):
    with LOCK:
        cur = CON.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return cols, cur.fetchall()


def _q_one(sql: str, params: list | None = None):
    _, rows = _q_all(sql, params)
    return rows[0] if rows else None


# row counts over huge datasets (tens of thousands of segment files) take a
# while — cache them briefly so paging through a day stays responsive
_COUNT_CACHE: dict = {}
_COUNT_TTL = 120.0


def _cached_count(key: tuple, fn) -> int:
    now = time.monotonic()
    hit = _COUNT_CACHE.get(key)
    if hit and now - hit[0] < _COUNT_TTL:
        return hit[1]
    v = fn()
    _COUNT_CACHE[key] = (now, v)
    if len(_COUNT_CACHE) > 64:  # drop the oldest entry
        _COUNT_CACHE.pop(min(_COUNT_CACHE, key=lambda k: _COUNT_CACHE[k][0]), None)
    return v


def schema_cols(rel: str) -> list[dict]:
    _, rows = _q_all(f"DESCRIBE SELECT * FROM {rel}")
    out = []
    for name, ctype, nullable, *_ in rows:
        out.append({
            "name": name,
            "type": ctype,
            "nullable": str(nullable).upper() != "NO",
            "nested": any(ctype.upper().startswith(t) for t in NESTED_TYPES),
        })
    return out


def jval(v):
    """Convert a duckdb value to something JSON-safe."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, float):
        if v != v:  # NaN — show it as written, null would read as "no data"
            return "NaN"
        if v in (float("inf"), float("-inf")):
            return "inf" if v > 0 else "-inf"
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (date, dtime)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        return "0x" + b.hex()[:64] + ("…" if len(b) > 32 else "")
    if isinstance(v, (list, tuple, dict)):
        try:
            return json.dumps(v, default=str, separators=(",", ":"))
        except Exception:
            return str(v)
    return v if isinstance(v, str) else str(v)


def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def build_where(q: str, where: str, colnames: list[str]) -> str:
    parts = []
    if q:
        lit = _sql_str("%" + _escape_like(q) + "%")
        conds = " OR ".join(
            f"CAST({_ident(c)} AS VARCHAR) ILIKE {lit} ESCAPE '\\'" for c in colnames
        )
        parts.append("(" + conds + ")")
    if where:
        parts.append("(" + where + ")")
    return " AND ".join(parts)


def file_meta(files: list[str], dataset: bool) -> dict:
    """Parquet file metadata (row groups / writer / compression). Degrades quietly."""
    meta: dict = {}
    try:
        if dataset:
            raise ValueError  # keep the single-file path exact; datasets use the fallback
        f = files[0]
        row = _q_one(
            f"SELECT created_by, num_rows, num_row_groups FROM parquet_file_metadata({_sql_str(f)})"
        )
        if row:
            meta["created_by"], meta["rows"], meta["row_groups"] = row
        comps = [
            r[0] for r in _q_all(
                f"SELECT DISTINCT compression FROM parquet_metadata({_sql_str(f)})"
            )[1]
        ]
        if comps:
            meta["compression"] = "+".join(sorted(str(c) for c in comps))
    except Exception:
        try:
            if len(files) == 1:
                meta["row_groups"] = _q_one(
                    f"SELECT count(*) FROM parquet_metadata({_sql_str(files[0])})"
                )
        except Exception:
            pass
    return meta


# --------------------------------------------------------------------------- API impl

def api_config() -> dict:
    return {"root": CURRENT_ROOT or ROOT, "initial": INITIAL, "version": VERSION}


def api_tree(params: dict) -> dict:
    """List the tree under CURRENT_ROOT. Passing ?path=<folder> re-roots first."""
    global CURRENT_ROOT
    req = params.get("path", "").strip()
    if req:
        p = os.path.realpath(req)
        if not os.path.isdir(p):
            raise ApiError(400, f"not a folder: {req}")
        CURRENT_ROOT = p
    if not CURRENT_ROOT:
        raise ApiError(400, "no folder selected")

    n_files = 0

    def walk(dirpath: str, depth: int) -> dict:
        nonlocal n_files
        node = {"name": os.path.basename(dirpath.rstrip(os.sep)) or dirpath,
                "path": dirpath, "dirs": [], "files": []}
        if depth >= TREE_DEPTH:
            return node
        try:
            entries = sorted(os.scandir(dirpath), key=lambda e: e.name.lower())
        except OSError as e:
            node["error"] = str(e)
            return node
        for e in entries:
            if e.is_dir(follow_symlinks=False):
                node["dirs"].append(walk(e.path, depth + 1))
            elif e.name.lower().endswith(".parquet"):
                if n_files >= MAX_TREE_FILES:
                    continue
                n_files += 1
                try:
                    size = e.stat(follow_symlinks=False).st_size
                except OSError:
                    size = 0
                node["files"].append({"name": e.name, "path": e.path, "size": size})
        return node

    return {"root": CURRENT_ROOT, "tree": walk(CURRENT_ROOT, 0), "n_files": n_files}


def api_pick() -> dict:
    """Open a native folder-picker dialog on this machine and re-root to it."""
    global CURRENT_ROOT
    start = CURRENT_ROOT or ROOT or os.path.expanduser("~")
    with _PICK_LOCK:
        try:
            import tkinter as tk
            from tkinter import filedialog
        except Exception as e:
            raise ApiError(500, f"folder dialog unavailable on this machine: {e}")
        result = {"picked": False}
        try:
            r = tk.Tk()
            r.withdraw()
            r.attributes("-topmost", True)
            chosen = filedialog.askdirectory(parent=r, initialdir=start,
                                             title="Parquet Viewer — choose folder to browse")
            r.destroy()
        except Exception as e:
            raise ApiError(500, f"folder dialog failed: {e}")
        if chosen:
            CURRENT_ROOT = os.path.realpath(chosen)
            result = {"picked": True, "root": CURRENT_ROOT}
    return result


def api_info(file: str) -> dict:
    rel, kind, files, display, hive_dropped = working_rel(file)
    cols = schema_cols(rel)
    rows = _cached_count(("info", rel), lambda: _q_one(f"SELECT count(*) FROM {rel}")[0])
    info = {
        "path": display,
        "name": os.path.basename(display.rstrip(os.sep)),
        "is_dataset": kind == "dataset",
        "n_files": len(files),
        "size": sum(os.path.getsize(f) for f in files),
        "rows": rows,
        "cols": cols,
    }
    if hive_dropped:
        info["hive_dropped"] = True
    info.update(file_meta(files, kind == "dataset"))
    return info


def _data_common(qs_params: dict):
    """Shared: parse params → (rel, sel_cols, colnames, where, sort, dir, limit)."""
    rel, _, _, _, _ = working_rel(qs_params.get("file", ""))
    cols = schema_cols(rel)
    colnames = [c["name"] for c in cols]
    colset = set(colnames)

    sel = qs_params.get("cols", "").strip()
    sel_cols = [_ident(c, colset) for c in sel.split(",") if c] if sel else None

    q = qs_params.get("q", "")
    where = qs_params.get("where", "")
    wsql = build_where(q, where, colnames)

    sort = qs_params.get("sort", "").strip()
    if sort:
        _ident(sort, colset)  # validates
    direction = "DESC" if qs_params.get("dir", "asc").lower() == "desc" else "ASC"
    return rel, sel_cols, cols, wsql, sort, direction


def api_data(params: dict) -> dict:
    rel, sel_cols, cols, wsql, sort, direction = _data_common(params)
    try:
        limit = max(1, min(1000, int(params.get("limit", 100))))
    except ValueError:
        limit = 100
    try:
        offset = max(0, int(params.get("offset", 0)))
    except ValueError:
        offset = 0

    proj = ", ".join(sel_cols) if sel_cols else "*"
    sql = f"SELECT {proj} FROM {rel}"
    if wsql:
        sql += " WHERE " + wsql
    t0 = time.perf_counter()
    total = _cached_count(("data", rel, wsql),
                          lambda: _q_one(f"SELECT count(*) FROM {rel}" + (f" WHERE {wsql}" if wsql else ""))[0])
    if sort:
        sql += f" ORDER BY {_ident(sort)} {direction}"
    sql += f" LIMIT {limit} OFFSET {offset}"
    names, rows = _q_all(sql)
    rows = [[jval(v) for v in r] for r in rows]
    return {
        "offset": offset, "limit": limit, "total": total,
        "cols": names, "rows": rows,
        "types": {c["name"]: c["type"] for c in cols},
        "took_ms": round((time.perf_counter() - t0) * 1000),
    }


def api_stats(params: dict) -> dict:
    rel, _, cols, _, _, _ = _data_common(params)
    t0 = time.perf_counter()

    def exprs_for(c: dict, k: str) -> list[str]:
        i = _ident(c["name"])
        return [f"min({i}) AS mn_{k}", f"max({i}) AS mx_{k}", f"count(*) - count({i}) AS nl_{k}"]

    aggregable = [c for c in cols if not c["nested"]]
    stats = None
    try:  # one full scan for everything
        aggs: list[str] = []
        for k, c in enumerate(aggregable):
            aggs += exprs_for(c, str(k))
        row = _q_one(f"SELECT {', '.join(aggs)} FROM {rel}")
        stats = []
        for k, c in enumerate(aggregable):
            stats.append({"name": c["name"], "min": jval(row[3 * k]),
                          "max": jval(row[3 * k + 1]), "nulls": row[3 * k + 2]})
    except Exception:
        stats = []  # fall back to per-column so one bad column can't kill the panel
        for c in cols:
            if c["nested"]:
                continue
            try:
                aggs = exprs_for(c, "x")
                row = _q_one(f"SELECT {', '.join(aggs)} FROM {rel}")
                stats.append({"name": c["name"], "min": jval(row[0]),
                              "max": jval(row[1]), "nulls": row[2]})
            except Exception as e:
                stats.append({"name": c["name"], "min": None, "max": None,
                              "nulls": None, "error": str(e)})
    return {"stats": stats, "took_ms": round((time.perf_counter() - t0) * 1000)}


def api_export(params: dict) -> tuple[str, str]:
    rel, sel_cols, cols, wsql, sort, direction = _data_common(params)
    try:
        limit = max(1, min(1_000_000, int(params.get("limit", 100_000))))
    except ValueError:
        limit = 100_000

    # ts_text: emit timestamp columns as ="..." so spreadsheet apps show
    # the full date AND time instead of formatting the column date-only.
    # ON by default — code-friendly exports pass ts_text=0.
    ts_text = params.get("ts_text", "1") not in ("0", "false", "no")
    out_names = ([c[1:-1].replace('""', '"') for c in sel_cols] if sel_cols
                 else [c["name"] for c in cols])
    ts_idx = ([i for i, n in enumerate(out_names)
               if any(c["name"] == n and c["type"].upper().startswith("TIMESTAMP") for c in cols)]
              if ts_text else [])

    proj = ", ".join(sel_cols) if sel_cols else "*"
    sql = f"SELECT {proj} FROM {rel}"
    if wsql:
        sql += " WHERE " + wsql
    if sort:
        sql += f" ORDER BY {_ident(sort)} {direction}"
    sql += f" LIMIT {limit}"

    names, rows = _q_all(sql)
    buf = io.StringIO()
    # quote every non-numeric field: spreadsheet apps split unquoted
    # "YYYY-MM-DD HH:MM:SS" timestamps at the space and misalign columns
    w = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
    w.writerow(names)
    for r in rows:
        vals = [jval(v) for v in r]
        for i in ts_idx:
            if vals[i] is not None:
                vals[i] = '="' + str(vals[i]) + '"'
        w.writerow(["" if v is None else v for v in vals])
    fname = os.path.basename(params.get("file", "data"))
    if fname.lower().endswith(".parquet"):
        fname = fname[:-len(".parquet")]
    return buf.getvalue(), f"attachment; filename={fname}.csv"


# --------------------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    server_version = f"ParquetViewer/{VERSION}"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep the console clean
        pass

    # -- responders --------------------------------------------------------
    def _headers(self, ctype: str, length: int):
        # never let the browser reuse a stale copy of the UI or data
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", "no-store")

    def _json(self, obj, status: int = 200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self._headers("application/json; charset=utf-8", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _html(self):
        body = PAGE.replace("__VER__", VERSION).encode("utf-8")
        self.send_response(200)
        self._headers("text/html; charset=utf-8", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, text: str, ctype: str, dispo: str):
        body = text.encode("utf-8")
        self.send_response(200)
        self._headers(ctype, len(body))
        self.send_header("Content-Disposition", dispo)
        self.end_headers()
        self.wfile.write(body)

    # -- routing -----------------------------------------------------------
    def do_GET(self):
        u = urlparse(self.path)
        params = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path in ("/", "/index.html"):
                self._html()
            elif u.path == "/api/config":
                self._json(api_config())
            elif u.path == "/api/tree":
                self._json(api_tree(params))
            elif u.path == "/api/pick":
                self._json(api_pick())
            elif u.path == "/api/info":
                self._json(api_info(params.get("file", "")))
            elif u.path == "/api/data":
                self._json(api_data(params))
            elif u.path == "/api/stats":
                self._json(api_stats(params))
            elif u.path == "/api/export":
                text, dispo = api_export(params)
                self._text(text, "text/csv; charset=utf-8", dispo)
            else:
                self._json({"error": f"no such endpoint: {u.path}"}, 404)
        except ApiError as e:
            self._json({"error": e.msg}, e.status)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:  # surface backend errors in the UI
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)


# --------------------------------------------------------------------------- page

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Parquet Viewer</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{
    --bg:#0e131d; --bg2:#141b29; --bg3:#1a2334; --line:#26324a; --txt:#dbe4f3;
    --dim:#8798b5; --acc:#4f8cff; --ok:#3fb96f; --err:#ff6b6b; --num:#8fd3ff;
  }
  *{box-sizing:border-box}
  html,body{height:100%}
  body{margin:0;background:var(--bg);color:var(--txt);
       font:13px/1.45 "Segoe UI",system-ui,sans-serif;overflow:hidden}
  #app{display:flex;height:100vh}

  /* ---------- sidebar ---------- */
  #side{width:280px;min-width:280px;background:var(--bg2);border-right:1px solid var(--line);
        display:flex;flex-direction:column}
  .sidehead{display:flex;align-items:center;justify-content:space-between;
            padding:10px 12px;font-weight:600;letter-spacing:.06em;font-size:11px;color:var(--dim)}
  #rootBox{padding:0 10px 8px}
  #rootInput{width:100%;background:var(--bg);border:1px solid var(--line);color:var(--txt);
             border-radius:5px;padding:4px 8px;font-size:11.5px;font-family:Consolas,monospace}
  #rootInput:focus{outline:none;border-color:var(--acc)}
  #rootBtns{display:flex;gap:6px;margin-top:6px}
  #rootBtns button{padding:3px 9px;font-size:11px}
  #rootPick{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  #rootBtns button:disabled{opacity:.5;cursor:default}
  #fileFilterBox{padding:0 10px 8px}
  #fileSearch{width:100%;background:var(--bg);border:1px solid var(--line);color:var(--txt);
              border-radius:5px;padding:4px 8px;font-size:11.5px;font-family:Consolas,monospace}
  #fileSearch:focus{outline:none;border-color:var(--acc)}
  #fileSeg{display:inline-flex}
  #fileSegRow{display:flex;gap:6px;margin-top:6px;align-items:stretch}
  #fileSegRow select{background:var(--bg);border:1px solid var(--line);color:var(--txt);
                     border-radius:5px;padding:3px 6px;font-size:11px;flex:1;min-width:0}
  .fseg{border-radius:0;border-right-width:0;padding:3px 12px;font-size:11px}
  .fseg:first-child{border-radius:5px 0 0 5px}
  .fseg:last-child{border-radius:0 5px 5px 0;border-right-width:1px}
  .fseg.on{background:#21304e;border-color:var(--acc);color:#fff;font-weight:600}
  .filterInfo{color:var(--dim);font-size:11px;padding:4px 6px 6px}
  #tree{flex:1;overflow:auto;padding:4px 6px 12px;font-size:12.5px}
  .trow{display:flex;align-items:center;gap:4px;padding:2px 4px;border-radius:4px;cursor:pointer;
        white-space:nowrap}
  .trow:hover{background:var(--bg3)}
  .trow.sel{background:#21304e}
  .trow .caret{width:14px;text-align:center;color:var(--dim);flex:none;font-size:10px}
  .trow .dlabel{overflow:hidden;text-overflow:ellipsis}
  .trow .fsize{color:var(--dim);font-size:10.5px;margin-left:auto;padding-left:6px}
  .trow .dsbtn{flex:none;border:0;background:none;color:var(--dim);cursor:pointer;font-size:11px;
               padding:0 3px;border-radius:3px}
  .trow .dsbtn:hover{color:var(--acc);background:var(--bg)}
  .tkids{margin-left:14px;border-left:1px dotted var(--line);padding-left:4px}
  .sidefoot{padding:6px 12px;border-top:1px solid var(--line);color:var(--dim);font-size:10.5px}

  /* ---------- main ---------- */
  #main{flex:1;display:flex;flex-direction:column;min-width:0}
  #hdr{padding:10px 14px 6px;border-bottom:1px solid var(--line);background:var(--bg2)}
  #title{font-weight:600;font-size:14px;word-break:break-all}
  #badges{margin-top:5px;display:flex;flex-wrap:wrap;gap:6px;align-items:center}
  .badge{background:var(--bg3);border:1px solid var(--line);border-radius:20px;
         padding:2px 9px;font-size:11px;color:var(--dim)}
  .badge b{color:var(--txt);font-weight:600}

  #toolbar{display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:8px 14px;
           border-bottom:1px solid var(--line);background:var(--bg2)}
  #toolbar input[type=text]{background:var(--bg);border:1px solid var(--line);color:var(--txt);
       border-radius:5px;padding:5px 8px;font-size:12.5px}
  #q{width:230px}
  #where{width:340px;font-family:Consolas,monospace}
  button{background:var(--bg3);border:1px solid var(--line);color:var(--txt);border-radius:5px;
         padding:5px 10px;font-size:12px;cursor:pointer}
  button:hover{border-color:var(--acc)}
  select{background:var(--bg);border:1px solid var(--line);color:var(--txt);border-radius:5px;
         padding:4px 6px;font-size:12px}
  .sep{width:1px;height:20px;background:var(--line)}
  #liveLbl{display:flex;align-items:center;gap:5px;cursor:pointer;color:var(--dim);font-size:12px}
  #tsTextLbl{display:flex;align-items:center;gap:4px;cursor:pointer;color:var(--dim);font-size:12px}
  #liveLbl.on{color:var(--ok)}
  .dot{width:7px;height:7px;border-radius:50%;background:var(--ok);display:none;
       animation:pulse 1s infinite}
  #liveLbl.on .dot{display:inline-block}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
  #optSeg{display:inline-flex}
  #optSeg.off{opacity:.35;pointer-events:none}
  .seg{border-radius:0;border-right-width:0;padding:5px 12px}
  .seg:first-child{border-radius:5px 0 0 5px}
  .seg:last-child{border-radius:0 5px 5px 0;border-right-width:1px}
  .seg.on{background:#21304e;border-color:var(--acc);color:#fff;font-weight:600}
  details#whereBox summary{cursor:pointer;color:var(--dim);font-size:12px;user-select:none}
  details#whereBox[open] summary{color:var(--acc)}
  #colWrap{position:relative}
  #colMenu{display:none;position:absolute;top:110%;left:0;z-index:30;background:var(--bg2);
           border:1px solid var(--line);border-radius:6px;box-shadow:0 8px 24px #000a;
           max-height:340px;overflow:auto;min-width:230px;padding:6px}
  #colMenu.open{display:block}
  .cmrow{display:flex;align-items:center;gap:6px;padding:2px 4px;border-radius:4px;cursor:pointer}
  .cmrow:hover{background:var(--bg3)}
  .cmrow span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .cmhead{display:flex;gap:10px;padding:2px 4px 6px;border-bottom:1px solid var(--line);
          margin-bottom:4px;font-size:11px}
  .cmhead a{color:var(--acc);cursor:pointer}

  #err{margin:8px 14px 0;background:#3a1d22;border:1px solid var(--err);color:#ffb4b4;
       padding:8px 12px;border-radius:6px;white-space:pre-wrap;word-break:break-all}

  /* ---------- table ---------- */
  #tableWrap{flex:1;overflow:auto;position:relative}
  table{border-collapse:separate;border-spacing:0;font:12px/1.4 Consolas,"SF Mono",monospace;
        min-width:100%}
  thead th{position:sticky;top:0;z-index:10;background:var(--bg3);color:var(--dim);font-weight:600;
           text-align:left;padding:6px 10px;border-bottom:1px solid var(--line);border-right:1px solid var(--line);
           white-space:nowrap;cursor:pointer;user-select:none}
  thead th:hover{color:var(--txt)}
  thead th.num{text-align:right}
  thead .arrow{color:var(--acc);margin-left:4px}
  tbody td{padding:4px 10px;border-bottom:1px solid #1a2233;border-right:1px solid #1a2233;
           white-space:nowrap;max-width:320px;overflow:hidden;text-overflow:ellipsis}
  tbody tr:hover td{background:#1c2740}
  td.num{color:var(--num);text-align:right}
  td.tmp{color:#b9a6ff}
  td.nil{color:#5b6a85;font-style:italic}
  td.idx{color:var(--dim);background:var(--bg2);position:sticky;left:0;z-index:5}
  #empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
         color:var(--dim);font-size:14px}

  /* ---------- schema drawer ---------- */
  #schema{border-bottom:1px solid var(--line);background:var(--bg2);max-height:42vh;
          overflow:auto;padding:8px 14px 12px}
  .schead{display:flex;align-items:center;gap:10px;font-weight:600;margin-bottom:6px}
  .schead .x{margin-left:auto}
  #schBody table{font:12px Consolas,monospace;min-width:0}
  #schBody thead th{cursor:default}
  #schBody td{max-width:420px}

  /* ---------- pager ---------- */
  #pager{display:flex;align-items:center;gap:8px;padding:8px 14px;border-top:1px solid var(--line);
         background:var(--bg2);color:var(--dim);font-size:12px;flex-wrap:wrap}
  #pager .grow{flex:1}
  #pager b{color:var(--txt)}
  #status{color:var(--dim);font-size:11px}
</style>
</head>
<body>
<div id="app">
  <aside id="side">
    <div class="sidehead"><span>PARQUET&nbsp;VIEWER</span><button id="treeRefresh" title="refresh tree">⟳</button></div>
    <div id="rootBox">
      <input id="rootInput" list="rootHistory" spellcheck="false" placeholder="folder path…" title="folder to browse">
      <datalist id="rootHistory"></datalist>
      <div id="rootBtns">
        <button id="rootGo">Go</button>
        <button id="rootUp" title="up one level">↑ up</button>
        <button id="rootPick" title="pick a folder with the native Windows dialog">📁 choose…</button>
      </div>
    </div>
    <div id="fileFilterBox">
      <input id="fileSearch" type="text" placeholder="search files…" spellcheck="false">
      <div id="fileSegRow">
        <div id="fileSeg" title="show only CE or PE files (by name)">
          <button class="fseg on" data-v="">All</button><button class="fseg" data-v="CE">CE</button><button class="fseg" data-v="PE">PE</button>
        </div>
        <select id="strikeSel" title="filter files by strike price (detected in file names)">
          <option value="">All strikes</option>
        </select>
      </div>
    </div>
    <div id="tree"></div>
    <div class="sidefoot">read-only · localhost · any folder · v__VER__</div>
  </aside>

  <main id="main">
    <div id="hdr">
      <div id="title">No file selected</div>
      <div id="badges"></div>
    </div>

    <div id="toolbar">
      <input type="text" id="q" placeholder="search all columns…">
      <button id="qBtn">Search</button>
      <button id="qClr" title="clear search">✕</button>
      <span id="optWrap"><span class="sep"></span>
        <span id="optSeg" title="filter by option type (option_type column)">
          <button class="seg on" data-v="">All</button><button class="seg" data-v="CE">CE</button><button class="seg" data-v="PE">PE</button>
        </span>
      </span>
      <span class="sep"></span>
      <details id="whereBox">
        <summary>SQL filter</summary>
        <div style="display:flex;gap:6px;margin-top:6px">
          <input type="text" id="where" placeholder='e.g.  aggressor = &quot;B&quot; AND ltp &gt; 100' spellcheck="false">
          <button id="whereBtn">Apply</button>
          <button id="whereClr">Clear</button>
        </div>
      </details>
      <span class="sep"></span>
      <span style="color:var(--dim);font-size:12px">rows/page</span>
      <select id="psize">
        <option>25</option><option>50</option><option selected>100</option>
        <option>200</option><option>500</option>
      </select>
      <span class="sep"></span>
      <div id="colWrap">
        <button id="colBtn">Columns ▾</button>
        <div id="colMenu"></div>
      </div>
      <button id="csvBtn" title="export current view (max 1M rows)">Export CSV</button>
      <label id="tsTextLbl" title="write timestamps as spreadsheet text so the time of day always shows. Untick for code-friendly exports (pandas, backtests).">
        <input type="checkbox" id="tsText" checked> ts as text
      </label>
      <label id="liveLbl" title="re-fetch automatically (collector flushes every ~5 s)">
        <input type="checkbox" id="live"><span class="dot"></span> live
      </label>
      <span class="sep"></span>
      <button id="schBtn" title="show schema &amp; stats">Schema</button>
    </div>

    <div id="err" hidden></div>

    <div id="schema" hidden>
      <div class="schead">Schema
        <button id="statsBtn" title="min / max / nulls per column (one full scan)">compute stats</button>
        <button class="x" id="schClose">close</button>
      </div>
      <div id="schBody"><i style="color:var(--dim)">Loading…</i></div>
    </div>

    <div id="tableWrap">
      <div id="empty">Select a parquet file in the sidebar — or a folder to view it as a dataset.</div>
      <table id="tbl" hidden><thead></thead><tbody></tbody></table>
    </div>

    <div id="pager">
      <button id="pFirst" title="first page">⏮</button>
      <button id="pPrev" title="previous (←)">◀</button>
      <span id="pInfo">—</span>
      <button id="pNext" title="next (→)">▶</button>
      <button id="pLast" title="last page">⏭</button>
      <span class="grow"></span>
      <span id="status"></span>
    </div>
  </main>
</div>

<script>
"use strict";
const $ = id => document.getElementById(id);
const NUMERIC = new Set(["TINYINT","SMALLINT","INTEGER","BIGINT","HUGEINT","UTINYINT","USMALLINT",
  "UINTEGER","UBIGINT","FLOAT","DOUBLE","DECIMAL","REAL"]);
const TEMPORAL = ["TIMESTAMP","DATE","TIME"];

const S = { file:null, info:null, offset:0, limit:100, sort:null, dir:"asc",
            q:"", where:"", opt:"", optCol:null, visible:null, liveTimer:null, liveTicks:0, root:null,
            fq:"", fside:"", fstrike:"", treeData:null };
const OPT_COLS = ["option_type", "side", "opt_type"];

function fmtSize(n){
  if (n == null) return "?";
  if (n < 1024) return n + " B";
  const u = ["KB","MB","GB","TB"]; let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < u.length-1);
  return n.toFixed(n < 10 ? 2 : 1) + " " + u[i];
}
function fmtInt(n){ return (n===null||n===undefined) ? "?" : Number(n).toLocaleString("en-US"); }
function showErr(m){ const e=$("err"); if(!m){e.hidden=true;e.textContent="";} else {e.hidden=false;e.textContent=m;} }
function clsFor(t){
  const up = (t||"").toUpperCase();
  if ([...NUMERIC].some(x => up.startsWith(x))) return "num";
  if (TEMPORAL.some(x => up.startsWith(x))) return "tmp";
  return "";
}

/* ------------------------------------------------ tree ------------------------------------------------ */
function rememberRoot(root){
  let h = [];
  try { h = JSON.parse(localStorage.getItem("pv.roots") || "[]"); } catch(e) {}
  h = [root, ...h.filter(x => x !== root)].slice(0, 8);
  try { localStorage.setItem("pv.roots", JSON.stringify(h)); } catch(e) {}
  const dl = $("rootHistory"); dl.innerHTML = "";
  h.forEach(p => { const o = document.createElement("option"); o.value = p; dl.appendChild(o); });
}
async function loadTree(path){
  const r = await fetch("/api/tree" + (path ? "?path=" + encodeURIComponent(path) : ""));
  const j = await r.json();
  if (j.error) return showErr(j.error);
  showErr(null);
  S.root = j.root;
  S.treeData = j.tree;
  $("rootInput").value = j.root;
  rememberRoot(j.root);
  fillStrikeOptions();
  renderTree();
}
/* strikes appear in tick-lake names as NIFTY<strike>CE/PE or strike=<n> partitions */
const STRIKE_RES = [/(?:strike[=_-])(\d{3,6})/i, /(\d{3,6})(?=CE|PE(?=[._\- ]|$))/i];
function strikesIn(name){
  const out = [];
  for (const re of STRIKE_RES){
    const m = name.match(re);
    if (m) out.push(m[1]);
  }
  return out;
}
function fillStrikeOptions(){
  const sel = $("strikeSel");
  const cur = sel.value;
  const strikes = new Set();
  if (S.treeData){
    (function walk(node){
      (node.files || []).forEach(f => strikesIn(f.name).forEach(s => strikes.add(s)));
      (node.dirs || []).forEach(walk);
    })(S.treeData);
  }
  sel.innerHTML = "";
  const all = document.createElement("option");
  all.value = ""; all.textContent = "All strikes";
  sel.appendChild(all);
  [...strikes].sort((a, b) => Number(a) - Number(b)).forEach(s => {
    const o = document.createElement("option");
    o.value = s; o.textContent = s;
    sel.appendChild(o);
  });
  if (cur && strikes.has(cur)) sel.value = cur;
  else S.fstrike = "";
  sel.style.display = strikes.size ? "" : "none";
}
/* sidebar file filters: free-text search + CE/PE name filter (flat match view) */
function fileMatches(f, folderName){
  const fq = S.fq.toLowerCase();
  if (fq && !(f.name.toLowerCase().includes(fq) || (folderName || "").toLowerCase().includes(fq))) return false;
  if (S.fside){
    // option type is the symbol suffix: NIFTY24900CE.part / ...CE.parquet
    const re = new RegExp(S.fside + "(?=[._\\- ]|$)");
    if (!re.test(f.name)) return false;
  }
  if (S.fstrike && !strikesIn(f.name).includes(S.fstrike)) return false;
  return true;
}
function renderTree(){
  const el = $("tree"); el.innerHTML = "";
  if (!S.treeData) return;
  if (S.fq || S.fside){
    const files = [];
    (function walk(node){
      (node.files || []).forEach(f => { if (fileMatches(f, node.name)) files.push(f); });
      (node.dirs || []).forEach(walk);
    })(S.treeData);
    const info = document.createElement("div"); info.className = "filterInfo";
    info.textContent = files.length + " file(s) match" +
      (S.fstrike ? " strike " + S.fstrike : "") +
      (S.fside ? " \u00b7 " + S.fside : "") +
      (S.fq ? " \u00b7 \"" + S.fq + "\"" : "");
    el.appendChild(info);
    files.forEach(f => {
      const fr = document.createElement("div"); fr.className = "trow"; fr.dataset.path = f.path;
      const c = document.createElement("span"); c.className = "caret";
      const nm = document.createElement("span"); nm.className = "dlabel"; nm.textContent = f.name; nm.title = f.path;
      const sz = document.createElement("span"); sz.className = "fsize"; sz.textContent = fmtSize(f.size);
      fr.append(c, nm, sz);
      fr.onclick = () => selectFile(f.path);
      el.appendChild(fr);
    });
    syncSel();
    return;
  }
  el.appendChild(renderNode(S.treeData, true));
  syncSel();
}
function renderNode(node, expanded){
  const wrap = document.createElement("div");
  const row = document.createElement("div"); row.className = "trow";
  const caret = document.createElement("span"); caret.className = "caret";
  caret.textContent = node.dirs.length ? (expanded ? "▾" : "▸") : "";
  row.appendChild(caret);
  const lab = document.createElement("span"); lab.className = "dlabel"; lab.textContent = node.name;
  row.appendChild(lab);
  const ds = document.createElement("button"); ds.className = "dsbtn";
  ds.textContent = "⤢"; ds.title = "view this folder as a dataset (all .parquet under it)";
  ds.onclick = ev => { ev.stopPropagation(); selectFile(node.path); };
  row.appendChild(ds);
  wrap.appendChild(row);
  row.onclick = () => { node._open = !node._open; renderKids(); syncSel(); };
  row.dataset.path = node.path;

  const kids = document.createElement("div"); kids.className = "tkids";
  if (!expanded) kids.style.display = "none";
  wrap.appendChild(kids);

  function renderKids(){
    kids.innerHTML = "";
    caret.textContent = node.dirs.length ? (node._open ? "▾" : "▸") : "";
    if (!node._open) { kids.style.display = "none"; return; }
    kids.style.display = "";
    node.dirs.forEach(d => kids.appendChild(renderNode(d, false)));
    (node.files || []).forEach(f => {
      const fr = document.createElement("div"); fr.className = "trow"; fr.dataset.path = f.path;
      const c = document.createElement("span"); c.className = "caret"; c.textContent = "";
      const nm = document.createElement("span"); nm.className = "dlabel"; nm.textContent = f.name; nm.title = f.name;
      const sz = document.createElement("span"); sz.className = "fsize"; sz.textContent = fmtSize(f.size);
      fr.append(c, nm, sz);
      fr.onclick = () => selectFile(f.path);
      kids.appendChild(fr);
    });
  }
  node._open = expanded; renderKids();
  return wrap;
}
function syncSel(){
  document.querySelectorAll(".trow").forEach(el =>
    el.classList.toggle("sel", S.file && el.dataset.path === S.file));
  const sel = document.querySelector(".trow.sel");
  if (sel) sel.scrollIntoView({block:"nearest"});
}

/* ------------------------------------------------ info ------------------------------------------------ */
async function selectFile(path){
  S.file = path; S.offset = 0; S.sort = null; S.dir = "asc"; S.visible = null; S.opt = ""; S.optCol = null;
  syncSeg();
  showErr(null);
  $("title").textContent = path;
  $("empty").style.display = "none";
  $("tbl").hidden = false;
  syncSel();
  await loadInfo();
  if (S.info && !S.info.error) await fetchData();
}
async function loadInfo(silent){
  const r = await fetch("/api/info?file=" + encodeURIComponent(S.file));
  const j = await r.json();
  if (j.error){
    if (!silent) showErr(j.error);
    return;
  }
  S.info = j;
  const optCol = j.cols.map(c => c.name)
    .find(n => OPT_COLS.includes(n.toLowerCase()));
  S.optCol = optCol || null;
  const seg = $("optSeg");
  seg.classList.toggle("off", !S.optCol);
  seg.title = S.optCol ? "filter CE/PE by column \"" + S.optCol + "\""
                       : "no CE/PE column (option_type / side) in this file — filter unavailable";
  if (!S.optCol) S.opt = "";
  syncSeg();
  const b = $("badges"); b.innerHTML = "";
  const add = (html, title) => {
    const s = document.createElement("span"); s.className = "badge";
    s.innerHTML = html; if (title) s.title = title; b.appendChild(s);
  };
  add("<b>" + fmtInt(j.rows) + "</b> rows");
  add("<b>" + j.cols.length + "</b> columns");
  add("<b>" + fmtSize(j.size) + "</b>");
  if (j.is_dataset) add("<b>" + j.n_files + "</b> files (dataset)");
  if (j.hive_dropped) add("⚠ partition cols dropped", "files at mixed partition depths — showing file contents only");
  if (j.row_groups != null) add("<b>" + j.row_groups + "</b> row groups");
  if (j.compression) add(j.compression);
  if (j.created_by) add(j.created_by, "parquet writer");
  renderColMenu();
  renderSchema();
}

/* ------------------------------------------------ data ------------------------------------------------ */
function whereClause(){
  const parts = [];
  if (S.opt && S.optCol){
    const id = '"' + S.optCol.replace(/"/g, '""') + '"';
    parts.push("UPPER(" + id + ") = '" + S.opt + "'");
  }
  if (S.where) parts.push("(" + S.where + ")");
  return parts.join(" AND ");
}
function pageQS(extra){
  const p = new URLSearchParams();
  p.set("file", S.file); p.set("offset", S.offset); p.set("limit", S.limit);
  if (S.sort){ p.set("sort", S.sort); p.set("dir", S.dir); }
  if (S.q) p.set("q", S.q);
  const w = whereClause();
  if (w) p.set("where", w);
  if (S.visible) p.set("cols", [...S.visible].join(","));
  if (extra) Object.entries(extra).forEach(([k,v]) => p.set(k, v));
  return p.toString();
}
async function fetchData(silent){
  if (!S.file) return;
  if (!silent) showErr(null);
  $("status").textContent = "loading…";
  try{
    const r = await fetch("/api/data?" + pageQS());
    const j = await r.json();
    if (j.error){ showErr(j.error); $("status").textContent = ""; return; }
    renderTable(j);
    const pages = Math.max(1, Math.ceil(j.total / S.limit));
    $("pInfo").innerHTML = "rows <b>" + fmtInt(j.offset + 1) + "–" +
      fmtInt(j.offset + j.rows.length) + "</b> of <b>" + fmtInt(j.total) +
      "</b> · page <b>" + (Math.floor(j.offset / S.limit) + 1) + "/" + pages + "</b>";
    $("status").textContent = j.took_ms + " ms" + (silent ? " · auto" : "");
  }catch(e){
    if (!silent) showErr(String(e));
    $("status").textContent = "";
  }
}
function renderTable(j){
  const head = $("tbl").tHead, body = $("tbl").tBodies[0];
  const types = j.types || {};
  const colCls = j.cols.map(c => clsFor(types[c]));
  head.innerHTML = "";
  const hr = head.insertRow();
  const th0 = document.createElement("th"); th0.textContent = "#"; hr.appendChild(th0);
  j.cols.forEach(c => {
    const th = document.createElement("th");
    if (colCls[j.cols.indexOf(c)] === "num") th.classList.add("num");
    th.textContent = c;
    th.title = (types[c] || "") + " — click to sort";
    if (S.sort === c){
      const a = document.createElement("span"); a.className = "arrow";
      a.textContent = S.dir === "asc" ? "▲" : "▼"; th.appendChild(a);
    }
    th.onclick = () => {
      if (S.sort === c) S.dir = S.dir === "asc" ? "desc" : "asc";
      else { S.sort = c; S.dir = "asc"; }
      if (S.sort === c && S.dir === "desc" && !th.dataset.t) { /* second click = desc */ }
      S.offset = 0; fetchData();
    };
    hr.appendChild(th);
  });

  const frag = document.createDocumentFragment();
  j.rows.forEach((row, i) => {
    const tr = document.createElement("tr");
    const idx = document.createElement("td"); idx.className = "idx";
    idx.textContent = fmtInt(j.offset + i + 1); tr.appendChild(idx);
    row.forEach((v, k) => {
      const td = document.createElement("td");
      td.className = colCls[k] || "";
      if (v === null || v === undefined){ td.classList.add("nil"); td.textContent = "∅"; }
      else {
        td.textContent = String(v);
        td.title = String(v);
      }
      tr.appendChild(td);
    });
    frag.appendChild(tr);
  });
  body.innerHTML = ""; body.appendChild(frag);
}

/* ------------------------------------------------ schema drawer -------------------------------------- */
async function renderSchema(){
  const box = $("schBody");
  if (!S.info){ box.innerHTML = ""; return; }
  const cols = S.info.cols;
  const t = document.createElement("table");
  const hr = t.tHead ? t.tHead.insertRow() : t.createTHead().insertRow();
  ["column","type","nullable","nulls","min","max"].forEach(h => {
    const th = document.createElement("th"); th.textContent = h; hr.appendChild(th);
  });
  const tb = t.createTBody();
  cols.forEach(c => {
    const tr = tb.insertRow();
    const put = (v, cls) => { const td = tr.insertCell(); if (cls) td.className = cls; td.textContent = v; };
    put(c.name); put(c.type); put(c.nullable ? "yes" : "no");
    put("", "num"); put("", "num"); put("", "num");
    tr.dataset.col = c.name;
  });
  box.innerHTML = ""; box.appendChild(t);
}
async function computeStats(){
  const btn = $("statsBtn"); btn.disabled = true; btn.textContent = "computing…";
  try{
    const w = whereClause();
    const r = await fetch("/api/stats?file=" + encodeURIComponent(S.file) +
      (w ? "&where=" + encodeURIComponent(w) : ""));
    const j = await r.json();
    if (j.error){ showErr(j.error); return; }
    const byName = Object.fromEntries(j.stats.map(s => [s.name, s]));
    $("schBody").querySelectorAll("tr[data-col]").forEach(tr => {
      const s = byName[tr.dataset.col]; if (!s) return;
      const cells = tr.cells;
      cells[3].textContent = s.nulls === null ? "?" : fmtInt(s.nulls);
      cells[4].textContent = s.min === null || s.min === undefined ? "∅" : String(s.min);
      cells[5].textContent = s.max === null || s.max === undefined ? "∅" : String(s.max);
    });
    $("status").textContent = "stats in " + j.took_ms + " ms";
  } finally { btn.disabled = false; btn.textContent = "compute stats"; }
}

/* ------------------------------------------------ column menu ---------------------------------------- */
function renderColMenu(){
  const menu = $("colMenu"); menu.innerHTML = "";
  if (!S.info) return;
  const hd = document.createElement("div"); hd.className = "cmhead";
  const mk = (txt, fn) => { const a = document.createElement("a"); a.textContent = txt; a.onclick = fn; return a; };
  hd.append(mk("all", () => setCols(null)), mk("none", () => setCols([])));
  menu.appendChild(hd);
  S.info.cols.forEach(c => {
    const row = document.createElement("div"); row.className = "cmrow";
    const cb = document.createElement("input"); cb.type = "checkbox";
    cb.checked = !S.visible || S.visible.has(c.name);
    cb.onchange = () => {
      const vis = S.visible ? new Set(S.visible) : new Set(S.info.cols.map(x => x.name));
      cb.checked ? vis.add(c.name) : vis.delete(c.name);
      setCols([...vis]);
    };
    const sp = document.createElement("span"); sp.textContent = c.name; sp.title = c.type;
    const ty = document.createElement("span"); ty.style.cssText = "color:var(--dim);font-size:10.5px;margin-left:auto";
    ty.textContent = c.type;
    row.append(cb, sp, ty); menu.appendChild(row);
  });
}
function setCols(list){
  S.visible = list && list.length ? new Set(list) : (list ? new Set() : null);
  if (S.visible && S.visible.size === 0) S.visible = new Set();
  if (S.visible) S.visible.size === 0 && renderColMenu(); else renderColMenu();
  fetchData();
}

/* ------------------------------------------------ wiring --------------------------------------------- */
function page(delta){
  if (!S.info) return;
  const total = S.info.rows, pages = Math.max(1, Math.ceil(total / S.limit));
  const cur = Math.floor(S.offset / S.limit);
  goPage(Math.min(pages - 1, Math.max(0, cur + delta)));
}
function goPage(p){ S.offset = p * S.limit; fetchData(); }

function syncSeg(){
  document.querySelectorAll(".seg").forEach(b => b.classList.toggle("on", b.dataset.v === S.opt));
}
$("optSeg").addEventListener("click", e => {
  if (!S.file || $("optSeg").classList.contains("off")) return;
  const b = e.target.closest(".seg"); if (!b) return;
  S.opt = b.dataset.v; S.offset = 0;
  syncSeg(); fetchData();
});
$("qBtn").onclick = () => { S.q = $("q").value.trim(); S.offset = 0; fetchData(); };
$("q").addEventListener("keydown", e => { if (e.key === "Enter") $("qBtn").click(); });
$("qClr").onclick = () => { $("q").value = ""; S.q = ""; S.offset = 0; fetchData(); };
$("whereBtn").onclick = () => { S.where = $("where").value.trim(); S.offset = 0; fetchData(); };
$("where").addEventListener("keydown", e => { if (e.key === "Enter") $("whereBtn").click(); });
$("whereClr").onclick = () => { $("where").value = ""; S.where = ""; S.offset = 0; fetchData(); };
$("psize").onchange = () => { S.limit = +$("psize").value; S.offset = 0; fetchData(); };
$("pFirst").onclick = () => goPage(0);
$("pPrev").onclick = () => page(-1);
$("pNext").onclick = () => page(1);
$("pLast").onclick = () => { if (S.info) goPage(Math.max(0, Math.ceil(S.info.rows / S.limit) - 1)); };
document.addEventListener("keydown", e => {
  if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
  if (e.key === "ArrowLeft") page(-1);
  if (e.key === "ArrowRight") page(1);
});
$("treeRefresh").onclick = () => loadTree();
let _fqTimer = null;
$("fileSearch").addEventListener("input", () => {
  clearTimeout(_fqTimer);
  _fqTimer = setTimeout(() => { S.fq = $("fileSearch").value.trim(); renderTree(); }, 180);
});
$("fileSearch").addEventListener("keydown", e => {
  if (e.key === "Escape"){ $("fileSearch").value = ""; S.fq = ""; renderTree(); }
});
$("fileSeg").addEventListener("click", e => {
  const b = e.target.closest(".fseg"); if (!b) return;
  S.fside = b.dataset.v;
  document.querySelectorAll(".fseg").forEach(x => x.classList.toggle("on", x.dataset.v === S.fside));
  renderTree();
});
$("strikeSel").addEventListener("change", () => {
  S.fstrike = $("strikeSel").value;
  renderTree();
});
$("rootGo").onclick = () => loadTree($("rootInput").value.trim());
$("rootInput").addEventListener("keydown", e => { if (e.key === "Enter") $("rootGo").click(); });
$("rootUp").onclick = () => { if (S.root) loadTree(S.root + "/.."); };
$("rootPick").onclick = async () => {
  const b = $("rootPick"), t = b.textContent;
  b.disabled = true; b.textContent = "pick on screen…";
  try{
    const r = await fetch("/api/pick"); const j = await r.json();
    if (j.error) showErr(j.error);
    else if (j.picked) await loadTree();
  } finally { b.disabled = false; b.textContent = t; }
};
$("csvBtn").onclick = () => {
  if (!S.file) return;
  const ts = "&ts_text=" + ($("tsText").checked ? "1" : "0");
  window.open("/api/export?" + pageQS({limit: 1000000}) + ts);
};
$("schBtn").onclick = () => { $("schema").hidden = !$("schema").hidden; };
$("schClose").onclick = () => { $("schema").hidden = true; };
$("statsBtn").onclick = computeStats;
$("colBtn").onclick = e => { e.stopPropagation(); $("colMenu").classList.toggle("open"); };
document.addEventListener("click", e => {
  if (!$("colWrap").contains(e.target)) $("colMenu").classList.remove("open");
});
$("live").onchange = () => {
  const on = $("live").checked;
  $("liveLbl").classList.toggle("on", on);
  clearInterval(S.liveTimer);
  if (on){
    S.liveTimer = setInterval(() => {
      S.liveTicks++;
      if (S.liveTicks % 10 === 0) loadInfo(true);
      fetchData(true);
    }, 2000);
    fetchData(true);
  }
};

(async function init(){
  await loadTree();
  const r = await fetch("/api/config"); const cfg = await r.json();
  if (cfg.initial) selectFile(cfg.initial);
})();
</script>
</body>
</html>
"""

# --------------------------------------------------------------------------- main

def _pick_port(preferred: int) -> int:
    for p in range(preferred, preferred + 20):
        try:
            ThreadingHTTPServer(("127.0.0.1", p), Handler).server_close()
            return p
        except OSError:
            continue
    raise SystemExit(f"no free port found at or above {preferred}")


def main() -> None:
    global ROOT, INITIAL, CURRENT_ROOT
    ap = argparse.ArgumentParser(description="local read-only parquet viewer")
    ap.add_argument("path", nargs="?", help="parquet file or directory (default: <project>/data)")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    if args.path:
        ROOT = os.path.abspath(args.path)
    elif getattr(sys, "frozen", False):
        # running as NIFTY_ParquetViewer.exe — browse next to the exe
        base = os.path.dirname(os.path.abspath(sys.executable))
        cand = os.path.join(base, "data")
        ROOT = cand if os.path.isdir(cand) else base
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project = os.path.dirname(script_dir)
        cand = os.path.join(project, "data")
        ROOT = cand if os.path.isdir(cand) else os.getcwd()
    if not os.path.exists(ROOT):
        raise SystemExit(f"path does not exist: {ROOT}")
    ROOT = os.path.realpath(ROOT)
    CURRENT_ROOT = ROOT
    if os.path.isfile(ROOT):
        INITIAL = ROOT
        ROOT = os.path.dirname(ROOT)

    port = _pick_port(args.port)
    url = f"http://127.0.0.1:{port}/"
    print(f"Parquet Viewer v{VERSION}")
    print(f"  root : {ROOT}")
    if INITIAL:
        print(f"  open : {INITIAL}")
    print(f"  url  : {url}   (Ctrl-C to stop)")

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    if not args.no_browser:
        threading.Timer(0.4, webbrowser.open, [url]).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
