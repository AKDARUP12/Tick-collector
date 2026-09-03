#!/usr/bin/env python3
"""
strike_finder.py — separate tool: find parquet files by STRIKE and CE/PE. Nothing else.

Double-click / no arguments  ->  small window: folder picker, strike dropdown,
                                All/CE/PE buttons, search box, CHECKBOX file list,
                                select all/clear, copy-checked-to-folder
With arguments               ->  command-line list mode, e.g.
    python tools/strike_finder.py "D:\\data" --strike 24900 --side CE

File naming dialects understood (strike/side read from the NAME):
    token=10001-NIFTY24900CE.part=000001.parquet   old/dry-run  -> 24900 CE
    token=46989-NIFTY01SEP26C24100.parquet         live 26/08+  -> 24100 CE
    .../strike=25100/...                           partitions   -> 25100
Stdlib only. Read-only until you explicitly copy files out.
"""
from __future__ import annotations

import argparse
import ctypes
import os
import re
import shutil
import sys
from pathlib import Path

STRIKE_RES = [
    re.compile(r"(?:strike[=_-])(\d{3,6})", re.IGNORECASE),
    re.compile(r"(\d{3,6})(?=CE|PE(?=[._\- ]|$))", re.IGNORECASE),
    re.compile(r"(?<=[CP])(\d{3,6})(?![0-9])"),          # NIFTY01SEP26C24000
]


def strikes_in(name: str) -> list[str]:
    out = []
    for rx in STRIKE_RES:
        m = rx.search(name)
        if m:
            out.append(m.group(1))
    return out


def side_in(name: str) -> str:
    if re.search(r"CE(?=[._\- ]|$)", name):
        return "CE"
    if re.search(r"PE(?=[._\- ]|$)", name):
        return "PE"
    if re.search(r"(?<![A-Za-z])C(?=\d)", name):
        return "CE"
    if re.search(r"(?<![A-Za-z])P(?=\d)", name):
        return "PE"
    return ""


def scan(folder: str) -> list[dict]:
    files = []
    for dirpath, dirnames, filenames in os.walk(folder):
        dirnames.sort()
        for fn in sorted(filenames):
            if fn.lower().endswith(".parquet"):
                p = os.path.join(dirpath, fn)
                try:
                    size = os.path.getsize(p)
                except OSError:
                    size = 0
                files.append({
                    "path": p,
                    "rel": os.path.relpath(p, folder),
                    "size": size,
                    "strikes": strikes_in(fn),
                    "side": side_in(fn),
                })
    return files


def apply_filters(files: list[dict], strike: str, side: str, q: str) -> list[dict]:
    q = (q or "").lower()
    out = []
    for f in files:
        if strike and strike not in f["strikes"]:
            continue
        if side and f["side"] != side:
            continue
        if q and q not in f["rel"].lower():
            continue
        out.append(f)
    return out


def fmt_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    x = float(n)
    for u in ("KB", "MB", "GB", "TB"):
        x /= 1024
        if x < 1024:
            return f"{x:.1f} {u}"
    return f"{x:.1f} TB"


def all_strikes(files: list[dict]) -> list[str]:
    s = set()
    for f in files:
        s.update(f["strikes"])
    return sorted(s, key=int)


SETTING_FILE = Path(os.environ.get("LOCALAPPDATA") or Path.home()) / "NIFTYStudio" / "strike_finder_folder.txt"


# ------------------------------------------------------------------ GUI

def run_gui() -> None:
    import tkinter as tk
    from tkinter import filedialog, ttk

    if getattr(sys, "frozen", False):  # built exe: hide the console window
        try:
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if hwnd:
                ctypes.windll.user32.ShowWindow(hwnd, 0)
        except Exception:
            pass

    state = {"folder": "", "files": []}
    checked = set()          # rel paths the user ticked (survives re-filtering)

    root = tk.Tk()
    root.title("Strike Finder — parquet files by strike / CE-PE")
    root.geometry("860x560")
    root.minsize(640, 400)

    try:
        ttk.Style(root).theme_use("vista")
    except Exception:
        pass

    # --- row 1: folder
    top = ttk.Frame(root, padding=(8, 8, 8, 4))
    top.pack(fill="x")
    folder_var = tk.StringVar(value="")

    def pick_folder():
        start = state["folder"] or str(Path.home())
        chosen = filedialog.askdirectory(initialdir=start, title="Choose folder to scan")
        if chosen:
            load_folder(chosen)

    def load_folder(folder: str):
        state["folder"] = folder
        folder_var.set(folder)
        checked.clear()
        try:
            SETTING_FILE.parent.mkdir(parents=True, exist_ok=True)
            SETTING_FILE.write_text(folder, encoding="utf-8")
        except OSError:
            pass
        state["files"] = scan(folder)
        strikes = all_strikes(state["files"])
        strike_box["values"] = ["All strikes"] + strikes
        strike_box.current(0)
        refresh()

    ttk.Button(top, text="Folder…", command=pick_folder).pack(side="left")
    ttk.Label(top, textvariable=folder_var, foreground="#555").pack(side="left", padx=8, fill="x", expand=True)

    # --- row 2: filters
    frow = ttk.Frame(root, padding=(8, 2, 8, 4))
    frow.pack(fill="x")
    ttk.Label(frow, text="search").pack(side="left")
    q_var = tk.StringVar()
    q_var.trace_add("write", lambda *_: refresh())
    ttk.Entry(frow, textvariable=q_var, width=22).pack(side="left", padx=(4, 12))

    ttk.Label(frow, text="strike").pack(side="left")
    strike_box = ttk.Combobox(frow, state="readonly", width=12, values=["All strikes"])
    strike_box.current(0)
    strike_box.bind("<<ComboboxSelected>>", lambda _: refresh())
    strike_box.pack(side="left", padx=(4, 12))

    side_var = tk.StringVar(value="")
    for label, val in (("All", ""), ("CE", "CE"), ("PE", "PE")):
        ttk.Radiobutton(frow, text=label, value=val, variable=side_var,
                        command=lambda: refresh()).pack(side="left", padx=2)

    # --- row 3: selection actions + checked counter
    arow = ttk.Frame(root, padding=(8, 2, 8, 2))
    arow.pack(fill="x")

    # --- file table (checkbox column + name + size)
    mid = ttk.Frame(root, padding=(8, 0, 8, 4))
    mid.pack(fill="both", expand=True)
    cols = ("sel", "file", "size")
    tree = ttk.Treeview(mid, columns=cols, show="headings", selectmode="none")
    tree.heading("sel", text="select")
    tree.heading("file", text="file")
    tree.heading("size", text="size")
    tree.column("sel", width=60, anchor="center", stretch=False)
    tree.column("file", width=560, anchor="w")
    tree.column("size", width=90, anchor="e", stretch=False)
    sb = ttk.Scrollbar(mid, command=tree.yview)
    tree.config(yscrollcommand=sb.set)
    tree.pack(side="left", fill="both", expand=True)
    sb.pack(side="right", fill="y")

    status = tk.StringVar(value="choose a folder to begin")
    checked_lbl = tk.StringVar(value="")

    def current_matches():
        strike = "" if strike_box.get() == "All strikes" else strike_box.get()
        return apply_filters(state["files"], strike, side_var.get(), q_var.get())

    def update_checked_label():
        files = {f["rel"]: f for f in state["files"]}
        sel = [files[r] for r in checked if r in files]
        tot = sum(f["size"] for f in sel)
        checked_lbl.set(f"{len(sel)} checked  ({fmt_size(tot)})" if sel else "nothing checked")

    def refresh():
        if not state["folder"]:
            return
        matches = current_matches()
        tree.delete(*tree.get_children())
        for f in matches:
            mark = "\u2611" if f["rel"] in checked else "\u2610"
            tree.insert("", "end", iid=f["rel"], values=(mark, f["rel"], fmt_size(f["size"])))
        strike = "" if strike_box.get() == "All strikes" else strike_box.get()
        bits = [f"{len(matches)} of {len(state['files'])} files"]
        if strike:
            bits.append(f"strike {strike}")
        if side_var.get():
            bits.append(side_var.get())
        if q_var.get():
            bits.append(f'"{q_var.get()}"')
        status.set("  ".join(bits))
        update_checked_label()

    def toggle(rel: str):
        if rel in checked:
            checked.discard(rel)
            tree.set(rel, "sel", "\u2610")
        else:
            checked.add(rel)
            tree.set(rel, "sel", "\u2611")
        update_checked_label()

    def on_click(_ev):
        row = tree.identify_row(_ev.y)
        if row:
            toggle(row)

    def on_space(_ev):
        row = tree.focus()
        if row:
            toggle(row)
            return "break"

    tree.bind("<Button-1>", on_click)
    tree.bind("<space>", on_space)

    def select_all():
        for f in current_matches():
            checked.add(f["rel"])
            tree.set(f["rel"], "sel", "\u2611")
        update_checked_label()

    def clear_sel():
        checked.clear()
        for iid in tree.get_children():
            tree.set(iid, "sel", "\u2610")
        update_checked_label()

    ttk.Button(arow, text="Select all", command=select_all).pack(side="left")
    ttk.Button(arow, text="Clear checks", command=clear_sel).pack(side="left", padx=(6, 12))
    ttk.Label(arow, textvariable=checked_lbl, foreground="#1a5276").pack(side="left")

    def copy_checked():
        files = {f["rel"]: f for f in state["files"]}
        sel = [files[r] for r in checked if r in files]
        if not sel:
            checked_lbl.set("check files first — click rows to tick them")
            return
        dest = filedialog.askdirectory(title="Copy checked files to…")
        if not dest:
            return
        ok = 0
        try:
            for f in sel:
                shutil.copy2(f["path"], os.path.join(dest, os.path.basename(f["path"])))
                ok += 1
        except OSError as e:
            checked_lbl.set(f"copy failed after {ok}: {e}")
            return
        checked_lbl.set(f"copied {ok} file(s) -> {dest}")

    bot = ttk.Frame(root, padding=(8, 0, 8, 8))
    bot.pack(fill="x")
    ttk.Button(bot, text="Copy checked to…", command=copy_checked).pack(side="left")
    ttk.Label(bot, text="click a row to tick / untick \u00b7 space bar toggles the focused row",
              foreground="#888").pack(side="left", padx=10)
    ttk.Label(bot, textvariable=status, foreground="#555").pack(side="right")

    try:
        if SETTING_FILE.exists():
            last = SETTING_FILE.read_text(encoding="utf-8").strip()
            if last and os.path.isdir(last):
                load_folder(last)
    except OSError:
        pass

    root.mainloop()


# ------------------------------------------------------------------ CLI

def main() -> None:
    ap = argparse.ArgumentParser(description="find parquet files by strike / CE-PE")
    ap.add_argument("folder", nargs="?", help="folder to scan (no args = open the window)")
    ap.add_argument("-s", "--strike", default="", help="strike price, e.g. 24900")
    ap.add_argument("-d", "--side", default="", choices=["", "CE", "PE"])
    ap.add_argument("-q", "--q", default="", help="text search in file name")
    args = ap.parse_args()

    if not args.folder:
        run_gui()
        return

    files = scan(args.folder)
    matches = apply_filters(files, args.strike, args.side, args.q)
    for f in matches:
        print(f"{f['path']}\t{fmt_size(f['size'])}")
    print(f"-- {len(matches)} of {len(files)} files"
          + (f" | strike {args.strike}" if args.strike else "")
          + (f" | {args.side}" if args.side else "")
          + (f' | "{args.q}"' if args.q else ""))


if __name__ == "__main__":
    main()
