#!/usr/bin/env python3
"""NIFTY Studio - windowed GUI for the tick collector & viewer.

Tabs:
  Collect - start/stop live collection, after-close jobs
  Data    - health check (dupes + volume), Excel export, open folder

Everything prints into the log box. Long jobs run in background threads so the
window stays responsive; live collection stops gracefully via a stop event.
"""
import os
import queue
import sys
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import subprocess
from pathlib import Path

if getattr(sys, "frozen", False):
    ROOT = Path(sys.executable).resolve().parent
else:
    ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

APP_VERSION = "2.2"

SCHEDULE_FILE = ROOT / "schedule_enabled.txt"
STARTUP_LNK = Path(os.environ.get("APPDATA", "")) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup" / "NIFTY Studio.lnk"


def schedule_enabled() -> bool:
    try:
        return SCHEDULE_FILE.exists() and SCHEDULE_FILE.read_text(encoding="utf-8").strip() == "1"
    except Exception:
        return False


def set_schedule_enabled(v: bool):
    try:
        SCHEDULE_FILE.write_text("1" if v else "0", encoding="utf-8")
    except Exception:
        pass


def scheduled_action(now_dt, enabled, running):
    """'start' | 'stop' | None - the auto-collect decision for one moment in time."""
    if not enabled or now_dt.weekday() >= 5:  # Mon-Fri only
        return None
    window_start = datetime(2000, 1, 3, 9, 15).time()
    window_end = datetime(2000, 1, 3, 15, 40).time()
    in_window = window_start <= now_dt.time() < window_end
    if in_window and not running:
        return "start"
    if not in_window and running:
        return "stop"
    return None


def startup_enabled() -> bool:
    return STARTUP_LNK.exists()


def set_startup(v: bool):
    if v:
        target = ROOT / "NIFTY_Studio.exe"
        if not target.exists() and getattr(sys, "frozen", False):
            target = Path(sys.executable)
        if not target.exists():
            raise RuntimeError("NIFTY_Studio.exe not found next to the app")
        ps = (f'$s = New-Object -ComObject WScript.Shell; '
              f'$sc = $s.CreateShortcut("{STARTUP_LNK}"); '
              f'$sc.TargetPath = "{target}"; '
              f'$sc.WorkingDirectory = "{ROOT}"; '
              f'$sc.Save()')
        subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True)
    elif STARTUP_LNK.exists():
        STARTUP_LNK.unlink()

try:
    from paths import data_root, set_data_root
except ImportError:
    def data_root():
        return ROOT / "data"

    def set_data_root(folder):
        from pathlib import Path as _P
        p = _P(folder)
        p.mkdir(parents=True, exist_ok=True)
        os.environ["NIFTY_DATA_DIR"] = str(p)
        return p


def _apply_saved_folder():
    candidates = [ROOT / "data_folder.txt"]
    try:
        from paths import SETTING_FILE
        candidates.append(SETTING_FILE)  # canonical - applied last, wins
    except Exception:
        pass
    for f in candidates:
        if f.exists():
            p = f.read_text(encoding="utf-8").strip()
            if p and Path(p).is_dir():
                os.environ["NIFTY_DATA_DIR"] = p


_apply_saved_folder()
DATA = data_root()
TICKS = DATA / "live" / "ticks"


INDEXES = ["NIFTY", "BANKNIFTY", "SENSEX"]


def _minute_root(index: str) -> Path:
    return DATA / "live/minute" if index == "NIFTY" else DATA / "live/minute" / index


def all_days(index: str = "NIFTY"):
    """Every day we hold data for: live tick days (NIFTY) + backfilled candle days."""
    days = set()
    if index == "NIFTY" and TICKS.exists():
        days |= {p.name.replace("date=", "") for p in TICKS.glob("date=*")}
    try:
        import pandas as pd
        extra = _minute_root(index) / "index.parquet"
        if extra.exists():
            df = pd.read_parquet(extra, columns=["timestamp"])
            days |= {d.isoformat() for d in pd.to_datetime(df["timestamp"]).dt.date.unique()}
    except Exception:
        pass
    return sorted(days, reverse=True)


def latest_day(index: str = "NIFTY") -> str:
    days = all_days(index)
    return days[0] if days else "(none yet)"


def env_path() -> Path:
    return ROOT / ".env"


def env_ok() -> bool:
    env = env_path()
    if not env.exists():
        return False
    txt = env.read_text(errors="ignore")
    pw = ""
    for line in txt.splitlines():
        if line.strip().startswith("ARROW_PASSWORD="):
            pw = line.split("=", 1)[1].strip()
    return bool(pw)


class Tail:
    """File-like that forwards prints from worker threads into the GUI queue."""

    def __init__(self, q: queue.Queue):
        self.q = q

    def write(self, s):
        try:
            if s and s.strip():
                self.q.put(str(s).rstrip())
        except Exception:
            pass  # logging must never crash a job

    def flush(self):
        pass


def run_gui(selftest: bool = False):
    import tkinter as tk
    from tkinter import ttk, messagebox

    root = tk.Tk()
    root.title("NIFTY Studio - Arrow tick collector")
    root.geometry("860x560")
    root.minsize(720, 480)

    q: queue.Queue = queue.Queue()
    state = {"busy": False, "live_thread": None, "stop_event": None}

    schedule_var = tk.BooleanVar(value=schedule_enabled())
    startup_var = tk.BooleanVar(value=startup_enabled())

    def on_toggle_schedule():
        set_schedule_enabled(bool(schedule_var.get()))
        log("auto-collect schedule " + ("ENABLED (09:15-15:40 IST, Mon-Fri)" if schedule_var.get() else "disabled"))

    def on_toggle_startup():
        try:
            set_startup(bool(startup_var.get()))
            log("launch at Windows startup " + ("enabled" if startup_var.get() else "disabled"))
        except Exception as e:
            log(f"could not change startup shortcut: {e}")

    log_box = tk.Text(root, height=10, bg="#111318", fg="#d7dde6", insertbackground="#d7dde6")
    font = ("Consolas", 9)

    def log(line: str):
        log_box.insert("end", line + "\n")
        log_box.see("end")

    def drain():
        while True:
            try:
                line = q.get_nowait()
            except queue.Empty:
                break
            log(line)
        root.after(150, drain)

    old_out, old_err = sys.stdout, sys.stderr
    sys.stdout = Tail(q)   # installed once and never swapped back - the frozen
    sys.stderr = Tail(q)   # app's real console can't print unicode, so all
                           # output must go through the queue permanently.

    def set_busy(b: bool, why: str = ""):
        state["busy"] = b
        status.config(text=why or ("running..." if b else "idle"))
        for w in (btn_live, btn_afterclose, btn_check, btn_export, btn_backfill):
            w.config(state="disabled" if b else "normal")
        btn_stop.config(state="normal" if b and state["live_thread"] else "disabled")

    def worker(fn):
        def wrap():
            try:
                fn()
            except Exception as e:
                log(f"ERROR: {e}")
            finally:
                root.after(0, lambda: set_busy(False))
        threading.Thread(target=wrap, daemon=True).start()

    # ---------- actions ----------
    def do_live():
        if not env_ok():
            log(f"ERROR: live needs credentials in {env_path()}")
            log("       click 'Load .env...' and pick your .env file (it will be copied there).")
            return
        set_busy(True, "LIVE - collecting...")
        import collector

        ev = threading.Event()
        state["stop_event"] = ev

        def job():
            collector.main(stop_event=ev, interactive=False)
            log("live collection stopped - data saved.")

        t = threading.Thread(target=job, daemon=True)
        state["live_thread"] = t
        t.start()
        btn_stop.config(state="normal")

    def do_load_env():
        from tkinter import filedialog
        src = filedialog.askopenfilename(
            title="Pick your .env file (from the project folder)",
            initialfile=".env", parent=root)
        if not src:
            return
        try:
            import shutil
            shutil.copyfile(src, env_path())
        except Exception as e:
            log(f"ERROR copying .env: {e}")
            return
        if env_ok():
            log(f"credentials installed: {env_path()} - LIVE and Backfill are ready.")
        else:
            log(f"copied, but no ARROW_PASSWORD found inside - check the file.")

    def do_stop():
        if state["stop_event"]:
            log("stopping live collection... (saves everything already received)")
            state["stop_event"].set()

    def do_afterclose():
        idx = selected_index()
        if idx != "NIFTY":
            log("after-close jobs process live-recorded ticks - live collection is NIFTY-only for now.")
            return
        set_busy(True, "after-close jobs...")
        day = selected_day()

        def job():
            if day == "(none yet)":
                log("no collected data yet - nothing to do.")
                return
            from compact_day import compact_one
            from build_minute import build_one
            print(f"-- merging segments for {day} --")
            compact_one(day)
            print(f"-- building 1-min bars for {day} --")
            build_one(day)
            print("-- done - the backtest cache is updated. --")
            refresh_days()

        worker(job)

    def do_check():
        idx = selected_index()
        if idx != "NIFTY":
            log(f"health check applies to live-recorded tick days (NIFTY only) - {idx} backfilled candles have nothing to duplicate-check.")
            return
        day = selected_day()
        if day == "(none yet)":
            log("no collected data yet - nothing to check.")
            return
        set_busy(True, f"health check {day}...")
        from view import check_dupes, find_files

        def job():
            check_dupes(find_files([day]))

        worker(job)

    def do_export():
        idx = selected_index()
        day = selected_day()
        if day == "(none yet)":
            log("no collected data yet - nothing to export.")
            return
        log(f"=== Export {idx} {day} -> Excel ===")
        set_busy(True, f"exporting {idx} {day} -> Excel...")
        from view import export_xlsx, find_files

        def job():
            out = ROOT / f"{idx}_{day}.xlsx"
            files = find_files([day]) if idx == "NIFTY" else []
            if files:
                export_xlsx(files, out, None)
                os.startfile(out)
                return
            # backfilled (candle-only) day: export the 1-min bars instead
            import pandas as pd
            sheets = {}
            idx_path = _minute_root(idx) / "index.parquet"
            if idx_path.exists():
                dfi = pd.read_parquet(idx_path)
                dfi = dfi[pd.to_datetime(dfi["timestamp"]).dt.date == pd.to_datetime(day).date()]
                if not dfi.empty:
                    sheets["index_1min"] = dfi
            for p in (_minute_root(idx) / "options").glob("expiry=*/data.parquet"):
                o = pd.read_parquet(p)
                o = o[pd.to_datetime(o["timestamp"]).dt.date == pd.to_datetime(day).date()]
                if not o.empty:
                    sheets[f"options_{p.parent.name.replace('expiry=', '')}"[:31]] = o
            for p in (_minute_root(idx) / "futures").glob("expiry=*/data.parquet"):
                f = pd.read_parquet(p)
                f = f[pd.to_datetime(f["timestamp"]).dt.date == pd.to_datetime(day).date()]
                if not f.empty:
                    sheets[f"futures_{p.parent.name.replace('expiry=', '')}"[:31]] = f
            if not sheets:
                log(f"no data for {idx} {day}")
                return
            with pd.ExcelWriter(out, engine="openpyxl") as xl:
                for name, df in sheets.items():
                    df.to_excel(xl, sheet_name=name, index=False)
            print(f"wrote {out.resolve()} ({len(sheets)} sheet(s)) - opening it for you...")
            os.startfile(out)

        worker(job)

    def scheduler_poll():
        try:
            now_ist = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Kolkata"))
            running = bool(state["live_thread"] and state["live_thread"].is_alive())
            act = scheduled_action(now_ist, bool(schedule_var.get()), running)
            if act == "start" and not state["busy"]:
                log(f"AUTO: {now_ist.strftime('%H:%M')} - scheduled window open, starting live collection")
                do_live()
            elif act == "stop" and running:
                log(f"AUTO: {now_ist.strftime('%H:%M')} - scheduled window closed, stopping live collection")
                do_stop()
        except Exception as e:
            log(f"scheduler error: {e}")
        root.after(30_000, scheduler_poll)

    def do_backfill():
        idx = selected_index()
        try:
            d1 = datetime.strptime(bf_from.get().strip(), "%Y-%m-%d").date()
            d2 = datetime.strptime(bf_to.get().strip(), "%Y-%m-%d").date()
        except ValueError:
            log("dates must be YYYY-MM-DD")
            return
        if d1 > d2:
            d1, d2 = d2, d1
        if (d2 - d1).days > 400:
            log("range too large - split it into chunks under 400 days.")
            return
        if not env_ok():
            log(f"ERROR: backfill needs credentials in {env_path()} (it uses the REST API).")
            log("       click 'Load .env...' and pick your .env file.")
            return
        log(f"=== Backfill {idx} {d1} .. {d2} ===")
        set_busy(True, f"backfilling {idx} {d1} .. {d2} (candles, no ticks)...")
        from backfill import run as bf_run

        def job():
            bf_run(d1, d2, idx)
            refresh_days()

        worker(job)

    def change_folder():
        from tkinter import filedialog, messagebox
        if state["busy"]:
            log("a job is running - wait for it to finish, then change the folder.")
            return
        cur = data_root()
        pick = filedialog.askdirectory(title="Choose folder for market data",
                                       initialdir=str(cur) if cur.exists() else str(Path.home()))
        if not pick:
            return
        chosen = Path(pick)
        if chosen.resolve() == cur.resolve():
            log("same folder - nothing changed.")
            return
        set_data_root(chosen)
        (ROOT / "data_folder.txt").write_text(str(chosen), encoding="utf-8")
        try:  # canonical location - so every copy of the app resolves the same folder
            from paths import SETTING_FILE
            SETTING_FILE.parent.mkdir(parents=True, exist_ok=True)
            SETTING_FILE.write_text(str(chosen), encoding="utf-8")
        except Exception:
            pass
        log(f"data folder changed to: {chosen}")
        log("note: existing days stay where they are - copy old date=... folders over if you want them visible here.")
        folder_lbl.config(text=f"Data: {data_root()}")
        refresh_days()

    def open_folder():
        ticks = data_root() / "live" / "ticks"
        ticks.mkdir(parents=True, exist_ok=True)
        os.startfile(ticks)

    def on_close():
        if state["live_thread"] and state["live_thread"].is_alive():
            if not messagebox.askyesno("Live collection running",
                                       "Live collection is still running.\nStop it and exit?"):
                return
            if state["stop_event"]:
                state["stop_event"].set()
            state["live_thread"].join(timeout=15)
        root.destroy()

    def selected_index() -> str:
        return index_box.get().strip() or "NIFTY"

    def selected_day() -> str:
        val = days_box.get().strip()
        return val if val else latest_day(selected_index())

    def refresh_days():
        idx = selected_index()
        days = all_days(idx)
        days_box["values"] = days or ["(none yet)"]
        current = days_box.get().strip()
        if not current or current not in days:
            days_box.set(days[0] if days else "(none yet)")

    # ---------- layout ----------
    top = ttk.Frame(root, padding=8)
    top.pack(fill="x")
    row = ttk.Frame(top)
    row.pack(fill="x")
    ttk.Label(row, text="Index:", font=("Segoe UI", 10, "bold")).pack(side="left")
    index_box = ttk.Combobox(row, width=10, state="readonly", font=("Segoe UI", 10),
                             values=INDEXES)
    index_box.set("NIFTY")
    index_box.pack(side="left", padx=(4, 12))
    index_box.bind("<<ComboboxSelected>>", lambda e: refresh_days())
    ttk.Label(row, text="Day:", font=("Segoe UI", 10, "bold")).pack(side="left")
    days_box = ttk.Combobox(row, width=14, state="readonly", font=("Segoe UI", 10))
    days_box.pack(side="left", padx=(4, 12))
    ttk.Label(row, text="Every button below works on this index + day.",
              foreground="#666").pack(side="left")
    refresh_days()

    btns = ttk.Frame(root, padding=(8, 2))
    btns.pack(fill="x")
    btn_live = ttk.Button(btns, text="2 · START LIVE", command=do_live)
    btn_stop = ttk.Button(btns, text="Stop", command=do_stop, state="disabled")
    btn_afterclose = ttk.Button(btns, text="3 · After-close jobs", command=do_afterclose)
    btn_check = ttk.Button(btns, text="Health check", command=do_check)
    btn_export = ttk.Button(btns, text="Export day -> Excel", command=do_export)
    btn_open = ttk.Button(btns, text="Open data folder", command=open_folder)
    btn_folder = ttk.Button(btns, text="Change data folder...", command=change_folder)
    for i, b in enumerate((btn_live, btn_stop, btn_afterclose, btn_check, btn_export, btn_open, btn_folder)):
        b.grid(row=0, column=i, padx=3, pady=4)

    bf = ttk.Frame(root, padding=(8, 0))
    bf.pack(fill="x")
    ttk.Label(bf, text="Download history (candles):").pack(side="left")
    ttk.Label(bf, text="from").pack(side="left", padx=(10, 2))
    bf_from = ttk.Entry(bf, width=11)
    bf_from.pack(side="left")
    ttk.Label(bf, text="to").pack(side="left", padx=(8, 2))
    bf_to = ttk.Entry(bf, width=11)
    bf_to.pack(side="left")
    btn_backfill = ttk.Button(bf, text="Backfill past days", command=do_backfill)
    btn_backfill.pack(side="left", padx=8)
    btn_env = ttk.Button(bf, text="Load .env...", command=do_load_env)
    btn_env.pack(side="left", padx=(0, 8))
    bf_from.insert(0, (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d"))
    bf_to.insert(0, datetime.now().strftime("%Y-%m-%d"))

    sched = ttk.Frame(root, padding=(8, 0))
    sched.pack(fill="x")
    ttk.Checkbutton(sched, text="Auto-collect 09:15-15:40 IST (Mon-Fri)", variable=schedule_var,
                    command=on_toggle_schedule).pack(side="left")
    ttk.Checkbutton(sched, text="Launch at Windows startup", variable=startup_var,
                    command=on_toggle_startup).pack(side="left", padx=(16, 0))

    folder_lbl = ttk.Label(root, text=f"Data: {data_root()}", foreground="#666", padding=(10, 0))
    folder_lbl.pack(fill="x")
    status = ttk.Label(root, text="idle", padding=(10, 0))
    status.pack(fill="x")
    log_box.pack(fill="both", expand=True, padx=8, pady=(2, 8))
    log_box.config(font=font)

    log(f"NIFTY Studio v{APP_VERSION} ready - close & reopen after any update to run the newest code.")
    log(f"Data folder: {data_root()}")
    if not env_ok():
        log(f"NOTE: no credentials at {env_path()}")
        log("      -> LIVE / Backfill need .env - click 'Load .env...' or copy the file there.")
    else:
        log("credentials: found - LIVE and Backfill are ready.")

    root.protocol("WM_DELETE_WINDOW", on_close)
    drain()
    root.after(30_000, scheduler_poll)
    if selftest:
        root.after(800, root.destroy)
        root.mainloop()
        sys.stdout = old_out
        print("GUI-SELFTEST-OK")
        return
    root.mainloop()


if __name__ == "__main__":
    run_gui(selftest="--selftest" in sys.argv)
