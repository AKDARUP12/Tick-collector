#!/usr/bin/env python3
"""Double-click friendly menu for collecting market data (companion to view_gui.py).

Menu:
  2) START live collection  - real API, runs until Ctrl-C (market hours only!)
  3) After-close jobs       - merge segments + build 1-min bars for the latest day
  4) Exit
"""
import os
import sys
from pathlib import Path

ROOT = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent)).parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent.parent
if getattr(sys, "frozen", False):
    ROOT = Path(sys.executable).resolve().parent
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT))  # run_collect, storage, collector live in the project root

DATA_ROOT = ROOT / "data" / "live" / "ticks"

MENU = """
=============================================
   NIFTY Data Collector  (Arrow live ticks)
=============================================
Saving data under: {data}

  1) START live collection  (real API, until Ctrl-C)
  2) After-close: compact + build 1-min bars
  3) Exit
"""


def latest_day():
    days = sorted(p.name.replace("date=", "") for p in DATA_ROOT.glob("date=*")) if DATA_ROOT.exists() else []
    return days[-1] if days else None


def ask(prompt, default=""):
    try:
        txt = input(prompt).strip()
    except EOFError:
        raise
    return txt or default


def pause():
    try:
        input("\nPress Enter to continue...")
    except EOFError:
        pass



def live():
    print("\nThis connects to Arrow with your real credentials.")
    print("Let it run until 15:30 IST. Press Ctrl-C in this window to stop -")
    print("everything already saved stays saved (CVD continues on restart).\n")
    if ask("Start live collection? (y/N): ").lower() != "y":
        print("cancelled\n")
        return
    import collector
    collector.main()


def after_close():
    day = latest_day()
    d = ask(f"Which date? YYYY-MM-DD (Enter = {day}): ", day or "")
    if not d:
        print("no data collected yet\n")
        return
    from compact_day import compact_one
    from build_minute import build_one
    print("\n-- merging segments --")
    compact_one(d)
    print("\n-- building 1-min bars --")
    build_one(d)
    print()


def main():
    while True:
        try:
            choice = ask(MENU.format(data=DATA_ROOT) + "Choose 1-3 (Enter = 1): ", "1")
            if choice == "1":
                live()
            elif choice == "2":
                after_close()
            elif choice == "3":
                break
            else:
                print("please type 1-3")
                continue
        except (EOFError, KeyboardInterrupt):
            break
        except SystemExit as e:
            print(f"\nstopped: {e}")
        except Exception as e:
            print(f"\nsomething went wrong: {e}")
        pause()
    print("bye!")


if __name__ == "__main__":
    main()
