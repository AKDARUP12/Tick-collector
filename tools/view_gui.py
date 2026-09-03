#!/usr/bin/env python3
"""Double-click friendly menu around tools/view.py - also built into NIFTY_Viewer.exe."""
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from view import DATA, check_dupes, export_xlsx, find_files, list_files, preview

MENU = """
=============================================
   NIFTY Data Viewer  (Parquet -> Excel)
=============================================
Looking for data in: {data}

  1) List all data files
  2) Peek at some rows on screen
  3) Convert one file -> Excel
  4) Convert a WHOLE DAY -> Excel  (recommended)
  5) Check data for duplicates
  6) Exit
"""


def latest_day():
    days = sorted(p.name.replace("date=", "") for p in (DATA / "live" / "ticks").glob("date=*"))
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


def peek():
    needle = ask("Which file? type part of the name (e.g. 26000 or NIFTY25100CE): ")
    files = find_files([needle] if needle else [])
    if not files:
        print("no files matched.\n")
        return
    rows = ask("How many rows? (Enter = 10): ", "10")
    tail = ask("Show LAST rows instead of first? (y/N): ").lower().startswith("y")
    try:
        preview(files[0], int(rows), tail, None)
    except ValueError:
        print("please type a number")
    print()


def convert_one():
    needle = ask("Which file? type part of the name (e.g. 26000): ")
    files = find_files([needle] if needle else [])
    if not files:
        print("no files matched.\n")
        return
    out = ask("Excel file name (Enter = converted.xlsx): ", "converted.xlsx")
    export_xlsx(files, out, None)
    print()


def convert_day():
    day = latest_day()
    d = ask(f"Which date? YYYY-MM-DD (Enter = {day}): ", day or "")
    if not d:
        print("no data yet - run the collector first.\n")
        return
    files = find_files([d])
    if not files:
        print(f"no files found for {d}\n")
        return
    print(f"found {len(files)} file(s) for {d}")
    out = ask(f"Excel file name (Enter = day_{d}.xlsx): ", f"day_{d}.xlsx")
    export_xlsx(files, out, None)
    print()


def main():
    while True:
        try:
            choice = ask(MENU.format(data=DATA) + "Choose 1-6 (Enter = 4): ", "4")
            if choice == "1":
                list_files(find_files([]))
            elif choice == "2":
                peek()
            elif choice == "3":
                convert_one()
            elif choice == "4":
                convert_day()
            elif choice == "5":
                check_dupes(find_files([]))
            elif choice == "6":
                break
            else:
                print("please type 1-5")
                continue
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print(f"\nsomething went wrong: {e}")
        pause()
    print("bye!")


if __name__ == "__main__":
    main()
