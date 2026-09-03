"""Shared path resolution for NIFTY Studio.

The data folder defaults to <app>/data but can be overridden:
  1. NIFTY_DATA_DIR environment variable (highest priority)
  2. The canonical setting file:
       %LOCALAPPDATA%\\NIFTYStudio\\data_folder.txt
     Written by the GUI "Change data folder..." button. One universal location,
     so EVERY copy of the app (installed or project) resolves the same folder.
Every module asks data_root() at the moment it needs the path, so a folder
change applies immediately.
"""
import os
import sys
from pathlib import Path

APP_ROOT = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) \
    else Path(__file__).resolve().parent

SETTING_FILE = Path(os.environ.get("LOCALAPPDATA") or APP_ROOT) / "NIFTYStudio" / "data_folder.txt"


def _read_setting() -> Path | None:
    try:
        if SETTING_FILE.exists():
            p = Path(SETTING_FILE.read_text(encoding="utf-8").strip())
            if str(p):
                p.mkdir(parents=True, exist_ok=True)
                return p
    except Exception:
        pass
    return None


def data_root() -> Path:
    """Folder that contains live/ticks, live/minute, live/cache."""
    custom = os.environ.get("NIFTY_DATA_DIR", "").strip()
    if custom:
        p = Path(custom)
        p.mkdir(parents=True, exist_ok=True)
        return p
    saved = _read_setting()
    if saved is not None:
        return saved
    return APP_ROOT / "data"


def set_data_root(folder) -> Path:
    p = Path(folder)
    p.mkdir(parents=True, exist_ok=True)
    os.environ["NIFTY_DATA_DIR"] = str(p)
    return p
