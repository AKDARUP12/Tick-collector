#!/usr/bin/env python3
"""Telegram /status handler for live collector - polls Bot API and replies.
Runs as daemon thread inside collector.py when TELEGRAM_BOT_TOKEN is set.
Only authorized chat_id (TELEGRAM_CHAT_ID) gets replies.
"""
import os
import time
import threading
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

def _send(token, chat_id, text):
    try:
        import requests
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": text},
                          timeout=10)
        print(f"telegram send {r.status_code} {r.text[:400]}")
        return r.ok
    except Exception as e:
        print(f"telegram send failed: {e}")
        return False

def start_polling(stop_event: threading.Event, token: str, allowed_chat: str, status_fn):
    """Poll getUpdates and answer /status, /help, /id."""
    if not token:
        return None
    allowed = str(allowed_chat).strip() if allowed_chat else ""

    def _loop():
        offset = 0
        import requests
        print(f"telegram status polling started for chat {allowed or '*'}")
        while not stop_event.is_set():
            try:
                r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates",
                                 params={"timeout": 25, "offset": offset, "allowed_updates": '["message"]'},
                                 timeout=35)
                data = r.json()
                if not data.get("ok"):
                    time.sleep(5); continue
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("edited_message")
                    if not msg: continue
                    chat = str(msg.get("chat", {}).get("id", ""))
                    if allowed and chat != allowed:
                        continue  # ignore unauthorized chats
                    text = (msg.get("text") or "").strip()
                    if not text.startswith("/"):
                        continue
                    cmd = text.split()[0].split("@")[0].lower()
                    if cmd == "/id":
                        _send(token, chat, f"Your chat_id: {chat}\nAllowed: {allowed}")
                    elif cmd == "/help":
                        _send(token, chat,
                              "Tick-collector\n"
                              "/status - detailed live/idle status\n"
                              "/id - show your chat_id\n"
                              "/help - this help\n"
                              "ZIPs sent at 15:00 & 15:40 IST")
                    elif cmd == "/sendnow":
                        # immediate 15m zip without stopping collector
                        try:
                            st = status_fn()
                            sess = st.get("session_date","?")
                            from pathlib import Path
                            from paths import data_root
                            import zipfile, pathlib
                            day_d = data_root() / "live" / "ticks" / f"date={sess}"
                            files = sorted(day_d.glob("token=*.parquet")) if day_d.exists() else []
                            if not files:
                                _send(token, chat, f"no ticks yet for {sess}")
                            else:
                                ts = datetime.now(timezone.utc).astimezone(IST).strftime("%H%M")
                                zip_name = f"ticks-{sess}-{ts}-now.zip"
                                with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
                                    for p in files[-200:]:  # last 200 files ~ recent 15m
                                        z.write(p, arcname=f"date={sess}/{p.name}")
                                sz = pathlib.Path(zip_name).stat().st_size
                                if sz > 48_000_000:
                                    _send(token, chat, f"now zip {sz//1024}KB >48MB, use 15m slices at :00,:15,:30,:45")
                                else:
                                    import requests
                                    with open(zip_name, "rb") as f:
                                        r = requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
                                                          data={"chat_id": chat, "caption": f"Ticks {sess} now {ts} {len(files)} files {st.get('ticks',0)} ticks"},
                                                          files={"document": (zip_name, f)}, timeout=120)
                                        _send(token, chat, f"sent now {zip_name} {r.status_code}")
                                pathlib.Path(zip_name).unlink(missing_ok=True)
                        except Exception as e:
                            _send(token, chat, f"sendnow error: {e}")
                    elif cmd in ("/status", "/last"):
                        try:
                            st = status_fn()
                        except Exception as e:
                            st = {"error": str(e)}
                        now = datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
                        if "error" in st:
                            _send(token, chat, f"status error: {st['error']}")
                        else:
                            running = st.get("running", False)
                            ticks = st.get("ticks", 0)
                            cvd = st.get("cvd", {})
                            files = st.get("files", 0)
                            sess = st.get("session_date", "?")
                            uptime = st.get("uptime", "?")
                            # size
                            try:
                                from pathlib import Path
                                from paths import data_root
                                d = data_root() / "live" / "ticks" / f"date={sess}"
                                sz = sum(p.stat().st_size for p in d.glob("*.parquet"))//1024 if d.exists() else 0
                                sz_str = f"{sz}KB" if sz else "0KB"
                            except:
                                sz_str = "n/a"
                            cvd_items = list(cvd.items())[:6]
                            cvd_str = ", ".join(f"{k}:{v}" for k,v in cvd_items) or "n/a"
                            if len(cvd) > 6: cvd_str += f" +{len(cvd)-6} more"
                            total_cvd = sum(cvd.values()) if cvd else 0
                            icon = "RUNNING" if running else "IDLE"
                            txt = (
                                f"{icon} {now}\n"
                                f"Session: {sess} up {uptime} market 09:15-15:30\n"
                                f"Ticks: {ticks}  Files: {files}  Size: {sz_str}\n"
                                f"CVD total:{total_cvd} per-token: {cvd_str}\n"
                                f"Data: data/live/ticks/date={sess}\n"
                                f"Strikes: 4 levels (8 legs) + NIFTY spot | Splits 09:15-15:00 & 15:00-15:40\n"
                            )
                            if not running:
                                txt += "Market closed - next 09:15 IST\n"
                            txt += "Artifacts: ticks-morning/afternoon zip auto-sent at session end"
                            _send(token, chat, txt)
                    else:
                        _send(token, chat, f"Unknown `{cmd}` — try /status, /help, /id")
            except Exception:
                # don't crash collector on telegram errors
                traceback.print_exc()
                time.sleep(5)
        print("telegram polling stopped")

    t = threading.Thread(target=_loop, daemon=True, name="telegram-status")
    t.start()
    return t
