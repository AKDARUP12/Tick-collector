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
                          json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                          timeout=10)
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
                        _send(token, chat, f"Your chat_id: `{chat}`\nAllowed: `{allowed}`")
                    elif cmd == "/help":
                        _send(token, chat,
                              "*Tick-collector*\n"
                              "/status - live session status\n"
                              "/id - show your chat_id\n"
                              "/help - this help")
                    elif cmd in ("/status", "/last"):
                        try:
                            st = status_fn()  # dict with keys
                        except Exception as e:
                            st = {"error": str(e)}
                        now = datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
                        if "error" in st:
                            _send(token, chat, f"❌ status error: {st['error']}")
                        else:
                            running = st.get("running", False)
                            icon = "🟢 RUNNING" if running else "⚪ IDLE"
                            ticks = st.get("ticks", 0)
                            cvd = st.get("cvd", {})
                            files = st.get("files", 0)
                            sess = st.get("session_date", "?")
                            uptime = st.get("uptime", "?")
                            # compact CVD
                            cvd_str = ", ".join(f"{k}:{v}" for k,v in list(cvd.items())[:5])
                            if len(cvd) > 5: cvd_str += f" +{len(cvd)-5} more"
                            txt = (
                                f"{icon} `{now}`\n"
                                f"Session: `{sess}` up {uptime}\n"
                                f"Ticks: `{ticks}`  Files: `{files}`\n"
                                f"CVD: {cvd_str or 'n/a'}\n"
                                f"Data: `data/live/ticks/date={sess}`\n"
                            )
                            if not running:
                                txt += "\n_Market closed — next run 09:15 IST (cron 45 3 UTC)._"
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
