#!/usr/bin/env python3
"""24/7 Telegram poller - answers /status even when collector idle.
Called every 5m by .github/workflows/telegram-status.yml.
Stores offset in .telegram_offset (cached via actions/cache).
"""
import os
import json
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests

BOT = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
CHAT_ALLOW = os.getenv("TELEGRAM_CHAT_ID","").strip()
GH_TOKEN = os.getenv("GH_TOKEN","") or os.getenv("GITHUB_TOKEN","")
REPO = os.getenv("GITHUB_REPOSITORY","AKDARUP12/Tick-collector")
OFFSET_FILE = Path(".telegram_offset")
IST = ZoneInfo("Asia/Kolkata")

def gh_latest():
    try:
        r = requests.get(f"https://api.github.com/repos/{REPO}/actions/runs",
                         params={"per_page": 5, "branch": "main"},
                         headers={"Authorization": f"Bearer {GH_TOKEN}", "Accept":"application/vnd.github+json"},
                         timeout=10)
        j = r.json()
        runs = j.get("workflow_runs",[])
        if not runs: return "no runs"
        lines=[]
        for run in runs[:3]:
            name = run.get("name","")
            status = run.get("status","")  # queued/in_progress/completed
            conc = run.get("conclusion","")
            created = run.get("created_at","")[:16].replace("T"," ")
            icon = "🟢" if status=="in_progress" else "✅" if conc=="success" else "❌" if conc=="failure" else "⚪"
            lines.append(f"{icon} {name[:22]} {status}/{conc} {created}")
        return "\n".join(lines)
    except Exception as e:
        return f"gh api err: {e}"

def send(chat, text):
    requests.post(f"https://api.telegram.org/bot{BOT}/sendMessage",
                  json={"chat_id": chat, "text": text, "parse_mode":"Markdown"}, timeout=10)

def main():
    if not BOT:
        print("no BOT token"); return
    offset = 0
    if OFFSET_FILE.exists():
        try: offset = int(OFFSET_FILE.read_text().strip() or "0")
        except: offset = 0
    r = requests.get(f"https://api.telegram.org/bot{BOT}/getUpdates",
                     params={"offset": offset, "timeout": 0, "allowed_updates": '["message"]'}, timeout=15)
    data = r.json()
    if not data.get("ok"):
        print(f"getUpdates failed {data}"); return
    max_off = offset
    for upd in data.get("result",[]):
        max_off = max(max_off, upd["update_id"]+1)
        msg = upd.get("message") or {}
        chat = str(msg.get("chat",{}).get("id",""))
        if CHAT_ALLOW and chat != CHAT_ALLOW:
            continue
        text = (msg.get("text") or "").strip()
        if not text.startswith("/"):
            continue
        cmd = text.split()[0].split("@")[0].lower()
        now = datetime.now(timezone.utc).astimezone(IST).strftime("%H:%M:%S IST")
        if cmd == "/id":
            send(chat, f"chat_id: `{chat}`")
        elif cmd == "/help":
            send(chat, "*Tick-collector*\n/status - live or last run\n/id - chat id\n/help - help")
        elif cmd in ("/status","/last"):
            status = gh_latest()
            send(chat, f"⏰ `{now}`\n{status}\n\n_Data: `data/live/ticks` | Cron 09:15(45 3 UTC) & 15:00(30 9 UTC)_")
        else:
            send(chat, f"Unknown `{cmd}` try /status")
    OFFSET_FILE.write_text(str(max_off))
    print(f"offset {offset}->{max_off}, handled {len(data.get('result',[]))} updates")

if __name__=="__main__":
    main()
