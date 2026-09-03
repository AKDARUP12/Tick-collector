#!/usr/bin/env python3
"""24/7 Telegram poller - answers /status even when collector idle.
Called every 1m by telegram-status.yml, or --daemon loops for instant reply.
Stores offset in .telegram_offset (cached).
"""
import os, sys, time
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

def gh_detailed():
    try:
        now_ist = datetime.now(timezone.utc).astimezone(IST)
        date_ist = now_ist.date().isoformat()
        # market window
        from datetime import time as dtime
        is_open = dtime(9,15) <= now_ist.time() <= dtime(15,30)
        market = "OPEN 09:15-15:30" if is_open and now_ist.weekday()<5 else "CLOSED"
        # runs - fetch all, then specifically collector workflows (pollers dominate top 10)
        r = requests.get(f"https://api.github.com/repos/{REPO}/actions/runs",
                         params={"per_page": 20, "branch": "main"},
                         headers={"Authorization": f"Bearer {GH_TOKEN}", "Accept":"application/vnd.github+json"},
                         timeout=10)
        j = r.json()
        runs = j.get("workflow_runs",[])
        # also fetch collector workflows directly to avoid being buried by poller runs
        coll = []
        for wf in ["collect-morning.yml","collect-closing.yml"]:
            try:
                cr = requests.get(f"https://api.github.com/repos/{REPO}/actions/workflows/{wf}/runs",
                                  params={"per_page": 3, "branch": "main"},
                                  headers={"Authorization": f"Bearer {GH_TOKEN}", "Accept":"application/vnd.github+json"},
                                  timeout=10).json()
                coll.extend(cr.get("workflow_runs",[]))
            except: pass
        coll = sorted(coll, key=lambda x: x.get("created_at",""), reverse=True)
        coll_running = next((x for x in coll if x.get("status")=="in_progress"), None)
        # also check any in_progress from main list
        if not coll_running:
            coll_running = next((x for x in runs if "Collect" in x.get("name","") and x.get("status")=="in_progress"), None)
        if coll_running:
            coll_line = f"🟢 RUNNING {coll_running['name']} since {coll_running['created_at'][11:16]} UTC"
        else:
            last = coll[0] if coll else None
            if last:
                conc = last.get("conclusion","")
                coll_line = f"⚪ IDLE last {last['name']} {conc} {last['created_at'][:16].replace('T',' ')}"
            else:
                coll_line = "⚪ IDLE no collector runs (next 09:15 IST 45 3 UTC)"
        # artifacts for today
        try:
            ar = requests.get(f"https://api.github.com/repos/{REPO}/actions/artifacts",
                              params={"per_page": 5},
                              headers={"Authorization": f"Bearer {GH_TOKEN}", "Accept":"application/vnd.github+json"},
                              timeout=10).json()
            arts = [a for a in ar.get("artifacts",[]) if date_ist in a.get("name","") or "ticks" in a.get("name","")]
            art_str = "; ".join(f"{a['name']} {a['size_in_bytes']//1024}KB" for a in arts[:2]) if arts else "no artifacts today"
        except:
            art_str = "artifacts n/a"
        # local data size if on collector runner (will be empty on poller)
        try:
            from pathlib import Path as _P
            # try live ticks path
            d = _P("data/live/ticks") 
            if d.exists():
                cnt = len(list(d.rglob("*.parquet")))
                sz = sum(p.stat().st_size for p in d.rglob("*.parquet"))//1024
                data_str = f"{cnt} parquet, {sz}KB"
            else:
                data_str = "no local ticks (ephemeral)"
        except:
            data_str = "n/a"
        lines = [
            f"Market: {market} | Date: {date_ist} | {now_ist.strftime('%H:%M:%S IST')}",
            coll_line,
            "Last 3 runs:"
        ]
        for run in runs[:3]:
            name = run.get("name","")[:22]
            status = run.get("status","")
            conc = run.get("conclusion","") or ""
            created = run.get("created_at","")[11:16]
            dur = ""
            try:
                if run.get("created_at") and run.get("updated_at"):
                    from dateutil.parser import isoparse
                    dur_s = (isoparse(run["updated_at"]) - isoparse(run["created_at"])).total_seconds()
                    dur = f" {int(dur_s//60)}m{int(dur_s%60)}s"
            except: pass
            icon = "🟢" if status=="in_progress" else "✅" if conc=="success" else "❌" if conc=="failure" else "⚪"
            lines.append(f" {icon} {name} {status}/{conc} {created}{dur}")
        lines += [
            f"Artifacts: {art_str}",
            f"Local: {data_str}",
            f"Splits: 09:15-15:00 (45 3 UTC) & 15:00-15:40 (30 9 UTC) | Repo: {REPO}",
            "ZIPs auto-sent to this chat at session end"
        ]
        return "\n".join(lines)
    except Exception as e:
        return f"status err: {e}"

def gh_latest(): return gh_detailed()

def send(chat, text):
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT}/sendMessage",
                          json={"chat_id": chat, "text": text}, timeout=10)
        print(f"send to {chat}: {r.status_code} {r.text[:500]}")
        return r.ok
    except Exception as e:
        print(f"send err: {e}")
        return False

def poll_once(offset, timeout=0):
    r = requests.get(f"https://api.telegram.org/bot{BOT}/getUpdates",
                     params={"offset": offset, "timeout": timeout, "allowed_updates": '["message"]'},
                     timeout=timeout+15 if timeout else 15)
    data = r.json()
    if not data.get("ok"):
        print(f"getUpdates failed {data}"); return offset
    max_off = offset
    print(f"poll offset={offset} found {len(data.get('result',[]))} updates")
    for upd in data.get("result",[]):
        max_off = max(max_off, upd["update_id"]+1)
        msg = upd.get("message") or {}
        chat = str(msg.get("chat",{}).get("id",""))
        text = (msg.get("text") or "").strip()
        print(f"upd {upd['update_id']} chat={chat} text='{text}'")
        if CHAT_ALLOW and chat != CHAT_ALLOW:
            continue
        if not text.startswith("/"):
            continue
        cmd = text.split()[0].split("@")[0].lower()
        now = datetime.now(timezone.utc).astimezone(IST).strftime("%H:%M:%S IST")
        if cmd == "/id":
            send(chat, f"chat_id: {chat}")
        elif cmd == "/help":
            send(chat, "Tick-collector\n/status - live or last run\n/id - chat id\n/help - help")
        elif cmd in ("/status","/last"):
            status = gh_latest()
            send(chat, f"{now}\n{status}\n\nData: data/live/ticks | Cron 09:15(45 3 UTC) & 15:00(30 9 UTC)")
        else:
            send(chat, f"Unknown {cmd} try /status")
    OFFSET_FILE.write_text(str(max_off))
    return max_off

def main():
    if not BOT:
        print("no BOT token"); return
    daemon = "--daemon" in sys.argv
    offset = 0
    if OFFSET_FILE.exists():
        try: offset = int(OFFSET_FILE.read_text().strip() or "0")
        except: offset = 0
    if daemon:
        print(f"daemon polling started offset={offset}")
        while True:
            try:
                offset = poll_once(offset, timeout=25)
            except Exception as e:
                print(f"daemon poll err: {e}")
                time.sleep(5)
    else:
        poll_once(offset, timeout=0)
        print(f"offset {offset}->{OFFSET_FILE.read_text().strip()}")

if __name__=="__main__":
    main()
