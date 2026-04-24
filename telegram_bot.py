from __future__ import annotations
import time
import threading
from pathlib import Path
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ПАПКА_ДАННЫХ
from export_engine import rebuild_exports
from logger import log

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
_polling_started = False
_offset = 0

def send_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"{BASE}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
    except Exception as exc:
        log(f"telegram send error: {exc}")

def send_document(path: Path, caption: str | None = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if not path.exists():
        send_message(f"Файл не найден: {path.name}")
        return
    try:
        with path.open("rb") as f:
            requests.post(f"{BASE}/sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption or path.name}, files={"document": (path.name, f)}, timeout=120)
    except Exception as exc:
        log(f"telegram send document error: {exc}")

def _handle(text: str) -> None:
    if text == "/ping":
        send_message("pong")
    elif text == "/status":
        files = sorted(p.name for p in ПАПКА_ДАННЫХ.glob("*") if p.is_file())
        send_message("🥇 Mighty Duck v3.4.3\n\nФайлы:\n" + ("\n".join(files[:20]) if files else "файлов пока нет"))
    elif text == "/manifest":
        send_document(ПАПКА_ДАННЫХ / "storage_manifest.txt")
    elif text == "/raw_market_5m":
        send_document(ПАПКА_ДАННЫХ / "raw_market_5m.csv")
    elif text == "/bot_aggregates":
        send_document(ПАПКА_ДАННЫХ / "bot_aggregates.csv")
    elif text == "/validation_audit":
        send_document(ПАПКА_ДАННЫХ / "validation_audit.csv")
    elif text == "/audit_report":
        send_document(ПАПКА_ДАННЫХ / "audit_report.txt")
    elif text == "/export_quick":
        send_document(rebuild_exports("quick"), "quick research bundle")
    elif text == "/export_research_7d":
        send_message("Готовлю research 7d bundle...")
        send_document(rebuild_exports("research_7d"), "research 7d bundle")
    elif text == "/export_research_30d":
        send_message("Готовлю research 30d bundle...")
        send_document(rebuild_exports("research_30d"), "research 30d bundle")

def _reset():
    global _offset
    try:
        requests.get(f"{BASE}/deleteWebhook", params={"drop_pending_updates": "true"}, timeout=20)
    except Exception as exc:
        log(f"deleteWebhook error: {exc}")
    _offset = 0

def _loop():
    global _offset
    time.sleep(6)
    _reset()
    while True:
        try:
            r = requests.get(f"{BASE}/getUpdates", params={"timeout": 30, "offset": _offset + 1}, timeout=40)
            r.raise_for_status()
            for item in r.json().get("result", []):
                _offset = item["update_id"]
                text = (item.get("message", {}) or {}).get("text", "")
                if text:
                    _handle(text.strip())
        except Exception as exc:
            log(f"telegram polling error: {exc}")
            time.sleep(10 if "409" in str(exc) else 5)

def start_polling() -> None:
    global _polling_started
    if _polling_started or not TELEGRAM_BOT_TOKEN:
        return
    _polling_started = True
    threading.Thread(target=_loop, daemon=True).start()
