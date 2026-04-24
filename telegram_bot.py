from __future__ import annotations
import time
import threading
from pathlib import Path
import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ПАПКА_ДАННЫХ
from status_engine import build_status_text
from logger import log

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
_polling_started = False
_offset = 0

FILE_COMMANDS = {
    "/manifest": ПАПКА_ДАННЫХ / "storage_manifest.txt",
    "/oi_5m_raw": ПАПКА_ДАННЫХ / "сырые_5м_ои.csv",
    "/price_5m_raw": ПАПКА_ДАННЫХ / "сырые_5м_цены.csv",
    "/volume_5m_raw": ПАПКА_ДАННЫХ / "сырые_5м_объёмы.csv",
    "/oi_aggregates": ПАПКА_ДАННЫХ / "агрегаты_ои.csv",
    "/oi_consistency": ПАПКА_ДАННЫХ / "сверка_ои.csv",
    "/oi_quality": ПАПКА_ДАННЫХ / "отчет_по_аудиту.txt",
    "/oi_debug": ПАПКА_ДАННЫХ / "calculation_debug.txt",
    "/audit_oi": ПАПКА_ДАННЫХ / "аудит_ои.csv",
    "/audit_price": ПАПКА_ДАННЫХ / "аудит_цены.csv",
    "/audit_volume": ПАПКА_ДАННЫХ / "аудит_объёма.csv",
    "/audit_report": ПАПКА_ДАННЫХ / "отчет_по_аудиту.txt",
    "/audit_debug": ПАПКА_ДАННЫХ / "debug_audit.txt",
}

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
            requests.post(f"{BASE}/sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption or path.name}, files={"document": (path.name, f)}, timeout=60)
    except Exception as exc:
        log(f"telegram send document error: {exc}")

def _handle(text: str) -> None:
    if text == "/ping":
        send_message("pong")
        return
    if text == "/status":
        send_message(build_status_text())
        return
    if text == "/help":
        send_message("/ping\n/status\n/manifest\n/oi_5m_raw\n/price_5m_raw\n/volume_5m_raw\n/oi_aggregates\n/oi_consistency\n/oi_quality\n/audit_oi\n/audit_price\n/audit_volume\n/audit_report\n/audit_debug")
        return
    if text in FILE_COMMANDS:
        send_document(FILE_COMMANDS[text])

def _reset_updates():
    global _offset
    try:
        requests.get(f"{BASE}/deleteWebhook", params={"drop_pending_updates": "true"}, timeout=20)
    except Exception as exc:
        log(f"deleteWebhook error: {exc}")
    _offset = 0

def _poll_loop():
    global _offset
    time.sleep(6)
    _reset_updates()
    while True:
        try:
            r = requests.get(f"{BASE}/getUpdates", params={"timeout": 30, "offset": _offset + 1}, timeout=40)
            r.raise_for_status()
            data = r.json()
            for item in data.get("result", []):
                _offset = item["update_id"]
                text = (item.get("message", {}) or {}).get("text", "")
                if text:
                    _handle(text.strip())
        except Exception as exc:
            msg = str(exc)
            log(f"telegram polling error: {exc}")
            if "409" in msg:
                time.sleep(20)
                _reset_updates()
            else:
                time.sleep(5)

def start_polling() -> None:
    global _polling_started
    if _polling_started or not TELEGRAM_BOT_TOKEN:
        return
    _polling_started = True
    threading.Thread(target=_poll_loop, daemon=True).start()
