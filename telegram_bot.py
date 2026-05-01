from __future__ import annotations

import time
import threading
import zipfile
import json
from pathlib import Path
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ПАПКА_ДАННЫХ, APP_VERSION
from export_engine import rebuild_exports
from logger import log

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
_polling_started = False
_offset = 0
_export_lock = threading.Lock()


def _panel_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Панель", "callback_data": "panel"},
                {"text": "Runtime", "callback_data": "runtime"},
            ],
            [
                {"text": "Exports", "callback_data": "exports"},
                {"text": "Reports", "callback_data": "reports"},
            ],
            [
                {"text": "Bundle", "callback_data": "bundle"},
                {"text": "Backup", "callback_data": "backup"},
            ],
        ]
    }


def send_message(text: str, reply_markup: dict | None = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        requests.post(
            f"{BASE}/sendMessage",
            json=payload,
            timeout=30,
        )
    except Exception as exc:
        log(f"telegram send error: {exc}")


def send_panel_message(text: str) -> None:
    send_message(text, _panel_keyboard())


def answer_callback(callback_id: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not callback_id:
        return
    try:
        requests.post(
            f"{BASE}/answerCallbackQuery",
            json={"callback_query_id": callback_id},
            timeout=20,
        )
    except Exception as exc:
        log(f"telegram callback answer error: {exc}")


def send_document(path: Path, caption: str | None = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    if not path.exists():
        send_message(f"Файл не найден: {path.name}")
        return

    try:
        with path.open("rb") as f:
            requests.post(
                f"{BASE}/sendDocument",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption or path.name},
                files={"document": (path.name, f)},
                timeout=180,
            )
    except Exception as exc:
        log(f"telegram send document error: {exc}")


def _build_runtime_reports_zip() -> Path:
    report_path = ПАПКА_ДАННЫХ / "runtime_reports.zip"

    files = [
        ПАПКА_ДАННЫХ / "runtime_timing_report.txt",
        ПАПКА_ДАННЫХ / "runtime_health_report.txt",
        ПАПКА_ДАННЫХ / "request_failure_report.csv",
        ПАПКА_ДАННЫХ / "gap_report.csv",
        ПАПКА_ДАННЫХ / "active_universe_report.csv",
        ПАПКА_ДАННЫХ / "storage_manifest.txt",
    ]

    with zipfile.ZipFile(report_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path in files:
            if path.exists():
                z.write(path, arcname=path.name)

    return report_path


def _read_kv_file(path: Path) -> dict:
    data = {}

    if not path.exists():
        return data

    for line in path.read_text(errors="ignore").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
        elif ":" in line:
            k, v = line.split(":", 1)
            data[k.strip()] = v.strip()

    return data


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0

    lines = path.read_text(errors="ignore").splitlines()

    if not lines:
        return 0

    return max(len(lines) - 1, 0)


def _quick_export_is_fresh(max_age_seconds: int = 60) -> bool:
    bundle_path = ПАПКА_ДАННЫХ / "market_research_bundle.zip"

    if not bundle_path.exists():
        return False

    age = time.time() - bundle_path.stat().st_mtime
    return age <= max_age_seconds


def _build_status_text() -> str:
    timing = _read_kv_file(ПАПКА_ДАННЫХ / "runtime_timing_report.txt")
    health = _read_kv_file(ПАПКА_ДАННЫХ / "runtime_health_report.txt")

    files_count = len([path for path in ПАПКА_ДАННЫХ.glob("*") if path.is_file()])
    failures_count = _count_csv_rows(ПАПКА_ДАННЫХ / "request_failure_report.csv")
    gaps_count = _count_csv_rows(ПАПКА_ДАННЫХ / "gap_report.csv")
    active_count = _count_csv_rows(ПАПКА_ДАННЫХ / "active_universe_report.csv")

    total_seconds = timing.get("total_seconds", "n/a")
    generated_at = timing.get("generated_at", "n/a")
    memory_mb = health.get("memory_max_rss_mb", "n/a")
    export_mode = health.get("export_mode", "n/a")

    return (
        f"🥇 Mighty Duck / {APP_VERSION}\n\n"
        f"Cycle: OK\n"
        f"Last timing: {generated_at}\n"
        f"Duration: {total_seconds}s\n"
        f"Memory max RSS: {memory_mb} MB\n"
        f"Export mode: {export_mode}\n\n"
        f"Runtime reports:\n"
        f"Failures: {failures_count}\n"
        f"Gaps: {gaps_count}\n"
        f"Active universe rows: {active_count}\n"
        f"Runtime files: {files_count}\n\n"
        f"Downloads:\n"
        f"/bundle — research bundle\n"
        f"/reports — runtime reports bundle"
    )


def _ensure_quick_exports() -> None:
    if _quick_export_is_fresh():
        return

    rebuild_exports("quick")


def _read_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}


def _fmt_file(path: Path) -> str:
    if not path.exists():
        return f"{path.name}: missing"
    age = int(time.time() - path.stat().st_mtime)
    size_mb = path.stat().st_size / 1024 / 1024
    return f"{path.name}: {size_mb:.2f} MB, age={age}s"


def _runtime_snapshot() -> tuple[dict, dict]:
    runtime = _read_json_file(Path("runtime_reports/runtime_health.json"))
    cycle = _read_json_file(Path("runtime_reports/cycle_status.json"))
    return runtime, cycle


def _build_control_panel_text() -> str:
    runtime, cycle = _runtime_snapshot()

    return (
        f"🥇 Mighty Duck Control Panel / {APP_VERSION}\n\n"
        f"Runtime:\n"
        f"rss_health={runtime.get('rss_health', 'n/a')}\n"
        f"watchdog_health={runtime.get('watchdog_health', 'n/a')}\n"
        f"collect_reserve_health={runtime.get('collect_reserve_health', 'n/a')}\n"
        f"runtime_alert_count={runtime.get('runtime_alert_count', 'n/a')}\n\n"
        f"Cycle:\n"
        f"cycle_health={cycle.get('cycle_health', 'n/a')}\n"
        f"elapsed={cycle.get('cycle_elapsed_seconds', 'n/a')}s\n"
        f"sleep={cycle.get('cycle_sleep_seconds', 'n/a')}s\n"
        f"reserve_pct={cycle.get('cycle_reserve_pct', 'n/a')}\n"
        f"overrun_streak={cycle.get('overrun_streak', 'n/a')}\n\n"
        f"Commands:\n"
        f"/status /runtime /exports /reports\n"
        f"/bundle /manifest /backup /help"
    )


def _build_runtime_text() -> str:
    runtime, cycle = _runtime_snapshot()
    alerts = runtime.get("runtime_alerts", [])

    return (
        f"⚙️ Runtime\n\n"
        f"rss={runtime.get('rss_mb', 'n/a')} MB / {runtime.get('rss_health', 'n/a')}\n"
        f"watchdog={runtime.get('watchdog_health', 'n/a')}\n"
        f"collect={runtime.get('collect_seconds', 'n/a')}s\n"
        f"collect_reserve={runtime.get('collect_reserve_seconds', 'n/a')}s "
        f"({runtime.get('collect_reserve_health', 'n/a')})\n"
        f"cycle={cycle.get('cycle_elapsed_seconds', 'n/a')}s / {cycle.get('cycle_health', 'n/a')}\n"
        f"sleep={cycle.get('cycle_sleep_seconds', 'n/a')}s\n"
        f"alerts={alerts}"
    )


def _build_exports_text() -> str:
    files = [
        ПАПКА_ДАННЫХ / "market_research_bundle.zip",
        ПАПКА_ДАННЫХ / "market_research_bundle_quick.zip",
        ПАПКА_ДАННЫХ / "audit_report.txt",
        ПАПКА_ДАННЫХ / "research_report.txt",
        ПАПКА_ДАННЫХ / "storage_manifest.txt",
        ПАПКА_ДАННЫХ / "runtime_health_report.txt",
        ПАПКА_ДАННЫХ / "request_failure_report.csv",
    ]

    lines = ["📦 Exports", ""]
    lines.extend(_fmt_file(path) for path in files)
    lines.extend([
        "",
        "Commands:",
        "/export_quick",
        "/export_research_7d",
        "/export_research_30d",
    ])
    return "\n".join(lines)


def _build_backup_text() -> str:
    return (
        "🧱 Backup / DB\n\n"
        "Telegram отдаёт лёгкие runtime/export файлы.\n"
        "Тяжёлый backup БД делаем отдельно через Postgres/Railway backup или pg_dump.\n\n"
        "Current files:\n"
        f"{_fmt_file(ПАПКА_ДАННЫХ / 'market_research_bundle.zip')}\n"
        f"{_fmt_file(ПАПКА_ДАННЫХ / 'storage_manifest.txt')}\n\n"
        "Next stage: отдельный безопасный backup/export контур без нагрузки на runtime loop."
    )


def _build_help_text() -> str:
    return (
        "🦆 Commands\n\n"
        "/panel — главная панель\n"
        "/status — короткий статус\n"
        "/runtime — runtime/cycle/watchdog\n"
        "/exports — состояние export файлов\n"
        "/reports — runtime reports zip\n"
        "/bundle — quick bundle\n"
        "/manifest — storage manifest\n"
        "/health — runtime health report\n"
        "/failures — request failures\n"
        "/gaps — gap report\n"
        "/active_universe — active universe\n"
        "/backup — backup policy/status\n"
        "/ping — pong"
    )


def _rebuild_exports_locked(mode: str):
    if not _export_lock.acquire(blocking=False):
        send_message("Export уже выполняется. Повтори команду позже.")
        return None

    try:
        return rebuild_exports(mode)
    finally:
        _export_lock.release()


def _handle(text: str) -> None:
    if text in {"/start", "/help"}:
        send_message(_build_control_panel_text(), _panel_keyboard())

    elif text in {"/panel", "/control"}:
        send_message(_build_control_panel_text(), _panel_keyboard())

    elif text == "/runtime":
        send_message(_build_runtime_text())

    elif text == "/exports":
        send_message(_build_exports_text())

    elif text == "/backup":
        send_message(_build_backup_text())

    elif text == "/ping":
        send_message("pong")

    elif text == "/status":
        send_message(_build_status_text())

    elif text == "/manifest":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "storage_manifest.txt", "manifest")

    elif text == "/bundle":
        send_document(ПАПКА_ДАННЫХ / "market_research_bundle.zip", "quick bundle")

    elif text == "/audit_report":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "audit_report.txt", "audit report")

    elif text == "/research_report":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "research_report.txt", "research report")

    elif text == "/reports":
        send_document(_build_runtime_reports_zip(), "runtime reports bundle")

    elif text == "/timing":
        send_document(ПАПКА_ДАННЫХ / "runtime_timing_report.txt", "runtime timing report")

    elif text == "/health":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "runtime_health_report.txt", "runtime health report")

    elif text == "/failures":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "request_failure_report.csv", "request failures")

    elif text == "/gaps":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "gap_report.csv", "gap report")

    elif text == "/active_universe":
        _ensure_quick_exports()
        send_document(ПАПКА_ДАННЫХ / "active_universe_report.csv", "active universe")

    elif text == "/export_quick":
        path = _rebuild_exports_locked("quick")
        if path:
            send_document(path, "quick bundle")

    elif text == "/export_research_7d":
        send_message("Готовлю research 7d bundle...")
        path = _rebuild_exports_locked("research_7d")
        if path:
            send_document(path, "research 7d bundle")

    elif text == "/export_research_30d":
        send_message("Готовлю research 30d bundle...")
        path = _rebuild_exports_locked("research_30d")
        if path:
            send_document(path, "research 30d bundle")



def _handle_callback(callback_id: str, data: str) -> None:
    answer_callback(callback_id)

    if data == "panel":
        send_message(_build_control_panel_text(), _panel_keyboard())
    elif data == "runtime":
        send_message(_build_runtime_text(), _panel_keyboard())
    elif data == "exports":
        send_message(_build_exports_text(), _panel_keyboard())
    elif data == "reports":
        send_document(_build_runtime_reports_zip(), "runtime reports bundle")
    elif data == "bundle":
        send_document(ПАПКА_ДАННЫХ / "market_research_bundle.zip", "quick bundle")
    elif data == "backup":
        send_message(_build_backup_text(), _panel_keyboard())

def _reset() -> None:
    global _offset

    try:
        requests.get(
            f"{BASE}/deleteWebhook",
            params={"drop_pending_updates": "true"},
            timeout=20,
        )
    except Exception as exc:
        log(f"deleteWebhook error: {exc}")

    _offset = 0


def _loop() -> None:
    global _offset

    time.sleep(6)
    _reset()

    while True:
        try:
            response = requests.get(
                f"{BASE}/getUpdates",
                params={"timeout": 30, "offset": _offset + 1},
                timeout=40,
            )
            response.raise_for_status()

            for item in response.json().get("result", []):
                _offset = item["update_id"]
                callback = item.get("callback_query") or {}
                if callback:
                    _handle_callback(
                        callback.get("id", ""),
                        callback.get("data", ""),
                    )
                    continue

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
