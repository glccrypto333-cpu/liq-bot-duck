from __future__ import annotations

import time
import atexit
import threading
import fcntl
import zipfile
import json
import os
import subprocess
from datetime import datetime, timezone
import csv
from pathlib import Path
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ПАПКА_ДАННЫХ, APP_VERSION
from logger import log
from db import fetch, execute
from reset_stage3 import reset_stage3

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
_polling_started = False
_offset = 0
_export_lock = threading.Lock()
_csv_lock = threading.Lock()
RUNTIME_REPORTS_DIR = Path(__file__).resolve().parent / "runtime_reports"
POLLING_LOCK_PATH = ПАПКА_ДАННЫХ / "telegram_polling.lock"
_polling_lock_file = None



def _release_polling_lock() -> None:
    global _polling_lock_file
    lock_file = _polling_lock_file
    _polling_lock_file = None
    if lock_file is None:
        return
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        lock_file.close()
    except Exception:
        pass


def _acquire_polling_lock() -> bool:
    global _polling_lock_file
    if _polling_lock_file is not None:
        return True

    POLLING_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_file = POLLING_LOCK_PATH.open("a+", encoding="utf-8")

    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        try:
            lock_file.seek(0)
            holder = lock_file.read().strip()
        except Exception:
            holder = ""
        log(f"telegram polling skipped: lock busy {holder}".strip())
        lock_file.close()
        return False

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(
        f"pid={os.getpid()} started_at={datetime.now(timezone.utc).isoformat()}\n"
    )
    lock_file.flush()
    _polling_lock_file = lock_file
    return True


atexit.register(_release_polling_lock)


def _main_keyboard() -> dict:
    return {
        "keyboard": [
            ["⚙️ Фазы", "📈 Топ ОИ"],
            ["⬇️ Скачать", "🧱 Карантин"],
            ["❓ Помощь"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _phases_keyboard() -> dict:
    return {
        "keyboard": [
            ["🥉 Фаза 1", "🥈 Фаза 2"],
            ["🥇 Фаза 3", "🧯 Сброс фазы 3"],
            ["⬅️ Назад"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _stage3_reset_keyboard() -> dict:
    return {
        "keyboard": [
            ["Сбросить по тикеру", "Сбросить все"],
            ["⬅️ Назад"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _top_oi_keyboard() -> dict:
    return {
        "keyboard": [
            ["🏆 BINANCE /30м", "🏆 BYBIT /30м"],
            ["🏆 BINANCE /4ч", "🏆 BYBIT /4ч"],
            ["🏆 BINANCE /24ч", "🏆 BYBIT /24ч"],
            ["⬅️ Назад"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _downloads_keyboard() -> dict:
    buttons = [f"/download {alias}" for alias, _ in _download_files()]
    keyboard = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    keyboard.append(["⬅️ Назад"])
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}


def _safe_tg_text(text: str, limit: int = 3900) -> str:
    text = str(text or "")
    if len(text) <= limit:
        return text
    return text[:limit - 80] + "\n\n... truncated. Use download/report for full output."


def send_message(text: str, reply_markup: dict | None = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": _safe_tg_text(text)}
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
    send_message(text, _main_keyboard())


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

    data_files = [
        ПАПКА_ДАННЫХ / "runtime_timing_report.txt",
        ПАПКА_ДАННЫХ / "runtime_health_report.txt",
        ПАПКА_ДАННЫХ / "request_failure_report.csv",
        ПАПКА_ДАННЫХ / "gap_report.csv",
        ПАПКА_ДАННЫХ / "active_universe_report.csv",
        ПАПКА_ДАННЫХ / "storage_manifest.txt",
    ]
    runtime_files = [
        RUNTIME_REPORTS_DIR / "runtime_health.json",
        RUNTIME_REPORTS_DIR / "cycle_status.json",
        RUNTIME_REPORTS_DIR / "watchdog_status.txt",
        RUNTIME_REPORTS_DIR / "snapshot_status.txt",
        RUNTIME_REPORTS_DIR / "runtime_health.txt",
        RUNTIME_REPORTS_DIR / "cycle_status.txt",
    ]

    added = 0
    with zipfile.ZipFile(report_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path in data_files:
            if path.exists():
                z.write(path, arcname=path.name)
                added += 1
        for path in runtime_files:
            if path.exists():
                z.write(path, arcname=f"runtime_reports/{path.name}")
                added += 1

    if added == 0:
        raise FileNotFoundError("runtime reports are missing")

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
    runtime = _read_json_file(RUNTIME_REPORTS_DIR / "runtime_health.json")
    cycle = _read_json_file(RUNTIME_REPORTS_DIR / "cycle_status.json")
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
        f"Управление: кнопки снизу"
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
    return "\n".join([
        "❓ Помощь",
        "",
        "Меню:",
        "⚙️ Фазы — Фаза 1 / Фаза 2 / Фаза 3 / Сброс фазы 3",
        "📈 Топ ОИ — BINANCE/BYBIT 30м / 4ч / 24ч",
        "⬇️ Скачать — файлы по кнопкам + OK/STALE/EMPTY/MISSING",
        "🧱 Карантин — управление видимостью/alerts",
        "",
        "Команды:",
        "/phases",
        "/phase1 /phase2 /phase3",
        "/top_oi BINANCE 30м",
        "/top_oi BYBIT 4ч",
        "/coin SYMBOL",
        "/feedback SYMBOL текст",
        "/feedback SYMBOL TF текст",
        "/debug_cases [SYMBOL]",
        "/post_stage [SYMBOL]",
        "/reset_stage3 SYMBOL reason",
        "/confirm_reset SYMBOL",
        "/cancel_reset",
        "/download filename",
        "/backup_db",
        "/archive",
        "/download backup_latest",
        "/health",
        "",
        "Фазы читаются из oi_core_state / oi_window_state / oi_stage_history.",
        "Карточка монеты и top OI работают на каноническом OI-only контуре.",
    ])


def _is_admin_chat(chat_id: str | int | None = None) -> bool:
    if not TELEGRAM_CHAT_ID:
        return False
    if chat_id is None:
        return True
    return str(chat_id) == str(TELEGRAM_CHAT_ID)


def _is_admin() -> bool:
    return _is_admin_chat()


def _safe_rows(sql: str, params: tuple = ()) -> list[dict]:
    try:
        return fetch(sql, params) or []
    except Exception as exc:
        log(f"telegram db fetch error: {exc}")
        return []


def _admin_only(chat_id=None) -> bool:
    if not _is_admin_chat(chat_id):
        send_message("⛔ Admin-only команда.", _main_keyboard())
        return False
    return True



def _tf_norm(value) -> str | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    aliases = {
        "15m": "15м", "15м": "15м",
        "30m": "30м", "30м": "30м",
        "1h": "1ч", "1ч": "1ч",
        "4h": "4ч", "4ч": "4ч",
        "24h": "24ч", "24ч": "24ч",
    }
    return aliases.get(v, v)


def _tf_sql(value) -> str | None:
    v = _tf_norm(value)
    aliases = {
        "15м": "15м",
        "30м": "30м",
        "1ч": "1ч",
        "4ч": "4ч",
        "24ч": "24ч",
    }
    return aliases.get(v, v)


def _window_rank_sql(column: str = "window_code") -> str:
    return (
        f"CASE {column} "
        "WHEN '15м' THEN 1 "
        "WHEN '30м' THEN 2 "
        "WHEN '1ч' THEN 3 "
        "WHEN '4ч' THEN 4 "
        "WHEN '12ч' THEN 5 "
        "WHEN '24ч' THEN 6 "
        "ELSE 9 END"
    )


def _stage_label(stage: int | None) -> str:
    return {
        0: "вне сценария",
        1: "тихое накопление",
        2: "развивающийся набор",
        3: "подтвержденный набор",
    }.get(int(stage or 0), "неизвестно")


def _exchange_code(exchange) -> str:
    return "BY" if str(exchange).upper() == "BYBIT" else "BN"


def _symbol_links(symbol: str, exchange=None) -> str:
    sym = str(symbol or "").upper()
    ex_code = _exchange_code(exchange)
    cg = f"https://www.coinglass.com/tv/Binance_{sym}"
    by = f"https://www.bybit.com/trade/usdt/{sym}"
    bn = f"https://www.binance.com/en/futures/{sym}"
    ex_url = by if ex_code == "BY" else bn
    return f'[CG]({cg}) | [{ex_code}]({ex_url}) | `{sym}`'


def _short_ts(value) -> str:
    return str(value or "n/a").replace("+00:00", " UTC")


def _table_health(table: str, ts_col: str, stale_minutes: int = 15) -> dict:
    allowed = {
        ("oi_raw", "ts_close"),
        ("aggregate_windows", "ts_close"),
        ("oi_core_state", "latest_cycle_ts"),
        ("oi_window_state", "cycle_ts"),
        ("oi_stage_history", "cycle_ts"),
        ("core_state_v2", "latest_cycle_ts"),
        ("window_state_v2", "cycle_ts"),
        ("transition_history_v2", "cycle_ts"),
    }
    if (table, ts_col) not in allowed:
        return {"table": table, "status": "ERROR", "rows": 0, "latest": None, "age_minutes": None}

    rows = _safe_rows(f"""
        SELECT
            COUNT(*) AS rows,
            MAX({ts_col}) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX({ts_col}))) / 60.0 AS age_minutes
        FROM {table}
    """)

    if not rows:
        return {"table": table, "status": "ERROR", "rows": 0, "latest": None, "age_minutes": None}

    r = rows[0]
    count = int(r.get("rows") or 0)
    latest = r.get("latest")
    age = r.get("age_minutes")

    if count <= 0:
        status = "EMPTY"
    elif latest is None:
        status = "EMPTY"
    elif float(age or 999999) > stale_minutes:
        status = "STALE"
    else:
        status = "OK"

    return {
        "table": table,
        "status": status,
        "rows": count,
        "latest": latest,
        "age_minutes": round(float(age or 0), 1) if age is not None else None,
    }


def _sync_health_pair(
    legacy_table: str,
    legacy_ts_col: str,
    v2_table: str,
    v2_ts_col: str,
    label: str,
) -> dict:
    legacy = _table_health(legacy_table, legacy_ts_col, 15)
    v2 = _table_health(v2_table, v2_ts_col, 15)

    if legacy["status"] != "OK" or v2["status"] != "OK":
        status = "DEGRADED"
    elif int(legacy["rows"] or 0) != int(v2["rows"] or 0):
        status = "DRIFT"
    elif str(legacy["latest"]) != str(v2["latest"]):
        status = "DRIFT"
    else:
        status = "OK"

    return {
        "label": label,
        "status": status,
        "legacy_rows": legacy["rows"],
        "v2_rows": v2["rows"],
        "legacy_latest": legacy["latest"],
        "v2_latest": v2["latest"],
    }


def _build_health_text() -> str:
    checks = [
        ("oi_raw", "ts_close", 15),
        ("aggregate_windows", "ts_close", 15),
        ("oi_core_state", "latest_cycle_ts", 15),
        ("core_state_v2", "latest_cycle_ts", 15),
        ("oi_window_state", "cycle_ts", 15),
        ("window_state_v2", "cycle_ts", 15),
        ("oi_stage_history", "cycle_ts", 60),
        ("transition_history_v2", "cycle_ts", 60),
    ]

    lines = ["🩺 Health — OI runtime", ""]

    for table, ts_col, stale_min in checks:
        h = _table_health(table, ts_col, stale_min)
        lines.append(
            f"{h['status']} | {h['table']} | rows={h['rows']} | "
            f"latest={_short_ts(h['latest'])} | age_min={h['age_minutes']}"
        )

    parity = [
        _sync_health_pair("oi_core_state", "latest_cycle_ts", "core_state_v2", "latest_cycle_ts", "core"),
        _sync_health_pair("oi_window_state", "cycle_ts", "window_state_v2", "cycle_ts", "window"),
    ]
    lines.append("")
    lines.append("legacy-v2 parity:")
    for item in parity:
        lines.append(
            f"{item['status']} | {item['label']} | "
            f"legacy_rows={item['legacy_rows']} | v2_rows={item['v2_rows']} | "
            f"legacy_latest={_short_ts(item['legacy_latest'])} | v2_latest={_short_ts(item['v2_latest'])}"
        )

    runtime, cycle = _runtime_snapshot()
    lines.extend([
        "",
        f"runtime_rss={runtime.get('rss_mb', 'n/a')} MB / {runtime.get('rss_health', 'n/a')}",
        f"watchdog={runtime.get('watchdog_health', 'n/a')}",
        f"cycle={cycle.get('cycle_elapsed_seconds', 'n/a')}s / {cycle.get('cycle_health', 'n/a')}",
        f"stop_reason={cycle.get('stop_reason', 'n/a')}",
        "",
        "OK <= stale window | STALE > stale window | EMPTY rows=0 | ERROR query failed",
    ])
    return "\n".join(lines)


def _health_banner_for_table(table: str, ts_col: str, stale_minutes: int = 10) -> str:
    h = _table_health(table, ts_col, stale_minutes)
    return (
        f"health={h['status']} | rows={h['rows']} | "
        f"latest={_short_ts(h['latest'])} | age_min={h['age_minutes']}"
    )


def _fmt_pct(value) -> str:
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "n/a"


def _tf_rank_sql() -> str:
    return "CASE timeframe WHEN '4h' THEN 1 WHEN '1h' THEN 2 WHEN '30m' THEN 3 WHEN '15m' THEN 4 ELSE 9 END"


def _build_phases_text(phase: int | None = None) -> str:
    if phase is None:
        rows = _safe_rows("""
            SELECT current_stage, COUNT(*) AS cnt, MAX(latest_cycle_ts) AS latest
            FROM core_state_v2
            WHERE current_stage > 0
            GROUP BY current_stage
            ORDER BY current_stage DESC
        """)
        if not rows:
            return "⚙️ Фазы\n\nАктивных фаз нет."

        lines = ["⚙️ Фазы", ""]
        for r in rows:
            lines.append(
                f"phase={r.get('current_stage')} {_stage_label(r.get('current_stage'))} | cnt={r.get('cnt')} | latest={_short_ts(r.get('latest'))}"
            )
        lines.extend(["", "Открыть: Фаза 1 / Фаза 2 / Фаза 3"])
        return "\n".join(lines)

    rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE current_stage = %s
        ORDER BY stage_age_minutes DESC, latest_cycle_ts DESC, exchange, symbol
        LIMIT 80
    """, (phase,))

    title = f"Фаза {phase}"
    if not rows:
        return f"{title}\n\nСейчас монет в фазе нет."

    lines = [title, "Детали: /coin SYMBOL", ""]
    for r in rows:
        symbol = r.get("symbol")
        ex = r.get("exchange")
        oi = r.get("oi_summary") or {}
        price = r.get("price_summary") or {}
        volume = r.get("volume_summary") or {}
        lines.append(f"{symbol} [{ex}]")
        lines.append(f"🔗 {_symbol_links(symbol, ex)} | /coin {symbol} | /feedback {symbol} текст")
        lines.append(
            f"pattern={oi.get('oi_pattern_label') or oi.get('oi_pattern_code')} | "
            f"price={price.get('price_state') or 'n/a'} | volume={volume.get('volume_state') or 'n/a'}"
        )
        lines.append(
            f"age={r.get('stage_age_minutes')}m | "
            f"transition={r.get('transition_permission')} | latest={_short_ts(r.get('latest_cycle_ts'))}"
        )
        lines.append("")

    return "\n".join(lines)


def _build_stage3_text() -> str:
    return _build_phases_text(3)


def _parse_top_oi_args(text: str) -> tuple[str | None, str | None]:
    exchange = None
    timeframe = None

    for part in (text or "").split():
        token = str(part or "").strip()
        upper = token.upper()
        tf = _tf_sql(token)

        if upper in {"BINANCE", "BYBIT"} and exchange is None:
            exchange = upper
        elif tf in {"15м", "30м", "1ч", "4ч", "12ч", "24ч"} and timeframe is None:
            timeframe = tf

    return timeframe, exchange


def _build_top_oi_text(timeframe: str | None = None, exchange: str | None = None) -> str:
    timeframe = _tf_sql(timeframe) if timeframe else None
    exchange = str(exchange or "").upper().strip() or None

    params = []
    where = ["COALESCE(oi_slope_value, 1.0) <> 1.0"]

    if exchange in {"BINANCE", "BYBIT"}:
        where.append("w.exchange = %s")
        params.append(exchange)

    if timeframe in {"15м", "30м", "1ч", "4ч", "12ч", "24ч"}:
        where.append("w.window_code = %s")
        params.append(timeframe)

    where_sql = "WHERE " + " AND ".join(where)

    rows = _safe_rows(f"""
        SELECT
            w.exchange,
            w.symbol,
            w.window_code,
            w.oi_slope_class,
            w.oi_slope_value,
            w.price_regime,
            w.price_direction,
            w.volume_class,
            w.cycle_ts,
            c.current_stage,
            c.stage_age_minutes,
            c.oi_summary,
            c.price_summary,
            c.volume_summary
        FROM window_state_v2 w
        LEFT JOIN core_state_v2 c
          ON c.exchange = w.exchange
         AND c.symbol = w.symbol
        {where_sql}
        ORDER BY ABS(COALESCE(w.oi_slope_value, 1.0) - 1.0) DESC,
                 c.current_stage DESC,
                 w.cycle_ts DESC,
                 w.exchange,
                 w.symbol
        LIMIT 80
    """, tuple(params))

    rows = sorted(
        rows,
        key=lambda r: (abs(float(r.get("oi_slope_value") or 1.0) - 1.0), int(r.get("current_stage") or 0)),
        reverse=True,
    )[:10]

    ex_title = exchange or "ALL"
    tf_title = timeframe or "ALL"
    title = f"🏆 TOP OI за {tf_title} — {ex_title}"

    if not rows:
        return f"{title}\n\nНет строк в window_state_v2."

    lines = [title, "_window_state_v2 snapshot_"]

    for i, r in enumerate(rows, 1):
        symbol = r.get("symbol")
        ex = r.get("exchange")
        oi = r.get("oi_slope_value")
        try:
            oi_text = f"{float(oi):.6f}"
        except Exception:
            oi_text = "n/a"
        oi_summary = r.get("oi_summary") or {}
        price_summary = r.get("price_summary") or {}
        volume_summary = r.get("volume_summary") or {}
        links = _symbol_links(symbol, ex)

        lines.append(
            f"{i}. `{symbol}` — OI {r.get('oi_slope_class')} ({oi_text}) | phase={r.get('current_stage')} {_stage_label(r.get('current_stage'))} | "
            f"pattern={oi_summary.get('oi_pattern_label') or oi_summary.get('oi_pattern_code')} | "
            f"price={price_summary.get('price_state') or (str(r.get('price_regime')) + '/' + str(r.get('price_direction')))} | "
            f"volume={volume_summary.get('volume_state') or r.get('volume_class')} | {links}"
        )

    return "\n".join(lines)
def _build_coin_card(symbol: str) -> str:
    symbol = symbol.upper().strip()

    core_rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 20
    """, (symbol,))

    window_rows = _safe_rows(f"""
        SELECT *
        FROM window_state_v2
        WHERE symbol = %s
        ORDER BY exchange, {_window_rank_sql()}, cycle_ts DESC
    """, (symbol,))

    history_rows = _safe_rows("""
        SELECT *
        FROM transition_history_v2
        WHERE symbol = %s
        ORDER BY cycle_ts DESC
        LIMIT 12
    """, (symbol,))

    if not core_rows and not window_rows:
        return f"🪙 {symbol}\n\nНет данных. Формат: /coin BTCUSDT"

    lines = [f"🪙 {symbol}", ""]

    if core_rows:
        lines.append("OI CORE V2:")
        for r in core_rows:
            ex = r.get("exchange")
            ex_windows = [row for row in window_rows if row.get("exchange") == ex]
            oi = r.get("oi_summary") or {}
            price = r.get("price_summary") or {}
            volume = r.get("volume_summary") or {}

            lines.extend([
                "",
                f"{symbol} [{ex}]",
                _symbol_links(symbol, ex),
                f"phase={r.get('current_stage')} {_stage_label(r.get('current_stage'))} | latest={_short_ts(r.get('latest_cycle_ts'))}",
                f"pattern={oi.get('oi_pattern_label') or oi.get('oi_pattern_code')}",
                f"OI summary: dir={oi.get('oi_direction_summary')} | angle={oi.get('oi_angle_summary')} | hold={oi.get('oi_retention_summary')} | stability={oi.get('oi_stability_summary')}",
                f"OI slopes: 1h={oi.get('oi_slope_class_1h')} ({oi.get('oi_slope_ratio_1h')}) | 4h={oi.get('oi_slope_class_4h')} ({oi.get('oi_slope_ratio_4h')})",
                f"PRICE: {price.get('price_state')} | block={price.get('price_block')} | hard_ban={r.get('price_hard_ban')}",
                f"VOLUME: {volume.get('volume_state')} | confirm={volume.get('volume_confidence')}",
                f"age={r.get('stage_age_minutes')}m | transition={r.get('transition_permission')} | manual_reset_required={r.get('manual_reset_required')}",
                f"reason={r.get('phase_reason')}",
                f"Feedback: /feedback {symbol} текст",
                f"Debug: /debug_cases {symbol} | Analytics: /post_stage {symbol}",
            ])
            if ex_windows:
                lines.append("windows_v2:")
                for w in ex_windows:
                    slope_value = w.get('oi_slope_value')
                    try:
                        slope_text = f"{float(slope_value):.6f}"
                    except Exception:
                        slope_text = 'n/a'
                    lines.append(
                        f"{w.get('window_code')}: oi={w.get('oi_slope_class')} ({slope_text}) "
                        f"| hold={w.get('oi_hold_class')} | pullback={w.get('oi_pullback_class')} "
                        f"| smooth={w.get('oi_smoothness_class')} | price={w.get('price_regime')}/{w.get('price_direction')} "
                        f"| volume={w.get('volume_class')} | v10x={w.get('volume_10x_confirmed')}"
                    )

    if history_rows:
        lines.extend(["", "STAGE HISTORY V2:"])
        for r in history_rows[:8]:
            lines.append(
                f"{r.get('exchange')} | {r.get('from_stage')} -> {r.get('to_stage')} | "
                f"allowed={r.get('transition_allowed')} | age_before={r.get('stage_age_before_transition')}m | "
                f"{_short_ts(r.get('cycle_ts'))} | reason={r.get('reason')}"
            )

    return "\n".join(lines)

def _save_debug_cases(symbol: str, comment: str, core_rows: list[dict], window_rows: list[dict], history_rows: list[dict]) -> int:
    written = 0
    for r in core_rows:
        execute(
            """
            INSERT INTO debug_cases_v2(
                exchange, symbol, cycle_ts, current_stage,
                oi_summary, price_summary, volume_summary,
                user_comment, status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, NOW(), NOW())
            """,
            (
                r.get("exchange"),
                symbol,
                r.get("latest_cycle_ts"),
                r.get("current_stage"),
                json.dumps(r.get("oi_summary") or {}, ensure_ascii=False, default=str),
                json.dumps(r.get("price_summary") or {}, ensure_ascii=False, default=str),
                json.dumps(r.get("volume_summary") or {}, ensure_ascii=False, default=str),
                comment,
                "new",
            ),
        )
        written += 1
    return written

def _build_debug_cases_text(symbol: str | None = None) -> str:
    symbol = str(symbol or "").upper().strip() or None
    params = []
    where = ""
    title = "🧪 Debug cases"
    if symbol:
        where = "WHERE symbol = %s"
        params.append(symbol)
        title = f"🧪 Debug cases — {symbol}"

    rows = _safe_rows(
        f"""
        SELECT exchange, symbol, cycle_ts, current_stage, oi_summary, user_comment, status, created_at
        FROM debug_cases_v2
        {where}
        ORDER BY created_at DESC
        LIMIT 12
        """,
        tuple(params),
    )

    if not rows:
        return f"{title}\n\nНет строк в debug_cases_v2."

    lines = [title, "_latest v2 debug snapshots_"]
    for idx, row in enumerate(rows, 1):
        oi = row.get("oi_summary") or {}
        lines.append(
            f"{idx}. {row.get('symbol')} [{row.get('exchange')}] | "
            f"stage={row.get('current_stage')} | pattern={oi.get('oi_pattern_label') or oi.get('oi_pattern_code')} | "
            f"status={row.get('status') or 'n/a'} | cycle={_short_ts(row.get('cycle_ts'))}"
        )
        comment = str(row.get("user_comment") or "").strip()
        if comment:
            lines.append(f"   comment={comment[:180]}")
    return "\n".join(lines)

def _fmt_post_stage_delta(trigger_price, future_price) -> str:
    if trigger_price in (None, 0) or future_price is None:
        return "n/a"
    try:
        delta_pct = ((float(future_price) - float(trigger_price)) / float(trigger_price)) * 100.0
    except Exception:
        return "n/a"
    return f"{delta_pct:+.2f}%"


def _post_stage_quality_ru(value: str | None) -> str:
    mapping = {
        "positive": "positive",
        "negative": "negative",
        "flat": "flat",
        "partial": "partial",
        "pending_24h": "pending_24h",
        "pending": "pending",
    }
    return mapping.get(str(value or "").strip(), str(value or "n/a"))


def _build_post_stage_text(symbol: str | None = None) -> str:
    symbol = str(symbol or "").upper().strip() or None
    params = []
    where = ""
    title = "📊 Post-stage analytics"
    if symbol:
        where = "WHERE symbol = %s"
        params.append(symbol)
        title = f"📊 Post-stage analytics — {symbol}"

    rows = _safe_rows(
        f"""
        SELECT exchange, symbol, stage_triggered, triggered_at, trigger_price,
               price_after_1h, price_after_4h, price_after_12h, price_after_24h,
               quality_label, notes
        FROM post_stage_analytics_v2
        {where}
        ORDER BY triggered_at DESC
        LIMIT 12
        """,
        tuple(params),
    )

    if not rows:
        return f"{title}\n\nНет строк в post_stage_analytics_v2."

    quality_counts = {}
    matured_24h = 0
    for row in rows:
        quality = str(row.get("quality_label") or "n/a")
        quality_counts[quality] = quality_counts.get(quality, 0) + 1
        if row.get("price_after_24h") is not None:
            matured_24h += 1

    summary = ", ".join(f"{k}={v}" for k, v in sorted(quality_counts.items()))
    lines = [
        title,
        f"rows={len(rows)} | matured_24h={matured_24h} | {summary}",
        "_latest v2 post-stage outcomes_",
    ]
    for idx, row in enumerate(rows, 1):
        lines.append(
            f"{idx}. {row.get('symbol')} [{row.get('exchange')}] | stage={row.get('stage_triggered')} | "
            f"trigger={_short_ts(row.get('triggered_at'))} | quality={_post_stage_quality_ru(row.get('quality_label'))}"
        )
        lines.append(
            f"   1h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_1h'))} | "
            f"4h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_4h'))} | "
            f"12h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_12h'))} | "
            f"24h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_24h'))}"
        )
        notes = str(row.get("notes") or "").strip()
        if notes:
            lines.append(f"   notes={notes[:180]}")
    return "\n".join(lines)
def _feedback_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_feedback.csv"



def _save_feedback(text: str) -> str:
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        return "Формат: /feedback SYMBOL текст"

    _, symbol, comment = parts
    symbol = symbol.upper().strip()

    core_rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 20
    """, (symbol,))

    if not core_rows:
        return f"Нет core_state_v2 snapshot для {symbol}. Комментарий не сохранён."

    window_rows = _safe_rows(f"""
        SELECT *
        FROM window_state_v2
        WHERE symbol = %s
        ORDER BY exchange, {_window_rank_sql()}, cycle_ts DESC
    """, (symbol,))

    history_rows = _safe_rows("""
        SELECT *
        FROM transition_history_v2
        WHERE symbol = %s
        ORDER BY cycle_ts DESC
        LIMIT 20
    """, (symbol,))

    debug_written = _save_debug_cases(symbol, comment, core_rows, window_rows, history_rows)

    path = _feedback_path()
    new_file = not path.exists()

    header = [
        "created_at_utc",
        "symbol",
        "exchange",
        "current_stage",
        "oi_pattern_code",
        "oi_pattern_label",
        "price_state_summary",
        "volume_state_summary",
        "oi_stage_age_minutes",
        "oi_transition_permission",
        "blocked_stage_max",
        "latest_cycle_ts",
        "decision_reason",
        "core_state_json",
        "window_state_json",
        "stage_history_json",
        "user_comment",
    ]

    now = datetime.now(timezone.utc).isoformat()
    written = 0

    with _csv_lock:
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(header)

            for r in core_rows:
                ex = r.get("exchange")
                ex_windows = [row for row in window_rows if row.get("exchange") == ex]
                ex_history = [row for row in history_rows if row.get("exchange") == ex]
                oi = r.get("oi_summary") or {}
                price = r.get("price_summary") or {}
                volume = r.get("volume_summary") or {}

                w.writerow([
                    now,
                    symbol,
                    ex,
                    r.get("current_stage"),
                    oi.get("oi_pattern_code"),
                    oi.get("oi_pattern_label"),
                    price.get("price_state"),
                    volume.get("volume_state"),
                    r.get("stage_age_minutes"),
                    r.get("transition_permission"),
                    price.get("blocked_stage_max"),
                    r.get("latest_cycle_ts"),
                    r.get("phase_reason"),
                    json.dumps(r, ensure_ascii=False, default=str),
                    json.dumps(ex_windows, ensure_ascii=False, default=str),
                    json.dumps(ex_history, ensure_ascii=False, default=str),
                    comment,
                ])
                written += 1

    return f"✅ Feedback snapshot v2 сохранён: {symbol}, rows={written}, debug_cases={debug_written}"

def _download_files() -> list[tuple[str, str]]:
    return [
        ("bundle", "market_research_bundle.zip"),
        ("reports", "runtime_reports.zip"),
        ("manifest", "storage_manifest.txt"),
        ("health", "runtime_health_report.txt"),
        ("timing", "runtime_timing_report.txt"),
        ("failures", "request_failure_report.csv"),
        ("gaps", "gap_report.csv"),
        ("universe", "active_universe_report.csv"),
        ("feedback", "telegram_feedback.csv"),
        ("quarantine", "telegram_quarantine.csv"),
        ("q_history", "telegram_quarantine_history.csv"),
        ("stage3_alerts", "telegram_stage3_alert_history.csv"),
        ("pending_reset", "telegram_pending_reset_stage3.json"),
    ]


def _download_name_map() -> dict[str, str]:
    out = {}
    for alias, filename in _download_files():
        out[alias] = filename
        out[filename] = filename
    return out


def _file_status(path: Path, stale_minutes: int = 60) -> dict:
    if not path.exists():
        return {"status": "MISSING", "size": 0, "rows": 0, "age_min": None, "mtime": None}

    size = path.stat().st_size
    mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    age_min = round((datetime.now(timezone.utc) - mtime).total_seconds() / 60.0, 1)

    if size <= 0:
        status = "EMPTY"
    elif age_min > stale_minutes:
        status = "STALE"
    else:
        status = "OK"

    rows = 0
    if path.suffix.lower() in {".csv", ".txt"}:
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                rows = max(sum(1 for _ in f) - 1, 0) if path.suffix.lower() == ".csv" else sum(1 for _ in f)
        except Exception:
            rows = -1

    return {"status": status, "size": size, "rows": rows, "age_min": age_min, "mtime": mtime}


def _build_downloads_text() -> str:
    lines = ["⬇️ Скачать файлы", "", "Статус runtime files:"]
    for alias, filename in _download_files():
        path = ПАПКА_ДАННЫХ / filename
        st = _file_status(path)
        age = "—" if st.get("age_min") is None else f'{st.get("age_min"):.1f}m'
        size = st.get("size", 0)
        rows = st.get("rows", 0)
        lines.append(
            f"/download {alias} — {st.get('status')} | rows={rows} | age={age} | size={size}"
        )
    return "\n".join(lines)


def _send_download(name: str) -> None:
    if name == "backup_latest":
        last = _latest_archive_entry("backup_db")
        if not last or not last.get("file"):
            send_message("Файл backup_latest не найден.", _main_keyboard())
            return
        send_document(Path(last["file"]), "latest postgres backup")
        return

    allowed = _download_name_map()
    allowed["active"] = "active_universe_report.csv"
    filename = allowed.get(name)
    if not filename:
        send_message(
            "Формат: /download bundle|reports|manifest|health|timing|failures|gaps|universe|feedback|quarantine|q_history|stage3_alerts|pending_reset",
            _main_keyboard(),
        )
        return

    path = ПАПКА_ДАННЫХ / filename
    if filename == "runtime_reports.zip":
        try:
            path = _build_runtime_reports_zip()
        except FileNotFoundError:
            send_message("Runtime reports пока не собраны.", _main_keyboard())
            return

    send_document(path, filename)

def _quarantine_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_quarantine.csv"


def _quarantine_history_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_quarantine_history.csv"


def _read_quarantine() -> dict[str, str]:
    path = _quarantine_path()
    data = {}
    if not path.exists():
        return data
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            data[row["symbol"]] = row.get("reason", "")
    return data


def _write_quarantine(data: dict[str, str]) -> None:
    path = _quarantine_path()
    with _csv_lock:
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["symbol", "reason", "updated_at_utc"])
            now = datetime.now(timezone.utc).isoformat()
            for symbol, reason in sorted(data.items()):
                w.writerow([symbol, reason, now])


def _append_quarantine_history(action: str, symbol: str, reason: str) -> None:
    path = _quarantine_history_path()
    new_file = not path.exists()
    with _csv_lock:
        with path.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["created_at_utc", "action", "symbol", "reason"])
            w.writerow([datetime.now(timezone.utc).isoformat(), action, symbol, reason])



def _build_quarantine_status_text() -> str:
    data = _read_quarantine()

    code_hits = []
    for path in Path(".").glob("*.py"):
        if path.name == "telegram_bot.py":
            continue
        text = path.read_text(errors="ignore")
        if "telegram_quarantine" in text or "_read_quarantine" in text or "quarantine" in text.lower():
            code_hits.append(path.name)

    mode = "CORE-LINKED" if code_hits else "UI-ONLY"

    lines = [
        "🧱 Quarantine status",
        "",
        f"mode={mode}",
        f"symbols={len(data)}",
        f"file={_quarantine_path()}",
        f"history={_quarantine_history_path()}",
        "",
    ]

    if code_hits:
        lines.append("Core references:")
        lines.extend(f"- {name}" for name in sorted(set(code_hits)))
    else:
        lines.append("Core references: not found")
        lines.append("Важно: quarantine сейчас не доказан как core-фильтр. UI-only до отдельной интеграции.")

    if data:
        lines.append("")
        lines.append("Symbols:")
        lines.extend(f"{s}: {r}" for s, r in sorted(data.items())[:50])

    return "\n".join(lines)


def _handle_quarantine(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    parts = text.split(maxsplit=3)
    data = _read_quarantine()

    if len(parts) >= 2 and parts[1] == "status":
        send_message(_build_quarantine_status_text(), _main_keyboard())
        return

    if len(parts) == 1 or parts[1] == "list":
        if not data:
            send_message("🧱 Quarantine\n\nСписок пуст.", _main_keyboard())
            return
        send_message("🧱 Quarantine\n\n" + "\n".join(f"{s}: {r}" for s, r in sorted(data.items())), _main_keyboard())
        return

    action = parts[1]
    symbol = parts[2].upper() if len(parts) >= 3 else ""
    reason = parts[3] if len(parts) >= 4 else ""

    if action == "add" and symbol:
        data[symbol] = reason or "manual"
        _write_quarantine(data)
        _append_quarantine_history("add", symbol, data[symbol])
        send_message(f"✅ Quarantine add: {symbol}", _main_keyboard())
    elif action == "remove" and symbol:
        old = data.pop(symbol, "")
        _write_quarantine(data)
        _append_quarantine_history("remove", symbol, old)
        send_message(f"✅ Quarantine remove: {symbol}", _main_keyboard())
    elif action == "history":
        send_document(_quarantine_history_path(), "quarantine history")
    else:
        send_message("Формат: /quarantine list | add SYMBOL reason | remove SYMBOL | history", _main_keyboard())



def _pending_reset_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_pending_reset_stage3.json"


def _save_pending_reset(symbol: str, reason: str) -> None:
    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol.upper(),
        "reason": reason,
    }
    _pending_reset_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def _load_pending_reset() -> dict:
    path = _pending_reset_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}


def _clear_pending_reset() -> None:
    path = _pending_reset_path()
    if path.exists():
        path.unlink()


def _handle_stage3_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        send_message("Формат: /reset_stage3 SYMBOL reason", _main_keyboard())
        return

    _, symbol, reason = parts
    symbol = symbol.upper().strip()
    reason = reason.strip()

    _save_pending_reset(symbol, reason)

    send_message(
        "\n".join([
            "⚠️ Pending Stage3 reset создан",
            "",
            f"symbol={symbol}",
            f"reason={reason}",
            "",
            f"Подтвердить: /confirm_reset {symbol}",
            "Отменить: /cancel_reset",
        ]),
        _main_keyboard(),
    )


def _handle_confirm_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    pending = _load_pending_reset()
    if not pending:
        send_message("Нет pending reset.", _main_keyboard())
        return

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        send_message("Формат: /confirm_reset SYMBOL", _main_keyboard())
        return

    _, symbol = parts
    symbol = symbol.upper().strip()

    if symbol != pending.get("symbol"):
        send_message(
            f"Pending не совпадает. Сейчас pending: {pending.get('symbol')}",
            _main_keyboard(),
        )
        return

    total = 0
    reason = pending.get("reason") or "confirmed_reset"

    for exchange in ("BYBIT", "BINANCE"):
        try:
            total += max(reset_stage3(exchange, symbol, "n/a", reason, dry_run=False), 0)
        except Exception as exc:
            log(f"telegram confirm_reset error: {exc}")

    _clear_pending_reset()

    send_message(
        f"✅ Stage3 reset confirmed: {symbol}, rows={total}",
        _main_keyboard(),
    )


def _handle_cancel_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    pending = _load_pending_reset()
    _clear_pending_reset()

    if pending:
        send_message(
            f"✅ Pending reset отменён: {pending.get('symbol')}",
            _main_keyboard(),
        )
    else:
        send_message("Pending reset не найден.", _main_keyboard())
def _stage3_alert_history_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_stage3_alert_history.csv"


def _read_stage3_alerted_keys() -> set[str]:
    path = _stage3_alert_history_path()
    if not path.exists():
        return set()

    keys = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = row.get("alert_key")
            if key:
                keys.add(key)
    return keys


def _append_stage3_alert_history(row: dict, alert_key: str) -> None:
    path = _stage3_alert_history_path()
    new_file = not path.exists()

    header = [
        "created_at_utc",
        "alert_key",
        "exchange",
        "symbol",
        "current_stage",
        "oi_pattern_code",
        "oi_pattern_label",
        "price_state_summary",
        "volume_state_summary",
        "oi_stage_age_minutes",
        "latest_cycle_ts",
        "decision_reason",
    ]

    with _csv_lock:
        with path.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(header)

            w.writerow([
                datetime.now(timezone.utc).isoformat(),
                alert_key,
                row.get("exchange"),
                row.get("symbol"),
                row.get("current_stage"),
                row.get("oi_pattern_code"),
                row.get("oi_pattern_label"),
                row.get("price_state_summary"),
                row.get("volume_state_summary"),
                row.get("oi_stage_age_minutes"),
                row.get("latest_cycle_ts"),
                row.get("decision_reason"),
            ])


def _build_stage3_alert_text(r: dict) -> str:
    symbol = r.get("symbol")
    ex = r.get("exchange")
    oi = r.get("oi_summary") or {}
    price = r.get("price_summary") or {}
    volume = r.get("volume_summary") or {}
    ex_windows = _safe_rows(f"""
        SELECT *
        FROM window_state_v2
        WHERE exchange = %s
          AND symbol = %s
        ORDER BY {_window_rank_sql()}, cycle_ts DESC
    """, (ex, symbol))
    window_line = " | ".join(
        f"{w.get('window_code')}={w.get('oi_slope_class')}"
        for w in ex_windows[:4]
    )

    return "\n".join([
        "🥇 NEW STAGE 3",
        "",
        f"{symbol} [{ex}]",
        _symbol_links(symbol, ex),
        f"phase={r.get('current_stage')} {_stage_label(r.get('current_stage'))} | updated={_short_ts(r.get('latest_cycle_ts'))}",
        f"pattern={oi.get('oi_pattern_label') or oi.get('oi_pattern_code')}",
        f"PRICE: {price.get('price_state')}",
        f"VOL: {volume.get('volume_state')}",
        f"age={r.get('stage_age_minutes')}m",
        f"reason={r.get('phase_reason')}",
        f"windows={window_line or 'n/a'}",
        "",
        f"Card: /coin {symbol}",
        f"Feedback: /feedback {symbol} текст",
        f"Reset: /reset_stage3 {symbol} reason",
    ])
def check_stage3_alerts() -> int:
    alerted = _read_stage3_alerted_keys()

    rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE current_stage = 3
        ORDER BY latest_cycle_ts DESC
        LIMIT 50
    """)

    sent = 0

    for r in rows:
        key = "|".join([
            str(r.get("exchange")),
            str(r.get("symbol")),
            "stage=3",
        ])

        if key in alerted:
            continue

        send_message(_build_stage3_alert_text(r), _main_keyboard())
        _append_stage3_alert_history(r, key)
        sent += 1

    return sent
def _archive_index_path() -> Path:
    return Path("archive") / "manifests" / "archive_index.json"


def _read_archive_index() -> list[dict]:
    p = _archive_index_path()
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _latest_archive_entry(kind: str | None = None) -> dict | None:
    rows = _read_archive_index()
    if kind:
        rows = [r for r in rows if r.get("type") == kind]
    return rows[-1] if rows else None


def _build_archive_text() -> str:
    rows = _read_archive_index()
    if not rows:
        return "🗂 Archive\n\nПока manifest пуст."

    last = rows[-1]
    backups = [r for r in rows if r.get("type") == "backup_db"]
    last_backup = backups[-1] if backups else None

    lines = [
        "🗂 Archive",
        "",
        f"entries={len(rows)}",
    ]

    if last_backup:
        lines += [
            "",
            "Last DB backup:",
            f"status={last_backup.get('status')}",
            f"file={last_backup.get('file')}",
            f"size_mb={last_backup.get('size_mb')}",
            f"duration_sec={last_backup.get('duration_sec')}",
            f"finished_at={last_backup.get('finished_at')}",
        ]

    lines += [
        "",
        "Commands:",
        "/backup_db",
        "/download backup_latest",
    ]

    return "\n".join(lines)


def _run_backup_db() -> str:
    lock = Path("archive") / "locks" / "heavy_job.lock"

    if lock.exists():
        return f"⛔ Heavy job уже идёт\n\nlock={lock}"

    send_message("⏳ DB backup started\n\nЭто heavy job. Runtime не трогаем.")

    started = time.time()

    proc = subprocess.run(
        ["python3", "backup_db.py"],
        capture_output=True,
        text=True,
        timeout=1800,
    )

    duration = round(time.time() - started, 2)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "")[-3000:]
        return f"❌ DB backup failed\n\nduration_sec={duration}\n\n{err}"

    last = _latest_archive_entry("backup_db")
    if not last:
        return f"⚠️ Backup finished, но manifest не найден\nduration_sec={duration}"

    return "\n".join([
        "✅ DB backup complete",
        "",
        f"file={last.get('file')}",
        f"size_mb={last.get('size_mb')}",
        f"duration_sec={last.get('duration_sec')}",
        f"total_runtime_sec={duration}",
        f"finished_at={last.get('finished_at')}",
    ])


def _handle(text: str, chat_id=None) -> None:
    text = text.strip()

    if not _is_admin_chat(chat_id):
        log(f"telegram unauthorized chat ignored: chat_id={chat_id} text={text[:120]}")
        return

    if text in {"/start", "/help", "❓ Помощь"}:
        send_message(_build_help_text(), _main_keyboard())

    elif text in {"/panel", "/control"}:
        send_message(_build_control_panel_text(), _main_keyboard())

    elif text in {"/phases", "⚙️ Фазы"}:
        send_message(_build_phases_text(), _phases_keyboard())

    elif text in {"/phase1", "🥉 Фаза 1"}:
        send_message(_build_phases_text(1), _phases_keyboard())

    elif text in {"/phase2", "🥈 Фаза 2"}:
        send_message(_build_phases_text(2), _phases_keyboard())

    elif text in {"/phase3", "🥇 Фаза 3"}:
        send_message(_build_stage3_text(), _phases_keyboard())

    elif text in {"/top_oi", "📈 ТОП OI", "📈 Топ ОИ"}:
        send_message("📈 Топ ОИ\n\nВыбери биржу и окно ниже.", _top_oi_keyboard())


    elif text in {"⬅️ Назад", "/menu"}:
        send_message("Главное меню", _main_keyboard())

    elif text == "🧯 Сброс фазы 3":
        send_message("Сброс фазы 3", _stage3_reset_keyboard())

    elif text == "Сбросить по тикеру":
        send_message("Формат: /reset_stage3 SYMBOL reason", _stage3_reset_keyboard())

    elif text == "Сбросить все":
        send_message("Массовый reset пока не исполняется из кнопки. Используй точечно: /reset_stage3 SYMBOL reason", _stage3_reset_keyboard())

    elif text in {"15м", "30м", "4ч", "24ч"}:
        send_message(_build_top_oi_text(text), _top_oi_keyboard())

    elif text in {"🧱 Карантин", "🧱 Quarantine", "/quarantine"}:
        send_message(_build_quarantine_status_text(), _main_keyboard())


    elif text in {"🏆 BINANCE /30м", "🏆 BINANCE /30m"}:
        send_message(_build_top_oi_text("30м", "BINANCE"), _top_oi_keyboard())

    elif text in {"🏆 BYBIT /30м", "🏆 BYBIT /30m"}:
        send_message(_build_top_oi_text("30м", "BYBIT"), _top_oi_keyboard())

    elif text in {"🏆 BINANCE /4ч", "🏆 BINANCE /4h"}:
        send_message(_build_top_oi_text("4ч", "BINANCE"), _top_oi_keyboard())

    elif text in {"🏆 BYBIT /4ч", "🏆 BYBIT /4h"}:
        send_message(_build_top_oi_text("4ч", "BYBIT"), _top_oi_keyboard())

    elif text in {"🏆 BINANCE /24ч", "🏆 BINANCE /24h"}:
        send_message(_build_top_oi_text("24ч", "BINANCE"), _top_oi_keyboard())

    elif text in {"🏆 BYBIT /24ч", "🏆 BYBIT /24h"}:
        send_message(_build_top_oi_text("24ч", "BYBIT"), _top_oi_keyboard())

    elif text.startswith("/top_oi "):
        timeframe, exchange = _parse_top_oi_args(text.split(maxsplit=1)[1].strip())
        send_message(
            _build_top_oi_text(timeframe, exchange),
            _main_keyboard()
        )

    elif text in {"/coin", "🪙 Coin"}:
        send_message("Формат: /coin BTCUSDT", _main_keyboard())

    elif text.startswith("/coin "):
        send_message(_build_coin_card(text.split(maxsplit=1)[1]), _main_keyboard())

    elif text.startswith("/feedback "):
        send_message(_save_feedback(text), _main_keyboard())

    elif text == "/debug_cases":
        send_message(_build_debug_cases_text(), _main_keyboard())

    elif text.startswith("/debug_cases "):
        send_message(_build_debug_cases_text(text.split(maxsplit=1)[1]), _main_keyboard())

    elif text == "/post_stage":
        send_message(_build_post_stage_text(), _main_keyboard())

    elif text.startswith("/post_stage "):
        send_message(_build_post_stage_text(text.split(maxsplit=1)[1]), _main_keyboard())

    elif text in {"/downloads", "⬇️ Скачать"}:
        send_message(_build_downloads_text(), _downloads_keyboard())

    elif text.startswith("/download "):
        _send_download(text.split(maxsplit=1)[1].strip())

    elif text == "/backup_db":
        send_message(_run_backup_db(), _main_keyboard())

    elif text == "/archive":
        send_message(_build_archive_text(), _main_keyboard())

    elif text in {"/quarantine", "🧱 Quarantine"} or text.startswith("/quarantine "):
        _handle_quarantine(text, chat_id)

    elif text.startswith("/reset_stage3 "):
        _handle_stage3_reset(text, chat_id)

    elif text.startswith("/confirm_reset "):
        _handle_confirm_reset(text, chat_id)

    elif text == "/cancel_reset":
        _handle_cancel_reset(text, chat_id)

    elif text == "/ping":
        send_message("pong", _main_keyboard())

    elif text == "/manifest":
        send_document(ПАПКА_ДАННЫХ / "storage_manifest.txt", "manifest")

    elif text == "/audit_report":
        send_document(ПАПКА_ДАННЫХ / "audit_report.txt", "audit report")

    elif text == "/research_report":
        send_document(ПАПКА_ДАННЫХ / "research_report.txt", "research report")

    elif text == "/timing":
        send_document(ПАПКА_ДАННЫХ / "runtime_timing_report.txt", "runtime timing report")

    elif text == "/health":
        send_message(_build_health_text(), _main_keyboard())

    elif text == "/failures":
        send_document(ПАПКА_ДАННЫХ / "request_failure_report.csv", "request failures")

    elif text == "/gaps":
        send_document(ПАПКА_ДАННЫХ / "gap_report.csv", "gap report")

    elif text == "/active_universe":
        send_document(ПАПКА_ДАННЫХ / "active_universe_report.csv", "active universe")

    elif text == "/export_quick":
        send_message("⛔ Rebuild через Telegram отключён. Используй /download bundle.", _main_keyboard())

    elif text in {"/export_research_7d", "/export_research_30d"}:
        send_message("⛔ Heavy export через Telegram отключён. Только готовые файлы через /downloads.", _main_keyboard())


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

                message = item.get("message", {}) or {}
                text = message.get("text", "")
                chat_id = (message.get("chat", {}) or {}).get("id")

                if text:
                    _handle(text.strip(), chat_id)
        except Exception as exc:
            log(f"telegram polling error: {exc}")
            time.sleep(10 if "409" in str(exc) else 5)


def start_polling() -> None:
    global _polling_started

    if _polling_started or not TELEGRAM_BOT_TOKEN:
        return

    if not _acquire_polling_lock():
        return

    _polling_started = True
    try:
        threading.Thread(target=_loop, daemon=True).start()
    except Exception:
        _polling_started = False
        _release_polling_lock()
        raise
