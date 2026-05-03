from __future__ import annotations

import time
import threading
import zipfile
import json
from datetime import datetime, timezone
import csv
from pathlib import Path
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ПАПКА_ДАННЫХ, APP_VERSION
from logger import log
from db import fetch
from reset_stage3 import reset_stage3

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
_polling_started = False
_offset = 0
_export_lock = threading.Lock()
_csv_lock = threading.Lock()


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
        "/reset_stage3 SYMBOL TF reason",
        "/confirm_reset SYMBOL TF",
        "/cancel_reset",
        "/download filename",
        "/backup_db",
        "/archive",
        "/download backup_latest",
        "/health",
        "",
        "Фазы читаются только из market_phase.",
        "Метрики открываются через /coin SYMBOL.",
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


def _build_health_text() -> str:
    checks = [
        ("market_phase", "phase_updated_at", 10),
        ("market_oi_slope", "ts_close", 10),
        ("market_price_state", "ts_close", 10),
        ("market_volume_state", "ts_close", 10),
        ("market_phase_source", "ts_close", 10),
    ]

    lines = ["🩺 Health — core tables", ""]

    for table, ts_col, stale_min in checks:
        h = _table_health(table, ts_col, stale_min)
        lines.append(
            f"{h['status']} | {h['table']} | rows={h['rows']} | "
            f"latest={_short_ts(h['latest'])} | age_min={h['age_minutes']}"
        )

    lines.append("")
    lines.append("OK <= 10m | STALE > 10m | EMPTY rows=0 | ERROR query failed")
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
            SELECT phase, timeframe, COUNT(*) AS cnt, MAX(phase_updated_at) AS latest
            FROM market_phase
            WHERE phase > 0
            GROUP BY phase, timeframe
            ORDER BY phase DESC,
                     CASE timeframe WHEN '4ч' THEN 1 WHEN '4h' THEN 1 WHEN '1ч' THEN 2 WHEN '1h' THEN 2 WHEN '30м' THEN 3 WHEN '30m' THEN 3 WHEN '15м' THEN 4 WHEN '15m' THEN 4 ELSE 9 END
        """)
        if not rows:
            return "⚙️ Фазы\n\nАктивных фаз нет."

        lines = ["⚙️ Фазы", ""]
        for r in rows:
            lines.append(
                f"phase={r.get('phase')} | {r.get('timeframe')} | cnt={r.get('cnt')} | latest={_short_ts(r.get('latest'))}"
            )
        lines.extend(["", "Открыть: Фаза 1 / Фаза 2 / Фаза 3"])
        return "\n".join(lines)

    rows = _safe_rows("""
        SELECT *
        FROM market_phase
        WHERE phase = %s
        ORDER BY CASE timeframe WHEN '4ч' THEN 1 WHEN '4h' THEN 1 WHEN '1ч' THEN 2 WHEN '1h' THEN 2 WHEN '30м' THEN 3 WHEN '30m' THEN 3 WHEN '15м' THEN 4 WHEN '15m' THEN 4 ELSE 9 END,
                 priority ASC,
                 phase_updated_at DESC
        LIMIT 80
    """, (phase,))

    title = f"Фаза {phase}"
    if not rows:
        return f"{title}\n\nСейчас монет в фазе нет."

    lines = [title, "Детали: /coin SYMBOL", ""]
    for r in rows:
        symbol = r.get("symbol")
        tf = r.get("timeframe")
        ex = r.get("exchange")
        lines.append(f"{symbol} [{tf}]")
        lines.append(f"🔗 {_symbol_links(symbol, ex)} | /coin {symbol} | /feedback {symbol} текст")
        lines.append("")

    return "\n".join(lines)


def _build_stage3_text() -> str:
    return _build_phases_text(3)


def _build_top_oi_text(timeframe: str | None = None, exchange: str | None = None) -> str:
    timeframe = _tf_sql(timeframe) if timeframe else None
    exchange = str(exchange or "").upper().strip() or None

    params = []
    where = ["stage >= 1"]

    if exchange in {"BINANCE", "BYBIT"}:
        where.append("exchange = %s")
        params.append(exchange)

    if timeframe in {"15м", "30м", "1ч"}:
        aliases = {
            "15м": ["15м", "15m"],
            "30м": ["30м", "30m"],
            "1ч": ["1ч", "1h"],
        }[timeframe]
        where.append("timeframe = ANY(%s)")
        params.append(aliases)

    elif timeframe == "4ч":
        where.append("oi_trend_4h IS NOT NULL AND oi_trend_4h <> ''")

    elif timeframe == "24ч":
        where.append("oi_trend_24h IS NOT NULL AND oi_trend_24h <> ''")

    where_sql = "WHERE " + " AND ".join(where)

    rows = _safe_rows(f"""
        SELECT DISTINCT ON (exchange, symbol)
               exchange, symbol, timeframe, stage, stage_name,
               oi_delta_pct, oi_acceleration, oi_structure,
               oi_trend_15m, oi_trend_30m, oi_trend_1h, oi_trend_4h, oi_trend_24h,
               ts_close
        FROM market_oi_slope
        {where_sql}
        ORDER BY exchange, symbol, ABS(oi_delta_pct) DESC, ABS(oi_acceleration) DESC, ts_close DESC
        LIMIT 80
    """, tuple(params))

    rows = sorted(
        rows,
        key=lambda r: (abs(float(r.get("oi_delta_pct") or 0)), abs(float(r.get("oi_acceleration") or 0))),
        reverse=True,
    )[:10]

    ex_title = exchange or "ALL"
    tf_title = timeframe or "ALL"
    title = f"🏆 TOP OI за {tf_title} — {ex_title}"

    if not rows:
        return f"{title}\n\nНет строк в market_oi_slope."

    lines = [title, "_market_oi_slope snapshot_"]

    for i, r in enumerate(rows, 1):
        symbol = r.get("symbol")
        ex = r.get("exchange")
        oi = _fmt_pct(r.get("oi_delta_pct"))
        acc = _fmt_pct(r.get("oi_acceleration"))
        links = _symbol_links(symbol, ex)

        lines.append(
            f"{i}. `{symbol}` — OI {oi} | acc {acc} | {links}"
        )

    return "\n".join(lines)



def _latest_metric_row(table: str, symbol: str, exchange: str, timeframe: str) -> dict:
    ts_col = "phase_updated_at" if table == "market_phase" else "ts_close"
    rows = _safe_rows(f"""
        SELECT *
        FROM {table}
        WHERE symbol = %s
          AND exchange = %s
          AND timeframe = %s
        ORDER BY {ts_col} DESC
        LIMIT 1
    """, (symbol, exchange, timeframe))
    return rows[0] if rows else {}


def _build_coin_card(symbol: str) -> str:
    symbol = symbol.upper().strip()

    phase_rows = _safe_rows("""
        SELECT *
        FROM market_phase
        WHERE symbol = %s
        ORDER BY
            phase DESC,
            CASE timeframe
                WHEN '4h' THEN 1
                WHEN '1h' THEN 2
                WHEN '30m' THEN 3
                WHEN '15m' THEN 4
                ELSE 9
            END,
            priority ASC,
            phase_updated_at DESC
        LIMIT 20
    """, (symbol,))

    oi_rows = _safe_rows("""
        SELECT DISTINCT ON (exchange, symbol, timeframe)
            *
        FROM market_oi_slope
        WHERE symbol = %s
        ORDER BY exchange, symbol, timeframe, ts_close DESC
    """, (symbol,))

    if not phase_rows and not oi_rows:
        return f"🪙 {symbol}\n\nНет данных. Формат: /coin BTCUSDT"

    lines = [f"🪙 {symbol}", ""]

    if phase_rows:
        lines.append("PHASE / HYBRID:")
        for r in phase_rows:
            ex = r.get("exchange")
            tf = r.get("timeframe")
            link = _exchange_code(ex)

            price = _latest_metric_row("market_price_state", symbol, ex, tf)
            volume = _latest_metric_row("market_volume_state", symbol, ex, tf)
            oi = _latest_metric_row("market_oi_slope", symbol, ex, tf)

            lines.extend([
                "",
                f"{symbol} [{tf}]",
                _symbol_links(symbol, ex),
                f"phase={r.get('phase')} {r.get('phase_name')} | status={r.get('phase_status')} | prio={r.get('priority')} | conf={r.get('confidence')}",
                f"updated={_short_ts(r.get('phase_updated_at'))}",
                f"OI: structure={r.get('oi_structure')} | priority={r.get('oi_priority')} | hold={r.get('oi_hold_state')}",
                f"OI trends: 1h={r.get('oi_trend_1h')} | 4h={r.get('oi_trend_4h')} | 24h={r.get('oi_trend_24h')}",
                f"OI slope: stage={oi.get('stage')} {oi.get('stage_name')} | delta={_fmt_pct(oi.get('oi_delta_pct'))} | acc={_fmt_pct(oi.get('oi_acceleration'))}",
                f"PRICE: structure={r.get('price_structure')} | quality={r.get('price_quality')} | slope={r.get('price_slope_state')} | delta={_fmt_pct(price.get('price_delta_pct'))} | range={_fmt_pct(price.get('range_width_pct'))}",
                f"VOLUME: structure={r.get('volume_structure')} | quality={r.get('volume_quality')} | hold={r.get('volume_hold_state')} | norm={volume.get('normalized_volume')} | pct={volume.get('volume_percentile')}",
                f"transition={r.get('transition_reason')}",
                f"Feedback: /feedback {symbol} текст",
            ])

    if oi_rows:
        lines.extend(["", "LATEST OI ENGINE:"])
        for r in oi_rows[:8]:
            lines.append(
                f"{r.get('exchange')} {r.get('timeframe')} | stage={r.get('stage')} {r.get('stage_name')} | "
                f"OI={_fmt_pct(r.get('oi_delta_pct'))} | acc={_fmt_pct(r.get('oi_acceleration'))} | "
                f"{r.get('oi_structure')} | hold={r.get('oi_hold_state')}"
            )

    return "\n".join(lines)


def _feedback_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_feedback.csv"



def _save_feedback(text: str) -> str:
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        return "Формат: /feedback SYMBOL текст"

    _, symbol, comment = parts
    symbol = symbol.upper().strip()

    phase_rows = _safe_rows("""
        SELECT *
        FROM market_phase
        WHERE symbol = %s
        ORDER BY phase DESC, priority ASC, phase_updated_at DESC
        LIMIT 20
    """, (symbol,))

    if not phase_rows:
        return f"Нет phase snapshot для {symbol}. Комментарий не сохранён."

    path = _feedback_path()
    new_file = not path.exists()

    header = [
        "created_at_utc",
        "symbol",
        "exchange",
        "timeframe",
        "phase",
        "phase_name",
        "phase_status",
        "priority",
        "confidence",
        "phase_updated_at",
        "oi_structure",
        "oi_priority",
        "oi_hold_state",
        "oi_trend_1h",
        "oi_trend_4h",
        "oi_trend_24h",
        "price_structure",
        "price_quality",
        "price_slope_state",
        "volume_structure",
        "volume_quality",
        "volume_hold_state",
        "transition_reason",
        "full_reason",
        "latest_oi_json",
        "latest_price_json",
        "latest_volume_json",
        "user_comment",
    ]

    now = datetime.now(timezone.utc).isoformat()
    written = 0

    with _csv_lock:
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(header)

            for r in phase_rows:
                ex = r.get("exchange")
                tf = r.get("timeframe")

                oi = _latest_metric_row("market_oi_slope", symbol, ex, tf)
                price = _latest_metric_row("market_price_state", symbol, ex, tf)
                volume = _latest_metric_row("market_volume_state", symbol, ex, tf)

                w.writerow([
                    now,
                    symbol,
                    ex,
                    tf,
                    r.get("phase"),
                    r.get("phase_name"),
                    r.get("phase_status"),
                    r.get("priority"),
                    r.get("confidence"),
                    r.get("phase_updated_at"),
                    r.get("oi_structure"),
                    r.get("oi_priority"),
                    r.get("oi_hold_state"),
                    r.get("oi_trend_1h"),
                    r.get("oi_trend_4h"),
                    r.get("oi_trend_24h"),
                    r.get("price_structure"),
                    r.get("price_quality"),
                    r.get("price_slope_state"),
                    r.get("volume_structure"),
                    r.get("volume_quality"),
                    r.get("volume_hold_state"),
                    r.get("transition_reason"),
                    r.get("reason"),
                    json.dumps(oi, ensure_ascii=False, default=str),
                    json.dumps(price, ensure_ascii=False, default=str),
                    json.dumps(volume, ensure_ascii=False, default=str),
                    comment,
                ])
                written += 1

    return f"✅ Feedback snapshot сохранён: {symbol}, rows={written}"

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
    allowed = {
        "bundle": "market_research_bundle.zip",
        "reports": "runtime_reports.zip",
        "manifest": "storage_manifest.txt",
        "health": "runtime_health_report.txt",
        "timing": "runtime_timing_report.txt",
        "failures": "request_failure_report.csv",
        "gaps": "gap_report.csv",
        "active": "active_universe_report.csv",
        "feedback": "telegram_feedback.csv",
        "quarantine": "telegram_quarantine.csv",
    }
    filename = allowed.get(name)
    if not filename:
        send_message("Формат: /download bundle|reports|manifest|health|timing|failures|gaps|active|feedback|quarantine", _main_keyboard())
        return
    send_document(ПАПКА_ДАННЫХ / filename, filename)


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


def _save_pending_reset(symbol: str, timeframe: str, reason: str) -> None:
    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol.upper(),
        "timeframe": timeframe,
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

    parts = text.split(maxsplit=3)
    if len(parts) < 4:
        send_message("Формат: /reset_stage3 SYMBOL TIMEFRAME reason", _main_keyboard())
        return

    _, symbol, timeframe, reason = parts
    symbol = symbol.upper().strip()
    timeframe = timeframe.strip()

    _save_pending_reset(symbol, timeframe, reason)

    send_message(
        "\n".join([
            "⚠️ Pending Stage3 reset создан",
            "",
            f"symbol={symbol}",
            f"timeframe={timeframe}",
            f"reason={reason}",
            "",
            f"Подтвердить: /confirm_reset {symbol} {timeframe}",
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

    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        send_message("Формат: /confirm_reset SYMBOL TIMEFRAME", _main_keyboard())
        return

    _, symbol, timeframe = parts
    symbol = symbol.upper().strip()
    timeframe = timeframe.strip()

    if symbol != pending.get("symbol") or timeframe != pending.get("timeframe"):
        send_message(
            f"Pending не совпадает. Сейчас pending: {pending.get('symbol')} {pending.get('timeframe')}",
            _main_keyboard(),
        )
        return

    total = 0
    reason = pending.get("reason") or "confirmed_reset"

    for exchange in ("BYBIT", "BINANCE"):
        try:
            total += reset_stage3(exchange, symbol, timeframe, reason, dry_run=False)
        except Exception as exc:
            log(f"telegram confirm_reset error: {exc}")

    _clear_pending_reset()

    send_message(
        f"✅ Stage3 reset confirmed: {symbol} {timeframe}, rows={total}",
        _main_keyboard(),
    )


def _handle_cancel_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    pending = _load_pending_reset()
    _clear_pending_reset()

    if pending:
        send_message(
            f"✅ Pending reset отменён: {pending.get('symbol')} {pending.get('timeframe')}",
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
        "timeframe",
        "phase",
        "phase_name",
        "phase_status",
        "priority",
        "confidence",
        "phase_updated_at",
        "oi_structure",
        "oi_priority",
        "oi_hold_state",
        "price_structure",
        "price_quality",
        "volume_structure",
        "volume_quality",
        "transition_reason",
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
                row.get("timeframe"),
                row.get("phase"),
                row.get("phase_name"),
                row.get("phase_status"),
                row.get("priority"),
                row.get("confidence"),
                row.get("phase_updated_at"),
                row.get("oi_structure"),
                row.get("oi_priority"),
                row.get("oi_hold_state"),
                row.get("price_structure"),
                row.get("price_quality"),
                row.get("volume_structure"),
                row.get("volume_quality"),
                row.get("transition_reason"),
            ])


def _build_stage3_alert_text(r: dict) -> str:
    symbol = r.get("symbol")
    timeframe = r.get("timeframe")
    ex = r.get("exchange")
    link = _exchange_code(ex)

    return "\n".join([
        "🥇 NEW STAGE 3",
        "",
        f"{symbol} [{timeframe}]",
        f"🔗 CG | {link}",
        f"`{symbol}`",
        f"phase={r.get('phase')} {r.get('phase_name')} | status={r.get('phase_status')} | prio={r.get('priority')} | conf={r.get('confidence')}",
        f"updated={_short_ts(r.get('phase_updated_at'))}",
        f"OI: {r.get('oi_structure')} | p={r.get('oi_priority')} | hold={r.get('oi_hold_state')} | 1h={r.get('oi_trend_1h')} | 4h={r.get('oi_trend_4h')}",
        f"PRICE: {r.get('price_structure')} | {r.get('price_quality')} | slope={r.get('price_slope_state')}",
        f"VOL: {r.get('volume_structure')} | {r.get('volume_quality')} | hold={r.get('volume_hold_state')}",
        f"transition={r.get('transition_reason')}",
        "",
        f"Card: /coin {symbol}",
        f"Feedback: /feedback {symbol} текст",
        f"Reset: /reset_stage3 {symbol} {timeframe} reason",
    ])


def check_stage3_alerts() -> int:
    alerted = _read_stage3_alerted_keys()

    rows = _safe_rows("""
        SELECT *
        FROM market_phase
        WHERE phase = 3
          AND COALESCE(phase_status, '') IN ('active', 'holding')
        ORDER BY phase_updated_at DESC
        LIMIT 50
    """)

    sent = 0

    for r in rows:
        key = "|".join([
            str(r.get("exchange")),
            str(r.get("symbol")),
            str(r.get("timeframe")),
            "phase=3",
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
        send_message("Формат: /reset_stage3 SYMBOL TF reason", _stage3_reset_keyboard())

    elif text == "Сбросить все":
        send_message("Массовый reset пока не исполняется из кнопки. Используй точечно: /reset_stage3 SYMBOL TF reason", _stage3_reset_keyboard())

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
        send_message(
            _build_top_oi_text(text.split(maxsplit=1)[1].strip()),
            _main_keyboard()
        )

    elif text in {"/coin", "🪙 Coin"}:
        send_message("Формат: /coin BTCUSDT", _main_keyboard())

    elif text.startswith("/coin "):
        send_message(_build_coin_card(text.split(maxsplit=1)[1]), _main_keyboard())

    elif text.startswith("/feedback "):
        send_message(_save_feedback(text), _main_keyboard())

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

    _polling_started = True
    threading.Thread(target=_loop, daemon=True).start()
