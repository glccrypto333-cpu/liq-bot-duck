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
            ["📊 Статус", "⚙️ Фазы"],
            ["🥉 Stage 1", "🥈 Stage 2", "🥇 Stage 3"],
            ["📈 ТОП OI", "🪙 Coin"],
            ["⬇️ Скачать", "🧱 Quarantine"],
            ["🧭 Runtime", "❓ Помощь"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


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


def _short_ts(value) -> str:
    return str(value or "n/a").replace("+00:00", " UTC")


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
            SELECT phase, phase_name, timeframe, COUNT(*) AS cnt, MAX(phase_updated_at) AS latest
            FROM market_phase
            WHERE phase > 0
            GROUP BY phase, phase_name, timeframe
            ORDER BY phase DESC,
                     CASE timeframe WHEN '4h' THEN 1 WHEN '1h' THEN 2 WHEN '30m' THEN 3 WHEN '15m' THEN 4 ELSE 9 END,
                     cnt DESC
            LIMIT 60
        """)
        if not rows:
            return "⚙️ Фазы\n\nАктивных фаз нет."

        lines = ["⚙️ Фазы — зеркало market_phase", ""]
        for r in rows:
            lines.append(
                f"phase={r.get('phase')} | {r.get('timeframe')} | cnt={r.get('cnt')} | latest={_short_ts(r.get('latest'))}"
            )
        lines.append("")
        lines.append("/phase1 /phase2 /phase3")
        return "\n".join(lines)

    rows = _safe_rows("""
        SELECT exchange, symbol, timeframe, phase, phase_name, phase_status, priority,
               confidence, phase_updated_at, oi_structure, oi_priority, oi_hold_state,
               oi_trend_1h, oi_trend_4h, oi_trend_24h,
               price_structure, price_quality, price_slope_state,
               volume_structure, volume_quality, volume_hold_state,
               transition_reason
        FROM market_phase
        WHERE phase = %s
        ORDER BY CASE timeframe WHEN '4h' THEN 1 WHEN '1h' THEN 2 WHEN '30m' THEN 3 WHEN '15m' THEN 4 ELSE 9 END,
                 priority ASC,
                 phase_updated_at DESC
        LIMIT 40
    """, (phase,))

    title = f"🥉 Stage {phase}" if phase == 1 else (f"🥈 Stage {phase}" if phase == 2 else f"🥇 Stage {phase}")
    if not rows:
        return f"{title}\n\nСейчас монет в фазе нет."

    lines = [f"{title} — текущая голова бота", ""]
    for r in rows:
        ex = r.get("exchange")
        link = "BY" if ex == "BYBIT" else "BN"
        lines.extend([
            f"{r.get('symbol')} [{r.get('timeframe')}]",
            f"🔗 CG | {link}",
            f"`{r.get('symbol')}`",
            f"phase={r.get('phase')} | status={r.get('phase_status')} | prio={r.get('priority')} | conf={r.get('confidence')}",
            f"OI: {r.get('oi_structure')} | p={r.get('oi_priority')} | hold={r.get('oi_hold_state')} | 1h={r.get('oi_trend_1h')} | 4h={r.get('oi_trend_4h')}",
            f"PRICE: {r.get('price_structure')} | {r.get('price_quality')} | slope={r.get('price_slope_state')}",
            f"VOL: {r.get('volume_structure')} | {r.get('volume_quality')} | hold={r.get('volume_hold_state')}",
            f"Card: /coin {r.get('symbol')}",
            f"Feedback: /feedback {r.get('symbol')} текст",
            "",
        ])
    return "\n".join(lines)


def _build_stage3_text() -> str:
    return _build_phases_text(3)


def _build_top_oi_text(timeframe: str | None = None) -> str:
    params = ()
    tf_filter = ""
    if timeframe:
        tf_filter = "AND timeframe = %s"
        params = (timeframe,)

    rows = _safe_rows(f"""
        SELECT exchange, symbol, timeframe, stage, stage_name,
               oi_structure, oi_priority, oi_hold_state,
               oi_trend_15m, oi_trend_30m, oi_trend_1h, oi_trend_4h, oi_trend_24h,
               oi_delta_pct, oi_acceleration, price_delta_pct, volume_delta_pct, ts_close
        FROM market_oi_slope
        WHERE ts_close >= (SELECT MAX(ts_close) FROM market_oi_slope) - INTERVAL '90 minutes'
          AND stage >= 1
          {tf_filter}
        ORDER BY ABS(oi_delta_pct) DESC, ABS(oi_acceleration) DESC, oi_priority ASC
        LIMIT 30
    """, params)

    latest = _safe_rows("SELECT MAX(ts_close) AS latest FROM market_oi_slope")
    latest_ts = latest[0].get("latest") if latest else None

    title = f"📈 ТОП OI {timeframe}" if timeframe else "📈 ТОП OI"
    if not rows:
        return f"{title}\n\nНет свежих строк в market_oi_slope."

    lines = [title, f"latest_ts={_short_ts(latest_ts)}", ""]
    for r in rows:
        ex = r.get("exchange")
        link = "BY" if ex == "BYBIT" else "BN"
        lines.extend([
            f"{r.get('symbol')} [{r.get('timeframe')}]",
            f"🔗 CG | {link}",
            f"`{r.get('symbol')}`",
            f"stage={r.get('stage')} {r.get('stage_name')} | OI={_fmt_pct(r.get('oi_delta_pct'))} | acc={_fmt_pct(r.get('oi_acceleration'))}",
            f"structure={r.get('oi_structure')} | priority={r.get('oi_priority')} | hold={r.get('oi_hold_state')}",
            f"trend 15m={r.get('oi_trend_15m')} | 30m={r.get('oi_trend_30m')} | 1h={r.get('oi_trend_1h')} | 4h={r.get('oi_trend_4h')} | 24h={r.get('oi_trend_24h')}",
            f"phase card: /coin {r.get('symbol')}",
            "",
        ])
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
        return f"🪙 {symbol}

Нет данных. Формат: /coin BTCUSDT"

    lines = [f"🪙 {symbol}", ""]

    if phase_rows:
        lines.append("PHASE / HYBRID:")
        for r in phase_rows:
            ex = r.get("exchange")
            tf = r.get("timeframe")
            link = "BY" if ex == "BYBIT" else "BN"

            price = _latest_metric_row("market_price_state", symbol, ex, tf)
            volume = _latest_metric_row("market_volume_state", symbol, ex, tf)
            oi = _latest_metric_row("market_oi_slope", symbol, ex, tf)

            lines.extend([
                "",
                f"{symbol} [{tf}]",
                f"🔗 CG | {link}",
                f"`{symbol}`",
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

    return "
".join(lines)


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

def _build_downloads_text() -> str:
    files = [
        "market_research_bundle.zip",
        "runtime_reports.zip",
        "storage_manifest.txt",
        "runtime_health_report.txt",
        "runtime_timing_report.txt",
        "request_failure_report.csv",
        "gap_report.csv",
        "active_universe_report.csv",
        "telegram_feedback.csv",
        "telegram_quarantine.csv",
        "telegram_quarantine_history.csv",
    ]
    lines = ["⬇️ Скачать", "", "Только готовые файлы. Rebuild не запускается.", ""]
    for name in files:
        path = ПАПКА_ДАННЫХ / name
        lines.append(_fmt_file(path))
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


def _handle_quarantine(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    parts = text.split(maxsplit=3)
    data = _read_quarantine()

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


def _handle_stage3_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    parts = text.split(maxsplit=3)
    if len(parts) < 4:
        send_message("Формат: /reset_stage3 SYMBOL TIMEFRAME reason", _main_keyboard())
        return

    _, symbol, timeframe, reason = parts
    total = 0
    for exchange in ("BYBIT", "BINANCE"):
        try:
            total += reset_stage3(exchange, symbol.upper(), timeframe, reason, dry_run=False)
        except Exception as exc:
            log(f"telegram reset_stage3 error: {exc}")

    send_message(f"✅ Stage3 reset done: {symbol.upper()} {timeframe}, rows={total}", _main_keyboard())


def _handle(text: str, chat_id=None) -> None:
    text = text.strip()

    if text in {"/start", "/help", "❓ Помощь"}:
        send_message(_build_help_text(), _main_keyboard())

    elif text in {"/panel", "/control"}:
        send_message(_build_control_panel_text(), _main_keyboard())

    elif text in {"/runtime", "🧭 Runtime"}:
        send_message(_build_runtime_text(), _main_keyboard())

    elif text in {"/status", "📊 Статус"}:
        send_message(_build_status_text(), _main_keyboard())

    elif text in {"/phases", "⚙️ Фазы"}:
        send_message(_build_phases_text(), _main_keyboard())

    elif text in {"/phase1", "🥉 Stage 1"}:
        send_message(_build_phases_text(1), _main_keyboard())

    elif text in {"/phase2", "🥈 Stage 2"}:
        send_message(_build_phases_text(2), _main_keyboard())

    elif text in {"/phase3", "🥇 Stage 3"}:
        send_message(_build_stage3_text(), _main_keyboard())

    elif text in {"/top_oi", "📈 ТОП OI"}:
        send_message(_build_top_oi_text(), _main_keyboard())

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

    elif text in {"/exports", "📦 Exports"}:
        send_message(_build_exports_text(), _main_keyboard())

    elif text in {"/downloads", "⬇️ Скачать"}:
        send_message(_build_downloads_text(), _main_keyboard())

    elif text.startswith("/download "):
        _send_download(text.split(maxsplit=1)[1].strip())

    elif text in {"/reports", "📄 Reports"}:
        send_document(_build_runtime_reports_zip(), "runtime reports bundle")

    elif text in {"/bundle"}:
        send_document(ПАПКА_ДАННЫХ / "market_research_bundle.zip", "quick bundle")

    elif text in {"/backup", "🔒 Backup"}:
        send_message(_build_backup_text(), _main_keyboard())

    elif text in {"/quarantine", "🧱 Quarantine"} or text.startswith("/quarantine "):
        _handle_quarantine(text, chat_id)

    elif text.startswith("/reset_stage3 "):
        _handle_stage3_reset(text, chat_id)

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
        send_document(ПАПКА_ДАННЫХ / "runtime_health_report.txt", "runtime health report")

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
