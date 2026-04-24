from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv
import zipfile

from config import ПАПКА_ДАННЫХ, APP_VERSION, QUICK_EXPORT_CANDLES, RESEARCH_EXPORT_DAYS, RESEARCH_30D_EXPORT_DAYS
from db import fetch


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _zip(zip_path: Path, files: list[Path]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for file in files:
            if file.exists():
                z.write(file, arcname=file.name)


def _safe_fetch(sql: str, params: tuple = ()) -> list[dict]:
    try:
        return fetch(sql, params)
    except Exception:
        return []


def rebuild_exports(mode: str = "quick") -> Path:
    now = datetime.now(timezone.utc)

    if mode == "research_30d":
        since = now - timedelta(days=RESEARCH_30D_EXPORT_DAYS)
        suffix = "research_30d"
    elif mode == "research_7d":
        since = now - timedelta(days=RESEARCH_EXPORT_DAYS)
        suffix = "research_7d"
    else:
        since = now - timedelta(minutes=QUICK_EXPORT_CANDLES * 5)
        suffix = "quick"

    oi = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые
        WHERE ts_open >= %s
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    price = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые
        WHERE ts_open >= %s
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    volume = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые
        WHERE ts_open >= %s
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    oi_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in oi}
    price_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in price}
    volume_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in volume}

    keys = sorted(set(oi_map.keys()) | set(price_map.keys()) | set(volume_map.keys()), key=lambda x: (x[0], x[1], x[2]))

    raw_rows = []
    for exchange, symbol, ts_open in keys:
        oi_row = oi_map.get((exchange, symbol, ts_open))
        price_row = price_map.get((exchange, symbol, ts_open))
        volume_row = volume_map.get((exchange, symbol, ts_open))
        close_norm = ts_open + timedelta(minutes=5)

        raw_rows.append([
            ts_open, close_norm, close_norm, exchange, symbol,
            oi_row["oi_open"] if oi_row else None,
            oi_row["oi_high"] if oi_row else None,
            oi_row["oi_low"] if oi_row else None,
            oi_row["oi_close"] if oi_row else None,
            price_row["price_open"] if price_row else None,
            price_row["price_high"] if price_row else None,
            price_row["price_low"] if price_row else None,
            price_row["price_close"] if price_row else None,
            volume_row["volume"] if volume_row else None,
        ])

    aggregates = fetch("""
        SELECT *
        FROM bot_aggregates
        WHERE ts_close >= %s
        ORDER BY metric, exchange, symbol, timeframe, ts_close
    """, (since,))

    audit = fetch("""
        SELECT *
        FROM validation_audit
        WHERE ts_close >= %s
        ORDER BY metric, exchange, symbol, timeframe, ts_close
    """, (since,))

    integrity = _safe_fetch("SELECT * FROM raw_integrity_report ORDER BY metric, exchange, symbol")

    market_research = _safe_fetch("""
        SELECT *
        FROM market_research
        WHERE ts_close >= %s
        ORDER BY exchange, symbol, timeframe, ts_close
    """, (since,))

    market_states = _safe_fetch("""
        SELECT
            exchange,
            symbol,
            timeframe,
            market_state,
            COUNT(*) AS state_count,
            AVG(continuation_score) AS avg_continuation_score,
            AVG(exhaustion_score) AS avg_exhaustion_score,
            AVG(compression_score) AS avg_compression_score
        FROM market_research
        WHERE ts_close >= %s
        GROUP BY exchange, symbol, timeframe, market_state
        ORDER BY exchange, symbol, timeframe, state_count DESC
    """, (since,))

    raw_path = ПАПКА_ДАННЫХ / "raw_market_5m.csv"
    aggregates_path = ПАПКА_ДАННЫХ / "bot_aggregates.csv"
    audit_path = ПАПКА_ДАННЫХ / "validation_audit.csv"
    market_research_path = ПАПКА_ДАННЫХ / "market_research.csv"
    market_states_path = ПАПКА_ДАННЫХ / "market_states.csv"
    manifest_path = ПАПКА_ДАННЫХ / "storage_manifest.txt"
    audit_report_path = ПАПКА_ДАННЫХ / "audit_report.txt"
    research_report_path = ПАПКА_ДАННЫХ / "research_report.txt"

    _write_csv(
        raw_path,
        ["ts_open", "ts_close", "candle_close_norm", "exchange", "symbol", "oi_open", "oi_high", "oi_low", "oi_close", "price_open", "price_high", "price_low", "price_close", "volume"],
        raw_rows,
    )

    _write_csv(
        aggregates_path,
        ["metric", "timeframe", "ts_open", "ts_close", "exchange", "symbol", "open_value", "high_value", "low_value", "close_value", "sum_value", "avg_value", "delta_pct", "unique_candles"],
        [[r["metric"], r["timeframe"], r["ts_open"], r["ts_close"], r["exchange"], r["symbol"], r["open_value"], r["high_value"], r["low_value"], r["close_value"], r["sum_value"], r["avg_value"], r["delta_pct"], r["unique_candles"]] for r in aggregates],
    )

    _write_csv(
        audit_path,
        ["calculated_at", "metric", "timeframe", "ts_close", "exchange", "symbol", "bot_open", "audit_open", "bot_close", "audit_close", "bot_delta_pct", "audit_delta_pct", "bot_sum", "audit_sum", "bot_avg", "audit_avg", "drift", "unique_candles", "validation_status"],
        [[r["calculated_at"], r["metric"], r["timeframe"], r["ts_close"], r["exchange"], r["symbol"], r["bot_open"], r["audit_open"], r["bot_close"], r["audit_close"], r["bot_delta_pct"], r["audit_delta_pct"], r["bot_sum"], r["audit_sum"], r["bot_avg"], r["audit_avg"], r["drift"], r["unique_candles"], r["validation_status"]] for r in audit],
    )

    _write_csv(
        market_research_path,
        ["calculated_at", "ts_close", "exchange", "symbol", "timeframe", "oi_delta_pct", "price_delta_pct", "volume_delta_pct", "oi_velocity", "oi_acceleration", "range_width_pct", "continuation_score", "exhaustion_score", "compression_score", "market_state"],
        [[r["calculated_at"], r["ts_close"], r["exchange"], r["symbol"], r["timeframe"], r["oi_delta_pct"], r["price_delta_pct"], r["volume_delta_pct"], r["oi_velocity"], r["oi_acceleration"], r["range_width_pct"], r["continuation_score"], r["exhaustion_score"], r["compression_score"], r["market_state"]] for r in market_research],
    )

    _write_csv(
        market_states_path,
        ["exchange", "symbol", "timeframe", "market_state", "state_count", "avg_continuation_score", "avg_exhaustion_score", "avg_compression_score"],
        [[r["exchange"], r["symbol"], r["timeframe"], r["market_state"], r["state_count"], r["avg_continuation_score"], r["avg_exhaustion_score"], r["avg_compression_score"]] for r in market_states],
    )

    invalid = [r for r in audit if r["validation_status"] != "валидно"]

    audit_lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"mode: {mode}",
        "",
        "canonical_close: active",
        "timestamp_migration: active",
        "export_compression: active",
        "",
        f"raw_rows: {len(raw_rows)}",
        f"bot_aggregates_rows: {len(aggregates)}",
        f"validation_audit_rows: {len(audit)}",
        f"invalid_rows: {len(invalid)}",
        f"integrity_rows: {len(integrity)}",
        f"market_research_rows: {len(market_research)}",
        f"market_states_rows: {len(market_states)}",
        "",
        "Top invalid:",
    ]

    for r in invalid[:100]:
        audit_lines.append(f'{r["metric"]} {r["symbol"]} {r["exchange"]} {r["timeframe"]} drift={r["drift"]} status={r["validation_status"]}')

    _write_text(audit_report_path, "\n".join(audit_lines))

    research_lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"mode: {mode}",
        "",
        "Исследовательский слой структуры рынка",
        "Источник: реальные bot_aggregates",
        "Fake rows: нет",
        "",
        f"market_research_rows: {len(market_research)}",
        f"market_states_rows: {len(market_states)}",
        "",
        "Состояния:",
    ]

    for r in market_states[:200]:
        research_lines.append(
            f'{r["exchange"]} {r["symbol"]} {r["timeframe"]} {r["market_state"]} '
            f'count={r["state_count"]} '
            f'continuation={r["avg_continuation_score"]} '
            f'exhaustion={r["avg_exhaustion_score"]} '
            f'compression={r["avg_compression_score"]}'
        )

    _write_text(research_report_path, "\n".join(research_lines))

    _write_text(
        manifest_path,
        (
            f"Mighty Duck {APP_VERSION}\n"
            f"mode={mode}\n"
            "main_downloads=market_research_bundle.zip, audit_report.txt, research_report.txt\n"
            "inside_bundle=raw_market_5m.csv, bot_aggregates.csv, validation_audit.csv, market_research.csv, market_states.csv, storage_manifest.txt\n"
            "timestamp_migration=active\n"
            "research_source=real_bot_aggregates\n"
        ),
    )

    bundle_path = ПАПКА_ДАННЫХ / "market_research_bundle.zip"
    mode_bundle_path = ПАПКА_ДАННЫХ / f"market_research_bundle_{suffix}.zip"

    bundle_files = [raw_path, aggregates_path, audit_path, market_research_path, market_states_path, manifest_path]

    _zip(bundle_path, bundle_files)
    _zip(mode_bundle_path, bundle_files + [audit_report_path, research_report_path])

    return bundle_path
