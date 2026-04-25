from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv
import zipfile
import os
import resource

from config import ПАПКА_ДАННЫХ, APP_VERSION, QUICK_EXPORT_CANDLES, RESEARCH_EXPORT_DAYS, RESEARCH_30D_EXPORT_DAYS
from db import fetch, active_universe_sql


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


def _runtime_memory_mb() -> float:
    try:
        # macOS returns bytes, Linux returns KB. Railway/Linux will be KB.
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if usage > 10_000_000:
            return usage / 1024 / 1024
        return usage / 1024
    except Exception:
        return 0.0


def _fmt_pct(value) -> str:
    try:
        return f"{float(value):.4f}%"
    except Exception:
        return "n/a"


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

    oi = fetch(f"""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые x
        WHERE ts_open >= %s
          AND {active_universe_sql("x")}
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    price = fetch(f"""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые x
        WHERE ts_open >= %s
          AND {active_universe_sql("x")}
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    volume = fetch(f"""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые x
        WHERE ts_open >= %s
          AND {active_universe_sql("x")}
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    oi_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in oi}
    price_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in price}
    volume_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in volume}

    keys = sorted(set(oi_map.keys()) | set(price_map.keys()) | set(volume_map.keys()), key=lambda x: (x[0], x[1], x[2]))

    raw_rows = []
    missing_price = 0
    missing_volume = 0
    missing_oi = 0

    for exchange, symbol, ts_open in keys:
        oi_row = oi_map.get((exchange, symbol, ts_open))
        price_row = price_map.get((exchange, symbol, ts_open))
        volume_row = volume_map.get((exchange, symbol, ts_open))
        close_norm = ts_open + timedelta(minutes=5)

        if not oi_row:
            missing_oi += 1
        if not price_row:
            missing_price += 1
        if not volume_row:
            missing_volume += 1

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
    coverage = _safe_fetch("SELECT * FROM coverage_report ORDER BY metric, exchange, symbol")
    gaps = _safe_fetch("SELECT * FROM gap_report ORDER BY metric, exchange, symbol, gap_start")

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

    market_oi_slope = _safe_fetch("""
        SELECT *
        FROM market_oi_slope
        WHERE ts_close >= %s
        ORDER BY stage DESC, strength DESC, exchange, symbol, timeframe, ts_close
    """, (since,))

    oi_slope_top = _safe_fetch("""
        SELECT *
        FROM market_oi_slope
        WHERE stage >= 1
          AND timeframe IN ('30m','1h','4h')
        ORDER BY stage DESC, strength DESC
        LIMIT 300
    """)

    market_silence = _safe_fetch("""
        SELECT *
        FROM market_silence
        WHERE ts_close >= %s
        ORDER BY exchange, symbol, timeframe, ts_close
    """, (since,))

    silence_states = _safe_fetch("""
        SELECT exchange, symbol, timeframe, stage, stage_name, COUNT(*) AS stage_count, AVG(score) AS avg_score
        FROM market_silence
        WHERE ts_close >= %s
        GROUP BY exchange, symbol, timeframe, stage, stage_name
        ORDER BY exchange, symbol, timeframe, stage
    """, (since,))

    market_regime = _safe_fetch("""
        SELECT *
        FROM market_regime
        WHERE ts_close >= %s
        ORDER BY exchange, symbol, timeframe, ts_close
    """, (since,))

    regime_states = _safe_fetch("""
        SELECT
            exchange,
            symbol,
            timeframe,
            scenario,
            confidence,
            COUNT(*) AS scenario_count,
            AVG(continuation_score) AS avg_continuation_score,
            AVG(exhaustion_score) AS avg_exhaustion_score,
            AVG(compression_score) AS avg_compression_score
        FROM market_regime
        WHERE ts_close >= %s
        GROUP BY exchange, symbol, timeframe, scenario, confidence
        ORDER BY exchange, symbol, timeframe, scenario_count DESC
    """, (since,))

    storage_summary = _safe_fetch(f"""
        SELECT metric, MIN(ts_open) AS oldest_ts, MAX(ts_open) AS newest_ts, COUNT(*) AS rows_count
        FROM (
            SELECT 'OI' AS metric, ts_open, exchange, symbol FROM oi_5m_сырые
            UNION ALL
            SELECT 'PRICE' AS metric, ts_open, exchange, symbol FROM price_5m_сырые
            UNION ALL
            SELECT 'VOLUME' AS metric, ts_open, exchange, symbol FROM volume_5m_сырые
        ) x
        WHERE {active_universe_sql("x")}
        GROUP BY metric
        ORDER BY metric
    """)

    active_universe = _safe_fetch("""
        SELECT exchange, symbol, activated_at, source
        FROM active_symbol_universe
        ORDER BY exchange, symbol
    """)

    request_failures = _safe_fetch("""
        SELECT calculated_at, exchange, symbol, data_type, error_type, error_message
        FROM request_failure_report
        ORDER BY exchange, symbol, data_type
    """)

    invalid_reasons = _safe_fetch("""
        SELECT invalid_reason, COUNT(*) AS total
        FROM market_research
        WHERE ts_close >= %s
          AND market_state = 'invalid_data'
        GROUP BY invalid_reason
        ORDER BY total DESC
    """, (since,))

    raw_path = ПАПКА_ДАННЫХ / "raw_market_5m.csv"
    aggregates_path = ПАПКА_ДАННЫХ / "bot_aggregates.csv"
    audit_path = ПАПКА_ДАННЫХ / "validation_audit.csv"
    market_research_path = ПАПКА_ДАННЫХ / "market_research.csv"
    market_states_path = ПАПКА_ДАННЫХ / "market_states.csv"
    market_silence_path = ПАПКА_ДАННЫХ / "market_silence.csv"
    market_oi_slope_path = ПАПКА_ДАННЫХ / "market_oi_slope.csv"
    oi_slope_top_path = ПАПКА_ДАННЫХ / "oi_slope_top.csv"
    silence_states_path = ПАПКА_ДАННЫХ / "silence_states.csv"
    market_regime_path = ПАПКА_ДАННЫХ / "market_regime.csv"
    regime_states_path = ПАПКА_ДАННЫХ / "regime_states.csv"
    coverage_path = ПАПКА_ДАННЫХ / "coverage_report.csv"
    gap_path = ПАПКА_ДАННЫХ / "gap_report.csv"
    manifest_path = ПАПКА_ДАННЫХ / "storage_manifest.txt"
    audit_report_path = ПАПКА_ДАННЫХ / "audit_report.txt"
    research_report_path = ПАПКА_ДАННЫХ / "research_report.txt"
    storage_health_path = ПАПКА_ДАННЫХ / "storage_health_report.txt"
    runtime_health_path = ПАПКА_ДАННЫХ / "runtime_health_report.txt"
    active_universe_path = ПАПКА_ДАННЫХ / "active_universe_report.csv"
    request_failures_path = ПАПКА_ДАННЫХ / "request_failure_report.csv"
    invalid_reasons_path = ПАПКА_ДАННЫХ / "invalid_reason_report.csv"

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
        coverage_path,
        ["calculated_at", "metric", "exchange", "symbol", "first_ts_open", "last_ts_open", "expected_candles", "actual_candles", "missing_candles", "coverage_pct", "missing_pct", "invalid_timestamps", "quality_status"],
        [[r["calculated_at"], r["metric"], r["exchange"], r["symbol"], r["first_ts_open"], r["last_ts_open"], r["expected_candles"], r["actual_candles"], r["missing_candles"], r["coverage_pct"], r["missing_pct"], r["invalid_timestamps"], r["quality_status"]] for r in coverage],
    )

    _write_csv(
        gap_path,
        ["calculated_at", "metric", "exchange", "symbol", "gap_start", "gap_end", "missing_candles", "gap_minutes"],
        [[r["calculated_at"], r["metric"], r["exchange"], r["symbol"], r["gap_start"], r["gap_end"], r["missing_candles"], r["gap_minutes"]] for r in gaps],
    )

    _write_csv(
        active_universe_path,
        ["exchange", "symbol", "activated_at", "source"],
        [[r["exchange"], r["symbol"], r["activated_at"], r["source"]] for r in active_universe],
    )

    _write_csv(
        request_failures_path,
        ["calculated_at", "exchange", "symbol", "data_type", "error_type", "error_message"],
        [[r["calculated_at"], r["exchange"], r["symbol"], r["data_type"], r["error_type"], r["error_message"]] for r in request_failures],
    )

    _write_csv(
        invalid_reasons_path,
        ["invalid_reason", "total"],
        [[r["invalid_reason"], r["total"]] for r in invalid_reasons],
    )

    _write_csv(
        market_research_path,
        ["calculated_at", "ts_close", "exchange", "symbol", "timeframe", "oi_delta_pct", "price_delta_pct", "volume_delta_pct", "oi_velocity", "oi_acceleration", "range_width_pct", "continuation_score", "exhaustion_score", "compression_score", "market_state", "invalid_reason"],
        [[r["calculated_at"], r["ts_close"], r["exchange"], r["symbol"], r["timeframe"], r["oi_delta_pct"], r["price_delta_pct"], r["volume_delta_pct"], r["oi_velocity"], r["oi_acceleration"], r["range_width_pct"], r["continuation_score"], r["exhaustion_score"], r["compression_score"], r["market_state"], r.get("invalid_reason")] for r in market_research],
    )

    _write_csv(
        market_states_path,
        ["exchange", "symbol", "timeframe", "market_state", "state_count", "avg_continuation_score", "avg_exhaustion_score", "avg_compression_score"],
        [[r["exchange"], r["symbol"], r["timeframe"], r["market_state"], r["state_count"], r["avg_continuation_score"], r["avg_exhaustion_score"], r["avg_compression_score"]] for r in market_states],
    )

    _write_csv(
        market_oi_slope_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","reason","oi_delta_pct","oi_acceleration","oi_prev_avg","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage","silence_stage_name"],
        [[r["calculated_at"],r["ts_close"],r["exchange"],r["symbol"],r["timeframe"],r["stage"],r["stage_name"],r["strength"],r["reason"],r["oi_delta_pct"],r["oi_acceleration"],r["oi_prev_avg"],r["price_delta_pct"],r["volume_delta_pct"],r["range_width_pct"],r["silence_stage"],r["silence_stage_name"]] for r in market_oi_slope],
    )

    _write_csv(
        oi_slope_top_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","reason","oi_delta_pct","oi_acceleration","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage_name"],
        [[r["calculated_at"],r["ts_close"],r["exchange"],r["symbol"],r["timeframe"],r["stage"],r["stage_name"],r["strength"],r["reason"],r["oi_delta_pct"],r["oi_acceleration"],r["price_delta_pct"],r["volume_delta_pct"],r["range_width_pct"],r["silence_stage_name"]] for r in oi_slope_top],
    )

    _write_csv(
        market_silence_path,
        ["calculated_at", "ts_close", "exchange", "symbol", "timeframe", "stage", "stage_name", "score", "reason", "oi_delta_pct", "price_delta_pct", "volume_delta_pct", "range_width_pct", "market_state", "invalid_reason"],
        [[r["calculated_at"], r["ts_close"], r["exchange"], r["symbol"], r["timeframe"], r["stage"], r["stage_name"], r["score"], r["reason"], r["oi_delta_pct"], r["price_delta_pct"], r["volume_delta_pct"], r["range_width_pct"], r["market_state"], r["invalid_reason"]] for r in market_silence],
    )

    _write_csv(
        silence_states_path,
        ["exchange", "symbol", "timeframe", "stage", "stage_name", "stage_count", "avg_score"],
        [[r["exchange"], r["symbol"], r["timeframe"], r["stage"], r["stage_name"], r["stage_count"], r["avg_score"]] for r in silence_states],
    )

    _write_csv(
        market_regime_path,
        ["calculated_at", "ts_close", "exchange", "symbol", "timeframe", "market_state", "scenario", "confidence", "reason", "oi_delta_pct", "price_delta_pct", "volume_delta_pct", "range_width_pct", "continuation_score", "exhaustion_score", "compression_score", "invalid_reason"],
        [[r["calculated_at"], r["ts_close"], r["exchange"], r["symbol"], r["timeframe"], r["market_state"], r["scenario"], r["confidence"], r["reason"], r["oi_delta_pct"], r["price_delta_pct"], r["volume_delta_pct"], r["range_width_pct"], r["continuation_score"], r["exhaustion_score"], r["compression_score"], r["invalid_reason"]] for r in market_regime],
    )

    _write_csv(
        regime_states_path,
        ["exchange", "symbol", "timeframe", "scenario", "confidence", "scenario_count", "avg_continuation_score", "avg_exhaustion_score", "avg_compression_score"],
        [[r["exchange"], r["symbol"], r["timeframe"], r["scenario"], r["confidence"], r["scenario_count"], r["avg_continuation_score"], r["avg_exhaustion_score"], r["avg_compression_score"]] for r in regime_states],
    )

    invalid = [r for r in audit if r["validation_status"] != "валидно"]
    critical_coverage = [r for r in coverage if r["quality_status"] == "critical"]
    warning_coverage = [r for r in coverage if r["quality_status"] == "warning"]
    invalid_data_states = [r for r in market_states if r["market_state"] == "invalid_data"]

    audit_lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"mode: {mode}",
        "",
        "v3.5.4 data quality foundation: active",
        "canonical_close: active",
        "contiguous_window_validation: active",
        "coverage_report: active",
        "gap_report: active",
        "invalid_data_protection: active",
        "",
        f"raw_rows: {len(raw_rows)}",
        f"raw_missing_oi_rows: {missing_oi}",
        f"raw_missing_price_rows: {missing_price}",
        f"raw_missing_volume_rows: {missing_volume}",
        f"bot_aggregates_rows: {len(aggregates)}",
        f"validation_audit_rows: {len(audit)}",
        f"invalid_rows: {len(invalid)}",
        f"integrity_rows: {len(integrity)}",
        f"coverage_rows: {len(coverage)}",
        f"gap_rows: {len(gaps)}",
        f"active_universe_rows: {len(active_universe)}",
        f"request_failure_rows: {len(request_failures)}",
        f"coverage_critical_rows: {len(critical_coverage)}",
        f"coverage_warning_rows: {len(warning_coverage)}",
        f"active_universe_rows: {len(active_universe)}",
        f"request_failure_rows: {len(request_failures)}",
        f"market_research_rows: {len(market_research)}",
        f"market_states_rows: {len(market_states)}",
        f"market_regime_rows: {len(market_regime)}",
        f"regime_states_rows: {len(regime_states)}",
        f"invalid_data_state_rows: {len(invalid_data_states)}",
        f"request_failure_rows: {len(request_failures)}",
        "",
        "Top invalid audit:",
    ]

    for r in invalid[:100]:
        audit_lines.append(f'{r["metric"]} {r["symbol"]} {r["exchange"]} {r["timeframe"]} drift={r["drift"]} status={r["validation_status"]}')

    audit_lines.append("")
    audit_lines.append("Invalid data reasons:")
    for r in invalid_reasons:
        audit_lines.append(f"{r['invalid_reason']}: {r['total']}")

    audit_lines.append("")
    audit_lines.append("Worst coverage:")
    for r in sorted(coverage, key=lambda x: (float(x["coverage_pct"] or 0), x["metric"], x["exchange"], x["symbol"]))[:100]:
        audit_lines.append(
            f'{r["metric"]} {r["exchange"]} {r["symbol"]} '
            f'coverage={_fmt_pct(r["coverage_pct"])} missing={r["missing_candles"]} '
            f'invalid_ts={r["invalid_timestamps"]} status={r["quality_status"]}'
        )

    _write_text(audit_report_path, "\n".join(audit_lines))

    research_lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"mode: {mode}",
        "",
        "Исследовательский слой структуры рынка",
        "Источник: реальные bot_aggregates + coverage_report",
        "Fake rows: нет",
        "invalid_data protection: active",
        "",
        f"market_research_rows: {len(market_research)}",
        f"market_states_rows: {len(market_states)}",
        f"market_regime_rows: {len(market_regime)}",
        f"regime_states_rows: {len(regime_states)}",
        f"invalid_data_state_rows: {len(invalid_data_states)}",
        f"request_failure_rows: {len(request_failures)}",
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

    storage_lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "Storage health report",
        "",
        f"coverage_rows: {len(coverage)}",
        f"gap_rows: {len(gaps)}",
        f"coverage_critical_rows: {len(critical_coverage)}",
        f"coverage_warning_rows: {len(warning_coverage)}",
        "",
        "Storage summary:",
    ]

    for r in storage_summary:
        oldest = r["oldest_ts"]
        newest = r["newest_ts"]
        days = 0.0
        if oldest and newest:
            try:
                days = (newest - oldest).total_seconds() / 86400.0
            except Exception:
                days = 0.0
        storage_lines.append(
            f'{r["metric"]}: rows={r["rows_count"]} oldest={oldest} newest={newest} estimated_days={days:.2f}'
        )

    storage_lines.append("")
    storage_lines.append("Worst coverage:")
    for r in sorted(coverage, key=lambda x: (float(x["coverage_pct"] or 0), x["metric"], x["exchange"], x["symbol"]))[:200]:
        storage_lines.append(
            f'{r["metric"]} {r["exchange"]} {r["symbol"]}: '
            f'coverage={_fmt_pct(r["coverage_pct"])} missing_pct={_fmt_pct(r["missing_pct"])} '
            f'missing={r["missing_candles"]} invalid_ts={r["invalid_timestamps"]} status={r["quality_status"]}'
        )

    _write_text(storage_health_path, "\n".join(storage_lines))

    runtime_lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "Runtime health report",
        "",
        f"process_id: {os.getpid()}",
        f"memory_max_rss_mb: {_runtime_memory_mb():.2f}",
        f"export_mode: {mode}",
        f"raw_rows_exported: {len(raw_rows)}",
        f"bot_aggregates_rows_exported: {len(aggregates)}",
        f"validation_audit_rows_exported: {len(audit)}",
        f"market_research_rows_exported: {len(market_research)}",
        "",
        "Runtime note:",
        "This report is generated during export rebuild. It does not restart the bot.",
    ]

    _write_text(runtime_health_path, "\n".join(runtime_lines))

    _write_text(
        manifest_path,
        (
            f"Mighty Duck {APP_VERSION}\n"
            f"mode={mode}\n"
            "main_downloads=market_research_bundle.zip, audit_report.txt, research_report.txt\n"
            "inside_bundle=raw_market_5m.csv, bot_aggregates.csv, validation_audit.csv, market_research.csv, market_states.csv, market_regime.csv, regime_states.csv, coverage_report.csv, gap_report.csv, active_universe_report.csv, request_failure_report.csv, invalid_reason_report.csv, storage_manifest.txt, storage_health_report.txt, runtime_health_report.txt\n"
            "timestamp_migration=active\n"
            "canonical_close=active\n"
            "contiguous_window_validation=active\n"
            "coverage_report=active\n"
            "gap_report=active\n"
            "invalid_data_protection=active\n"
            "research_source=real_bot_aggregates_plus_coverage_report\n"
        ),
    )

    bundle_path = ПАПКА_ДАННЫХ / "market_research_bundle.zip"
    mode_bundle_path = ПАПКА_ДАННЫХ / f"market_research_bundle_{suffix}.zip"

    bundle_files = [
        raw_path,
        aggregates_path,
        audit_path,
        market_research_path,
        market_states_path,
        market_regime_path,
        regime_states_path,
        coverage_path,
        gap_path,
        active_universe_path,
        request_failures_path,
        invalid_reasons_path,
        manifest_path,
        storage_health_path,
        runtime_health_path,
    ]

    _zip(bundle_path, bundle_files)
    _zip(mode_bundle_path, bundle_files + [audit_report_path, research_report_path])

    return bundle_path
