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
        rows = fetch(sql, params) or []
        return [row for row in rows if row is not None]
    except Exception:
        return []


def _v(row, key: str, default=None):
    if row is None:
        return default

    try:
        return row.get(key, default)
    except AttributeError:
        try:
            return row[key]
        except Exception:
            return default


def _rows(rows):
    return [row for row in (rows or []) if row is not None]


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


def _fetch_top_oi_rows(since, timeframe: str, limit: int = 100):
    latest = _safe_fetch("""
        SELECT MAX(ts_close) AS max_ts
        FROM market_oi_slope
        WHERE timeframe = %s
          AND stage >= 1
    """, (timeframe,))

    latest_ts = _v(latest[0], "max_ts") if latest else None

    if not latest_ts:
        return []

    return _safe_fetch("""
        SELECT *
        FROM market_oi_slope
        WHERE timeframe = %s
          AND ts_close >= %s - INTERVAL '60 minutes'
          AND stage >= 1
        ORDER BY
            stage DESC,
            strength DESC,
            raw_strength DESC,
            ts_close DESC,
            exchange,
            symbol
        LIMIT %s
    """, (timeframe, latest_ts, limit))


def _num(row, key: str, default: float = 0.0) -> float:
    try:
        return float(_v(row, key, default) or default)
    except Exception:
        return default


def _continuation_score(oi, price, volume, alignment) -> float:
    score = 0.0

    oi_delta = abs(_num(oi, "oi_delta_pct"))
    oi_accel = abs(_num(oi, "oi_acceleration"))
    price_delta = abs(_num(price, "price_delta_pct"))
    volume_norm = _num(volume, "volume_normalized")
    alignment_score = _num(alignment, "alignment_score")

    if oi_delta >= 5:
        score += 25
    if oi_accel >= 3:
        score += 25
    if price_delta >= 2:
        score += 20
    if volume_norm >= 2:
        score += 15
    if alignment_score > 0:
        score += min(alignment_score, 15)

    return round(min(score, 100), 2)


def _exhaustion_score(oi, price, volume, alignment) -> float:
    score = 0.0

    oi_delta = abs(_num(oi, "oi_delta_pct"))
    oi_accel = abs(_num(oi, "oi_acceleration"))
    price_delta = abs(_num(price, "price_delta_pct"))
    volume_norm = _num(volume, "volume_normalized")
    alignment_score = _num(alignment, "alignment_score")

    if oi_delta >= 8 and price_delta < 1:
        score += 35
    if oi_accel >= 5 and price_delta < 1.5:
        score += 25
    if volume_norm >= 2 and price_delta < 1:
        score += 20
    if alignment_score < 0:
        score += min(abs(alignment_score), 20)

    return round(min(score, 100), 2)


def _liquidity_event_flag(oi, price, volume, alignment) -> int:
    continuation = _continuation_score(oi, price, volume, alignment)
    exhaustion = _exhaustion_score(oi, price, volume, alignment)

    if continuation >= 60 or exhaustion >= 60:
        return 1

    return 0


STAGE_ENGINE_RULES = [
    {
        "stage_engine_state": "continuation",
        "stage_engine_score": 90,
        "min_alignment": 40,
        "min_continuation": 70,
        "max_exhaustion": 40,
        "liquidity_event": 1,
        "reason": "alignment confirms continuation: OI expansion + price/volume follow-through",
    },
    {
        "stage_engine_state": "exhaustion",
        "stage_engine_score": 85,
        "max_alignment": -20,
        "min_exhaustion": 60,
        "reason": "exhaustion: OI/volume expanded but price failed to continue",
    },
    {
        "stage_engine_state": "range",
        "stage_engine_score": 70,
        "min_exhaustion": 45,
        "max_continuation": 55,
        "reason": "range candidate: liquidity event without clean continuation",
    },
    {
        "stage_engine_state": "watch",
        "stage_engine_score": 45,
        "min_continuation": 35,
        "reason": "watch: partial activity, no deterministic regime confirmation",
    },
]


def _stage_rule_match(rule: dict, alignment_score: float, continuation_score: float, exhaustion_score: float, liquidity_event_flag: int) -> bool:
    if "min_alignment" in rule and alignment_score < rule["min_alignment"]:
        return False
    if "max_alignment" in rule and alignment_score > rule["max_alignment"]:
        return False
    if "min_continuation" in rule and continuation_score < rule["min_continuation"]:
        return False
    if "max_continuation" in rule and continuation_score > rule["max_continuation"]:
        return False
    if "min_exhaustion" in rule and exhaustion_score < rule["min_exhaustion"]:
        return False
    if "max_exhaustion" in rule and exhaustion_score > rule["max_exhaustion"]:
        return False
    if "liquidity_event" in rule and liquidity_event_flag != rule["liquidity_event"]:
        return False
    return True


def _stage_engine(oi, price, volume, alignment) -> dict:
    alignment_score = _num(alignment, "alignment_score")
    continuation_score = _continuation_score(oi, price, volume, alignment)
    exhaustion_score = _exhaustion_score(oi, price, volume, alignment)
    liquidity_event_flag = _liquidity_event_flag(oi, price, volume, alignment)

    for rule in STAGE_ENGINE_RULES:
        if _stage_rule_match(
            rule,
            alignment_score,
            continuation_score,
            exhaustion_score,
            liquidity_event_flag,
        ):
            return {
                "stage_engine_state": rule["stage_engine_state"],
                "stage_engine_score": rule["stage_engine_score"],
                "stage_engine_reason": rule["reason"],
            }

    return {
        "stage_engine_state": "neutral",
        "stage_engine_score": 0,
        "stage_engine_reason": "no deterministic stage rule matched",
    }


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

    top_volume_anomalies = _safe_fetch("""
        SELECT *
        FROM market_volume_state
        WHERE ts_close >= %s
          AND (
              volume_state_name = 'аномальный объем'
              OR noise_state != 'не шум'
              OR volume_percentile >= 95
          )
        ORDER BY
            CASE WHEN noise_state != 'не шум' THEN 1 ELSE 0 END DESC,
            volume_percentile DESC,
            volume_delta_pct DESC,
            normalized_volume DESC,
            ts_close DESC
        LIMIT 500
    """, (since,))

    volume_state_summary = _safe_fetch("""
        SELECT
            exchange,
            symbol,
            timeframe,
            volume_state_name,
            COUNT(*) AS rows_count,
            AVG(volume_delta_pct) AS avg_volume_delta_pct,
            AVG(normalized_volume) AS avg_normalized_volume,
            AVG(volume_percentile) AS avg_volume_percentile,
            SUM(CASE WHEN noise_state != 'не шум' THEN 1 ELSE 0 END) AS noise_count
        FROM market_volume_state
        WHERE ts_close >= %s
        GROUP BY exchange, symbol, timeframe, volume_state_name
        ORDER BY noise_count DESC, rows_count DESC, exchange, symbol, timeframe
    """, (since,))

    market_volume_state = _safe_fetch("""
        SELECT *
        FROM market_volume_state
        WHERE ts_close >= %s
        ORDER BY exchange, symbol, timeframe, ts_close
    """, (since,))

    market_price_state = _safe_fetch("""
        SELECT *
        FROM market_price_state
        WHERE ts_close >= %s
        ORDER BY exchange, symbol, timeframe, ts_close
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
        WHERE ts_close >= %s
          AND stage >= 1
        ORDER BY
            stage DESC,
            strength DESC,
            raw_strength DESC,
            exchange,
            symbol,
            timeframe,
            ts_close
        LIMIT 300
    """, (since,))

    top_oi_15m = _fetch_top_oi_rows(since, "15м")
    top_oi_30m = _fetch_top_oi_rows(since, "30м")
    top_oi_1h = _fetch_top_oi_rows(since, "1ч")
    top_oi_4h = _fetch_top_oi_rows(since, "4ч")


    oi_slope_summary = _safe_fetch("""
        SELECT
            exchange,
            timeframe,
            stage_name,
            oi_quality,

            COUNT(*) AS rows_count,

            MIN(strength) AS min_strength,
            AVG(strength) AS avg_strength,
            MAX(strength) AS max_strength,

            AVG(raw_strength) AS avg_raw_strength,

            SUM(CASE WHEN strength >= 95 THEN 1 ELSE 0 END) AS strength_ge_95,
            SUM(CASE WHEN strength >= 100 THEN 1 ELSE 0 END) AS strength_eq_100

        FROM market_oi_slope
        WHERE ts_close >= %s

        GROUP BY
            exchange,
            timeframe,
            stage_name,
            oi_quality

        ORDER BY
            timeframe,
            stage_name,
            avg_strength DESC,
            rows_count DESC
    """, (since,))

    metric_alignment = []

    oi_persistence = _safe_fetch("""
        WITH base AS (
            SELECT
                exchange,
                symbol,
                timeframe,
                ts_close,
                oi_delta_pct,
                oi_acceleration,

                CASE
                    WHEN oi_delta_pct > 0 THEN 1
                    ELSE 0
                END AS positive_flag

            FROM market_oi_slope
            WHERE ts_close >= %s
              AND stage_name != 'нет сигнала'
        ),

        grouped AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY exchange, symbol, timeframe
                    ORDER BY ts_close
                )
                -
                ROW_NUMBER() OVER (
                    PARTITION BY exchange, symbol, timeframe, positive_flag
                    ORDER BY ts_close
                ) AS grp
            FROM base
        )

        SELECT
            exchange,
            symbol,
            timeframe,
            MAX(ts_close) AS ts_close,

            COUNT(*) FILTER (
                WHERE positive_flag = 1
            ) AS positive_oi_windows,

            ROUND(
                SUM(
                    CASE
                        WHEN positive_flag = 1
                        THEN oi_delta_pct
                        ELSE 0
                    END
                )::numeric,
                4
            ) AS cumulative_oi_delta_pct,

            ROUND(
                AVG(
                    CASE
                        WHEN positive_flag = 1
                        THEN oi_delta_pct
                    END
                )::numeric,
                4
            ) AS avg_oi_delta_pct,

            ROUND(
                AVG(
                    CASE
                        WHEN positive_flag = 1
                        THEN oi_acceleration
                    END
                )::numeric,
                4
            ) AS avg_oi_acceleration,

            CASE
                WHEN COUNT(*) FILTER (
                    WHERE positive_flag = 1
                ) >= 6
                AND SUM(
                    CASE
                        WHEN positive_flag = 1
                        THEN oi_delta_pct
                        ELSE 0
                    END
                ) >= 15
                THEN 'устойчивый набор'

                WHEN COUNT(*) FILTER (
                    WHERE positive_flag = 1
                ) >= 3
                AND AVG(
                    CASE
                        WHEN positive_flag = 1
                        THEN oi_acceleration
                    END
                ) >= 0.5
                THEN 'ступенчатый набор'

                WHEN COUNT(*) FILTER (
                    WHERE positive_flag = 1
                ) >= 1
                THEN 'локальный всплеск'

                ELSE 'нет набора'
            END AS persistence_state

        FROM grouped
        GROUP BY exchange, symbol, timeframe, grp
        ORDER BY ts_close DESC
    """, (since,))

    symbol_baseline = _safe_fetch("""
        SELECT
            exchange,
            symbol,
            timeframe,
            COUNT(*) AS rows_count,

            percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(range_width_pct)) AS median_range_width_pct,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(price_delta_pct)) AS median_abs_price_delta_pct,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(volume_delta_pct)) AS median_abs_volume_delta_pct,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(oi_delta_pct)) AS median_abs_oi_delta_pct,

            AVG(ABS(range_width_pct)) AS avg_range_width_pct,
            AVG(ABS(price_delta_pct)) AS avg_abs_price_delta_pct,
            AVG(ABS(volume_delta_pct)) AS avg_abs_volume_delta_pct,
            AVG(ABS(oi_delta_pct)) AS avg_abs_oi_delta_pct
        FROM market_research
        WHERE ts_close >= %s
          AND market_state != 'invalid_data'
        GROUP BY exchange, symbol, timeframe
        ORDER BY exchange, symbol, timeframe
    """, (since,))

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
    market_price_state_path = ПАПКА_ДАННЫХ / "market_price_state.csv"
    market_volume_state_path = ПАПКА_ДАННЫХ / "market_volume_state.csv"
    volume_state_summary_path = ПАПКА_ДАННЫХ / "volume_state_summary.csv"
    top_volume_anomalies_path = ПАПКА_ДАННЫХ / "top_volume_anomalies.csv"
    market_oi_slope_path = ПАПКА_ДАННЫХ / "market_oi_slope.csv"
    oi_slope_top_path = ПАПКА_ДАННЫХ / "oi_slope_top.csv"

    top_oi_15m_path = ПАПКА_ДАННЫХ / "top_oi_slope_15m.csv"
    top_oi_30m_path = ПАПКА_ДАННЫХ / "top_oi_slope_30m.csv"
    top_oi_1h_path = ПАПКА_ДАННЫХ / "top_oi_slope_1h.csv"
    top_oi_4h_path = ПАПКА_ДАННЫХ / "top_oi_slope_4h.csv"

    oi_slope_summary_path = ПАПКА_ДАННЫХ / "oi_slope_summary.csv"
    silence_states_path = ПАПКА_ДАННЫХ / "silence_states.csv"
    market_regime_path = ПАПКА_ДАННЫХ / "market_regime.csv"
    regime_states_path = ПАПКА_ДАННЫХ / "regime_states.csv"
    engine_summary_path = ПАПКА_ДАННЫХ / "engine_summary.csv"
    stage_calibration_template_path = ПАПКА_ДАННЫХ / "stage_calibration_template.csv"
    symbol_baseline_path = ПАПКА_ДАННЫХ / "symbol_baseline.csv"
    oi_persistence_path = ПАПКА_ДАННЫХ / "oi_persistence.csv"
    metric_alignment_path = ПАПКА_ДАННЫХ / "metric_alignment.csv"
    stage_metrics_table_path = ПАПКА_ДАННЫХ / "stage_metrics_table.csv"
    coverage_path = ПАПКА_ДАННЫХ / "coverage_report.csv"
    gap_path = ПАПКА_ДАННЫХ / "gap_report.csv"
    manifest_path = ПАПКА_ДАННЫХ / "storage_manifest.txt"
    audit_report_path = ПАПКА_ДАННЫХ / "audit_report.txt"
    research_report_path = ПАПКА_ДАННЫХ / "research_report.txt"
    storage_health_path = ПАПКА_ДАННЫХ / "storage_health_report.txt"
    runtime_health_path = ПАПКА_ДАННЫХ / "runtime_health_report.txt"
    runtime_timing_path = ПАПКА_ДАННЫХ / "runtime_timing_report.txt"
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
        top_volume_anomalies_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","volume_state","volume_state_name","reason","volume_delta_pct","normalized_volume","volume_percentile","noise_state","market_state","invalid_reason"],
        [[r["calculated_at"],r["ts_close"],r["exchange"],r["symbol"],r["timeframe"],r["volume_state"],r["volume_state_name"],r["reason"],r["volume_delta_pct"],r["normalized_volume"],r["volume_percentile"],r["noise_state"],r["market_state"],r["invalid_reason"]] for r in top_volume_anomalies],
    )

    _write_csv(
        volume_state_summary_path,
        ["exchange","symbol","timeframe","volume_state_name","rows_count","avg_volume_delta_pct","avg_normalized_volume","avg_volume_percentile","noise_count"],
        [[r["exchange"],r["symbol"],r["timeframe"],r["volume_state_name"],r["rows_count"],r["avg_volume_delta_pct"],r["avg_normalized_volume"],r["avg_volume_percentile"],r["noise_count"]] for r in volume_state_summary],
    )

    _write_csv(
        market_volume_state_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","volume_state","volume_state_name","reason","volume_delta_pct","normalized_volume","volume_percentile","noise_state","market_state","invalid_reason"],
        [[r["calculated_at"],r["ts_close"],r["exchange"],r["symbol"],r["timeframe"],r["volume_state"],r["volume_state_name"],r["reason"],r["volume_delta_pct"],r["normalized_volume"],r["volume_percentile"],r["noise_state"],r["market_state"],r["invalid_reason"]] for r in market_volume_state],
    )

    _write_csv(
        market_price_state_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","price_state","price_state_name","reason","price_delta_pct","range_width_pct","market_state","invalid_reason"],
        [[r["calculated_at"],r["ts_close"],r["exchange"],r["symbol"],r["timeframe"],r["price_state"],r["price_state_name"],r["reason"],r["price_delta_pct"],r["range_width_pct"],r["market_state"],r["invalid_reason"]] for r in market_price_state],
    )

    _write_csv(
        market_oi_slope_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","raw_strength","oi_quality","reason","oi_delta_pct","oi_acceleration","oi_prev_avg","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage","silence_stage_name"],
        [[r["calculated_at"],r["ts_close"],r["exchange"],r["symbol"],r["timeframe"],r["stage"],r["stage_name"],r["strength"],r["raw_strength"],r["oi_quality"],r["reason"],r["oi_delta_pct"],r["oi_acceleration"],r["oi_prev_avg"],r["price_delta_pct"],r["volume_delta_pct"],r["range_width_pct"],r["silence_stage"],r["silence_stage_name"]] for r in market_oi_slope],
    )


    _write_csv(
        top_oi_15m_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","raw_strength","oi_quality","reason","oi_delta_pct","oi_acceleration","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage_name"],
        [[_v(r,"calculated_at"),_v(r,"ts_close"),_v(r,"exchange"),_v(r,"symbol"),_v(r,"timeframe"),_v(r,"stage"),_v(r,"stage_name"),_v(r,"strength"),_v(r,"raw_strength"),_v(r,"oi_quality"),_v(r,"reason"),_v(r,"oi_delta_pct"),_v(r,"oi_acceleration"),_v(r,"price_delta_pct"),_v(r,"volume_delta_pct"),_v(r,"range_width_pct"),_v(r,"silence_stage_name")] for r in _rows(top_oi_15m)],
    )

    _write_csv(
        top_oi_30m_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","raw_strength","oi_quality","reason","oi_delta_pct","oi_acceleration","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage_name"],
        [[_v(r,"calculated_at"),_v(r,"ts_close"),_v(r,"exchange"),_v(r,"symbol"),_v(r,"timeframe"),_v(r,"stage"),_v(r,"stage_name"),_v(r,"strength"),_v(r,"raw_strength"),_v(r,"oi_quality"),_v(r,"reason"),_v(r,"oi_delta_pct"),_v(r,"oi_acceleration"),_v(r,"price_delta_pct"),_v(r,"volume_delta_pct"),_v(r,"range_width_pct"),_v(r,"silence_stage_name")] for r in _rows(top_oi_30m)],
    )

    _write_csv(
        top_oi_1h_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","raw_strength","oi_quality","reason","oi_delta_pct","oi_acceleration","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage_name"],
        [[_v(r,"calculated_at"),_v(r,"ts_close"),_v(r,"exchange"),_v(r,"symbol"),_v(r,"timeframe"),_v(r,"stage"),_v(r,"stage_name"),_v(r,"strength"),_v(r,"raw_strength"),_v(r,"oi_quality"),_v(r,"reason"),_v(r,"oi_delta_pct"),_v(r,"oi_acceleration"),_v(r,"price_delta_pct"),_v(r,"volume_delta_pct"),_v(r,"range_width_pct"),_v(r,"silence_stage_name")] for r in _rows(top_oi_1h)],
    )

    _write_csv(
        top_oi_4h_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","raw_strength","oi_quality","reason","oi_delta_pct","oi_acceleration","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage_name"],
        [[_v(r,"calculated_at"),_v(r,"ts_close"),_v(r,"exchange"),_v(r,"symbol"),_v(r,"timeframe"),_v(r,"stage"),_v(r,"stage_name"),_v(r,"strength"),_v(r,"raw_strength"),_v(r,"oi_quality"),_v(r,"reason"),_v(r,"oi_delta_pct"),_v(r,"oi_acceleration"),_v(r,"price_delta_pct"),_v(r,"volume_delta_pct"),_v(r,"range_width_pct"),_v(r,"silence_stage_name")] for r in _rows(top_oi_4h)],
    )

    _write_csv(
        oi_slope_summary_path,
        [
            "exchange",
            "timeframe",
            "stage_name",
            "oi_quality",
            "rows_count",
            "min_strength",
            "avg_strength",
            "max_strength",
            "avg_raw_strength",
            "strength_ge_95",
            "strength_eq_100",
        ],
        [
            [
                _v(r, "exchange"),
                _v(r, "timeframe"),
                _v(r, "stage_name"),
                _v(r, "oi_quality"),
                _v(r, "rows_count", 0),
                _v(r, "min_strength", 0),
                _v(r, "avg_strength", 0),
                _v(r, "max_strength", 0),
                _v(r, "avg_raw_strength", 0),
                _v(r, "strength_ge_95", 0),
                _v(r, "strength_eq_100", 0),
            ]
            for r in _rows(oi_slope_summary)
        ],
    )

    _write_csv(
        oi_slope_top_path,
        ["calculated_at","ts_close","exchange","symbol","timeframe","stage","stage_name","strength","raw_strength","oi_quality","reason","oi_delta_pct","oi_acceleration","price_delta_pct","volume_delta_pct","range_width_pct","silence_stage_name"],
        [[_v(r,"calculated_at"),_v(r,"ts_close"),_v(r,"exchange"),_v(r,"symbol"),_v(r,"timeframe"),_v(r,"stage"),_v(r,"stage_name"),_v(r,"strength"),_v(r,"raw_strength"),_v(r,"oi_quality"),_v(r,"reason"),_v(r,"oi_delta_pct"),_v(r,"oi_acceleration"),_v(r,"price_delta_pct"),_v(r,"volume_delta_pct"),_v(r,"range_width_pct"),_v(r,"silence_stage_name")] for r in _rows(oi_slope_top)],
    )

    oi_groups = {}

    for r in market_oi_slope:
        key = (r["timeframe"], r["stage"], r["stage_name"])
        g = oi_groups.setdefault(key, {
            "timeframe": r["timeframe"],
            "stage": r["stage"],
            "stage_name": r["stage_name"],
            "rows_count": 0,
            "strength_sum": 0.0,
            "strength_min": None,
            "strength_max": None,
            "raw_strength_max": None,
            "strength_100_count": 0,
            "unique_strength": set(),
        })

        strength = float(r["strength"] or 0.0)
        raw_strength = float(r["raw_strength"] or 0.0)

        g["rows_count"] += 1
        g["strength_sum"] += strength
        g["strength_min"] = strength if g["strength_min"] is None else min(g["strength_min"], strength)
        g["strength_max"] = strength if g["strength_max"] is None else max(g["strength_max"], strength)
        g["raw_strength_max"] = raw_strength if g["raw_strength_max"] is None else max(g["raw_strength_max"], raw_strength)

        if strength >= 100:
            g["strength_100_count"] += 1

        g["unique_strength"].add(strength)


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

    engine_summary = [
        [
            "market_silence",
            len(_rows(market_silence)),
            len([r for r in _rows(market_silence) if _v(r, "stage_name") == "тишина"]),
            len([r for r in _rows(market_silence) if _v(r, "stage_name") == "сухой рынок"]),
            None,
            None,
        ],
        [
            "market_price_state",
            len(_rows(market_price_state)),
            len([r for r in _rows(market_price_state) if _v(r, "price_state_name") == "сжатие"]),
            len([r for r in _rows(market_price_state) if "импульс" in str(_v(r, "price_state_name", ""))]),
            None,
            None,
        ],
        [
            "market_volume_state",
            len(_rows(market_volume_state)),
            len([r for r in _rows(market_volume_state) if _v(r, "noise_state") == "шум"]),
            len([r for r in _rows(market_volume_state) if _v(r, "volume_state_name") == "аномальный объем"]),
            round(sum(float(_v(r, "volume_percentile", 0) or 0) for r in _rows(market_volume_state)) / max(len(_rows(market_volume_state)), 1), 2),
            None,
        ],
        [
            "market_oi_slope",
            len(_rows(market_oi_slope)),
            len([r for r in _rows(market_oi_slope) if _v(r, "stage_name") == "наблюдение"]),
            len([r for r in _rows(market_oi_slope) if _v(r, "stage_name") == "возня"]),
            round(sum(float(_v(r, "strength", 0) or 0) for r in _rows(market_oi_slope)) / max(len(_rows(market_oi_slope)), 1), 2),
            max([float(_v(r, "strength", 0) or 0) for r in _rows(market_oi_slope)] or [0]),
        ],
        [
            "market_regime",
            len(_rows(market_regime)),
            len([r for r in _rows(market_regime) if _v(r, "scenario") == "compression"]),
            len([r for r in _rows(market_regime) if _v(r, "scenario") == "continuation"]),
            len([r for r in _rows(market_regime) if _v(r, "scenario") == "range"]),
            len([r for r in _rows(market_regime) if _v(r, "scenario") == "exhaustion"]),
        ],
    ]

    oi_state_map = {
        (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close")): r
        for r in _rows(market_oi_slope)
    }

    price_state_map = {
        (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close")): r
        for r in _rows(market_price_state)
    }

    volume_state_map = {
        (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close")): r
        for r in _rows(market_volume_state)
    }

    metric_alignment = []

    for r in _rows(market_research):
        if _v(r, "market_state") == "invalid_data":
            continue

        key = (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close"))

        oi_row = oi_state_map.get(key)
        price_row = price_state_map.get(key)
        volume_row = volume_state_map.get(key)

        oi_stage = _v(oi_row, "stage_name", "нет сигнала")
        price_state_name = _v(price_row, "price_state_name", "")
        volume_state_name = _v(volume_row, "volume_state_name", "")

        if (
            oi_stage in ("наблюдение", "возня")
            and price_state_name in ("сжатие", "спокойный боковик")
            and volume_state_name in ("обычный объем", "объем растет")
        ):
            alignment_state = "silent accumulation"

        elif (
            oi_stage in ("наблюдение", "возня")
            and price_state_name == "импульс вниз"
            and volume_state_name in ("аномальный объем", "всплеск объема")
        ):
            alignment_state = "conflicted"

        elif (
            oi_stage == "нет сигнала"
            and price_state_name in ("импульс вверх", "импульс вниз")
            and volume_state_name in ("аномальный объем", "всплеск объема")
        ):
            alignment_state = "exhausted"

        elif (
            oi_stage in ("наблюдение", "возня")
            and price_state_name in ("импульс вверх", "импульс вниз")
            and volume_state_name in ("объем растет", "аномальный объем")
        ):
            alignment_state = "aligned"

        elif (
            price_state_name == "широкий боковик"
            and volume_state_name in ("аномальный объем", "всплеск объема")
        ):
            alignment_state = "noisy expansion"

        else:
            alignment_state = "neutral"

        metric_alignment.append({
            "exchange": _v(r, "exchange"),
            "symbol": _v(r, "symbol"),
            "timeframe": _v(r, "timeframe"),
            "ts_close": _v(r, "ts_close"),
            "oi_stage": oi_stage,
            "price_state_name": price_state_name,
            "volume_state_name": volume_state_name,
            "market_state": _v(r, "market_state"),
            "oi_delta_pct": _v(r, "oi_delta_pct", 0),
            "price_delta_pct": _v(r, "price_delta_pct", 0),
            "volume_delta_pct": _v(r, "volume_delta_pct", 0),
            "range_width_pct": _v(r, "range_width_pct", 0),
            "alignment_state": alignment_state,
        })

    _write_csv(
        metric_alignment_path,
        [
            "exchange",
            "symbol",
            "timeframe",
            "ts_close",
            "oi_stage",
            "price_state_name",
            "volume_state_name",
            "market_state",
            "oi_delta_pct",
            "price_delta_pct",
            "volume_delta_pct",
            "range_width_pct",
            "alignment_state",
        ],
        [
            [
                _v(r, "exchange"),
                _v(r, "symbol"),
                _v(r, "timeframe"),
                _v(r, "ts_close"),
                _v(r, "oi_stage"),
                _v(r, "price_state_name"),
                _v(r, "volume_state_name"),
                _v(r, "market_state"),
                _v(r, "oi_delta_pct", 0),
                _v(r, "price_delta_pct", 0),
                _v(r, "volume_delta_pct", 0),
                _v(r, "range_width_pct", 0),
                _v(r, "alignment_state"),
            ]
            for r in _rows(metric_alignment)
        ],
    )

    _write_csv(
        oi_persistence_path,
        [
            "exchange",
            "symbol",
            "timeframe",
            "ts_close",
            "positive_oi_windows",
            "cumulative_oi_delta_pct",
            "avg_oi_delta_pct",
            "avg_oi_acceleration",
            "persistence_state",
        ],
        [
            [
                _v(r, "exchange"),
                _v(r, "symbol"),
                _v(r, "timeframe"),
                _v(r, "ts_close"),
                _v(r, "positive_oi_windows", 0),
                _v(r, "cumulative_oi_delta_pct", 0),
                _v(r, "avg_oi_delta_pct", 0),
                _v(r, "avg_oi_acceleration", 0),
                _v(r, "persistence_state"),
            ]
            for r in _rows(oi_persistence)
        ],
    )

    _write_csv(
        symbol_baseline_path,
        [
            "exchange",
            "symbol",
            "timeframe",
            "rows_count",
            "median_range_width_pct",
            "median_abs_price_delta_pct",
            "median_abs_volume_delta_pct",
            "median_abs_oi_delta_pct",
            "avg_range_width_pct",
            "avg_abs_price_delta_pct",
            "avg_abs_volume_delta_pct",
            "avg_abs_oi_delta_pct",
        ],
        [
            [
                _v(r, "exchange"),
                _v(r, "symbol"),
                _v(r, "timeframe"),
                _v(r, "rows_count", 0),
                _v(r, "median_range_width_pct", 0),
                _v(r, "median_abs_price_delta_pct", 0),
                _v(r, "median_abs_volume_delta_pct", 0),
                _v(r, "median_abs_oi_delta_pct", 0),
                _v(r, "avg_range_width_pct", 0),
                _v(r, "avg_abs_price_delta_pct", 0),
                _v(r, "avg_abs_volume_delta_pct", 0),
                _v(r, "avg_abs_oi_delta_pct", 0),
            ]
            for r in _rows(symbol_baseline)
        ],
    )

    stage_metrics_rows = []
    price_by_key = {
        (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close")): r
        for r in _rows(market_price_state)
    }
    volume_by_key = {
        (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close")): r
        for r in _rows(market_volume_state)
    }
    alignment_by_key = {
        (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close")): r
        for r in _rows(metric_alignment)
    }

    for r in _rows(market_oi_slope):
        key = (_v(r, "exchange"), _v(r, "symbol"), _v(r, "timeframe"), _v(r, "ts_close"))
        pr = price_by_key.get(key, {})
        vr = volume_by_key.get(key, {})
        ar = alignment_by_key.get(key, {})

        se = _stage_engine(r, pr, vr, ar)

        stage_metrics_rows.append([
            _v(r, "calculated_at"),
            _v(r, "ts_close"),
            _v(r, "exchange"),
            _v(r, "symbol"),
            _v(r, "timeframe"),

            _v(r, "oi_delta_pct"),
            _v(r, "oi_acceleration"),
            _v(r, "oi_prev_avg"),
            abs(float(_v(r, "oi_delta_pct", 0) or 0)),
            abs(float(_v(r, "oi_acceleration", 0) or 0)),
            _v(r, "raw_strength"),
            _v(r, "strength"),
            _v(r, "oi_quality"),
            _v(r, "stage"),
            _v(r, "stage_name"),
            _v(r, "reason"),

            _v(pr, "price_delta_pct"),
            _v(pr, "price_delta_pct"),
            _v(pr, "range_width_pct"),
            _v(pr, "price_state"),
            _v(pr, "price_state_name"),
            _v(pr, "reason"),

            _v(vr, "volume_delta_pct"),
            _v(vr, "normalized_volume"),
            _v(vr, "volume_percentile"),
            _v(vr, "noise_state"),
            _v(vr, "volume_state"),
            _v(vr, "volume_state_name"),
            _v(vr, "reason"),

            _v(ar, "alignment_state"),
            _v(ar, "alignment_score"),
            _v(ar, "reason"),

            _continuation_score(r, pr, vr, ar),
            _exhaustion_score(r, pr, vr, ar),
            _liquidity_event_flag(r, pr, vr, ar),

            se["stage_engine_state"],
            se["stage_engine_score"],
            se["stage_engine_reason"],

            _v(r, "silence_stage"),
            _v(r, "silence_stage_name"),
        ])

    _write_csv(
        stage_metrics_table_path,
        [
            "calculated_at",
            "ts_close",
            "exchange",
            "symbol",
            "timeframe",

            "oi_delta_pct",
            "oi_acceleration",
            "oi_prev_avg",
            "oi_abs_delta_pct",
            "oi_abs_acceleration",
            "oi_raw_strength",
            "oi_strength",
            "oi_quality",
            "oi_engine_stage",
            "oi_engine_stage_name",
            "oi_engine_reason",

            "price_delta_pct",
            "price_slope_pct",
            "price_range_width_pct",
            "price_state",
            "price_state_name",
            "price_reason",

            "volume_delta_pct",
            "volume_normalized",
            "volume_percentile",
            "volume_noise_state",
            "volume_state",
            "volume_state_name",
            "volume_reason",

            "alignment_state",
            "alignment_score",
            "alignment_reason",

            "continuation_score",
            "exhaustion_score",
            "liquidity_event_flag",

            "stage_engine_state",
            "stage_engine_score",
            "stage_engine_reason",

            "silence_stage",
            "silence_stage_name",
        ],
        stage_metrics_rows,
    )

    _write_csv(
        stage_calibration_template_path,
        [
            "case_id",
            "reviewed_at",
            "exchange",
            "symbol",
            "timeframe",
            "ts_close",
            "human_stage_0_1_2_3",
            "bot_stage_name",
            "oi_delta_pct",
            "oi_acceleration",
            "strength",
            "raw_strength",
            "oi_quality",
            "price_state_name",
            "price_delta_pct",
            "range_width_pct",
            "volume_state_name",
            "volume_delta_pct",
            "noise_state",
            "manual_comment",
            "false_positive",
            "false_negative",
            "keep_for_thresholds",
        ],
        [],
    )

    _write_csv(
        engine_summary_path,
        ["engine","rows","metric_a","metric_b","metric_c","metric_d"],
        engine_summary,
    )


    _write_csv(
        regime_states_path,
        ["exchange", "symbol", "timeframe", "scenario", "confidence", "scenario_count", "avg_continuation_score", "avg_exhaustion_score", "avg_compression_score"],
        [[r["exchange"], r["symbol"], r["timeframe"], r["scenario"], r["confidence"], r["scenario_count"], r["avg_continuation_score"], r["avg_exhaustion_score"], r["avg_compression_score"]] for r in regime_states],
    )

    invalid = [r for r in _rows(audit) if r["validation_status"] != "валидно"]
    critical_coverage = [r for r in _rows(coverage) if r["quality_status"] == "critical"]
    warning_coverage = [r for r in _rows(coverage) if r["quality_status"] == "warning"]
    invalid_data_states = [r for r in _rows(market_states) if r["market_state"] == "invalid_data"]

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
            "inside_bundle=raw_market_5m.csv, bot_aggregates.csv, validation_audit.csv, market_research.csv, market_states.csv, market_volume_state.csv, volume_state_summary.csv, top_volume_anomalies.csv, market_price_state.csv, market_oi_slope.csv, oi_slope_top.csv, top_oi_slope_15m.csv, top_oi_slope_30m.csv, top_oi_slope_1h.csv, top_oi_slope_4h.csv, oi_slope_summary.csv, market_regime.csv, regime_states.csv, engine_summary.csv, stage_calibration_template.csv, stage_metrics_table.csv, symbol_baseline.csv, oi_persistence.csv, metric_alignment.csv, coverage_report.csv, gap_report.csv, active_universe_report.csv, request_failure_report.csv, invalid_reason_report.csv, storage_manifest.txt, storage_health_report.txt, runtime_health_report.txt, runtime_timing_report.txt\n"
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
        market_silence_path,
        market_states_path,
        market_volume_state_path,
        volume_state_summary_path,
        top_volume_anomalies_path,
        market_price_state_path,
        market_oi_slope_path,
        oi_slope_top_path,
        top_oi_15m_path,
        top_oi_30m_path,
        top_oi_1h_path,
        top_oi_4h_path,
        oi_slope_summary_path,
        market_regime_path,
        regime_states_path,
        engine_summary_path,
        stage_calibration_template_path,
        stage_metrics_table_path,
        symbol_baseline_path,
        oi_persistence_path,
        metric_alignment_path,
        coverage_path,
        gap_path,
        active_universe_path,
        request_failures_path,
        invalid_reasons_path,
        manifest_path,
        storage_health_path,
        runtime_health_path,
        runtime_timing_path,
    ]

    _zip(bundle_path, bundle_files)
    _zip(mode_bundle_path, bundle_files + [audit_report_path, research_report_path])

    return bundle_path
