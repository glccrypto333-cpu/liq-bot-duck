from __future__ import annotations
import time
import os
import json
import sys
import subprocess
import resource
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import threading
import traceback
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

from config import (
    APP_VERSION,
    СТАРТОВОЕ_СООБЩЕНИЕ,
    ДНЕЙ_ХРАНЕНИЯ,
    КОМАНДЫ,
    ИНТЕРВАЛ_ЦИКЛА_СЕК,
    ЛИМИТ_СИМВОЛОВ_BYBIT,
    ЛИМИТ_СИМВОЛОВ_BINANCE,
    BYBIT_COLLECT_WORKERS,
    BINANCE_COLLECT_WORKERS,
    ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК,
    AGGREGATES_EVERY_CYCLES,
    MAX_COLLECT_SECONDS_FOR_AGGREGATES,
)
from logger import log
from time_utils import iso_мск, текст_мск
from db import (
    init_db,
    upsert_oi,
    upsert_price,
    upsert_volume,
    cleanup_old,
    migrate_canonical_ts_close,
    replace_active_universe,
    replace_request_failures,
    load_quarantine_symbols,
    load_data_quality_quarantine_symbols,
    sync_data_quality_quarantine,
    refresh_quote_turnover_state,
    quote_turnover_state_summary,
    select_quote_turnover_backfill_targets,
    fetch,
    prune_inactive_state_rows,
)
from exchange_clients import (
    fetch_bybit_symbols,
    fetch_binance_symbols,
    fetch_bybit_oi_5m,
    fetch_binance_oi_5m,
    fetch_bybit_kline_5m,
    fetch_binance_kline_5m,
    get_request_stats,
    reset_request_stats,
)
from aggregation_engine import rebuild_aggregate_windows, rebuild_latest_aggregate_windows
from autonomous_oi_service import (
    build_stage_chain_continuity_report,
    build_quarantine_lifecycle,
    build_window_freshness_by_kind,
    collect_stage1_near_maturity_diagnostics,
    get_runtime_observability_metrics,
    run_autonomous_oi_service,
    run_post_stage_analytics_tail,
)
from cycle_cadence import should_run_this_cycle, should_run_maintenance_this_cycle
from export_engine import rebuild_exports
from telegram_bot import start_polling, send_panel_message, check_stage3_alerts
from runtime_mode import runtime_mode_text
from raw_validate import validate_collected_raw


class CycleStop(RuntimeError):
    def __init__(self, stop_reason: str, message: str, severity: str = "stop"):
        super().__init__(message)
        self.stop_reason = stop_reason
        self.severity = severity


_PROCESS_STARTED_AT_MSK = iso_мск()
_UNIVERSE_LOCK = threading.Lock()
_RUNTIME_UNIVERSE = {
    "bybit_symbols": [],
    "binance_symbols": [],
    "bybit_total": 0,
    "binance_total": 0,
    "active_total": 0,
    "quarantine_total": 0,
    "data_quality_quarantine_total": 0,
    "last_refresh_msk": None,
    "last_refresh_cycle": 0,
    "last_refresh_reason": "startup",
    "added_total": 0,
    "removed_total": 0,
    "added_samples": [],
    "removed_samples": [],
    "listing_problem_pairs": [],
    "discovery_removed_total": 0,
    "limit_removed_total": 0,
    "pruned_state_counts": {},
    "pruned_state_total": 0,
    "listing_health": "ok",
    "listing_alerts": [],
}

_LAST_OI_GAP_REPAIR = {
    "attempted_pairs": 0,
    "repaired_pairs": 0,
    "inserted_rows": 0,
    "rebuild_runs": 0,
    "before_alerts": [],
    "after_alerts": [],
    "pair_notes": [],
}

_LAST_LISTING_SELF_HEAL = {
    "attempted": False,
    "before_alerts": [],
    "after_alerts": [],
    "before_problem_pairs": [],
    "after_problem_pairs": [],
}


def _write_text_atomic(path: str | Path, payload: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.name}.tmp")
    tmp_path.write_text(payload)
    tmp_path.replace(target)


def _write_json_atomic(path: str | Path, payload: dict) -> None:
    _write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
    )


def _canonical_global_block_reason(runtime_health: dict, cycle_status: dict | None = None) -> str | None:
    cycle_status = cycle_status or {}
    stop_reason = cycle_status.get("stop_reason")
    stop_severity = cycle_status.get("stop_severity")
    cycle_health = cycle_status.get("cycle_health") or runtime_health.get("cycle_health")
    overrun_streak = int(cycle_status.get("overrun_streak", 0) or 0)
    summary = runtime_health.get("duck_universe_summary") or {}
    symbols_total = int(runtime_health.get("symbols_total", 0) or 0)

    if stop_reason and stop_reason != "ok" and stop_severity in {"error", "stop"}:
        return "internal_error"

    if cycle_health == "overrun" and overrun_streak >= int(os.getenv("CYCLE_OVERRUN_HARD_STREAK", "3")):
        return "cycle_overrun_hard"

    if symbols_total > 0:
        canonical_quality_ok = (
            runtime_health.get("duck_universe_health") == "ok"
            and runtime_health.get("data_quality_state") == "ok"
        )
        if not canonical_quality_ok:
            no_windows = int(summary.get("no_windows_pairs", 0) or 0)
            stale30 = int(summary.get("stale30_pairs", 0) or 0)
            incomplete = int(summary.get("incomplete_pairs", 0) or 0)
            if no_windows >= symbols_total or stale30 >= symbols_total or incomplete >= symbols_total:
                return "data_pipeline_stalled"

    listing_summary = runtime_health.get("listing_summary") or {}
    if runtime_health.get("duck_listing_health") == "error" and int(listing_summary.get("active_total", 0) or 0) == 0:
        return "universe_fetch_failed"

    if runtime_health.get("snapshot_health") == "critical":
        return "internal_error"

    return None


def _canonical_source_status(exchange: str, runtime_health: dict) -> dict:
    exchange_upper = exchange.upper()
    by_exchange = runtime_health.get("symbols_by_exchange") or {}
    listing_summary = runtime_health.get("listing_summary") or {}
    problem_pairs = listing_summary.get("listing_problem_pairs") or []
    exchange_issues = [
        row for row in problem_pairs
        if (row.get("exchange") or "").upper() == exchange_upper
    ]
    total = int(by_exchange.get(exchange_upper, 0) or 0)
    status = "ok" if not exchange_issues else "degraded"
    return {
        "status": status,
        "total": total,
        "monitored": total,
        "issues": exchange_issues[:20],
    }


def _data_quality_quarantine_rows(limit: int = 100) -> list[dict]:
    try:
        rows = sorted(load_data_quality_quarantine_symbols())
    except Exception:
        return []
    return [
        {
            "exchange": exchange,
            "symbol": symbol,
            "reason": "карантин_качества_данных",
        }
        for exchange, symbol in rows[:limit]
    ]


def _write_canonical_health(runtime_health: dict, cycle_status: dict | None = None) -> None:
    cycle_status = cycle_status or {}
    runtime_dir = Path("runtime")
    runtime_dir.mkdir(exist_ok=True)

    global_block_reason = _canonical_global_block_reason(runtime_health, cycle_status)
    alerts = list(runtime_health.get("runtime_alerts") or [])
    if global_block_reason:
        alerts.append(f"global_block_reason={global_block_reason}")

    universe_summary = runtime_health.get("duck_universe_summary") or {}
    listing_summary = runtime_health.get("listing_summary") or {}
    symbols_by_exchange = runtime_health.get("symbols_by_exchange") or {}
    data_quality_quarantine = _data_quality_quarantine_rows()
    all_problem_pairs = list(runtime_health.get("universe_problem_pairs") or [])
    blocking_problem_pairs = [
        row for row in all_problem_pairs
        if row.get("blocking", True)
    ]
    info_problem_pairs = [
        row for row in all_problem_pairs
        if not row.get("blocking", True)
    ]

    def _problem_counts(rows: list[dict]) -> dict[str, int]:
        incomplete = sum(1 for row in rows if int(row.get("missing_cnt", 0) or 0) > 0)
        absent = sum(1 for row in rows if int(row.get("present_cnt", 0) or 0) == 0)
        stale30 = sum(1 for row in rows if float(row.get("max_lag_min", 0) or 0) > 30)
        stale60 = sum(1 for row in rows if float(row.get("max_lag_min", 0) or 0) > 60)
        stale180 = sum(1 for row in rows if float(row.get("max_lag_min", 0) or 0) > 180)
        return {
            "pairs": len(rows),
            "incomplete_pairs": incomplete,
            "no_windows_pairs": absent,
            "stale30_pairs": stale30,
            "stale60_pairs": stale60,
            "stale180_pairs": stale180,
        }

    blocking_summary = {
        "universe": int(runtime_health.get("symbols_total", 0) or 0),
        **_problem_counts(blocking_problem_pairs),
    }
    info_summary = {
        "universe": int(runtime_health.get("symbols_total", 0) or 0),
        **_problem_counts(info_problem_pairs),
    }

    health = {
        "bot": "duck",
        "status": "running" if runtime_health.get("pid") else "unknown",
        "pid": runtime_health.get("pid"),
        "started_at": _PROCESS_STARTED_AT_MSK,
        "updated_at": runtime_health.get("updated_at_utc") or iso_мск(),
        "universe": {
            "sources": {
                "binance": _canonical_source_status("BINANCE", runtime_health),
                "bybit": _canonical_source_status("BYBIT", runtime_health),
            },
            "total_symbols": int(runtime_health.get("symbols_total", 0) or 0),
            "monitored_symbols": int(runtime_health.get("symbols_total", 0) or 0)
            - int(runtime_health.get("symbols_in_data_quality_quarantine", 0) or 0),
            "symbols_by_exchange": symbols_by_exchange,
            "listing_health": runtime_health.get("duck_listing_health", "error"),
            "universe_health": runtime_health.get("duck_universe_health", "error"),
            "data_quality": runtime_health.get("data_quality_state", "error"),
            "quarantine": blocking_problem_pairs,
            "quarantine_total": len(blocking_problem_pairs),
            "info_pairs": info_problem_pairs,
            "info_pairs_total": len(info_problem_pairs),
            "data_quality_quarantine": data_quality_quarantine,
            "data_quality_quarantine_total": int(runtime_health.get("symbols_in_data_quality_quarantine", 0) or 0),
            "stale_windows": int(blocking_summary.get("stale30_pairs", 0) or 0),
            "incomplete_windows": int(blocking_summary.get("incomplete_pairs", 0) or 0),
            "absent_in_duck": int(blocking_summary.get("no_windows_pairs", 0) or 0),
            "info_stale_windows": int(info_summary.get("stale30_pairs", 0) or 0),
            "info_incomplete_windows": int(info_summary.get("incomplete_pairs", 0) or 0),
            "info_absent_in_duck": int(info_summary.get("no_windows_pairs", 0) or 0),
            "summary": blocking_summary,
            "info_summary": info_summary,
            "raw_summary": universe_summary,
            "listing_summary": listing_summary,
        },
        "global_block_reason": global_block_reason,
        "alerts": alerts,
        "metrics": {
            "cycle_health": cycle_status.get("cycle_health") or runtime_health.get("cycle_health"),
            "cycle_latency_class": cycle_status.get("cycle_latency_class"),
            "cycle_elapsed_seconds": cycle_status.get("cycle_elapsed_seconds"),
            "cycle_reserve_seconds": cycle_status.get("cycle_reserve_seconds"),
            "cycle_reserve_pct": cycle_status.get("cycle_reserve_pct"),
            "overrun_streak": cycle_status.get("overrun_streak", 0),
            "rss_mb": runtime_health.get("rss_mb"),
            "rss_peak_mb": runtime_health.get("rss_peak_mb"),
            "rss_health": runtime_health.get("rss_health"),
            "collect_seconds": runtime_health.get("collect_seconds"),
            "collect_reserve_seconds": runtime_health.get("collect_reserve_seconds"),
            "collect_reserve_health": runtime_health.get("collect_reserve_health"),
            "watchdog_health": runtime_health.get("watchdog_health"),
            "signals_observations": runtime_health.get("signal_observations_total", 0),
            "signals_waiting_confirmation": runtime_health.get("signals_waiting_confirmation", 0),
            "signals_waiting_volume": runtime_health.get("signals_waiting_volume", 0),
            "quote_turnover": runtime_health.get("quote_turnover", {}),
            "stage3_volume_queue": runtime_health.get("stage3_volume_queue", {}),
            "signals_already_active": runtime_health.get("signals_already_active", 0),
            "signals_repeat_on_cooldown": runtime_health.get("signals_repeat_on_cooldown", 0),
            "stage1_near_maturity": runtime_health.get("stage1_near_maturity", {}),
            "price_freshness_guard": runtime_health.get("price_freshness_guard", {}),
            "stage1_history_recheck": runtime_health.get("stage1_history_recheck", {}),
            "stage2_history_recheck": runtime_health.get("stage2_history_recheck", {}),
            "transition_metrics": runtime_health.get("transition_metrics", {}),
            "degrade_reasons": runtime_health.get("degrade_reasons", {}),
            "stage_chain_continuity": runtime_health.get("stage_chain_continuity", {}),
            "window_freshness_by_kind": runtime_health.get("window_freshness_by_kind", {}),
            "quarantine_lifecycle": runtime_health.get("quarantine_lifecycle", {}),
            "new_signals": runtime_health.get("new_signals", []),
            "auto_heal_oi_gaps": runtime_health.get("auto_heal_oi_gaps", {}),
            "auto_heal_listing": runtime_health.get("auto_heal_listing", {}),
        },
        "details": runtime_health.get("details", {}),
    }
    _write_json_atomic(runtime_dir / "health.json", health)


UNIVERSE_HEALTH_SUMMARY_SQL = """
WITH req(metric, window_code) AS (
  VALUES
  ('PRICE','15м'),('PRICE','30м'),('PRICE','1ч'),('PRICE','4ч'),
  ('OI','15м'),('OI','30м'),('OI','1ч'),('OI','4ч')
), latest_exchange AS (
  SELECT exchange, max(ts_close) AS exchange_latest_ts
  FROM price_raw
  GROUP BY exchange
), latest_windows AS (
  SELECT DISTINCT ON (exchange, symbol, metric, window_code)
    exchange,
    symbol,
    metric,
    window_code,
    ts_close,
    source_cycle_ts,
    built_at
  FROM aggregate_windows
  WHERE (metric, window_code) IN (SELECT metric, window_code FROM req)
  ORDER BY
    exchange, symbol, metric, window_code,
    source_cycle_ts DESC NULLS LAST,
    built_at DESC NULLS LAST,
    ts_close DESC NULLS LAST
), joined AS (
  SELECT
    u.exchange,
    u.symbol,
    r.metric,
    r.window_code,
    lw.ts_close,
    le.exchange_latest_ts,
    EXTRACT(EPOCH FROM (le.exchange_latest_ts - lw.ts_close))/60.0 AS lag_min
  FROM active_symbol_universe u
  CROSS JOIN req r
  LEFT JOIN latest_windows lw
    ON lw.exchange=u.exchange AND lw.symbol=u.symbol AND lw.metric=r.metric AND lw.window_code=r.window_code
  LEFT JOIN latest_exchange le
    ON le.exchange=u.exchange
), agg AS (
  SELECT exchange, symbol,
         count(*) FILTER (WHERE ts_close IS NOT NULL) AS present_cnt,
         count(*) FILTER (WHERE ts_close IS NULL) AS missing_cnt,
         max(lag_min) FILTER (WHERE lag_min IS NOT NULL) AS max_lag_min
  FROM joined
  GROUP BY exchange, symbol
)
SELECT
  count(*) AS universe,
  count(*) FILTER (WHERE missing_cnt > 0) AS incomplete_pairs,
  count(*) FILTER (WHERE present_cnt = 0) AS no_windows_pairs,
  count(*) FILTER (WHERE max_lag_min > 30) AS stale30_pairs,
  count(*) FILTER (WHERE max_lag_min > 60) AS stale60_pairs,
  count(*) FILTER (WHERE max_lag_min > 180) AS stale180_pairs
FROM agg
"""


UNIVERSE_HEALTH_BY_EXCHANGE_SQL = """
WITH req(metric, window_code) AS (
  VALUES
  ('PRICE','15м'),('PRICE','30м'),('PRICE','1ч'),('PRICE','4ч'),
  ('OI','15м'),('OI','30м'),('OI','1ч'),('OI','4ч')
), latest_exchange AS (
  SELECT exchange, max(ts_close) AS exchange_latest_ts
  FROM price_raw
  GROUP BY exchange
), latest_windows AS (
  SELECT DISTINCT ON (exchange, symbol, metric, window_code)
    exchange,
    symbol,
    metric,
    window_code,
    ts_close,
    source_cycle_ts,
    built_at
  FROM aggregate_windows
  WHERE (metric, window_code) IN (SELECT metric, window_code FROM req)
  ORDER BY
    exchange, symbol, metric, window_code,
    source_cycle_ts DESC NULLS LAST,
    built_at DESC NULLS LAST,
    ts_close DESC NULLS LAST
), joined AS (
  SELECT
    u.exchange,
    u.symbol,
    r.metric,
    r.window_code,
    lw.ts_close,
    le.exchange_latest_ts,
    EXTRACT(EPOCH FROM (le.exchange_latest_ts - lw.ts_close))/60.0 AS lag_min
  FROM active_symbol_universe u
  CROSS JOIN req r
  LEFT JOIN latest_windows lw
    ON lw.exchange=u.exchange AND lw.symbol=u.symbol AND lw.metric=r.metric AND lw.window_code=r.window_code
  LEFT JOIN latest_exchange le
    ON le.exchange=u.exchange
), agg AS (
  SELECT exchange, symbol,
         count(*) FILTER (WHERE ts_close IS NOT NULL) AS present_cnt,
         count(*) FILTER (WHERE ts_close IS NULL) AS missing_cnt,
         max(lag_min) FILTER (WHERE lag_min IS NOT NULL) AS max_lag_min
  FROM joined
  GROUP BY exchange, symbol
)
SELECT
  exchange,
  count(*) AS universe_cnt,
  count(*) FILTER (WHERE missing_cnt > 0) AS incomplete_cnt,
  count(*) FILTER (WHERE present_cnt = 0) AS no_windows_cnt,
  count(*) FILTER (WHERE max_lag_min > 30) AS stale30_cnt,
  count(*) FILTER (WHERE max_lag_min > 60) AS stale60_cnt,
  count(*) FILTER (WHERE max_lag_min > 180) AS stale180_cnt
FROM agg
GROUP BY exchange
ORDER BY exchange
"""


UNIVERSE_HEALTH_DETAILS_SQL = """
WITH req(metric, window_code) AS (
  VALUES
  ('PRICE','15м'),('PRICE','30м'),('PRICE','1ч'),('PRICE','4ч'),
  ('OI','15м'),('OI','30м'),('OI','1ч'),('OI','4ч')
), latest_exchange AS (
  SELECT exchange, max(ts_close) AS exchange_latest_ts
  FROM price_raw
  GROUP BY exchange
), latest_windows AS (
  SELECT DISTINCT ON (exchange, symbol, metric, window_code)
    exchange,
    symbol,
    metric,
    window_code,
    ts_close,
    source_cycle_ts,
    built_at
  FROM aggregate_windows
  WHERE (metric, window_code) IN (SELECT metric, window_code FROM req)
  ORDER BY
    exchange, symbol, metric, window_code,
    source_cycle_ts DESC NULLS LAST,
    built_at DESC NULLS LAST,
    ts_close DESC NULLS LAST
), joined AS (
  SELECT
    u.exchange,
    u.symbol,
    r.metric,
    r.window_code,
    lw.ts_close,
    le.exchange_latest_ts,
    EXTRACT(EPOCH FROM (le.exchange_latest_ts - lw.ts_close))/60.0 AS lag_min
  FROM active_symbol_universe u
  CROSS JOIN req r
  LEFT JOIN latest_windows lw
    ON lw.exchange=u.exchange AND lw.symbol=u.symbol AND lw.metric=r.metric AND lw.window_code=r.window_code
  LEFT JOIN latest_exchange le
    ON le.exchange=u.exchange
), agg AS (
  SELECT exchange, symbol,
         count(*) FILTER (WHERE ts_close IS NOT NULL) AS present_cnt,
         count(*) FILTER (WHERE ts_close IS NULL) AS missing_cnt,
         round(max(lag_min)::numeric, 1) AS max_lag_min,
         string_agg(metric || ':' || window_code, ', ' ORDER BY metric, window_code)
             FILTER (WHERE ts_close IS NULL) AS missing_list,
         string_agg(
             metric || ':' || window_code || '=' || round(lag_min)::text || 'м',
             ', ' ORDER BY lag_min DESC, metric, window_code
         ) FILTER (WHERE lag_min > 30) AS stale_list
  FROM joined
  GROUP BY exchange, symbol
)
SELECT exchange, symbol, present_cnt, missing_cnt, max_lag_min, missing_list, stale_list
FROM agg
WHERE missing_cnt > 0 OR max_lag_min > 30
ORDER BY missing_cnt DESC, max_lag_min DESC NULLS LAST, exchange, symbol
LIMIT 50
"""


UNIVERSE_HEALTH_LATEST_ROWS_SQL = """
WITH req(metric, window_code) AS (
  VALUES
  ('PRICE','15м'),('PRICE','30м'),('PRICE','1ч'),('PRICE','4ч'),
  ('OI','15м'),('OI','30м'),('OI','1ч'),('OI','4ч')
), latest_windows AS (
  SELECT DISTINCT ON (exchange, symbol, metric, window_code)
    exchange,
    symbol,
    metric,
    window_code,
    ts_close,
    source_cycle_ts,
    built_at
  FROM aggregate_windows
  WHERE (metric, window_code) IN (SELECT metric, window_code FROM req)
  ORDER BY
    exchange, symbol, metric, window_code,
    source_cycle_ts DESC NULLS LAST,
    built_at DESC NULLS LAST,
    ts_close DESC NULLS LAST
), joined AS (
  SELECT
    u.exchange,
    u.symbol,
    r.metric,
    r.window_code,
    lw.ts_close,
    EXTRACT(EPOCH FROM (%s::timestamptz - lw.ts_close))/60.0 AS lag_min
  FROM active_symbol_universe u
  CROSS JOIN req r
  LEFT JOIN latest_windows lw
    ON lw.exchange=u.exchange AND lw.symbol=u.symbol AND lw.metric=r.metric AND lw.window_code=r.window_code
), agg AS (
  SELECT exchange, symbol,
         count(*) FILTER (WHERE ts_close IS NOT NULL) AS present_cnt,
         count(*) FILTER (WHERE ts_close IS NULL) AS missing_cnt,
         round(max(lag_min)::numeric, 1) AS max_lag_min,
         string_agg(metric || ':' || window_code, ', ' ORDER BY metric, window_code)
             FILTER (WHERE ts_close IS NULL) AS missing_list,
         string_agg(
             metric || ':' || window_code || '=' || round(lag_min)::text || 'м',
             ', ' ORDER BY lag_min DESC, metric, window_code
         ) FILTER (WHERE lag_min > 30) AS stale_list
  FROM joined
  GROUP BY exchange, symbol
)
SELECT exchange, symbol, present_cnt, missing_cnt, max_lag_min, missing_list, stale_list
FROM agg
ORDER BY exchange, symbol
"""


UNIVERSE_HEALTH_FALLBACK_ANCHOR_SQL = """
SELECT max(source_cycle_ts) AS source_cycle_ts
FROM aggregate_windows
WHERE source_cycle_ts IS NOT NULL
"""


def _fetch_universe_problem_context(problem_pairs: list[dict]) -> dict[tuple[str, str], dict]:
    if not problem_pairs:
        return {}

    placeholders = ", ".join(["(%s, %s)"] * len(problem_pairs))
    params: list[str] = []
    for row in problem_pairs:
        params.extend([row["exchange"], row["symbol"]])

    sql = f"""
WITH target(exchange, symbol) AS (
  VALUES {placeholders}
), latest_exchange AS (
  SELECT exchange, max(ts_close) AS exchange_latest_ts
  FROM price_raw
  GROUP BY exchange
), uni AS (
  SELECT t.exchange, t.symbol, u.activated_at, u.source
  FROM target t
  LEFT JOIN active_symbol_universe u
    ON u.exchange=t.exchange AND u.symbol=t.symbol
), oi AS (
  SELECT t.exchange, t.symbol,
         count(*) AS oi_raw_cnt,
         min(r.ts_close) AS oi_first_ts,
         max(r.ts_close) AS oi_latest_ts
  FROM target t
  LEFT JOIN oi_raw r
    ON r.exchange=t.exchange AND r.symbol=t.symbol
  GROUP BY t.exchange, t.symbol
), price AS (
  SELECT t.exchange, t.symbol,
         count(*) AS price_raw_cnt,
         min(r.ts_close) AS price_first_ts,
         max(r.ts_close) AS price_latest_ts
  FROM target t
  LEFT JOIN price_raw r
    ON r.exchange=t.exchange AND r.symbol=t.symbol
  GROUP BY t.exchange, t.symbol
), volume AS (
  SELECT t.exchange, t.symbol,
         count(*) AS volume_raw_cnt,
         max(r.ts_close) AS volume_latest_ts
  FROM target t
  LEFT JOIN volume_raw r
    ON r.exchange=t.exchange AND r.symbol=t.symbol
  GROUP BY t.exchange, t.symbol
), agg4h AS (
  SELECT t.exchange, t.symbol,
         max(aw.ts_close) FILTER (WHERE aw.metric='OI' AND aw.window_code='4ч') AS oi_4h_latest_ts,
         max(aw.ts_close) FILTER (WHERE aw.metric='PRICE' AND aw.window_code='4ч') AS price_4h_latest_ts,
         max(aw.ts_close) FILTER (WHERE aw.metric='VOLUME' AND aw.window_code='4ч') AS volume_4h_latest_ts
  FROM target t
  LEFT JOIN aggregate_windows aw
    ON aw.exchange=t.exchange AND aw.symbol=t.symbol
  GROUP BY t.exchange, t.symbol
)
SELECT
  t.exchange,
  t.symbol,
  u.activated_at,
  u.source,
  le.exchange_latest_ts,
  o.oi_raw_cnt,
  o.oi_first_ts,
  o.oi_latest_ts,
  p.price_raw_cnt,
  p.price_first_ts,
  p.price_latest_ts,
  v.volume_raw_cnt,
  v.volume_latest_ts,
  a.oi_4h_latest_ts,
  a.price_4h_latest_ts,
  a.volume_4h_latest_ts
FROM target t
LEFT JOIN uni u
  ON u.exchange=t.exchange AND u.symbol=t.symbol
LEFT JOIN latest_exchange le
  ON le.exchange=t.exchange
LEFT JOIN oi o
  ON o.exchange=t.exchange AND o.symbol=t.symbol
LEFT JOIN price p
  ON p.exchange=t.exchange AND p.symbol=t.symbol
LEFT JOIN volume v
  ON v.exchange=t.exchange AND v.symbol=t.symbol
LEFT JOIN agg4h a
  ON a.exchange=t.exchange AND a.symbol=t.symbol
ORDER BY t.exchange, t.symbol
"""

    rows = fetch(sql, tuple(params))
    return {(row["exchange"], row["symbol"]): row for row in rows}


def _classify_universe_problem(problem_row: dict, context: dict | None) -> dict:
    if not context:
        return {
            "reason_code": "неизвестная_причина",
            "reason_level": "warning",
            "reason_hint": "нет_контекста_по_паре",
        }

    exchange_latest_ts = context.get("exchange_latest_ts")
    activated_at = context.get("activated_at")
    oi_raw_cnt = int(context.get("oi_raw_cnt", 0) or 0)
    price_raw_cnt = int(context.get("price_raw_cnt", 0) or 0)
    missing_list = problem_row.get("missing_list") or ""
    stale_list = problem_row.get("stale_list") or ""

    activated_age_min = None
    if activated_at and exchange_latest_ts:
        activated_age_min = max(
            0.0,
            (exchange_latest_ts - activated_at).total_seconds() / 60.0,
        )

    oi_4h_latest_ts = context.get("oi_4h_latest_ts")
    price_4h_latest_ts = context.get("price_4h_latest_ts")
    oi_latest_ts = context.get("oi_latest_ts")
    price_latest_ts = context.get("price_latest_ts")

    price_4h_gap_min = None
    if exchange_latest_ts and price_4h_latest_ts:
        price_4h_gap_min = max(
            0.0,
            (exchange_latest_ts - price_4h_latest_ts).total_seconds() / 60.0,
        )

    oi_raw_gap_min = None
    if exchange_latest_ts and oi_latest_ts:
        oi_raw_gap_min = max(
            0.0,
            (exchange_latest_ts - oi_latest_ts).total_seconds() / 60.0,
        )

    raw_points_floor = min(oi_raw_cnt, price_raw_cnt) if oi_raw_cnt and price_raw_cnt else 0

    missing_items = {item.strip() for item in missing_list.split(",") if item.strip()}
    warmup_missing_items = {
        "OI:30м", "OI:1ч", "OI:4ч",
        "PRICE:30м", "PRICE:1ч", "PRICE:4ч",
    }
    if (
        missing_items
        and missing_items.issubset(warmup_missing_items)
        and float(problem_row.get("max_lag_min", 0) or 0) <= 10.0
        and activated_age_min is not None
        and activated_age_min < 300.0
        and raw_points_floor < 48
        and raw_points_floor >= 3
    ):
        return {
            "reason_code": "новый_листинг_прогрев_4ч",
            "reason_level": "info",
            "reason_hint": "новая_монета_еще_не_накопила_старший_фон",
        }

    if (
        missing_list == "OI:4ч"
        and price_4h_gap_min is not None
        and price_4h_gap_min <= 5.0
        and oi_raw_gap_min is not None
        and oi_raw_gap_min <= 5.0
    ):
        return {
            "reason_code": "дырка_внутри_4ч_oi",
            "reason_level": "info",
            "reason_hint": "сырой_oi_свежий_это_ремонтопригодная_дырка_4ч_oi",
        }

    if (
        stale_list.startswith("OI:4ч=")
        and not missing_list
        and price_4h_gap_min is not None
        and price_4h_gap_min <= 5.0
        and oi_raw_gap_min is not None
        and oi_raw_gap_min <= 5.0
    ):
        return {
            "reason_code": "дырка_внутри_4ч_oi",
            "reason_level": "info",
            "reason_hint": "сырой_oi_свежий_это_ремонтопригодная_дырка_4ч_oi",
        }

    if (
        stale_list.startswith("OI:4ч=")
        and not missing_list
        and price_4h_gap_min is not None
        and price_4h_gap_min <= 5.0
        and oi_raw_gap_min is not None
        and oi_raw_gap_min > 5.0
    ):
        return {
            "reason_code": "разрыв_сырого_oi",
            "reason_level": "warning",
            "reason_hint": "price_volume_свежие_а_oi_сырье_идет_с_дырой",
        }

    stale_items = {
        item.split("=", 1)[0].strip()
        for item in stale_list.split(",")
        if item.strip()
    }
    if (
        stale_items
        and stale_items.issubset({"OI:4ч", "PRICE:4ч"})
        and not missing_list
    ):
        return {
            "reason_code": "старший_фон_4ч_восстановление",
            "reason_level": "info",
            "reason_hint": "короткий_контур_живой_старший_фон_догонит_после_прогрева",
        }

    if problem_row.get("missing_cnt", 0):
        return {
            "reason_code": "неполные_окна",
            "reason_level": "warning",
            "reason_hint": "нужно_разобрать_покрытие_окон",
        }

    return {
        "reason_code": "протухшие_окна",
        "reason_level": "warning",
        "reason_hint": "окна_отстают_от_текущего_цикла",
    }


def _runtime_universe_state() -> dict:
    with _UNIVERSE_LOCK:
        return {
            "bybit_symbols": list(_RUNTIME_UNIVERSE["bybit_symbols"]),
            "binance_symbols": list(_RUNTIME_UNIVERSE["binance_symbols"]),
            "bybit_total": int(_RUNTIME_UNIVERSE["bybit_total"]),
            "binance_total": int(_RUNTIME_UNIVERSE["binance_total"]),
            "active_total": int(_RUNTIME_UNIVERSE["active_total"]),
            "quarantine_total": int(_RUNTIME_UNIVERSE["quarantine_total"]),
            "data_quality_quarantine_total": int(_RUNTIME_UNIVERSE["data_quality_quarantine_total"]),
            "last_refresh_msk": _RUNTIME_UNIVERSE["last_refresh_msk"],
            "last_refresh_cycle": int(_RUNTIME_UNIVERSE["last_refresh_cycle"]),
            "last_refresh_reason": _RUNTIME_UNIVERSE["last_refresh_reason"],
            "added_total": int(_RUNTIME_UNIVERSE["added_total"]),
            "removed_total": int(_RUNTIME_UNIVERSE["removed_total"]),
            "added_samples": list(_RUNTIME_UNIVERSE["added_samples"]),
            "removed_samples": list(_RUNTIME_UNIVERSE["removed_samples"]),
            "listing_problem_pairs": list(_RUNTIME_UNIVERSE["listing_problem_pairs"]),
            "discovery_removed_total": int(_RUNTIME_UNIVERSE["discovery_removed_total"]),
            "limit_removed_total": int(_RUNTIME_UNIVERSE["limit_removed_total"]),
            "pruned_state_counts": dict(_RUNTIME_UNIVERSE["pruned_state_counts"]),
            "pruned_state_total": int(_RUNTIME_UNIVERSE["pruned_state_total"]),
            "listing_health": _RUNTIME_UNIVERSE["listing_health"],
            "listing_alerts": list(_RUNTIME_UNIVERSE["listing_alerts"]),
        }


def _fetch_previous_active_universe() -> dict[str, set[str]]:
    rows = fetch("SELECT exchange, symbol FROM active_symbol_universe ORDER BY exchange, symbol")
    previous = {"BYBIT": set(), "BINANCE": set()}
    for row in rows:
        exchange = (row.get("exchange") or "").upper()
        symbol = row.get("symbol")
        if exchange in previous and symbol:
            previous[exchange].add(symbol)
    return previous


def _classify_removed_listing_pairs(removed_pairs: list[str], snapshot: dict) -> dict:
    if not removed_pairs:
        return {
            "listing_problem_pairs": [],
            "discovery_removed_total": 0,
            "limit_removed_total": 0,
        }

    bybit_all = set(snapshot.get("bybit_symbols_all", []))
    binance_all = set(snapshot.get("binance_symbols_all", []))
    bybit_limited = set(snapshot.get("bybit_symbols", []))
    binance_limited = set(snapshot.get("binance_symbols", []))

    listing_problem_pairs = []
    discovery_removed_total = 0
    limit_removed_total = 0

    for pair in removed_pairs:
        try:
            exchange, symbol = pair.split(":", 1)
        except ValueError:
            continue

        all_symbols = bybit_all if exchange == "BYBIT" else binance_all
        limited_symbols = bybit_limited if exchange == "BYBIT" else binance_limited

        if symbol in all_symbols and symbol not in limited_symbols:
            limit_removed_total += 1
            listing_problem_pairs.append(
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "reason": "выпала_за_лимит_символов",
                    "blocking": False,
                }
            )
            continue

        if symbol not in all_symbols:
            discovery_removed_total += 1
            listing_problem_pairs.append(
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "reason": "исчезла_из_discovery_после_live",
                    "blocking": True,
                }
            )

    return {
        "listing_problem_pairs": listing_problem_pairs,
        "discovery_removed_total": discovery_removed_total,
        "limit_removed_total": limit_removed_total,
    }


def _collect_listing_health(
    snapshot: dict,
    added: list[str],
    removed: list[str],
    listing_problem_pairs: list[dict],
    discovery_removed_total: int,
    limit_removed_total: int,
    pruned_state_counts: dict[str, int],
    reason: str,
) -> dict:
    alerts = []

    bybit_total = int(snapshot.get("bybit_total", 0) or 0)
    binance_total = int(snapshot.get("binance_total", 0) or 0)
    min_bybit = int(os.getenv("MIN_DISCOVERY_BYBIT_SYMBOLS", "300"))
    min_binance = int(os.getenv("MIN_DISCOVERY_BINANCE_SYMBOLS", "250"))
    max_removed_per_refresh = int(os.getenv("MAX_DISCOVERY_REMOVED_PER_REFRESH", "50"))

    if bybit_total < min_bybit:
        alerts.append("сбой_листинга_bybit")
    if binance_total < min_binance:
        alerts.append("сбой_листинга_binance")
    if len(removed) > max_removed_per_refresh:
        alerts.append("аномальный_отток_листинга")
    if any(item.get("blocking") for item in listing_problem_pairs):
        alerts.append("исчезли_живые_пары_из_discovery")

    pruned_total = int(sum(int(v or 0) for v in pruned_state_counts.values()))

    health = "ok" if not alerts else "degraded"
    return {
        "health": health,
        "alerts": alerts,
        "bybit_total": bybit_total,
        "binance_total": binance_total,
        "added_total": len(added),
        "removed_total": len(removed),
        "discovery_removed_total": int(discovery_removed_total),
        "limit_removed_total": int(limit_removed_total),
        "pruned_state_total": pruned_total,
        "pruned_state_counts": {k: int(v or 0) for k, v in pruned_state_counts.items()},
        "added_samples": added[:10],
        "removed_samples": removed[:10],
        "listing_problem_pairs": listing_problem_pairs[:20],
    }


def _build_runtime_universe_snapshot() -> dict:
    bybit_symbols_all = fetch_bybit_symbols()
    binance_symbols_all = fetch_binance_symbols()
    coverage_quarantine_symbols = load_quarantine_symbols(95.0)
    data_quality_quarantine_symbols = load_data_quality_quarantine_symbols()
    quarantine_symbols = coverage_quarantine_symbols | data_quality_quarantine_symbols

    bybit_symbols = list(bybit_symbols_all)
    if ЛИМИТ_СИМВОЛОВ_BYBIT > 0:
        bybit_symbols = bybit_symbols[:ЛИМИТ_СИМВОЛОВ_BYBIT]

    binance_symbols = list(binance_symbols_all)
    if ЛИМИТ_СИМВОЛОВ_BINANCE > 0:
        binance_symbols = binance_symbols[:ЛИМИТ_СИМВОЛОВ_BINANCE]

    active_universe = (
        [("BYBIT", symbol, "runtime_limit_usdt_filtered") for symbol in bybit_symbols]
        + [("BINANCE", symbol, "runtime_limit_usdt_filtered") for symbol in binance_symbols]
    )

    return {
        "bybit_symbols_all": bybit_symbols_all,
        "binance_symbols_all": binance_symbols_all,
        "bybit_symbols": bybit_symbols,
        "binance_symbols": binance_symbols,
        "bybit_total": len(bybit_symbols_all),
        "binance_total": len(binance_symbols_all),
        "active_total": len(active_universe),
        "quarantine_total": len(quarantine_symbols),
        "coverage_quarantine_total": len(coverage_quarantine_symbols),
        "data_quality_quarantine_total": len(data_quality_quarantine_symbols),
        "active_universe": active_universe,
    }


def _refresh_runtime_universe(cycle_no: int, reason: str) -> dict:
    snapshot = _build_runtime_universe_snapshot()
    previous_active = _fetch_previous_active_universe()

    replace_active_universe(snapshot["active_universe"])
    pruned_state_counts = prune_inactive_state_rows()

    next_bybit = set(snapshot["bybit_symbols"])
    next_binance = set(snapshot["binance_symbols"])

    with _UNIVERSE_LOCK:
        prev_bybit = set(previous_active["BYBIT"] or [])
        prev_binance = set(previous_active["BINANCE"] or [])

        added = (
            [f"BYBIT:{symbol}" for symbol in sorted(next_bybit - prev_bybit)]
            + [f"BINANCE:{symbol}" for symbol in sorted(next_binance - prev_binance)]
        )
        removed = (
            [f"BYBIT:{symbol}" for symbol in sorted(prev_bybit - next_bybit)]
            + [f"BINANCE:{symbol}" for symbol in sorted(prev_binance - next_binance)]
        )
        removed_listing_meta = _classify_removed_listing_pairs(removed, snapshot)
        listing_health = _collect_listing_health(
            snapshot,
            added,
            removed,
            removed_listing_meta["listing_problem_pairs"],
            removed_listing_meta["discovery_removed_total"],
            removed_listing_meta["limit_removed_total"],
            pruned_state_counts,
            reason,
        )

        _RUNTIME_UNIVERSE.update({
            "bybit_symbols": list(snapshot["bybit_symbols"]),
            "binance_symbols": list(snapshot["binance_symbols"]),
            "bybit_total": snapshot["bybit_total"],
            "binance_total": snapshot["binance_total"],
            "active_total": snapshot["active_total"],
            "quarantine_total": snapshot["quarantine_total"],
            "data_quality_quarantine_total": snapshot["data_quality_quarantine_total"],
            "last_refresh_msk": текст_мск(),
            "last_refresh_cycle": cycle_no,
            "last_refresh_reason": reason,
            "added_total": len(added),
            "removed_total": len(removed),
            "added_samples": added[:10],
            "removed_samples": removed[:10],
            "listing_problem_pairs": list(removed_listing_meta["listing_problem_pairs"]),
            "discovery_removed_total": int(removed_listing_meta["discovery_removed_total"]),
            "limit_removed_total": int(removed_listing_meta["limit_removed_total"]),
            "pruned_state_counts": dict(pruned_state_counts),
            "pruned_state_total": int(listing_health["pruned_state_total"]),
            "listing_health": listing_health["health"],
            "listing_alerts": list(listing_health["alerts"]),
        })

    state = _runtime_universe_state()
    log(
        "runtime universe refresh: "
        f"reason={reason} cycle={cycle_no} "
        f"bybit_all={state['bybit_total']} binance_all={state['binance_total']} "
        f"active_total={state['active_total']} "
            f"added={state['added_total']} removed={state['removed_total']} "
            f"pruned={state['pruned_state_total']} "
            f"quarantine_seen={state['quarantine_total']} "
            f"data_quality_quarantine={state['data_quality_quarantine_total']} "
            f"listing_health={state['listing_health']}"
        )
    if state["added_samples"]:
        log("runtime universe added: " + ", ".join(state["added_samples"]))
    if state["removed_samples"]:
        log("runtime universe removed: " + ", ".join(state["removed_samples"]))
    if state["listing_problem_pairs"]:
        log("runtime listing problem pairs: " + json.dumps(state["listing_problem_pairs"], ensure_ascii=False))
    if state["pruned_state_total"] > 0:
        log("runtime universe pruned inactive state: " + json.dumps(state["pruned_state_counts"], ensure_ascii=False))
    if state["listing_alerts"]:
        log("runtime listing alerts: " + ", ".join(state["listing_alerts"]))
    return state


def _collect_universe_health(source_cycle_ts: datetime | None = None) -> dict:
    try:
        if source_cycle_ts is None:
            anchor_rows = fetch(UNIVERSE_HEALTH_FALLBACK_ANCHOR_SQL)
            source_cycle_ts = (anchor_rows[0] or {}).get("source_cycle_ts") if anchor_rows else None
        if source_cycle_ts is None:
            raise RuntimeError("empty aggregate_windows.source_cycle_ts")

        health_rows = fetch(UNIVERSE_HEALTH_LATEST_ROWS_SQL, (source_cycle_ts,))
        detail_rows = [
            row for row in health_rows
            if int(row.get("missing_cnt", 0) or 0) > 0
            or float(row.get("max_lag_min", 0) or 0) > 30
        ][:50]

        universe = len(health_rows)
        incomplete_pairs = sum(1 for row in health_rows if int(row.get("missing_cnt", 0) or 0) > 0)
        no_windows_pairs = sum(1 for row in health_rows if int(row.get("present_cnt", 0) or 0) == 0)
        stale30_pairs = sum(1 for row in health_rows if float(row.get("max_lag_min", 0) or 0) > 30)
        stale60_pairs = sum(1 for row in health_rows if float(row.get("max_lag_min", 0) or 0) > 60)
        stale180_pairs = sum(1 for row in health_rows if float(row.get("max_lag_min", 0) or 0) > 180)

        summary = {
            "universe": universe,
            "incomplete_pairs": incomplete_pairs,
            "no_windows_pairs": no_windows_pairs,
            "stale30_pairs": stale30_pairs,
            "stale60_pairs": stale60_pairs,
            "stale180_pairs": stale180_pairs,
        }

        exchange_groups: dict[str, list[dict]] = {}
        for row in health_rows:
            exchange_groups.setdefault(row["exchange"], []).append(row)
        by_exchange = []
        for exchange, rows in sorted(exchange_groups.items()):
            by_exchange.append({
                "exchange": exchange,
                "universe_cnt": len(rows),
                "incomplete_cnt": sum(1 for row in rows if int(row.get("missing_cnt", 0) or 0) > 0),
                "no_windows_cnt": sum(1 for row in rows if int(row.get("present_cnt", 0) or 0) == 0),
                "stale30_cnt": sum(1 for row in rows if float(row.get("max_lag_min", 0) or 0) > 30),
                "stale60_cnt": sum(1 for row in rows if float(row.get("max_lag_min", 0) or 0) > 60),
                "stale180_cnt": sum(1 for row in rows if float(row.get("max_lag_min", 0) or 0) > 180),
            })

        problem_pairs = []
        problem_context = _fetch_universe_problem_context(detail_rows)
        for row in detail_rows:
            base_row = {
                "exchange": row["exchange"],
                "symbol": row["symbol"],
                "present_cnt": int(row.get("present_cnt", 0) or 0),
                "missing_cnt": int(row.get("missing_cnt", 0) or 0),
                "max_lag_min": float(row.get("max_lag_min", 0) or 0),
                "missing_list": row.get("missing_list"),
                "stale_list": row.get("stale_list"),
            }
            reason = _classify_universe_problem(
                base_row,
                problem_context.get((row["exchange"], row["symbol"])),
            )
            base_row.update(reason)
            base_row["blocking"] = reason["reason_level"] != "info"
            problem_pairs.append(base_row)

        alerts = []
        if any(row["missing_cnt"] > 0 and row["blocking"] for row in problem_pairs):
            alerts.append("неполные_окна")
        if summary["no_windows_pairs"] > 0:
            alerts.append("пустые_окна_символа")
        if any(row["max_lag_min"] > 30 and row["blocking"] for row in problem_pairs):
            alerts.append("протухшие_окна_30м")
        if any(row["max_lag_min"] > 60 and row["blocking"] for row in problem_pairs):
            alerts.append("протухшие_окна_60м")
        if any(row["max_lag_min"] > 180 and row["blocking"] for row in problem_pairs):
            alerts.append("протухшие_окна_180м")

        health = "ok" if not alerts else "degraded"
        return {
            "health": health,
            "alerts": alerts,
            "summary": summary,
            "by_exchange": by_exchange,
            "problem_pairs": problem_pairs,
        }
    except Exception as exc:
        return {
            "health": "error",
            "alerts": ["ошибка_контроля_вселенной"],
            "summary": {},
            "by_exchange": [],
            "problem_pairs": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def _build_data_quality_alerts(universe_health: dict) -> list[str]:
    summary = universe_health.get("summary", {}) or {}
    blocking_problem_pairs = [
        row for row in universe_health.get("problem_pairs", []) or []
        if row.get("blocking", True)
    ]
    alerts: list[str] = []
    incomplete = sum(1 for row in blocking_problem_pairs if int(row.get("missing_cnt", 0) or 0) > 0)
    stale30 = sum(1 for row in blocking_problem_pairs if float(row.get("max_lag_min", 0) or 0) > 30)
    stale60 = sum(1 for row in blocking_problem_pairs if float(row.get("max_lag_min", 0) or 0) > 60)
    stale180 = sum(1 for row in blocking_problem_pairs if float(row.get("max_lag_min", 0) or 0) > 180)
    absent = sum(1 for row in blocking_problem_pairs if int(row.get("present_cnt", 0) or 0) == 0)
    if incomplete > 0:
        alerts.append(f"incomplete_windows={incomplete}")
    if stale30 > 0:
        alerts.append(f"stale30_windows={stale30}")
    if stale60 > 0:
        alerts.append(f"stale60_windows={stale60}")
    if stale180 > 0:
        alerts.append(f"stale180_windows={stale180}")
    if absent > 0:
        alerts.append(f"absent_symbols={absent}")
    error_text = universe_health.get("error")
    if error_text:
        alerts.append(f"universe_error={error_text}")
    return alerts


def _sync_data_quality_quarantine_from_health(universe_health: dict) -> dict[str, int]:
    rows = []
    for row in universe_health.get("problem_pairs", []) or []:
        if not row.get("blocking", True):
            continue
        rows.append((
            row.get("exchange"),
            row.get("symbol"),
            row.get("reason_code") or "проблема_окон",
            row.get("reason_hint") or "",
            row.get("missing_list") or "",
            row.get("stale_list") or "",
        ))
    result = sync_data_quality_quarantine(rows)
    current_quarantine_total = len(load_data_quality_quarantine_symbols())
    with _UNIVERSE_LOCK:
        _RUNTIME_UNIVERSE["data_quality_quarantine_total"] = current_quarantine_total
    if result.get("active") or result.get("restored"):
        log(
            "data quality quarantine sync: "
            f"active={result.get('active', 0)} restored={result.get('restored', 0)}"
        )
    return result


def _fetch_exchange_oi_rows(exchange: str, symbol: str, limit: int = 96) -> list[tuple]:
    exchange_upper = (exchange or "").upper()
    if exchange_upper == "BYBIT":
        return fetch_bybit_oi_5m(symbol, limit=limit)
    if exchange_upper == "BINANCE":
        return fetch_binance_oi_5m(symbol, limit=limit)
    return []


def _select_repairable_oi_gap_rows(
    universe_health: dict,
    problem_context: dict[tuple[str, str], dict],
) -> list[dict]:
    candidates: list[dict] = []
    for row in universe_health.get("problem_pairs", []):
        # The live cycle may only repair a pair that blocks data quality.
        # Informational 4h gaps wait for the next normal collection window.
        if not row.get("blocking"):
            continue
        stale_list = row.get("stale_list") or ""
        missing_list = row.get("missing_list") or ""
        reason_code = row.get("reason_code") or ""
        repairable_missing_oi4h = missing_list == "OI:4ч"
        repairable_stale_oi4h = stale_list.startswith("OI:4ч=") and not missing_list
        if not (repairable_missing_oi4h or repairable_stale_oi4h):
            continue
        if reason_code not in {"протухшие_окна", "разрыв_сырого_oi", "дырка_внутри_4ч_oi"}:
            continue

        key = (row["exchange"], row["symbol"])
        context = problem_context.get(key) or {}
        exchange_latest_ts = context.get("exchange_latest_ts")
        price_4h_latest_ts = context.get("price_4h_latest_ts")
        oi_latest_ts = context.get("oi_latest_ts")
        if not exchange_latest_ts or not price_4h_latest_ts:
            continue

        price_4h_gap_min = max(
            0.0,
            (exchange_latest_ts - price_4h_latest_ts).total_seconds() / 60.0,
        )
        if price_4h_gap_min > 5.0:
            continue

        oi_raw_gap_min = None
        if oi_latest_ts:
            oi_raw_gap_min = max(
                0.0,
                (exchange_latest_ts - oi_latest_ts).total_seconds() / 60.0,
            )

        context_copy = dict(context)
        context_copy["oi_raw_gap_min"] = oi_raw_gap_min
        enriched_row = dict(row)
        enriched_row["context"] = context_copy
        candidates.append(enriched_row)
    return candidates


def _repair_oi_gap_windows(source_cycle_ts: datetime) -> dict:
    before = _collect_universe_health()
    if before.get("health") == "error":
        result = {
            "attempted_pairs": 0,
            "repaired_pairs": 0,
            "inserted_rows": 0,
            "rebuild_runs": 0,
            "before_alerts": before.get("alerts", []),
            "after_alerts": before.get("alerts", []),
            "pair_notes": [],
        }
        _LAST_OI_GAP_REPAIR.update(result)
        return result

    before_problem_rows = before.get("problem_pairs", [])
    problem_context = _fetch_universe_problem_context(before_problem_rows)
    candidates = _select_repairable_oi_gap_rows(before, problem_context)
    if not candidates:
        result = {
            "attempted_pairs": 0,
            "repaired_pairs": 0,
            "inserted_rows": 0,
            "rebuild_runs": 0,
            "before_alerts": before.get("alerts", []),
            "after_alerts": before.get("alerts", []),
            "pair_notes": [],
        }
        _LAST_OI_GAP_REPAIR.update(result)
        return result

    attempted_pairs = 0
    inserted_rows = 0
    pair_notes: list[dict] = []
    for row in candidates:
        exchange = row["exchange"]
        symbol = row["symbol"]
        attempted_pairs += 1
        try:
            rows = _fetch_exchange_oi_rows(exchange, symbol, limit=96)
            if rows:
                upsert_oi(rows, cycle_ts=source_cycle_ts, source="repair_gap")
                inserted_rows += len(rows)
                pair_notes.append({
                    "exchange": exchange,
                    "symbol": symbol,
                    "status": "rows_upserted",
                    "rows": len(rows),
                    "oi_raw_gap_min": row["context"].get("oi_raw_gap_min"),
                })
            else:
                pair_notes.append({
                    "exchange": exchange,
                    "symbol": symbol,
                    "status": "empty_refetch",
                    "rows": 0,
                    "oi_raw_gap_min": row["context"].get("oi_raw_gap_min"),
                })
        except Exception as exc:
            pair_notes.append({
                "exchange": exchange,
                "symbol": symbol,
                "status": "refetch_error",
                "error": f"{type(exc).__name__}: {exc}",
                "oi_raw_gap_min": row["context"].get("oi_raw_gap_min"),
            })

    rebuild_runs = 0
    if inserted_rows > 0:
        rebuild_latest_aggregate_windows(source_cycle_ts)
        rebuild_runs = 1

    after = _collect_universe_health()
    after_problem_rows = after.get("problem_pairs", [])
    after_context = _fetch_universe_problem_context(after_problem_rows)
    after_candidates = _select_repairable_oi_gap_rows(after, after_context)
    after_keys = {(row["exchange"], row["symbol"]) for row in after_candidates}
    repaired_pairs = sum(
        1 for row in candidates if (row["exchange"], row["symbol"]) not in after_keys
    )

    if candidates:
        log(
            "oi gap repair: "
            f"attempted_pairs={attempted_pairs} repaired_pairs={repaired_pairs} "
            f"inserted_rows={inserted_rows} rebuild_runs={rebuild_runs} "
            f"before_alerts={before.get('alerts', [])} after_alerts={after.get('alerts', [])}"
        )
        log("oi gap repair details: " + json.dumps(pair_notes, ensure_ascii=False))

    result = {
        "attempted_pairs": attempted_pairs,
        "repaired_pairs": repaired_pairs,
        "inserted_rows": inserted_rows,
        "rebuild_runs": rebuild_runs,
        "before_alerts": before.get("alerts", []),
        "after_alerts": after.get("alerts", []),
        "pair_notes": pair_notes,
    }
    _LAST_OI_GAP_REPAIR.update(result)
    return result


def _self_heal_listing_before_alert(cycle_no: int) -> dict:
    before_state = _runtime_universe_state()
    before_alerts = list(before_state.get("listing_alerts", []))
    before_pairs = list(before_state.get("listing_problem_pairs", []))
    blocking_before = any(item.get("blocking") for item in before_pairs)
    if not before_alerts and not blocking_before:
        result = {
            "attempted": False,
            "before_alerts": before_alerts,
            "after_alerts": before_alerts,
            "before_problem_pairs": before_pairs,
            "after_problem_pairs": before_pairs,
        }
        _LAST_LISTING_SELF_HEAL.update(result)
        return result

    _refresh_runtime_universe(cycle_no, "heal_before_alert")
    after_state = _runtime_universe_state()
    result = {
        "attempted": True,
        "before_alerts": before_alerts,
        "after_alerts": list(after_state.get("listing_alerts", [])),
        "before_problem_pairs": before_pairs,
        "after_problem_pairs": list(after_state.get("listing_problem_pairs", [])),
    }
    _LAST_LISTING_SELF_HEAL.update(result)
    if before_alerts != result["after_alerts"] or before_pairs != result["after_problem_pairs"]:
        log(
            "listing self-heal: "
            f"cycle={cycle_no} before_alerts={before_alerts} "
            f"after_alerts={result['after_alerts']}"
        )
    return result


def _aligned_cycle_sleep_seconds(elapsed: float) -> float:
    cadence_seconds = max(60, ИНТЕРВАЛ_ЦИКЛА_СЕК)
    offset_seconds = max(0.0, float(os.getenv("CYCLE_ALIGN_OFFSET_SECONDS", "5")))
    now_epoch = time.time()
    next_boundary_epoch = ((int(now_epoch) // cadence_seconds) + 1) * cadence_seconds + offset_seconds
    sleep_seconds = max(0.0, next_boundary_epoch - now_epoch)
    # Не даем планировщику перескочить через целую свечу из-за offset.
    if sleep_seconds > cadence_seconds + offset_seconds:
        sleep_seconds = max(0.0, cadence_seconds - elapsed)
    return sleep_seconds


def _cycle_step_fits_budget(
    *,
    elapsed_seconds: float,
    expected_seconds: float,
    reserve_seconds: float,
) -> bool:
    return elapsed_seconds + expected_seconds + reserve_seconds <= ИНТЕРВАЛ_ЦИКЛА_СЕК


def _cleanup_old_fits_budget(elapsed_seconds: float) -> bool:
    expected_cleanup_seconds = float(os.getenv("CLEANUP_OLD_EXPECTED_SECONDS", "20"))
    cleanup_reserve_seconds = float(os.getenv("CLEANUP_OLD_RESERVE_SECONDS", "15"))
    return _cycle_step_fits_budget(
        elapsed_seconds=elapsed_seconds,
        expected_seconds=expected_cleanup_seconds,
        reserve_seconds=cleanup_reserve_seconds,
    )


def _validate_runtime_contract() -> None:
    violations: list[str] = []

    if os.getenv("SKIP_HEAVY_AGGREGATES") == "1":
        violations.append("SKIP_HEAVY_AGGREGATES=1")

    if AGGREGATES_EVERY_CYCLES != 1:
        violations.append(f"AGGREGATES_EVERY_CYCLES={AGGREGATES_EVERY_CYCLES}")

    if violations:
        raise RuntimeError(
            "runtime contract invalid: lower contour requires strict no-skip mode: "
            + ", ".join(violations)
        )


def _write_runtime_timing_report(timings: list[tuple[str, float]]) -> None:
    runtime_dir = Path("runtime")
    runtime_dir.mkdir(exist_ok=True)

    total = sum(seconds for _, seconds in timings)
    lines = [
        f"generated_at={iso_мск()}",
        f"total_seconds={round(total, 2)}",
        "",
        "step,seconds",
    ]

    for name, seconds in timings:
        lines.append(f"{name},{round(seconds, 2)}")

    (runtime_dir / "runtime_timing_report.txt").write_text("\n".join(lines) + "\n")


def _load_last_runtime_health() -> dict:
    path = Path("runtime_reports/runtime_health.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _load_last_cycle_status() -> dict:
    path = Path("runtime_reports/cycle_status.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _should_run_oi_gap_repair(cycle_no: int, every_cycles: int) -> bool:
    runtime_health = _load_last_runtime_health()
    if not runtime_health:
        return os.getenv("RUN_SYNC_OI_GAP_REPAIR", "0") == "1"

    if os.getenv("RUN_SYNC_OI_GAP_REPAIR", "0") != "1":
        return False

    # Expensive repair stays opt-in and periodic. The normal collector is the
    # primary recovery path; quarantine/alerts still protect real gaps.
    return should_run_maintenance_this_cycle(cycle_no, every_cycles)


def _collect_stage_chain_continuity() -> dict:
    lookback_hours = int(os.getenv("STAGE_CHAIN_DISCONTINUITY_LOOKBACK_HOURS", "24"))
    recent_minutes = int(os.getenv("STAGE_CHAIN_DISCONTINUITY_RECENT_MINUTES", "30"))
    try:
        total_rows = fetch(
            """
            WITH h AS (
                SELECT
                    exchange,
                    symbol,
                    cycle_ts,
                    from_stage,
                    to_stage,
                    LAG(to_stage) OVER (
                        PARTITION BY exchange, symbol
                        ORDER BY cycle_ts, id
                    ) AS prev_to
                FROM oi_stage_history
                WHERE cycle_ts >= NOW() - (%s || ' hours')::interval
            )
            SELECT COUNT(*) AS total
            FROM h
            WHERE prev_to IS NOT NULL
              AND prev_to <> from_stage
              AND NOT (prev_to = 1 AND from_stage = 0 AND to_stage = 1)
            """,
            (str(lookback_hours),),
        )
        recent_rows = fetch(
            """
            WITH h AS (
                SELECT
                    exchange,
                    symbol,
                    cycle_ts,
                    from_stage,
                    to_stage,
                    LAG(to_stage) OVER (
                        PARTITION BY exchange, symbol
                        ORDER BY cycle_ts, id
                    ) AS prev_to
                FROM oi_stage_history
                WHERE cycle_ts >= NOW() - (%s || ' hours')::interval
            )
            SELECT
                exchange,
                symbol,
                to_char(cycle_ts AT TIME ZONE 'Europe/Moscow', 'YYYY-MM-DD HH24:MI') AS cycle_ts_msk,
                prev_to,
                from_stage,
                to_stage
            FROM h
            WHERE prev_to IS NOT NULL
              AND prev_to <> from_stage
              AND NOT (prev_to = 1 AND from_stage = 0 AND to_stage = 1)
              AND cycle_ts >= GREATEST(
                  NOW() - (%s || ' minutes')::interval,
                  %s::timestamptz
              )
            ORDER BY cycle_ts DESC, exchange, symbol
            LIMIT 10
            """,
            (str(lookback_hours), str(recent_minutes), _PROCESS_STARTED_AT_MSK),
        )
        total_count = int((total_rows[0] or {}).get("total", 0) or 0) if total_rows else 0
        return build_stage_chain_continuity_report(
            recent_rows,
            lookback_hours=lookback_hours,
            recent_minutes=recent_minutes,
            total_count=total_count,
        )
    except Exception as exc:
        return {
            "health": "unknown",
            "lookback_hours": lookback_hours,
            "recent_minutes": recent_minutes,
            "total": 0,
            "recent_total": 0,
            "sample": [],
            "query_error": type(exc).__name__,
        }


def _write_runtime_health_snapshot(
    timings: list[tuple[str, float]],
    bybit_symbols: list[str],
    binance_symbols: list[str],
    cycle_health: str,
    stage3_alert_info: dict | None = None,
    source_cycle_ts: datetime | None = None,
) -> None:
    Path("runtime_reports").mkdir(exist_ok=True)
    universe_state = _runtime_universe_state()
    universe_health = _collect_universe_health(source_cycle_ts)

    watchdog_streaks = dict(getattr(_timed_watchdog_step, "_timeout_streaks", {}))
    watchdog_health = "critical" if any(
        streak >= int(os.getenv("WATCHDOG_CRITICAL_STREAK", "3"))
        for streak in watchdog_streaks.values()
    ) else ("degraded" if any(streak > 0 for streak in watchdog_streaks.values()) else "ok")

    Path("runtime_reports/watchdog_status.txt").write_text(
        "\n".join([
            f"watchdog_health={watchdog_health}",
            f"watchdog_streaks={watchdog_streaks}",
            f"updated_at_utc={iso_мск()}",
        ]) + "\n"
    )

    rss_mb = _runtime_memory_mb()
    rss_health = "ok"
    if rss_mb >= float(os.getenv("RSS_CRITICAL_MB", "1024")):
        rss_health = "critical"
    elif rss_mb >= float(os.getenv("RSS_WARNING_MB", "768")):
        rss_health = "warning"

    collect_seconds = next((seconds for name, seconds in timings if name == "collect"), 0.0)
    collect_target_seconds = float(os.getenv("COLLECT_TARGET_SECONDS", "90"))
    collect_reserve_seconds = max(0.0, collect_target_seconds - collect_seconds)
    collect_reserve_pct = round((collect_reserve_seconds / collect_target_seconds) * 100, 2) if collect_target_seconds else 0
    collect_reserve_health = "ok"
    if collect_reserve_seconds <= float(os.getenv("COLLECT_RESERVE_CRITICAL_SECONDS", "5")):
        collect_reserve_health = "critical"
    elif collect_reserve_seconds <= float(os.getenv("COLLECT_RESERVE_WARNING_SECONDS", "15")):
        collect_reserve_health = "warning"

    runtime_alerts = []
    if rss_health != "ok":
        runtime_alerts.append(f"rss_{rss_health}")
    if watchdog_health != "ok":
        runtime_alerts.append(f"watchdog_{watchdog_health}")
    if collect_reserve_health != "ok":
        runtime_alerts.append(f"collect_reserve_{collect_reserve_health}")
    runtime_alerts.extend(universe_health.get("alerts", []))
    runtime_alerts.extend(universe_state.get("listing_alerts", []))

    signal_info = stage3_alert_info or {}
    price_data_incidents = list(signal_info.get("stage3_price_data_incidents") or [])
    if price_data_incidents:
        examples = ",".join(
            f"{item.get('exchange')}:{item.get('symbol')}" for item in price_data_incidents[:5]
        )
        runtime_alerts.append(
            f"CRITICAL_stage3_price_data_missing count={int(signal_info.get('stage3_price_data_incident_count', len(price_data_incidents)) or 0)} pairs={examples}"
        )
    universe_summary = universe_health.get("summary", {}) or {}
    symbols_total = int(universe_state.get("active_total", 0) or 0)
    symbols_by_exchange = {
        "BINANCE": int(len(binance_symbols)),
        "BYBIT": int(len(bybit_symbols)),
    }
    data_quality_alerts = _build_data_quality_alerts(universe_health)
    data_quality_state = "ok" if not data_quality_alerts else "degraded"
    try:
        integrity_rows = fetch(
            """
            SELECT COUNT(*) AS total, MAX(detected_at) AS latest_at
            FROM core_state_integrity_incidents
            WHERE detected_at >= NOW() - INTERVAL '24 hours'
            """
        )
        integrity_health = integrity_rows[0] if integrity_rows else {"total": 0, "latest_at": None}
    except Exception as exc:
        integrity_health = {"total": 0, "latest_at": None, "query_error": type(exc).__name__}
    integrity_recoveries_24h = int(integrity_health.get("total", 0) or 0)
    stage1_near_maturity = collect_stage1_near_maturity_diagnostics()
    stage_observability = get_runtime_observability_metrics()
    stage_chain_continuity = _collect_stage_chain_continuity()
    if int(stage_chain_continuity.get("recent_total", 0) or 0) > 0:
        runtime_alerts.append(
            f"stage_chain_discontinuity={stage_chain_continuity.get('recent_total')}"
        )
    window_freshness_by_kind = build_window_freshness_by_kind(
        universe_health.get("problem_pairs", [])
    )
    data_quality_quarantine_rows = _data_quality_quarantine_rows(limit=20)
    quarantine_lifecycle = build_quarantine_lifecycle(
        universe_health.get("problem_pairs", []),
        data_quality_quarantine_rows,
    )
    timing_text = " ".join([f"{name}={round(seconds, 2)}s" for name, seconds in timings])
    try:
        quote_turnover_summary = quote_turnover_state_summary()
    except Exception as exc:
        quote_turnover_summary = {"total": 0, "ready": 0, "not_ready": 0, "warming": 0, "stale": 0, "excluded_by_universe": 0, "universe_unknown": 0, "universe_status": "unavailable", "error": type(exc).__name__}
    runtime_health = {
        "updated_at_utc": iso_мск(),
        "app_version": APP_VERSION,
        "pid": os.getpid(),
        "rss_mb": round(rss_mb, 2),
        "rss_peak_mb": round(_runtime_memory_peak_mb(), 2),
        "rss_health": rss_health,
        "watchdog_health": watchdog_health,
        "watchdog_streaks": watchdog_streaks,
        "cycle_timing": timing_text,
        "collect_seconds": round(collect_seconds, 2),
        "collect_target_seconds": round(collect_target_seconds, 2),
        "collect_reserve_seconds": round(collect_reserve_seconds, 2),
        "collect_reserve_pct": collect_reserve_pct,
        "collect_reserve_health": collect_reserve_health,
        "runtime_alerts": runtime_alerts,
        "runtime_alert_count": len(runtime_alerts),
        "quote_turnover": quote_turnover_summary,
        "stage3_volume_queue": dict((stage3_alert_info or {}).get("stage3_volume_queue") or {}),
        "stage3_price_data_incident_count": int(signal_info.get("stage3_price_data_incident_count", 0) or 0),
        "cycle_health": cycle_health,
        "bybit_symbols": len(bybit_symbols),
        "binance_symbols": len(binance_symbols),
        "bybit_workers": BYBIT_COLLECT_WORKERS,
        "binance_workers": BINANCE_COLLECT_WORKERS,
        "skip_heavy": os.getenv("SKIP_HEAVY_AGGREGATES"),
        "skip_stage2": os.getenv("SKIP_STAGE2_REBUILDS"),
        "force_stage2": os.getenv("FORCE_STAGE2_WITH_STALE_AGGREGATES"),
        "derived_window_hours": os.getenv("DERIVED_WINDOW_HOURS"),
        "derived_batch_size": os.getenv("DERIVED_BATCH_SIZE"),
        "derived_retention_hours": os.getenv("DERIVED_RETENTION_HOURS"),
        "universe_health": universe_health.get("health", "error"),
        "universe_alerts": universe_health.get("alerts", []),
        "universe_summary": universe_health.get("summary", {}),
        "universe_by_exchange": universe_health.get("by_exchange", []),
        "universe_problem_pairs": universe_health.get("problem_pairs", []),
        "universe_runtime": universe_state,
        "symbols_total": symbols_total,
        "symbols_by_exchange": symbols_by_exchange,
        "duck_universe_health": universe_health.get("health", "error"),
        "duck_listing_health": universe_state.get("listing_health", "ok"),
        "duck_universe_summary": universe_summary,
        "listing_health": universe_state.get("listing_health", "ok"),
        "listing_alerts": universe_state.get("listing_alerts", []),
        "listing_problem_pairs": universe_state.get("listing_problem_pairs", []),
        "listing_summary": {
            "bybit_total": universe_state.get("bybit_total", 0),
            "binance_total": universe_state.get("binance_total", 0),
            "active_total": universe_state.get("active_total", 0),
            "quarantine_total": universe_state.get("quarantine_total", 0),
            "data_quality_quarantine_total": universe_state.get("data_quality_quarantine_total", 0),
            "added_total": universe_state.get("added_total", 0),
            "removed_total": universe_state.get("removed_total", 0),
            "discovery_removed_total": universe_state.get("discovery_removed_total", 0),
            "limit_removed_total": universe_state.get("limit_removed_total", 0),
            "added_samples": universe_state.get("added_samples", []),
            "removed_samples": universe_state.get("removed_samples", []),
            "listing_problem_pairs": universe_state.get("listing_problem_pairs", []),
            "pruned_state_total": universe_state.get("pruned_state_total", 0),
            "pruned_state_counts": universe_state.get("pruned_state_counts", {}),
        },
        "symbols_incomplete_windows": int(universe_summary.get("incomplete_pairs", 0) or 0),
        "symbols_stale_windows": int(universe_summary.get("stale30_pairs", 0) or 0),
        "symbols_absent_in_duck": int(universe_summary.get("no_windows_pairs", 0) or 0),
        "symbols_in_data_quality_quarantine": int(universe_state.get("data_quality_quarantine_total", 0) or 0),
        "data_quality_state": data_quality_state,
        "data_quality_alerts": data_quality_alerts,
        "signal_observations_total": int(signal_info.get("signal_observations_total", 0) or 0),
        "signals_already_active": int(signal_info.get("signals_already_active", 0) or 0),
        "signals_waiting_confirmation": int(signal_info.get("signals_waiting_confirmation", 0) or 0),
        "signals_waiting_volume": int(signal_info.get("signals_waiting_volume", 0) or 0),
        "signals_repeat_on_cooldown": int(signal_info.get("signals_repeat_on_cooldown", 0) or 0),
        "stage1_near_maturity": stage1_near_maturity,
        "price_freshness_guard": stage_observability.get("price_freshness_guard", {}),
        "stage1_history_recheck": stage_observability.get("stage1_history_recheck", {}),
        "stage2_history_recheck": stage_observability.get("stage2_history_recheck", {}),
        "transition_metrics": stage_observability.get("transition_metrics", {}),
        "degrade_reasons": stage_observability.get("degrade_reasons", {}),
        "stage_chain_continuity": stage_chain_continuity,
        "window_freshness_by_kind": window_freshness_by_kind,
        "quarantine_lifecycle": quarantine_lifecycle,
        "new_signals": list(signal_info.get("new_signals", []) or []),
        "signal_delivery_failed": int(signal_info.get("delivery_failed", 0) or 0),
        "stage3_media_albums_sent": int(signal_info.get("media_albums_sent", 0) or 0),
        "stage3_media_photos_sent": int(signal_info.get("media_photos_sent", 0) or 0),
        "stage3_media_text_fallbacks": int(signal_info.get("media_text_fallbacks", 0) or 0),
        "stage3_media_capture_errors": int(signal_info.get("media_capture_errors", 0) or 0),
        "stage3_media_delivery_errors": int(signal_info.get("media_delivery_errors", 0) or 0),
        "stage3_charts_requested": int(signal_info.get("media_charts_requested", 0) or 0),
        "stage3_charts_captured": int(signal_info.get("media_charts_captured", 0) or 0),
        "stage3_chart_capture_seconds_total": round(
            float(signal_info.get("media_chart_capture_seconds_total", 0.0) or 0.0),
            3,
        ),
        "stage3_chart_send_seconds_total": round(
            float(signal_info.get("media_chart_send_seconds_total", 0.0) or 0.0),
            3,
        ),
        "stage3_total_delivery_seconds_total": round(
            float(signal_info.get("media_total_delivery_seconds_total", 0.0) or 0.0),
            3,
        ),
        "stage3_media_alerts": list(signal_info.get("media_alerts", []) or []),
        "auto_heal_oi_gaps": dict(_LAST_OI_GAP_REPAIR),
        "auto_heal_listing": dict(_LAST_LISTING_SELF_HEAL),
        "core_state_integrity": {
            "recoveries_24h": integrity_recoveries_24h,
            "latest_recovery_at": integrity_health.get("latest_at"),
            "query_error": integrity_health.get("query_error"),
        },
    }
    runtime_health["details"] = {
        "symbols_total": runtime_health["symbols_total"],
        "symbols_by_exchange": runtime_health["symbols_by_exchange"],
        "duck_universe_health": runtime_health["duck_universe_health"],
        "duck_listing_health": runtime_health["duck_listing_health"],
        "duck_universe_summary": runtime_health["duck_universe_summary"],
        "symbols_incomplete_windows": runtime_health["symbols_incomplete_windows"],
        "symbols_stale_windows": runtime_health["symbols_stale_windows"],
        "symbols_absent_in_duck": runtime_health["symbols_absent_in_duck"],
        "symbols_in_data_quality_quarantine": runtime_health["symbols_in_data_quality_quarantine"],
        "data_quality_state": runtime_health["data_quality_state"],
        "data_quality_alerts": runtime_health["data_quality_alerts"],
        "core_state_integrity": runtime_health["core_state_integrity"],
        "signal_observations_total": runtime_health["signal_observations_total"],
        "signals_already_active": runtime_health["signals_already_active"],
        "signals_waiting_confirmation": runtime_health["signals_waiting_confirmation"],
        "signals_waiting_volume": runtime_health["signals_waiting_volume"],
        "quote_turnover": runtime_health["quote_turnover"],
        "stage3_volume_queue": runtime_health["stage3_volume_queue"],
        "signals_repeat_on_cooldown": runtime_health["signals_repeat_on_cooldown"],
        "stage1_near_maturity": runtime_health["stage1_near_maturity"],
        "price_freshness_guard": runtime_health["price_freshness_guard"],
        "stage1_history_recheck": runtime_health["stage1_history_recheck"],
        "stage2_history_recheck": runtime_health["stage2_history_recheck"],
        "transition_metrics": runtime_health["transition_metrics"],
        "degrade_reasons": runtime_health["degrade_reasons"],
        "window_freshness_by_kind": runtime_health["window_freshness_by_kind"],
        "quarantine_lifecycle": runtime_health["quarantine_lifecycle"],
        "new_signals": runtime_health["new_signals"],
        "signal_delivery_failed": runtime_health["signal_delivery_failed"],
        "stage3_media_albums_sent": runtime_health["stage3_media_albums_sent"],
        "stage3_media_photos_sent": runtime_health["stage3_media_photos_sent"],
        "stage3_media_text_fallbacks": runtime_health["stage3_media_text_fallbacks"],
        "stage3_media_capture_errors": runtime_health["stage3_media_capture_errors"],
        "stage3_media_delivery_errors": runtime_health["stage3_media_delivery_errors"],
        "stage3_charts_requested": runtime_health["stage3_charts_requested"],
        "stage3_charts_captured": runtime_health["stage3_charts_captured"],
        "stage3_chart_capture_seconds_total": runtime_health["stage3_chart_capture_seconds_total"],
        "stage3_chart_send_seconds_total": runtime_health["stage3_chart_send_seconds_total"],
        "stage3_total_delivery_seconds_total": runtime_health["stage3_total_delivery_seconds_total"],
        "stage3_media_alerts": runtime_health["stage3_media_alerts"],
        "auto_heal_oi_gaps": runtime_health["auto_heal_oi_gaps"],
        "auto_heal_listing": runtime_health["auto_heal_listing"],
    }

    _write_text_atomic(
        "runtime_reports/runtime_health.txt",
        "\n".join([f"{k}={v}" for k, v in runtime_health.items()]) + "\n",
    )
    runtime_health_json_path = Path("runtime_reports/runtime_health.json")

    snapshot_health = "ok"
    runtime_health["snapshot_health"] = snapshot_health
    runtime_health["snapshot_size"] = 0
    _write_json_atomic(runtime_health_json_path, runtime_health)

    snapshot_size = runtime_health_json_path.stat().st_size
    if snapshot_size <= 32:
        snapshot_health = "critical"
        log(f"RUNTIME_SNAPSHOT_CORRUPTED size={snapshot_size}")

    runtime_health["snapshot_size"] = snapshot_size
    runtime_health["snapshot_health"] = snapshot_health
    _write_json_atomic(runtime_health_json_path, runtime_health)
    _write_canonical_health(runtime_health, _load_last_cycle_status())
    _write_json_atomic(
        "runtime_reports/universe_health.json",
        {
            "updated_at_utc": runtime_health["updated_at_utc"],
            "health": universe_health.get("health", "error"),
            "alerts": universe_health.get("alerts", []),
            "summary": universe_health.get("summary", {}),
            "by_exchange": universe_health.get("by_exchange", []),
            "problem_pairs": universe_health.get("problem_pairs", []),
            "runtime": universe_state,
        },
    )
    _write_text_atomic(
        "runtime_reports/universe_health.txt",
        "\n".join([
            f"updated_at_utc={runtime_health['updated_at_utc']}",
            f"health={universe_health.get('health', 'error')}",
            f"alerts={','.join(universe_health.get('alerts', []))}",
            f"summary={json.dumps(universe_health.get('summary', {}), ensure_ascii=False)}",
            f"problem_pairs={json.dumps(universe_health.get('problem_pairs', []), ensure_ascii=False)}",
            f"runtime={json.dumps(universe_state, ensure_ascii=False)}",
        ]) + "\n",
    )
    _write_json_atomic(
        "runtime_reports/listing_health.json",
        {
            "updated_at_utc": runtime_health["updated_at_utc"],
            "health": universe_state.get("listing_health", "ok"),
            "alerts": universe_state.get("listing_alerts", []),
            "summary": runtime_health["listing_summary"],
            "runtime": universe_state,
        },
    )
    _write_text_atomic(
        "runtime_reports/listing_health.txt",
        "\n".join([
            f"updated_at_utc={runtime_health['updated_at_utc']}",
            f"health={universe_state.get('listing_health', 'ok')}",
            f"alerts={','.join(universe_state.get('listing_alerts', []))}",
            f"summary={json.dumps(runtime_health['listing_summary'], ensure_ascii=False)}",
            f"runtime={json.dumps(universe_state, ensure_ascii=False)}",
        ]) + "\n",
    )

    _write_text_atomic(
        "runtime_reports/snapshot_status.txt",
        "\n".join([
            f"snapshot_health={snapshot_health}",
            f"snapshot_size={snapshot_size}",
            f"updated_at_utc={runtime_health['updated_at_utc']}",
        ]) + "\n",
    )



def _runtime_memory_mb() -> float:
    try:
        if sys.platform != "darwin":
            status = Path("/proc/self/status")
            if status.exists():
                for line in status.read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        return float(line.split()[1]) / 1024
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / 1024 / 1024
        return rss / 1024
    except Exception:
        return 0.0


def _runtime_memory_peak_mb() -> float:
    try:
        if sys.platform != "darwin":
            status = Path("/proc/self/status")
            if status.exists():
                for line in status.read_text().splitlines():
                    if line.startswith("VmHWM:"):
                        return float(line.split()[1]) / 1024
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / 1024 / 1024
        return rss / 1024
    except Exception:
        return 0.0


def _timed_step(timings: list[tuple[str, float]], name: str, fn):
    started = time.time()
    result = fn()
    elapsed = time.time() - started
    timings.append((name, elapsed))
    log(
        f"step resource: {name}={elapsed:.2f}s "
        f"memory_rss_mb={_runtime_memory_mb():.2f} memory_peak_rss_mb={_runtime_memory_peak_mb():.2f}"
    )
    return result



def _collect_binance_symbol(symbol: str, *, kline_limit: int = 24):
    oi_rows = []
    price_rows = []
    volume_rows = []
    failures = []

    try:
        oi_rows.extend(fetch_binance_oi_5m(symbol, 24))
    except Exception as exc:
        failures.append(("BINANCE", symbol, "OI", exc))

    try:
        p, v = fetch_binance_kline_5m(symbol, kline_limit)
        price_rows.extend(p)
        volume_rows.extend(v)
    except Exception as exc:
        failures.append(("BINANCE", symbol, "PRICE_VOLUME", exc))

    return oi_rows, price_rows, volume_rows, failures


def collect(symbols_bybit, symbols_binance, quote_turnover_backfill_targets=None):
    collect_started = time.time()
    reset_request_stats()
    oi_rows, price_rows, volume_rows = [], [], []
    failures = []
    now = datetime.now(timezone.utc)
    quote_turnover_backfill_targets = set(quote_turnover_backfill_targets or set())

    def kline_limit_for(exchange: str, symbol: str) -> int:
        return 120 if (exchange, symbol) in quote_turnover_backfill_targets else 24

    def record_failure(exchange: str, symbol: str, data_type: str, exc: Exception) -> None:
        failures.append((now, exchange, symbol, data_type, type(exc).__name__, str(exc)[:500]))

    bybit_collect_symbols = symbols_bybit

    def collect_bybit_symbol(s: str):
        local_oi, local_price, local_volume = [], [], []
        try:
            local_oi.extend(fetch_bybit_oi_5m(s, 24))
        except Exception as exc:
            record_failure("BYBIT", s, "OI", exc)

        try:
            p, v = fetch_bybit_kline_5m(s, kline_limit_for("BYBIT", s))
            local_price.extend(p)
            local_volume.extend(v)
        except Exception as exc:
            record_failure("BYBIT", s, "PRICE_VOLUME", exc)

        return local_oi, local_price, local_volume

    bybit_workers = max(1, BYBIT_COLLECT_WORKERS)
    bybit_started = time.time()

    with ThreadPoolExecutor(max_workers=bybit_workers) as executor:
        futures = [
            executor.submit(collect_bybit_symbol, symbol)
            for symbol in bybit_collect_symbols
        ]

        for future in as_completed(futures):
            local_oi, local_price, local_volume = future.result()
            oi_rows.extend(local_oi)
            price_rows.extend(local_price)
            volume_rows.extend(local_volume)

    bybit_seconds = time.time() - bybit_started

    binance_collect_symbols = (
        symbols_binance
        if ЛИМИТ_СИМВОЛОВ_BINANCE <= 0
        else symbols_binance[:ЛИМИТ_СИМВОЛОВ_BINANCE]
    )

    workers = max(1, BINANCE_COLLECT_WORKERS)
    binance_started = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _collect_binance_symbol,
                symbol,
                kline_limit=kline_limit_for("BINANCE", symbol),
            )
            for symbol in binance_collect_symbols
        ]

        for future in as_completed(futures):
            b_oi_rows, b_price_rows, b_volume_rows, symbol_failures = future.result()

            oi_rows.extend(b_oi_rows)
            price_rows.extend(b_price_rows)
            volume_rows.extend(b_volume_rows)

            for exchange, symbol, data_type, exc in symbol_failures:
                record_failure(exchange, symbol, data_type, exc)

    binance_seconds = time.time() - binance_started
    collect_seconds = time.time() - collect_started
    slow_side = "bybit" if bybit_seconds > binance_seconds else "binance"
    slow_side_delta_seconds = abs(bybit_seconds - binance_seconds)
    collect_target_seconds = float(os.getenv("COLLECT_TARGET_SECONDS", "90"))
    collect_critical_seconds = float(os.getenv("COLLECT_CRITICAL_SECONDS", "120"))
    collect_reserve_seconds = max(0.0, collect_target_seconds - collect_seconds)

    bybit_symbols_per_second = round(len(bybit_collect_symbols) / bybit_seconds, 2) if bybit_seconds > 0 else 0
    binance_symbols_per_second = round(len(binance_collect_symbols) / binance_seconds, 2) if binance_seconds > 0 else 0
    collect_symbols_total = len(bybit_collect_symbols) + len(binance_collect_symbols)
    collect_symbols_per_second = round(collect_symbols_total / collect_seconds, 2) if collect_seconds > 0 else 0
    bybit_seconds_per_symbol = round(bybit_seconds / len(bybit_collect_symbols), 4) if bybit_collect_symbols else 0
    binance_seconds_per_symbol = round(binance_seconds / len(binance_collect_symbols), 4) if binance_collect_symbols else 0

    worker_pressure = "ok"
    if collect_reserve_seconds <= float(os.getenv("COLLECT_RESERVE_CRITICAL_SECONDS", "5")):
        worker_pressure = "critical"
    elif collect_reserve_seconds <= float(os.getenv("COLLECT_RESERVE_WARNING_SECONDS", "15")):
        worker_pressure = "warning"
    elif slow_side_delta_seconds >= float(os.getenv("SLOW_SIDE_DELTA_WARNING_SECONDS", "10")):
        worker_pressure = "imbalanced"

    collect_health = "ok"
    if collect_seconds > collect_critical_seconds:
        collect_health = "critical"
    elif collect_seconds > collect_target_seconds:
        collect_health = "slow"

    collect_reserve_health = "ok"
    if collect_reserve_seconds <= float(os.getenv("COLLECT_RESERVE_CRITICAL_SECONDS", "5")):
        collect_reserve_health = "critical"
    elif collect_reserve_seconds <= float(os.getenv("COLLECT_RESERVE_WARNING_SECONDS", "15")):
        collect_reserve_health = "warning"

    failure_types = {}

    for _, exchange, symbol, data_type, error_type, _ in failures:
        key = f"{exchange}:{data_type}:{error_type}"
        failure_types[key] = failure_types.get(key, 0) + 1

    failure_health = "ok"
    if len(failures) >= 50:
        failure_health = "critical"
    elif len(failures) >= 10:
        failure_health = "warning"

    if failures:
        top_failures = sorted(
            failure_types.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        log(
            f"REQUEST_FAILURES "
            f"count={len(failures)} "
            f"failure_health={failure_health} "
            f"top={top_failures}"
        )

    if failure_health != "ok":
        log(f"REQUEST_FAILURE_{failure_health.upper()} count={len(failures)}")

    if collect_health != "ok":
        log(
            f"COLLECT_{collect_health.upper()} "
            f"elapsed={collect_seconds:.2f}s "
            f"target={collect_target_seconds:.2f}s "
            f"slow_side={slow_side} "
            f"slow_side_delta_seconds={slow_side_delta_seconds:.2f} "
            f"bybit_sps={bybit_symbols_per_second} "
            f"binance_sps={binance_symbols_per_second} "
            f"worker_pressure={worker_pressure}"
        )

    request_stats = get_request_stats()
    slow_request_total = sum(int(v.get("slow", 0)) for v in request_stats.values())
    timeout_total = sum(int(v.get("timeouts", 0)) for v in request_stats.values())
    retry_total = sum(int(v.get("retries", 0)) for v in request_stats.values())
    request_error_total = sum(int(v.get("errors", 0)) for v in request_stats.values())

    top_slow_endpoints = sorted(
        request_stats.items(),
        key=lambda x: (x[1].get("slow", 0), x[1].get("max_seconds", 0)),
        reverse=True
    )[:5]

    if collect_reserve_health != "ok":
        log(
            f"COLLECT_RESERVE_{collect_reserve_health.upper()} "
            f"reserve_seconds={collect_reserve_seconds:.2f} "
            f"target={collect_target_seconds:.2f}s"
        )

    if slow_request_total or timeout_total or retry_total:
        log(
            f"REQUEST_LATENCY "
            f"slow_requests={slow_request_total} "
            f"timeouts={timeout_total} "
            f"retries={retry_total} "
            f"errors={request_error_total} "
            f"top_slow={top_slow_endpoints}"
        )

    log(
        f"collect ok: oi={len(oi_rows)} price={len(price_rows)} volume={len(volume_rows)} "
        f"request_failures={len(failures)} "
        f"failure_health={failure_health} "
        f"bybit_symbols={len(bybit_collect_symbols)} "
        f"bybit_workers={bybit_workers} "
        f"bybit_seconds={bybit_seconds:.2f} "
        f"binance_symbols={len(binance_collect_symbols)} "
        f"binance_workers={workers} "
        f"binance_seconds={binance_seconds:.2f} "
        f"collect_seconds={collect_seconds:.2f} "
        f"collect_target_seconds={collect_target_seconds:.2f} "
        f"collect_reserve_seconds={collect_reserve_seconds:.2f} "
        f"collect_reserve_health={collect_reserve_health} "
        f"slow_side={slow_side} "
        f"slow_side_delta_seconds={slow_side_delta_seconds:.2f} "
        f"bybit_symbols_per_second={bybit_symbols_per_second} "
        f"binance_symbols_per_second={binance_symbols_per_second} "
        f"collect_symbols_per_second={collect_symbols_per_second} "
        f"bybit_seconds_per_symbol={bybit_seconds_per_symbol} "
        f"binance_seconds_per_symbol={binance_seconds_per_symbol} "
        f"worker_pressure={worker_pressure} "
        f"slow_requests={slow_request_total} "
        f"request_timeouts={timeout_total} "
        f"request_retries={retry_total} "
        f"request_errors={request_error_total} "
        f"collect_health={collect_health}"
    )

    return {
        "oi_rows": oi_rows,
        "price_rows": price_rows,
        "volume_rows": volume_rows,
        "failures": failures,
        "cycle_ts": now,
        "collect_seconds": collect_seconds,
        "collect_health": collect_health,
        "failure_health": failure_health,
        "quote_turnover_backfill_targets": len(quote_turnover_backfill_targets),
    }


def insert_collected_raw(batch: dict) -> int:
    if not batch:
        raise RuntimeError("insert_raw failed: empty collect batch")

    oi_rows = batch.get("oi_rows") or []
    price_rows = batch.get("price_rows") or []
    volume_rows = batch.get("volume_rows") or []
    failures = batch.get("failures") or []
    cycle_ts = batch.get("cycle_ts")

    upsert_oi(oi_rows, cycle_ts=cycle_ts, source="collect")
    upsert_price(price_rows, cycle_ts=cycle_ts, source="collect")
    upsert_volume(volume_rows, cycle_ts=cycle_ts, source="collect")
    replace_request_failures(failures)

    total_rows = len(oi_rows) + len(price_rows) + len(volume_rows)

    log(
        f"insert_raw ok: oi={len(oi_rows)} "
        f"price={len(price_rows)} "
        f"volume={len(volume_rows)} "
        f"request_failures={len(failures)} "
        f"total_rows={total_rows}"
    )

    return total_rows


def validate_aggregate_windows() -> dict:
    required_windows = ["15м", "30м", "1ч", "4ч", "12ч", "24ч"]
    blocking_windows = {"15м", "30м", "1ч", "4ч"}
    window_minutes = {
        "15м": 15,
        "30м": 30,
        "1ч": 60,
        "4ч": 240,
        "12ч": 720,
        "24ч": 1440,
    }
    rows = fetch("""
        SELECT metric, window_code, COUNT(*) AS row_count
        FROM aggregate_windows
        WHERE ts_close >= NOW() - INTERVAL '24 hours'
        GROUP BY metric, window_code
    """)

    spans = fetch("""
        SELECT
            'OI' AS metric,
            COUNT(*) AS row_count,
            EXTRACT(EPOCH FROM (MAX(ts_close) - MIN(ts_open))) / 60.0 AS span_minutes
        FROM oi_raw
        WHERE ts_close >= NOW() - INTERVAL '30 hours'

        UNION ALL

        SELECT
            'PRICE' AS metric,
            COUNT(*) AS row_count,
            EXTRACT(EPOCH FROM (MAX(ts_close) - MIN(ts_open))) / 60.0 AS span_minutes
        FROM price_raw
        WHERE ts_close >= NOW() - INTERVAL '30 hours'

        UNION ALL

        SELECT
            'VOLUME' AS metric,
            COUNT(*) AS row_count,
            EXTRACT(EPOCH FROM (MAX(ts_close) - MIN(ts_open))) / 60.0 AS span_minutes
        FROM volume_raw
        WHERE ts_close >= NOW() - INTERVAL '30 hours'
    """)

    counts = {(row["metric"], row["window_code"]): row["row_count"] for row in rows}
    span_map = {
        row["metric"]: {
            "row_count": int(row["row_count"] or 0),
            "span_minutes": float(row["span_minutes"] or 0.0),
        }
        for row in spans
    }
    missing = []
    warmup_pending = []
    for metric in ("OI", "PRICE", "VOLUME"):
        metric_span = span_map.get(metric, {"row_count": 0, "span_minutes": 0.0})
        for timeframe in required_windows:
            needed_minutes = window_minutes[timeframe]
            if metric_span["span_minutes"] + 5 < needed_minutes:
                warmup_pending.append(
                    f"{metric}:{timeframe}:span={round(metric_span['span_minutes'], 1)}m"
                )
                continue
            if counts.get((metric, timeframe), 0) <= 0:
                if timeframe in blocking_windows:
                    missing.append(f"{metric}:{timeframe}")
                else:
                    warmup_pending.append(f"{metric}:{timeframe}:optional_missing")

    if missing:
        raise RuntimeError(f"aggregates_validate failed: missing_windows={missing}")

    log(
        "aggregates_validate ok: "
        + " ".join(
            f"{metric}:{timeframe}={counts.get((metric, timeframe), 0)}"
            for metric in ("OI", "PRICE", "VOLUME")
            for timeframe in required_windows
        )
        + (
            " warmup_pending="
            + ",".join(warmup_pending)
            if warmup_pending
            else ""
        )
    )

    return {
        "required_windows": required_windows,
        "counts": counts,
        "span_map": span_map,
        "warmup_pending": warmup_pending,
    }



def _log_db_universe_check() -> None:
    try:
        tables = [
            "oi_raw",
            "price_raw",
            "volume_raw",
            "active_symbol_universe",
        ]

        for table in tables:
            rows = fetch(f"""
                SELECT
                    exchange,
                    COUNT(DISTINCT symbol) AS symbols,
                    COUNT(*) AS rows
                FROM {table}
                GROUP BY exchange
                ORDER BY exchange
            """)

            summary = " ".join(
                f'{r["exchange"]}:symbols={r["symbols"]}:rows={r["rows"]}'
                for r in rows
            )

            log(f"db universe check: {table} {summary}")

        universe_health = _collect_universe_health()
        summary = universe_health.get("summary", {})
        by_exchange = universe_health.get("by_exchange", [])
        exchange_text = " ".join(
            (
                f"{row['exchange']}:universe={row['universe_cnt']}"
                f":incomplete={row['incomplete_cnt']}"
                f":empty={row['no_windows_cnt']}"
                f":stale30={row['stale30_cnt']}"
                f":stale60={row['stale60_cnt']}"
                f":stale180={row['stale180_cnt']}"
            )
            for row in by_exchange
        )
        log(
            "db universe quality: "
            f"health={universe_health.get('health', 'error')} "
            f"alerts={','.join(universe_health.get('alerts', [])) or 'none'} "
            f"universe={summary.get('universe', 0)} "
            f"incomplete={summary.get('incomplete_pairs', 0)} "
            f"empty={summary.get('no_windows_pairs', 0)} "
            f"stale30={summary.get('stale30_pairs', 0)} "
            f"stale60={summary.get('stale60_pairs', 0)} "
            f"stale180={summary.get('stale180_pairs', 0)} "
            f"{exchange_text}"
        )

    except Exception as exc:
        log(f"db universe check error: {type(exc).__name__}: {exc}")


def _timed_watchdog_step(timings, name: str, func, timeout_env: str, default_timeout: int):
    timeout_seconds = float(os.getenv(timeout_env, str(default_timeout)))
    started = time.time()

    if not hasattr(_timed_watchdog_step, "_timeout_streaks"):
        _timed_watchdog_step._timeout_streaks = {}

    if not hasattr(_timed_watchdog_step, "_inflight"):
        _timed_watchdog_step._inflight = set()

    if name in _timed_watchdog_step._inflight:
        elapsed = time.time() - started
        timings.append((name, elapsed))
        log(
            f"WATCHDOG_INFLIGHT_SKIP "
            f"step={name} elapsed={elapsed:.2f}s "
            f"degraded=1"
        )
        return -3

    _timed_watchdog_step._inflight.add(name)

    def _run_and_release():
        try:
            return func()
        finally:
            _timed_watchdog_step._inflight.discard(name)

    if name == "autonomous_oi_service":
        script_path = Path(__file__).with_name("run_autonomous_oi_once.py")
        try:
            completed = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(Path(__file__).resolve().parent),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
            _timed_watchdog_step._inflight.discard(name)
            if completed.returncode != 0:
                stderr = (completed.stderr or "").strip()
                stdout = (completed.stdout or "").strip()
                details = stderr or stdout or f"returncode={completed.returncode}"
                raise RuntimeError(f"{name} subprocess failed: {details}")
            result_text = (completed.stdout or "").strip().splitlines()
            for stdout_line in result_text[:-1]:
                if "post_stage_analytics_seconds=" in stdout_line:
                    log(stdout_line)
            result = int(result_text[-1]) if result_text else 0
            elapsed = time.time() - started
            timings.append((name, elapsed))
            _timed_watchdog_step._timeout_streaks[name] = 0
            log(
                f"step resource: {name}={elapsed:.2f}s "
                f"watchdog=ok timeout={timeout_seconds}s "
                f"watchdog_streak=0 "
                f"memory_rss_mb={_runtime_memory_mb():.2f} memory_peak_rss_mb={_runtime_memory_peak_mb():.2f}"
            )
            return result
        except subprocess.TimeoutExpired as exc:
            _timed_watchdog_step._inflight.discard(name)
            elapsed = time.time() - started
            timings.append((name, elapsed))
            streak = _timed_watchdog_step._timeout_streaks.get(name, 0) + 1
            _timed_watchdog_step._timeout_streaks[name] = streak
            stdout_text = (
                exc.stdout.decode("utf-8", errors="replace")
                if isinstance(exc.stdout, bytes)
                else (exc.stdout or "")
            )
            stderr_text = (
                exc.stderr.decode("utf-8", errors="replace")
                if isinstance(exc.stderr, bytes)
                else (exc.stderr or "")
            )
            stdout_tail = stdout_text.strip().splitlines()[-20:]
            stderr_tail = stderr_text.strip().splitlines()[-20:]
            if stdout_tail:
                log(
                    f"WATCHDOG_TIMEOUT_STDOUT step={name} "
                    f"lines={len(stdout_tail)} tail={' || '.join(stdout_tail)}"
                )
            if stderr_tail:
                log(
                    f"WATCHDOG_TIMEOUT_STDERR step={name} "
                    f"lines={len(stderr_tail)} tail={' || '.join(stderr_tail)}"
                )
            log(
                f"WATCHDOG_TIMEOUT "
                f"step={name} elapsed={elapsed:.2f}s "
                f"timeout={timeout_seconds}s degraded=1 "
                f"watchdog_streak={streak}"
            )
            if streak >= int(os.getenv("WATCHDOG_CRITICAL_STREAK", "3")):
                log(
                    f"WATCHDOG_CRITICAL "
                    f"step={name} streak={streak} "
                    f"timeout={timeout_seconds}s"
                )
            return -2

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_run_and_release)

    try:
        result = future.result(timeout=timeout_seconds)
        elapsed = time.time() - started
        timings.append((name, elapsed))
        _timed_watchdog_step._timeout_streaks[name] = 0
        log(
            f"step resource: {name}={elapsed:.2f}s "
            f"watchdog=ok timeout={timeout_seconds}s "
            f"watchdog_streak=0 "
            f"memory_rss_mb={_runtime_memory_mb():.2f} memory_peak_rss_mb={_runtime_memory_peak_mb():.2f}"
        )
        executor.shutdown(wait=True, cancel_futures=False)
        return result

    except TimeoutError:
        elapsed = time.time() - started
        timings.append((name, elapsed))
        future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)

        streak = _timed_watchdog_step._timeout_streaks.get(name, 0) + 1
        _timed_watchdog_step._timeout_streaks[name] = streak

        log(
            f"WATCHDOG_TIMEOUT "
            f"step={name} elapsed={elapsed:.2f}s "
            f"timeout={timeout_seconds}s degraded=1 "
            f"watchdog_streak={streak}"
        )

        if streak >= int(os.getenv("WATCHDOG_CRITICAL_STREAK", "3")):
            log(
                f"WATCHDOG_CRITICAL "
                f"step={name} streak={streak} "
                f"timeout={timeout_seconds}s"
            )

        return -2

    except Exception as exc:
        elapsed = time.time() - started
        timings.append((name, elapsed))
        executor.shutdown(wait=False, cancel_futures=True)
        log(f"WATCHDOG_ERROR step={name} error={type(exc).__name__}: {exc}")
        raise


def _require_watchdog_success(step_name: str, result: int) -> int:
    if result == -3:
        raise CycleStop(
            f"{step_name}_watchdog_inflight",
            f"decision pipeline stopped: {step_name} watchdog reported inflight skip",
        )
    if result == -2:
        raise CycleStop(
            f"{step_name}_watchdog_timeout",
            f"decision pipeline stopped: {step_name} watchdog timeout",
        )
    return result


def _cycle_period_from_env(env_name: str, default_value: int) -> int:
    raw_value = os.getenv(env_name, str(default_value)).strip()
    try:
        parsed = int(raw_value)
    except ValueError:
        return default_value
    return max(1, parsed)


def background():
    last_export = 0.0
    cycle_no = 0
    aggregate_full_rebuild_every_cycles = _cycle_period_from_env("AGGREGATES_FULL_REBUILD_EVERY_CYCLES", 6)
    aggregate_validate_every_cycles = _cycle_period_from_env("AGGREGATES_VALIDATE_EVERY_CYCLES", 6)
    db_universe_check_every_cycles = _cycle_period_from_env("DB_UNIVERSE_CHECK_EVERY_CYCLES", 12)
    cleanup_old_every_cycles = _cycle_period_from_env("CLEANUP_OLD_EVERY_CYCLES", 6)
    universe_refresh_every_cycles = _cycle_period_from_env("UNIVERSE_REFRESH_EVERY_CYCLES", 3)
    oi_gap_repair_every_cycles = _cycle_period_from_env("OI_GAP_REPAIR_EVERY_CYCLES", 12)

    while True:
        cycle_no += 1
        cycle_started = time.time()
        stop_reason = "ok"
        stop_severity = "ok"
        stage3_alert_count = -1
        stage3_alert_info = {
            "sent_count": 0,
            "signal_observations_total": 0,
            "signals_already_active": 0,
            "signals_waiting_confirmation": 0,
            "signals_repeat_on_cooldown": 0,
            "delivery_failed": 0,
            "new_signals": [],
        }
        agg_count = 0
        timings = []
        try:
            if should_run_maintenance_this_cycle(cycle_no, universe_refresh_every_cycles):
                _timed_step(
                    timings,
                    "universe_refresh",
                    lambda: _refresh_runtime_universe(cycle_no, "periodic"),
                )
            else:
                log(
                    "universe refresh skipped: "
                    f"cycle={cycle_no} every={universe_refresh_every_cycles}"
                )

            universe_state = _runtime_universe_state()
            bybit_symbols = universe_state["bybit_symbols"]
            binance_symbols = universe_state["binance_symbols"]
            try:
                quote_backfill_limit = max(0, int(os.getenv("QUOTE_TURNOVER_BACKFILL_PAIRS_PER_CYCLE", "48") or "48"))
            except (TypeError, ValueError):
                quote_backfill_limit = 48
            quote_backfill_targets = _timed_step(
                timings,
                "quote_turnover_backfill_select",
                lambda: select_quote_turnover_backfill_targets(quote_backfill_limit),
            )
            collect_batch = _timed_step(
                timings,
                "collect",
                lambda: collect(bybit_symbols, binance_symbols, quote_backfill_targets),
            )
            collect_batch = _timed_step(timings, "raw_validate", lambda: validate_collected_raw(collect_batch, bybit_symbols, binance_symbols))
            _timed_step(timings, "insert_raw", lambda: insert_collected_raw(collect_batch))
            source_cycle_ts = collect_batch.get("cycle_ts")
            if source_cycle_ts is None:
                raise CycleStop("missing_collect_cycle_ts", "lower contour stopped: collect batch missing cycle_ts")
            _timed_step(timings, "aggregates_hot", lambda: rebuild_latest_aggregate_windows(source_cycle_ts))
            _timed_step(timings, "quote_turnover_state", lambda: refresh_quote_turnover_state(source_cycle_ts))
            _timed_step(
                timings,
                "data_quality_quarantine",
                lambda: _sync_data_quality_quarantine_from_health(_collect_universe_health(source_cycle_ts)),
            )
            if _should_run_oi_gap_repair(cycle_no, oi_gap_repair_every_cycles):
                _timed_step(timings, "repair_oi_gaps", lambda: _repair_oi_gap_windows(source_cycle_ts))
            else:
                log(
                    "repair_oi_gaps skipped: "
                    f"cycle={cycle_no} every={oi_gap_repair_every_cycles} sync_repair_enabled={os.getenv('RUN_SYNC_OI_GAP_REPAIR', '0')}"
                )
            _timed_step(timings, "listing_self_heal", lambda: _self_heal_listing_before_alert(cycle_no))
            collect_seconds = next((seconds for name, seconds in timings if name == "collect"), 0.0)

            autonomous_oi_count = _timed_watchdog_step(
                timings,
                "autonomous_oi_service",
                lambda: run_autonomous_oi_service(run_post_stage_analytics=False),
                "WATCHDOG_AUTONOMOUS_OI_SECONDS",
                60,
            )
            _require_watchdog_success("autonomous_oi_service", autonomous_oi_count)
            stage3_alert_info = _timed_step(timings, "stage3_alerts", check_stage3_alerts)
            stage3_alert_count = int((stage3_alert_info or {}).get("sent_count", 0) or 0)
            _timed_step(timings, "post_stage_analytics", run_post_stage_analytics_tail)

            if os.getenv("SKIP_HEAVY_AGGREGATES") == "1":
                log("aggregates_full skipped: SKIP_HEAVY_AGGREGATES=1")
            elif collect_seconds > MAX_COLLECT_SECONDS_FOR_AGGREGATES:
                log(
                    "aggregates_full skipped: "
                    f"collect too slow {collect_seconds:.2f}s > {MAX_COLLECT_SECONDS_FOR_AGGREGATES}s"
                )
            elif cycle_no % aggregate_full_rebuild_every_cycles == 0:
                expected_full_seconds = float(os.getenv("AGGREGATES_FULL_EXPECTED_SECONDS", "190"))
                full_reserve_seconds = float(os.getenv("AGGREGATES_FULL_RESERVE_SECONDS", "20"))
                elapsed_before_full = time.time() - cycle_started
                if elapsed_before_full + expected_full_seconds + full_reserve_seconds > ИНТЕРВАЛ_ЦИКЛА_СЕК:
                    log(
                        "aggregates_full skipped: "
                        f"cycle budget elapsed={elapsed_before_full:.2f}s "
                        f"expected={expected_full_seconds:.2f}s "
                        f"reserve={full_reserve_seconds:.2f}s "
                        f"interval={ИНТЕРВАЛ_ЦИКЛА_СЕК}s"
                    )
                else:
                    agg_count = _timed_watchdog_step(
                        timings,
                        "aggregates_full",
                        rebuild_aggregate_windows,
                        "WATCHDOG_AGGREGATES_SECONDS",
                        150,
                    )
                    _require_watchdog_success("aggregates_full", agg_count)
            else:
                log(f"aggregates_full skipped: cycle={cycle_no} every={aggregate_full_rebuild_every_cycles}")

            if cycle_no % aggregate_validate_every_cycles == 0:
                expected_validate_seconds = float(os.getenv("AGGREGATES_VALIDATE_EXPECTED_SECONDS", "12"))
                validate_reserve_seconds = float(os.getenv("AGGREGATES_VALIDATE_RESERVE_SECONDS", "10"))
                elapsed_before_validate = time.time() - cycle_started
                if not _cycle_step_fits_budget(
                    elapsed_seconds=elapsed_before_validate,
                    expected_seconds=expected_validate_seconds,
                    reserve_seconds=validate_reserve_seconds,
                ):
                    log(
                        "aggregates_validate skipped: "
                        f"cycle budget elapsed={elapsed_before_validate:.2f}s "
                        f"expected={expected_validate_seconds:.2f}s "
                        f"reserve={validate_reserve_seconds:.2f}s "
                        f"interval={ИНТЕРВАЛ_ЦИКЛА_СЕК}s"
                    )
                else:
                    _timed_step(timings, "aggregates_validate", validate_aggregate_windows)
            else:
                log(f"aggregates_validate skipped: cycle={cycle_no} every={aggregate_validate_every_cycles}")
            audit_count = -1
            if os.getenv("ENABLE_RUNTIME_VALIDATION_AUDIT") == "1":
                log("validation_audit skipped: legacy audit_engine archived")
            else:
                log("validation_audit skipped: ENABLE_RUNTIME_VALIDATION_AUDIT!=1")
            research_count = -1
            silence_count = -1
            price_count = -1
            volume_count = -1
            oi_slope_count = -1
            phase_source_count = -1
            phase_count = -1

            now = time.time()

            # quick export отключён из автоцикла.
            # Экспорт собирается только по запросу через Telegram.
            if now - last_export >= ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК:
                last_export = now

            timing_text = " ".join([f"{name}={round(seconds, 2)}s" for name, seconds in timings])
            log(f"cycle timing: {timing_text}")
            rss_mb = _runtime_memory_mb()
            rss_health = "ok"
            if rss_mb >= float(os.getenv("RSS_CRITICAL_MB", "1024")):
                rss_health = "critical"
            elif rss_mb >= float(os.getenv("RSS_WARNING_MB", "768")):
                rss_health = "warning"

            if rss_health != "ok":
                log(f"RSS_{rss_health.upper()} memory_rss_mb={rss_mb:.2f} memory_peak_rss_mb={_runtime_memory_peak_mb():.2f}")

            log(
                f"cycle resource: pid={os.getpid()} "
                f"memory_max_rss_mb={rss_mb:.2f} "
                f"rss_health={rss_health} "
                f"bybit_symbols={len(bybit_symbols)} bybit_workers={BYBIT_COLLECT_WORKERS} "
                f"binance_symbols={len(binance_symbols)} "
                f"binance_workers={BINANCE_COLLECT_WORKERS}"
            )

            Path("runtime_reports").mkdir(exist_ok=True)
            _write_runtime_timing_report(timings)
            _write_runtime_health_snapshot(
                timings,
                bybit_symbols,
                binance_symbols,
                "pending",
                stage3_alert_info=stage3_alert_info,
                source_cycle_ts=source_cycle_ts,
            )

            log(
                f"oi runtime cycle ok: aggregates={agg_count} "
                f"autonomous_oi={autonomous_oi_count} "
                f"stage3_alerts={stage3_alert_count} "
                f"audit={audit_count} "
                f"legacy_research={research_count} "
                f"legacy_silence={silence_count} "
                f"legacy_price={price_count} "
                f"legacy_volume={volume_count} "
                f"legacy_oi_slope={oi_slope_count} "
                f"legacy_phase_source={phase_source_count} "
                f"legacy_phase={phase_count}"
            )
            if should_run_maintenance_this_cycle(cycle_no, db_universe_check_every_cycles):
                expected_db_universe_seconds = float(os.getenv("DB_UNIVERSE_CHECK_EXPECTED_SECONDS", "190"))
                db_universe_reserve_seconds = float(os.getenv("DB_UNIVERSE_CHECK_RESERVE_SECONDS", "20"))
                elapsed_before_db_universe = time.time() - cycle_started
                if elapsed_before_db_universe + expected_db_universe_seconds + db_universe_reserve_seconds > ИНТЕРВАЛ_ЦИКЛА_СЕК:
                    log(
                        "db universe check skipped: "
                        f"cycle budget elapsed={elapsed_before_db_universe:.2f}s "
                        f"expected={expected_db_universe_seconds:.2f}s "
                        f"reserve={db_universe_reserve_seconds:.2f}s "
                        f"interval={ИНТЕРВАЛ_ЦИКЛА_СЕК}s"
                    )
                else:
                    _timed_step(timings, "db_universe_check", _log_db_universe_check)
            else:
                log(
                    "db universe check skipped: "
                    f"cycle={cycle_no} every={db_universe_check_every_cycles}"
                )

        except CycleStop as exc:
            stop_reason = exc.stop_reason
            stop_severity = exc.severity
            log(f"canonical validation cycle stopped: {stop_reason}: {exc}")
        except Exception as exc:
            stop_reason = type(exc).__name__
            stop_severity = "error"
            log(f"canonical validation cycle error: {type(exc).__name__}: {exc}")
            log(traceback.format_exc())

        try:
            if should_run_maintenance_this_cycle(cycle_no, cleanup_old_every_cycles):
                elapsed_before_cleanup = time.time() - cycle_started
                expected_cleanup_seconds = float(os.getenv("CLEANUP_OLD_EXPECTED_SECONDS", "20"))
                cleanup_reserve_seconds = float(os.getenv("CLEANUP_OLD_RESERVE_SECONDS", "15"))
                if not _cleanup_old_fits_budget(elapsed_before_cleanup):
                    log(
                        "cleanup_old skipped: "
                        f"cycle budget elapsed={elapsed_before_cleanup:.2f}s "
                        f"expected={expected_cleanup_seconds:.2f}s "
                        f"reserve={cleanup_reserve_seconds:.2f}s "
                        f"interval={ИНТЕРВАЛ_ЦИКЛА_СЕК}s"
                    )
                else:
                    _timed_step(timings, "cleanup_old", lambda: cleanup_old(ДНЕЙ_ХРАНЕНИЯ))
            else:
                log(
                    "cleanup_old skipped: "
                    f"cycle={cycle_no} every={cleanup_old_every_cycles}"
                )
        except Exception as exc:
            if stop_reason == "ok":
                stop_reason = "cleanup_old_failed"
                stop_severity = "error"
            log(f"cleanup_old error: {type(exc).__name__}: {exc}")
            log(traceback.format_exc())

        elapsed = time.time() - cycle_started

        if not hasattr(background, "_overrun_streak"):
            background._overrun_streak = 0

        if elapsed > ИНТЕРВАЛ_ЦИКЛА_СЕК:
            background._overrun_streak += 1
            log(
                f"CYCLE_OVERRUN "
                f"elapsed={elapsed:.2f}s "
                f"target={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
                f"overrun={(elapsed - ИНТЕРВАЛ_ЦИКЛА_СЕК):.2f}s "
                f"streak={background._overrun_streak}"
            )

            if background._overrun_streak >= 3:
                log(f"CYCLE_OVERRUN_CRITICAL streak={background._overrun_streak}")
        else:
            background._overrun_streak = 0

        sleep_seconds = _aligned_cycle_sleep_seconds(elapsed)
        reserve_seconds = max(0.0, ИНТЕРВАЛ_ЦИКЛА_СЕК - elapsed)

        cycle_health = "ok"
        if stop_reason != "ok":
            cycle_health = "stopped"
        elif elapsed > ИНТЕРВАЛ_ЦИКЛА_СЕК:
            cycle_health = "overrun"
        elif reserve_seconds < float(os.getenv("CYCLE_RESERVE_WARNING_SECONDS", "30")):
            cycle_health = "tight"

        cycle_reserve_pct = round((reserve_seconds / ИНТЕРВАЛ_ЦИКЛА_СЕК) * 100, 2) if ИНТЕРВАЛ_ЦИКЛА_СЕК else 0
        cycle_latency_class = "healthy"
        if cycle_health == "overrun":
            cycle_latency_class = "overrun"
        elif cycle_reserve_pct < float(os.getenv("CYCLE_RESERVE_WARNING_PCT", "20")):
            cycle_latency_class = "thin_reserve"

        log(
            f"cycle schedule: target={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
            f"elapsed={elapsed:.2f}s reserve={reserve_seconds:.2f}s "
            f"aligned_sleep={sleep_seconds:.2f}s "
            f"cycle_health={cycle_health}"
        )

        _write_runtime_health_snapshot(
            timings,
            bybit_symbols,
            binance_symbols,
            cycle_health,
            stage3_alert_info=locals().get("stage3_alert_info"),
            source_cycle_ts=locals().get("source_cycle_ts"),
        )

        Path("runtime_reports").mkdir(exist_ok=True)
        cycle_status = {
            "updated_at_utc": iso_мск(),
            "cycle_target_seconds": ИНТЕРВАЛ_ЦИКЛА_СЕК,
            "cycle_elapsed_seconds": round(elapsed, 2),
            "cycle_reserve_seconds": round(reserve_seconds, 2),
            "cycle_sleep_seconds": round(sleep_seconds, 2),
            "cycle_reserve_pct": cycle_reserve_pct,
            "cycle_latency_class": cycle_latency_class,
            "cycle_health": cycle_health,
            "stop_reason": stop_reason,
            "stop_severity": stop_severity,
            "overrun_streak": getattr(background, "_overrun_streak", 0),
        }

        _write_text_atomic(
            "runtime_reports/cycle_status.txt",
            "\n".join([f"{k}={v}" for k, v in cycle_status.items()]) + "\n",
        )
        _write_json_atomic("runtime_reports/cycle_status.json", cycle_status)
        try:
            runtime_health = _load_last_runtime_health()
            if runtime_health:
                _write_canonical_health(runtime_health, cycle_status)
        except Exception as exc:
            log(f"canonical health write error: {type(exc).__name__}: {exc}")

        time.sleep(sleep_seconds)


def main():
    log(f"Новая чистая база {APP_VERSION} запущена")
    log(f"runtime mode: {runtime_mode_text()}")
    log(
        "runtime env: "
        f"cycle_interval={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
        f"cycle_align_offset={os.getenv('CYCLE_ALIGN_OFFSET_SECONDS', '5')}s "
        f"skip_heavy={os.getenv('SKIP_HEAVY_AGGREGATES')} "
        f"skip_stage2={os.getenv('SKIP_STAGE2_REBUILDS')} "
        f"force_stage2={os.getenv('FORCE_STAGE2_WITH_STALE_AGGREGATES')} "
        f"derived_window_hours={os.getenv('DERIVED_WINDOW_HOURS')} "
        f"derived_batch_size={os.getenv('DERIVED_BATCH_SIZE')} "
        f"derived_retention_hours={os.getenv('DERIVED_RETENTION_HOURS')}"
    )
    _validate_runtime_contract()
    log("runtime contract ok: strict no-skip lower contour enabled")

    log("init_db start")
    init_db()
    log("init_db ok")
    migrate_canonical_ts_close()

    start_polling()
    log("Telegram polling стартовал")

    send_panel_message(
        СТАРТОВОЕ_СООБЩЕНИЕ.format(
            version=APP_VERSION,
            retention=ДНЕЙ_ХРАНЕНИЯ,
            started_at=текст_мск(),
        )
    )
    log("Telegram OK")

    universe_state = _refresh_runtime_universe(0, "startup")
    log(f"Bybit symbols: {universe_state['bybit_total']}")
    log(f"Binance symbols: {universe_state['binance_total']}")
    log(f"Limits: bybit={ЛИМИТ_СИМВОЛОВ_BYBIT}, binance={ЛИМИТ_СИМВОЛОВ_BINANCE}")
    log(
        "Active universe: "
        f"bybit={len(universe_state['bybit_symbols'])} "
        f"binance={len(universe_state['binance_symbols'])} "
        f"total={universe_state['active_total']}"
    )
    log(
        "Quarantine symbols observed: "
        f"total={universe_state['quarantine_total']} "
        f"data_quality={universe_state['data_quality_quarantine_total']} "
        "collection_continues=1 phase_excluded=1"
    )

    threading.Thread(target=background, daemon=True).start()

    log("background workers started")

    while True:
        log("heartbeat ok")
        time.sleep(60)


if __name__ == "__main__":
    main()
