from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone, timedelta
import json
import math
import os
import time
from pathlib import Path

from db import (
    active_universe_sql,
    execute,
    fetch,
    insert_oi_stage_history,
    insert_transition_history_v2,
    prune_inactive_state_rows,
    replace_core_state_v2,
    replace_oi_core_state,
    replace_oi_window_state,
    replace_window_state_v2,
)
from logger import log
from oi_service import (
    compute_oi_window_state as _compute_oi_window_state_impl,
    summarize_oi_window_states,
)
from phase_common import build_symbol_window_payload as _build_symbol_window_payload_impl
from phase_service import (
    apply_stage_guardrails as _apply_stage_guardrails_impl,
    compute_stage_age as _compute_stage_age_impl,
    compute_transition_permission as _compute_transition_permission_impl,
    determine_target_stage as _determine_target_stage_impl,
)
from price_service import (
    compute_price_window_state as _compute_price_window_state_impl,
    summarize_price as _summarize_price_impl,
)
from volume_service import (
    compute_volume_window_state as _compute_volume_window_state_impl,
    summarize_volume as _summarize_volume_impl,
)

WINDOWS = ["15м", "30м", "1ч", "4ч", "12ч", "24ч"]
WINDOW_WEIGHTS = {
    "15м": 1.0,
    "30м": 1.5,
    "1ч": 2.0,
    "4ч": 2.5,
    "12ч": 2.0,
    "24ч": 1.0,
}

POST_STAGE_HORIZONS = [
    (timedelta(hours=1), "price_after_1h", "oi_after_1h"),
    (timedelta(hours=4), "price_after_4h", "oi_after_4h"),
    (timedelta(hours=12), "price_after_12h", "oi_after_12h"),
    (timedelta(hours=24), "price_after_24h", "oi_after_24h"),
]

PATTERN_LABELS = {
    "мертвая_форма": "мертвая_форма",
    "тихое_накопление": "тихое_накопление",
    "развивающийся_набор": "развивающийся_набор",
    "подтвержденный_набор": "подтвержденный_набор",
    "ложный_всплеск": "ложный_всплеск",
    "рваный_хаос": "рваный_хаос",
    "поломка_набора": "поломка_набора",
}

RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"
AUTONOMOUS_OI_PROGRESS_PATH = RUNTIME_DIR / "autonomous_oi_progress.json"
SYMBOL_WINDOWS_STALE_MINUTES = int(os.getenv("SYMBOL_WINDOWS_STALE_MINUTES", "30"))


def attach_oi_trajectory_points(window_map_by_symbol: dict[tuple[str, str], dict[str, dict[str, dict]]]) -> None:
    oi_rows = []
    min_ts_open = None
    max_ts_close = None
    exchanges = set()
    symbols = set()

    for (exchange, symbol), window_map in window_map_by_symbol.items():
        exchanges.add(exchange)
        symbols.add(symbol)
        for metric_rows in window_map.values():
            oi_row = metric_rows.get("OI")
            if not oi_row:
                continue
            oi_rows.append((exchange, symbol, oi_row))
            row_open = oi_row.get("ts_open")
            row_close = oi_row.get("ts_close")
            if row_open is not None and (min_ts_open is None or row_open < min_ts_open):
                min_ts_open = row_open
            if row_close is not None and (max_ts_close is None or row_close > max_ts_close):
                max_ts_close = row_close

    if not oi_rows or min_ts_open is None or max_ts_close is None:
        return

    raw_rows = fetch(
        """
        SELECT
            ts_open,
            ts_close,
            exchange,
            symbol,
            oi_open,
            oi_high,
            oi_low,
            oi_close
        FROM oi_raw
        WHERE ts_open >= %s
          AND ts_close <= %s
          AND exchange = ANY(%s)
          AND symbol = ANY(%s)
        ORDER BY exchange, symbol, ts_open
        """,
        (min_ts_open, max_ts_close, list(exchanges), list(symbols)),
    )

    raw_by_symbol: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in raw_rows:
        raw_by_symbol[(row["exchange"], row["symbol"])].append(row)

    for exchange, symbol, oi_row in oi_rows:
        start = oi_row.get("ts_open")
        end = oi_row.get("ts_close")
        if start is None or end is None:
            continue
        candles = [
            raw
            for raw in raw_by_symbol.get((exchange, symbol), [])
            if raw["ts_open"] >= start and raw["ts_close"] <= end
        ]
        if not candles:
            continue
        points = [float(candles[0]["oi_open"])]
        points.extend(float(candle["oi_close"]) for candle in candles)
        oi_row["trajectory_points"] = points
        oi_row["trajectory_candles"] = len(candles)


def _window_source_table(window_source: str) -> str:
    if window_source == "history":
        return "aggregate_windows_history"
    return "aggregate_windows"


def _symbol_filter_sql(tracked_pairs: list[tuple[str, str]] | None) -> tuple[str, tuple]:
    tracked = list(tracked_pairs or [])
    if not tracked:
        return "", ()
    clauses = []
    params: list[str] = []
    for exchange, symbol in tracked:
        clauses.append("(exchange = %s AND symbol = %s)")
        params.extend([exchange, symbol])
    return "AND (" + " OR ".join(clauses) + ")", tuple(params)


def load_latest_window_map(
    cycle_ts: datetime | None = None,
    window_source: str = "hot",
    tracked_pairs: list[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], dict[str, dict[str, dict]]]:
    params: tuple = ()
    cycle_filter = ""
    if cycle_ts is not None:
        cycle_filter = "AND (source_cycle_ts <= %s OR source_cycle_ts IS NULL)"
        params = (cycle_ts,)
    symbol_filter_sql, symbol_filter_params = _symbol_filter_sql(tracked_pairs)
    table_name = _window_source_table(window_source)

    rows = fetch(
        f"""
        WITH ranked AS (
            SELECT
                metric,
                window_code,
                ts_open,
                ts_close,
                exchange,
                symbol,
                open_value,
                high_value,
                low_value,
                close_value,
                sum_value,
                avg_value,
                delta_pct,
                unique_candles,
                source_cycle_ts,
                built_at,
                ROW_NUMBER() OVER (
                    PARTITION BY metric, window_code, exchange, symbol
                    ORDER BY ts_close DESC
                ) AS rn
            FROM {table_name}
            WHERE metric IN ('OI', 'PRICE', 'VOLUME')
              AND window_code = ANY(%s)
              AND {active_universe_sql()}
              {symbol_filter_sql}
              {cycle_filter}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        """,
        (WINDOWS, *symbol_filter_params, *params),
    )

    window_map: dict[tuple[str, str], dict[str, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        key = (row["exchange"], row["symbol"])
        window_map[key][row["window_code"]][row["metric"]] = row
    attach_oi_trajectory_points(window_map)
    return window_map


def _latest_symbol_window_ts(payload: dict[str, dict[str, dict] | None]) -> datetime | None:
    latest_ts: datetime | None = None
    for window_code in WINDOWS:
        for metric_name in ("OI", "PRICE", "VOLUME"):
            row = payload[window_code].get(metric_name)
            if not row:
                continue
            ts_close = row.get("ts_close")
            if ts_close is None:
                continue
            if latest_ts is None or ts_close > latest_ts:
                latest_ts = ts_close
    return latest_ts


def _stale_windows_reason(
    payload: dict[str, dict[str, dict] | None],
    cycle_ts: datetime,
) -> tuple[bool, float, datetime | None]:
    latest_ts = _latest_symbol_window_ts(payload)
    if latest_ts is None:
        return True, float("inf"), None
    lag_minutes = max(0.0, (cycle_ts - latest_ts).total_seconds() / 60.0)
    return lag_minutes > SYMBOL_WINDOWS_STALE_MINUTES, lag_minutes, latest_ts


def _missing_senior_background_reason(
    payload: dict[str, dict[str, dict] | None],
) -> str | None:
    oi_4h = payload.get("4ч", {}).get("OI")
    price_4h = payload.get("4ч", {}).get("PRICE")
    if not oi_4h or not price_4h:
        return "новый_листинг_без_старшего_фона"
    return None


def load_source_cycle_timestamps(
    end_ts: datetime,
    start_exclusive: datetime | None = None,
    window_source: str = "hot",
    tracked_pairs: list[tuple[str, str]] | None = None,
) -> list[datetime]:
    symbol_filter_sql, symbol_filter_params = _symbol_filter_sql(tracked_pairs)
    table_name = _window_source_table(window_source)
    lower_filter = ""
    params: list = list(symbol_filter_params)
    if start_exclusive is not None:
        lower_filter = "AND source_cycle_ts > %s"
        params.append(start_exclusive)
    params.append(end_ts)
    rows = fetch(
        f"""
        SELECT source_cycle_ts
        FROM {table_name}
        WHERE source_cycle_ts IS NOT NULL
          {symbol_filter_sql}
          {lower_filter}
          AND source_cycle_ts <= %s
        GROUP BY source_cycle_ts
        ORDER BY source_cycle_ts ASC
        """,
        tuple(params),
    )
    return [row["source_cycle_ts"] for row in rows]


def load_window_updates_by_cycle(
    cycle_timestamps: list[datetime],
    window_source: str = "hot",
    tracked_pairs: list[tuple[str, str]] | None = None,
) -> dict[datetime, list[dict]]:
    if not cycle_timestamps:
        return {}
    table_name = _window_source_table(window_source)
    symbol_filter_sql, symbol_filter_params = _symbol_filter_sql(tracked_pairs)
    rows = fetch(
        f"""
        SELECT
            metric,
            window_code,
            ts_open,
            ts_close,
            exchange,
            symbol,
            open_value,
            high_value,
            low_value,
            close_value,
            sum_value,
            avg_value,
            delta_pct,
            unique_candles,
            source_cycle_ts,
            built_at
        FROM {table_name}
        WHERE metric IN ('OI', 'PRICE', 'VOLUME')
          AND window_code = ANY(%s)
          AND source_cycle_ts IS NOT NULL
          {symbol_filter_sql}
          AND source_cycle_ts >= %s
          AND source_cycle_ts <= %s
        ORDER BY source_cycle_ts ASC, metric, window_code, exchange, symbol
        """,
        (WINDOWS, *symbol_filter_params, cycle_timestamps[0], cycle_timestamps[-1]),
    )
    by_cycle_ts: dict[datetime, list[dict]] = defaultdict(list)
    for row in rows:
        by_cycle_ts[row["source_cycle_ts"]].append(row)
    return by_cycle_ts


def load_autonomous_oi_progress() -> datetime | None:
    if not AUTONOMOUS_OI_PROGRESS_PATH.exists():
        return None
    try:
        payload = json.loads(AUTONOMOUS_OI_PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    value = payload.get("last_source_cycle_ts")
    return _parse_state_ts(value)


def save_autonomous_oi_progress(last_source_cycle_ts: datetime | None) -> None:
    if last_source_cycle_ts is None:
        return
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"last_source_cycle_ts": last_source_cycle_ts.isoformat()}
    AUTONOMOUS_OI_PROGRESS_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_previous_core_state_map() -> dict[tuple[str, str], dict]:
    rows = fetch("SELECT * FROM oi_core_state")
    return {(row["exchange"], row["symbol"]): row for row in rows}


def build_symbol_window_payload(window_map: dict[str, dict[str, dict]]) -> dict[str, dict[str, dict] | None]:
    return _build_symbol_window_payload_impl(window_map)


def _delta(row: dict | None) -> float:
    if not row:
        return 0.0
    return float(row.get("delta_pct") or 0.0)


def _signed_log_delta(delta: float) -> float:
    if delta == 0:
        return 0.0
    return math.copysign(math.log10(1.0 + abs(delta)), delta)


def _range_position(row: dict | None) -> float:
    if not row:
        return 0.5
    high = float(row.get("high_value") or 0.0)
    low = float(row.get("low_value") or 0.0)
    close = float(row.get("close_value") or 0.0)
    if high <= low:
        return 0.5
    return max(0.0, min(1.0, (close - low) / (high - low)))


def compute_oi_window_state(window_payload: dict[str, dict[str, dict] | None], window_code: str) -> dict:
    return _compute_oi_window_state_impl(window_payload, window_code)


def compute_price_window_state(window_payload: dict[str, dict[str, dict] | None], oi_state: dict, window_code: str) -> dict:
    return _compute_price_window_state_impl(window_payload, oi_state, window_code)


def compute_volume_window_state(window_payload: dict[str, dict[str, dict] | None], oi_state: dict, window_code: str) -> dict:
    return _compute_volume_window_state_impl(window_payload, oi_state, window_code)


def _pick_summary(window_states: list[dict], key: str, priority_windows: list[str]) -> str:
    candidates = [state for state in window_states if state["window_code"] in priority_windows]
    if not candidates:
        candidates = window_states
    return max(candidates, key=lambda item: item["window_weight"]).get(key)


def summarize_price(window_states: list[dict], target_stage: int) -> tuple[str, str, bool, int]:
    return _summarize_price_impl(window_states, target_stage)


def summarize_volume(window_states: list[dict], target_stage: int) -> tuple[str, str]:
    return _summarize_volume_impl(window_states, target_stage)


def determine_target_stage(oi_summary: dict, price_summary: tuple[str, ...], volume_summary: tuple[str, str]) -> tuple[int, str]:
    return _determine_target_stage_impl(oi_summary, price_summary, volume_summary)


def apply_stage_guardrails(
    previous_state: dict | None,
    target_stage: int,
    oi_summary: dict,
    price_summary: tuple[str, ...],
    volume_summary: tuple[str, str],
    previous_stage_age_minutes: float,
    trigger_age_minutes: float = 0.0,
) -> tuple[int, str]:
    return _apply_stage_guardrails_impl(
        previous_state,
        target_stage,
        oi_summary,
        price_summary,
        volume_summary,
        previous_stage_age_minutes,
        trigger_age_minutes,
    )


def compute_stage_age(previous_state: dict | None, target_stage: int, cycle_ts: datetime) -> float:
    return _compute_stage_age_impl(previous_state, target_stage, cycle_ts)


def compute_transition_permission(
    previous_state: dict | None,
    target_stage: int,
    stage_age_minutes: float,
    oi_summary: dict,
    price_summary: tuple[str, ...],
    trigger_age_minutes: float = 0.0,
) -> str:
    return _compute_transition_permission_impl(
        previous_state,
        target_stage,
        stage_age_minutes,
        oi_summary,
        price_summary,
        trigger_age_minutes,
    )


def _parse_state_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_growth_class(slope_class: str) -> bool:
    return slope_class in {"weak_up", "good_up", "strong_up"}


def _is_trigger_start_class(slope_class: str, slope_class_30m: str = "flat") -> bool:
    del slope_class_30m
    return slope_class in {"good_up", "strong_up"}


def _resolve_growth_trigger_ts(
    previous_state: dict | None,
    oi_summary: dict,
    price_summary: tuple[str, ...],
    cycle_ts: datetime,
) -> datetime | None:
    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    current_15m = str(oi_summary.get("oi_slope_class_15m") or "flat")
    current_30m = str(oi_summary.get("oi_slope_class_30m") or "flat")
    current_4h = str(oi_summary.get("oi_slope_class_4h") or "flat")
    blocked_by_price = bool(price_summary[2])
    previous_15m = str(previous_state.get("oi_slope_class_15m") or "flat") if previous_state else "flat"
    trigger_ts = _parse_state_ts(previous_state.get("growth_trigger_ts")) if previous_state else None

    if blocked_by_price or current_4h == "strong_down":
        return None

    if trigger_ts is not None:
        if not _is_growth_class(current_15m) and not _is_growth_class(current_30m):
            trigger_ts = None
        else:
            return trigger_ts

    trigger_seed_ts = cycle_ts - timedelta(minutes=5)

    if previous_stage < 2 and _is_trigger_start_class(current_15m, current_30m) and not _is_growth_class(previous_15m):
        return trigger_seed_ts

    if previous_stage <= 1 and trigger_ts is None and _is_trigger_start_class(current_15m, current_30m):
        return trigger_seed_ts

    return trigger_ts


def build_window_record(
    exchange: str,
    symbol: str,
    oi_state: dict,
    price_state: dict,
    volume_state: dict,
    fallback_cycle_ts: datetime,
) -> tuple:
    visual = f"{oi_state['oi_pattern_label']} | {price_state['price_state_label']} | {volume_state['volume_state_label']}"
    window_cycle_ts = oi_state["cycle_ts"] or fallback_cycle_ts
    return (
        exchange,
        symbol,
        oi_state["window_code"],
        oi_state["oi_direction"],
        oi_state["oi_angle"],
        oi_state["oi_stability"],
        oi_state["oi_retention"],
        oi_state["oi_breakdown"],
        oi_state["oi_pattern_label"],
        oi_state["oi_pattern_code"],
        oi_state["oi_pattern_label"],
        price_state["price_state_code"],
        price_state["price_state_label"],
        price_state["stage_block_level"],
        volume_state["volume_state_code"],
        volume_state["volume_state_label"],
        volume_state["confidence_effect"],
        oi_state["window_growth_pct"],
        oi_state["window_weight"],
        visual,
        window_cycle_ts,
    )


def build_core_record(
    exchange: str,
    symbol: str,
    oi_summary: dict,
    price_summary: tuple[str, ...],
    volume_summary: tuple[str, str],
    target_stage: int,
    transition_permission: str,
    stage_age_minutes: float,
    cycle_ts: datetime,
    decision_reason: str,
    trigger_ts: datetime | None,
) -> tuple:
    price_state, price_block, blocked_by_price, blocked_stage_max = price_summary[:4]
    volume_state, volume_confidence = volume_summary
    block_reason = price_state if blocked_by_price else ""
    breakdown_reason = oi_summary["oi_breakdown_summary"] if oi_summary["oi_breakdown_summary"] != "нет" else ""
    return (
        exchange,
        symbol,
        target_stage,
        oi_summary["oi_pattern_label"],
        oi_summary["oi_pattern_code"],
        oi_summary["oi_pattern_label"],
        oi_summary["oi_direction_summary"],
        oi_summary["oi_angle_summary"],
        oi_summary["oi_stability_summary"],
        oi_summary["oi_retention_summary"],
        oi_summary["oi_breakdown_summary"],
        oi_summary["oi_direction_summary"],
        oi_summary["oi_angle_summary"],
        oi_summary["oi_stability_summary"],
        oi_summary["oi_retention_summary"],
        oi_summary["oi_breakdown_summary"],
        price_state,
        price_block,
        volume_state,
        volume_confidence,
        round(stage_age_minutes, 2),
        transition_permission,
        blocked_by_price,
        blocked_stage_max,
        volume_confidence,
        decision_reason,
        block_reason,
        breakdown_reason,
        trigger_ts,
        oi_summary["oi_slope_class_15m"],
        oi_summary["oi_slope_class_30m"],
        oi_summary["oi_slope_class_1h"],
        oi_summary["oi_slope_class_4h"],
        cycle_ts,
    )


def build_stage_history_record(previous_state: dict | None, exchange: str, symbol: str, target_stage: int, transition_permission: str, previous_stage_age_minutes: float, cycle_ts: datetime, reason: str) -> tuple | None:
    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    if previous_stage == target_stage:
        return None
    return (
        exchange,
        symbol,
        previous_stage,
        target_stage,
        reason,
        transition_permission != "нельзя",
        round(previous_stage_age_minutes, 2),
        cycle_ts,
    )


def _oi_hold_class(oi_state: dict) -> str:
    retention_ratio = float(oi_state.get("oi_retention_ratio") or 0.0)
    if retention_ratio >= 0.85:
        return "сильное"
    if retention_ratio >= 0.65:
        return "хорошее"
    if retention_ratio >= 0.40:
        return "слабое"
    if retention_ratio >= 0.0:
        return "плохое"
    return "нет"


def _oi_pullback_class(oi_state: dict) -> str:
    pullback_ratio = float(oi_state.get("oi_pullback_ratio") or 0.0)
    if pullback_ratio <= 0.15:
        return "почти_нет"
    if pullback_ratio <= 0.30:
        return "легкий"
    if pullback_ratio <= 0.50:
        return "заметный"
    if pullback_ratio <= 0.80:
        return "глубокий"
    return "срыв"


def _oi_smoothness_class(oi_state: dict) -> str:
    smoothness = float(oi_state.get("oi_smoothness_proxy") or 0.0)
    if smoothness >= 0.90:
        return "очень_гладко"
    if smoothness >= 0.65:
        return "гладко"
    if smoothness >= 0.50:
        return "средне"
    if smoothness >= 0.35:
        return "рвано"
    return "пила"


def build_window_record_v2(
    exchange: str,
    symbol: str,
    oi_state: dict,
    price_state: dict,
    volume_state: dict,
    fallback_cycle_ts: datetime,
) -> tuple:
    window_cycle_ts = oi_state["cycle_ts"] or fallback_cycle_ts
    return (
        exchange,
        symbol,
        window_cycle_ts,
        oi_state["window_code"],
        oi_state["window_weight"],
        oi_state.get("oi_slope_class"),
        oi_state.get("oi_slope_ratio"),
        _oi_hold_class(oi_state),
        _oi_pullback_class(oi_state),
        _oi_smoothness_class(oi_state),
        price_state.get("price_regime"),
        price_state.get("price_direction"),
        False,
        volume_state.get("volume_state_code"),
        False,
    )


def build_core_record_v2(
    exchange: str,
    symbol: str,
    oi_summary: dict,
    price_summary: tuple[str, ...],
    volume_summary: tuple[str, str],
    target_stage: int,
    transition_permission: str,
    stage_age_minutes: float,
    cycle_ts: datetime,
    decision_reason: str,
) -> tuple:
    price_state, price_block, blocked_by_price, blocked_stage_max = price_summary[:4]
    volume_state, volume_confidence = volume_summary
    return (
        exchange,
        symbol,
        cycle_ts,
        target_stage,
        round(stage_age_minutes, 2),
        transition_permission,
        target_stage == 3,
        blocked_by_price,
        decision_reason,
        json.dumps(oi_summary, ensure_ascii=False),
        json.dumps(
            {
                "price_state": price_state,
                "price_block": price_block,
                "blocked_by_price": blocked_by_price,
                "blocked_stage_max": blocked_stage_max,
            },
            ensure_ascii=False,
        ),
        json.dumps(
            {
                "volume_state": volume_state,
                "volume_confidence": volume_confidence,
            },
            ensure_ascii=False,
        ),
    )


def build_transition_history_record_v2(
    previous_state: dict | None,
    exchange: str,
    symbol: str,
    target_stage: int,
    transition_permission: str,
    previous_stage_age_minutes: float,
    cycle_ts: datetime,
    reason: str,
) -> tuple | None:
    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    if previous_stage == target_stage:
        return None
    return (
        exchange,
        symbol,
        previous_stage,
        target_stage,
        cycle_ts,
        transition_permission != "нельзя",
        round(previous_stage_age_minutes, 2),
        reason,
    )


def compute_autonomous_oi_snapshot_from_latest_window_map(
    latest_window_map: dict[tuple[str, str], dict[str, dict[str, dict]]],
    cycle_ts: datetime | None = None,
    previous_state_map: dict[tuple[str, str], dict] | None = None,
) -> tuple[list[tuple], list[tuple], list[tuple], dict[tuple[str, str], dict]]:
    cycle_ts = cycle_ts or datetime.now(timezone.utc)
    if previous_state_map is None:
        previous_state_map = load_previous_core_state_map()

    core_rows: list[tuple] = []
    window_rows: list[tuple] = []
    history_rows: list[tuple] = []
    core_rows_v2: list[tuple] = []
    window_rows_v2: list[tuple] = []
    history_rows_v2: list[tuple] = []
    next_state_map: dict[tuple[str, str], dict] = {}

    for (exchange, symbol), window_map in latest_window_map.items():
        payload = build_symbol_window_payload(window_map)
        if not any(payload[window]["OI"] for window in WINDOWS):
            continue
        stale_windows, stale_lag_minutes, latest_symbol_ts = _stale_windows_reason(payload, cycle_ts)

        oi_window_states = [compute_oi_window_state(payload, window) for window in WINDOWS]
        price_window_states = [compute_price_window_state(payload, oi_window_states[idx], window) for idx, window in enumerate(WINDOWS)]
        volume_window_states = [compute_volume_window_state(payload, oi_window_states[idx], window) for idx, window in enumerate(WINDOWS)]

        oi_summary = summarize_oi_window_states(oi_window_states)

        provisional_stage, _ = determine_target_stage(oi_summary, ("нейтральна", "нет", False, 3), ("пустой", "нейтрально"))
        price_summary = summarize_price(price_window_states, provisional_stage)
        volume_summary = summarize_volume(volume_window_states, provisional_stage)
        raw_target_stage, decision_reason = determine_target_stage(oi_summary, price_summary, volume_summary)

        previous_state = previous_state_map.get((exchange, symbol))
        previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
        previous_stage_age_minutes = compute_stage_age(previous_state, previous_stage, cycle_ts) if previous_state else 0.0
        trigger_ts = _resolve_growth_trigger_ts(previous_state, oi_summary, price_summary, cycle_ts)
        trigger_age_minutes = 0.0
        if trigger_ts is not None:
            trigger_age_minutes = max(0.0, (cycle_ts - trigger_ts).total_seconds() / 60.0)
        target_stage, guard_reason = apply_stage_guardrails(
            previous_state,
            raw_target_stage,
            oi_summary,
            price_summary,
            volume_summary,
            previous_stage_age_minutes,
            trigger_age_minutes,
        )
        missing_senior_background_reason = _missing_senior_background_reason(payload)
        if missing_senior_background_reason and target_stage > 1:
            target_stage = 1
            guard_reason = missing_senior_background_reason
        if stale_windows:
            target_stage = 0
            guard_reason = (
                f"stale_windows lag={round(stale_lag_minutes, 1)}m "
                f"latest_window_ts={latest_symbol_ts.isoformat() if latest_symbol_ts else 'none'}"
            )
        stage_age_minutes = compute_stage_age(previous_state, target_stage, cycle_ts)
        transition_permission = compute_transition_permission(
            previous_state,
            target_stage,
            stage_age_minutes,
            oi_summary,
            price_summary,
            trigger_age_minutes,
        )
        if stale_windows:
            transition_permission = "skip:stale_windows"
        elif missing_senior_background_reason and target_stage <= 1:
            transition_permission = "нет_старшего_фона_4ч"
        decision_reason = f"{decision_reason}; guard={guard_reason}"

        for index, window_code in enumerate(WINDOWS):
            window_rows.append(
                build_window_record(
                    exchange,
                    symbol,
                    oi_window_states[index],
                    price_window_states[index],
                    volume_window_states[index],
                    cycle_ts,
                )
            )
            window_rows_v2.append(
                build_window_record_v2(
                    exchange,
                    symbol,
                    oi_window_states[index],
                    price_window_states[index],
                    volume_window_states[index],
                    cycle_ts,
                )
            )

        core_rows.append(
            build_core_record(
                exchange,
                symbol,
                oi_summary,
                price_summary,
                volume_summary,
                target_stage,
                transition_permission,
                stage_age_minutes,
                cycle_ts,
                decision_reason,
                trigger_ts,
            )
        )
        core_rows_v2.append(
            build_core_record_v2(
                exchange,
                symbol,
                oi_summary,
                price_summary,
                volume_summary,
                target_stage,
                transition_permission,
                stage_age_minutes,
                cycle_ts,
                decision_reason,
            )
        )

        history_record = build_stage_history_record(
            previous_state,
            exchange,
            symbol,
            target_stage,
            transition_permission,
            previous_stage_age_minutes,
            cycle_ts,
            decision_reason,
        )
        if history_record:
            history_rows.append(history_record)
        history_record_v2 = build_transition_history_record_v2(
            previous_state,
            exchange,
            symbol,
            target_stage,
            transition_permission,
            previous_stage_age_minutes,
            cycle_ts,
            decision_reason,
        )
        if history_record_v2:
            history_rows_v2.append(history_record_v2)

        next_state_map[(exchange, symbol)] = {
            "exchange": exchange,
            "symbol": symbol,
            "current_stage": target_stage,
            "oi_stage_age_minutes": stage_age_minutes,
            "latest_cycle_ts": cycle_ts,
            "oi_transition_permission": transition_permission,
            "oi_pattern_code": oi_summary["oi_pattern_code"],
            "price_state_summary": price_summary[0],
            "volume_state_summary": volume_summary[0],
            "blocked_by_price": price_summary[2],
            "blocked_stage_max": price_summary[3],
            "decision_reason": decision_reason,
            "growth_trigger_ts": trigger_ts.isoformat() if trigger_ts else None,
            "oi_slope_class_15m": oi_summary["oi_slope_class_15m"],
            "oi_slope_class_30m": oi_summary["oi_slope_class_30m"],
            "oi_slope_class_1h": oi_summary["oi_slope_class_1h"],
            "oi_slope_class_4h": oi_summary["oi_slope_class_4h"],
            "oi_growth_pct_1h": oi_summary.get("oi_growth_pct_1h"),
            "oi_retention_ratio_1h": oi_summary.get("oi_retention_ratio_1h"),
            "oi_pullback_ratio_1h": oi_summary.get("oi_pullback_ratio_1h"),
            "oi_smoothness_proxy_1h": oi_summary.get("oi_smoothness_proxy_1h"),
            "oi_concentration_ratio_1h": oi_summary.get("oi_concentration_ratio_1h"),
            "oi_tail_share_1h": oi_summary.get("oi_tail_share_1h"),
            "oi_tail_share_ratio_1h": oi_summary.get("oi_tail_share_ratio_1h"),
            "oi_flat_tail_ratio_1h": oi_summary.get("oi_flat_tail_ratio_1h"),
            "oi_hold_class_1h": oi_summary.get("oi_hold_class_1h"),
            "oi_pullback_class_1h": oi_summary.get("oi_pullback_class_1h"),
            "oi_smoothness_class_1h": oi_summary.get("oi_smoothness_class_1h"),
            "oi_concentration_class_1h": oi_summary.get("oi_concentration_class_1h"),
            "oi_tail_share_class_1h": oi_summary.get("oi_tail_share_class_1h"),
            "oi_flat_tail_class_1h": oi_summary.get("oi_flat_tail_class_1h"),
            "oi_form_class_1h": oi_summary.get("oi_form_class_1h"),
            "oi_form_score_1h": oi_summary.get("oi_form_score_1h"),
        }

    next_state_map["__v2_rows__"] = {
        "core_rows_v2": core_rows_v2,
        "window_rows_v2": window_rows_v2,
        "history_rows_v2": history_rows_v2,
    }
    return core_rows, window_rows, history_rows, next_state_map


def _raw_value_at_or_before(table: str, value_col: str, exchange: str, symbol: str, target_ts: datetime) -> float | None:
    rows = fetch(
        f"""
        SELECT {value_col} AS value
        FROM {table}
        WHERE exchange = %s AND symbol = %s AND ts_close <= %s
        ORDER BY ts_close DESC
        LIMIT 1
        """,
        (exchange, symbol, target_ts),
    )
    if not rows:
        return None
    value = rows[0].get("value")
    return float(value) if value is not None else None



def _post_stage_quality_label(trigger_price: float | None, price_after_24h: float | None, matured: bool) -> str:
    if trigger_price in (None, 0):
        return "pending"
    if price_after_24h is None:
        return "pending_24h" if matured else "partial"
    delta_pct = ((price_after_24h - trigger_price) / trigger_price) * 100.0
    if delta_pct >= 1.0:
        return "positive"
    if delta_pct <= -1.0:
        return "negative"
    return "flat"



def update_post_stage_analytics(history_rows: list[tuple], cycle_ts: datetime) -> None:
    seed_rows = list(history_rows)
    backfill_rows = fetch(
        """
        SELECT exchange, symbol, from_stage, to_stage, transition_reason,
               transition_allowed, stage_age_before_transition, cycle_ts
        FROM oi_stage_history
        WHERE to_stage >= 2
        ORDER BY cycle_ts DESC, created_at DESC
        LIMIT 500
        """
    )
    for row in backfill_rows:
        seed_rows.append(
            (
                row.get("exchange"),
                row.get("symbol"),
                row.get("from_stage"),
                row.get("to_stage"),
                row.get("transition_reason"),
                row.get("transition_allowed"),
                row.get("stage_age_before_transition"),
                row.get("cycle_ts"),
            )
        )

    seen = set()
    for record in seed_rows:
        exchange, symbol, _from_stage, to_stage, reason, allowed, _age_before, triggered_at = record
        if not allowed or int(to_stage or 0) < 2 or triggered_at is None:
            continue
        key = (exchange, symbol, int(to_stage), triggered_at)
        if key in seen:
            continue
        seen.add(key)

        exists = fetch(
            """
            SELECT 1
            FROM oi_post_stage_analytics
            WHERE exchange = %s AND symbol = %s AND stage_triggered = %s AND triggered_at = %s
            LIMIT 1
            """,
            (exchange, symbol, to_stage, triggered_at),
        )
        if exists:
            continue

        trigger_price = _raw_value_at_or_before("price_raw", "price_close", exchange, symbol, triggered_at)
        trigger_oi = _raw_value_at_or_before("oi_raw", "oi_close", exchange, symbol, triggered_at)
        execute(
            """
            INSERT INTO oi_post_stage_analytics(
                exchange, symbol, stage_triggered, triggered_at,
                trigger_price, trigger_oi, quality_label, notes, created_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                exchange,
                symbol,
                to_stage,
                triggered_at,
                trigger_price,
                trigger_oi,
                "new",
                reason,
            ),
        )

    rows = fetch(
        """
        SELECT *
        FROM oi_post_stage_analytics
        WHERE price_after_1h IS NULL
           OR price_after_4h IS NULL
           OR price_after_12h IS NULL
           OR price_after_24h IS NULL
           OR oi_after_1h IS NULL
           OR oi_after_4h IS NULL
           OR oi_after_12h IS NULL
           OR oi_after_24h IS NULL
        ORDER BY triggered_at DESC
        LIMIT 500
        """
    )

    for row in rows:
        updates = []
        params = []
        triggered_at = row.get("triggered_at")
        if isinstance(triggered_at, str):
            triggered_at = datetime.fromisoformat(triggered_at)
        if triggered_at is None:
            continue
        if triggered_at.tzinfo is None:
            triggered_at = triggered_at.replace(tzinfo=timezone.utc)

        local_row = dict(row)
        for horizon_delta, price_col, oi_col in POST_STAGE_HORIZONS:
            horizon_ts = triggered_at + horizon_delta
            if cycle_ts < horizon_ts:
                continue
            if local_row.get(price_col) is None:
                price_val = _raw_value_at_or_before("price_raw", "price_close", row["exchange"], row["symbol"], horizon_ts)
                if price_val is not None:
                    updates.append(f"{price_col} = %s")
                    params.append(price_val)
                    local_row[price_col] = price_val
            if local_row.get(oi_col) is None:
                oi_val = _raw_value_at_or_before("oi_raw", "oi_close", row["exchange"], row["symbol"], horizon_ts)
                if oi_val is not None:
                    updates.append(f"{oi_col} = %s")
                    params.append(oi_val)
                    local_row[oi_col] = oi_val

        matured = cycle_ts >= triggered_at + POST_STAGE_HORIZONS[-1][0]
        next_label = _post_stage_quality_label(local_row.get("trigger_price"), local_row.get("price_after_24h"), matured)
        if next_label != row.get("quality_label"):
            updates.append("quality_label = %s")
            params.append(next_label)

        if updates:
            execute(
                f"UPDATE oi_post_stage_analytics SET {', '.join(updates)} WHERE id = %s",
                tuple(params + [row["id"]]),
            )

    execute(
        """
        INSERT INTO post_stage_analytics_v2(
            exchange,
            symbol,
            stage_triggered,
            triggered_at,
            trigger_price,
            trigger_oi,
            price_after_1h,
            price_after_4h,
            price_after_12h,
            price_after_24h,
            oi_after_1h,
            oi_after_4h,
            oi_after_12h,
            oi_after_24h,
            quality_label,
            notes,
            created_at
        )
        SELECT
            exchange,
            symbol,
            stage_triggered,
            triggered_at,
            trigger_price,
            trigger_oi,
            price_after_1h,
            price_after_4h,
            price_after_12h,
            price_after_24h,
            oi_after_1h,
            oi_after_4h,
            oi_after_12h,
            oi_after_24h,
            quality_label,
            notes,
            created_at
        FROM oi_post_stage_analytics
        ON CONFLICT (exchange, symbol, stage_triggered, triggered_at)
        DO UPDATE SET
            trigger_price = EXCLUDED.trigger_price,
            trigger_oi = EXCLUDED.trigger_oi,
            price_after_1h = EXCLUDED.price_after_1h,
            price_after_4h = EXCLUDED.price_after_4h,
            price_after_12h = EXCLUDED.price_after_12h,
            price_after_24h = EXCLUDED.price_after_24h,
            oi_after_1h = EXCLUDED.oi_after_1h,
            oi_after_4h = EXCLUDED.oi_after_4h,
            oi_after_12h = EXCLUDED.oi_after_12h,
            oi_after_24h = EXCLUDED.oi_after_24h,
            quality_label = EXCLUDED.quality_label,
            notes = EXCLUDED.notes
        """
    )



def compute_autonomous_oi_snapshot(
    cycle_ts: datetime | None = None,
    previous_state_map: dict[tuple[str, str], dict] | None = None,
) -> tuple[list[tuple], list[tuple], list[tuple], dict[tuple[str, str], dict]]:
    cycle_ts = cycle_ts or datetime.now(timezone.utc)
    latest_window_map = load_latest_window_map(cycle_ts)
    return compute_autonomous_oi_snapshot_from_latest_window_map(
        latest_window_map,
        cycle_ts=cycle_ts,
        previous_state_map=previous_state_map,
    )


def compute_autonomous_oi_snapshot_incremental_to_cycle(
    cycle_ts: datetime | None = None,
    previous_state_map: dict[tuple[str, str], dict] | None = None,
    last_source_cycle_ts: datetime | None = None,
    tracked_pairs: list[tuple[str, str]] | None = None,
    window_source: str = "hot",
) -> tuple[list[tuple], list[tuple], list[tuple], dict[tuple[str, str], dict], datetime | None]:
    cycle_ts = cycle_ts or datetime.now(timezone.utc)
    if previous_state_map is None:
        previous_state_map = load_previous_core_state_map()

    source_cycles = load_source_cycle_timestamps(
        cycle_ts,
        start_exclusive=last_source_cycle_ts,
        window_source=window_source,
        tracked_pairs=tracked_pairs,
    )
    if not source_cycles:
        return [], [], [], previous_state_map, last_source_cycle_ts
    if last_source_cycle_ts is None:
        source_cycles = [source_cycles[-1]]

    latest_window_map = load_latest_window_map(
        source_cycles[0],
        window_source=window_source,
        tracked_pairs=tracked_pairs,
    )
    updates_by_cycle = load_window_updates_by_cycle(
        source_cycles,
        window_source=window_source,
        tracked_pairs=tracked_pairs,
    )

    final_core_rows: list[tuple] = []
    final_window_rows: list[tuple] = []
    all_history_rows: list[tuple] = []
    final_core_rows_v2: list[tuple] = []
    final_window_rows_v2: list[tuple] = []
    all_history_rows_v2: list[tuple] = []
    state_map = previous_state_map

    for source_cycle in source_cycles:
        for row in updates_by_cycle.get(source_cycle, []):
            key = (row["exchange"], row["symbol"])
            latest_window_map.setdefault(key, {})
            latest_window_map[key].setdefault(row["window_code"], {})
            latest_window_map[key][row["window_code"]][row["metric"]] = row

        final_core_rows, final_window_rows, history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=source_cycle,
            previous_state_map=state_map,
        )
        all_history_rows.extend(history_rows)
        v2_rows = state_map.get("__v2_rows__", {}) if isinstance(state_map, dict) else {}
        final_core_rows_v2 = list(v2_rows.get("core_rows_v2", []))
        final_window_rows_v2 = list(v2_rows.get("window_rows_v2", []))
        all_history_rows_v2.extend(v2_rows.get("history_rows_v2", []))

    if isinstance(state_map, dict):
        state_map["__v2_rows__"] = {
            "core_rows_v2": final_core_rows_v2,
            "window_rows_v2": final_window_rows_v2,
            "history_rows_v2": all_history_rows_v2,
        }

    return final_core_rows, final_window_rows, all_history_rows, state_map, source_cycles[-1]


def run_autonomous_oi_service(cycle_ts: datetime | None = None) -> int:
    cycle_ts = cycle_ts or datetime.now(timezone.utc)
    last_source_cycle_ts = load_autonomous_oi_progress()
    core_rows, window_rows, history_rows, next_state_map, last_source_cycle_ts = compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=cycle_ts,
        last_source_cycle_ts=last_source_cycle_ts,
    )
    if not core_rows:
        log("autonomous_oi_service ok: no_new_source_cycles")
        return 0
    v2_rows = next_state_map.pop("__v2_rows__", {}) if isinstance(next_state_map, dict) else {}

    prune_counts = prune_inactive_state_rows()
    replace_oi_core_state(core_rows)
    replace_oi_window_state(window_rows)
    insert_oi_stage_history(history_rows)
    replace_core_state_v2(v2_rows.get("core_rows_v2", []))
    replace_window_state_v2(v2_rows.get("window_rows_v2", []))
    insert_transition_history_v2(v2_rows.get("history_rows_v2", []))
    if prune_counts:
        log(
            "prune_inactive_state_rows ok: "
            + " ".join(f"{k}={v}" for k, v in prune_counts.items())
        )
    save_autonomous_oi_progress(last_source_cycle_ts)
    post_stage_started = time.perf_counter()
    update_post_stage_analytics(history_rows, last_source_cycle_ts or cycle_ts)
    post_stage_seconds = time.perf_counter() - post_stage_started
    log(f"post_stage_analytics_seconds={post_stage_seconds:.2f}")
    log(
        f"autonomous_oi_service ok: symbols={len(core_rows)} "
        f"windows={len(window_rows)} history={len(history_rows)}"
    )
    return len(core_rows)
