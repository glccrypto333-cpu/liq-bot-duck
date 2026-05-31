from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone, timedelta
import json
import math

from db import (
    execute,
    fetch,
    insert_oi_stage_history,
    insert_transition_history_v2,
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


def load_latest_window_map(cycle_ts: datetime | None = None) -> dict[tuple[str, str], dict[str, dict[str, dict]]]:
    params: tuple = ()
    cycle_filter = ""
    if cycle_ts is not None:
        cycle_filter = "AND (source_cycle_ts <= %s OR source_cycle_ts IS NULL)"
        params = (cycle_ts,)

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
            FROM aggregate_windows
            WHERE metric IN ('OI', 'PRICE', 'VOLUME')
              AND window_code = ANY(%s)
              {cycle_filter}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        """,
        (WINDOWS, *params),
    )

    window_map: dict[tuple[str, str], dict[str, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        key = (row["exchange"], row["symbol"])
        window_map[key][row["window_code"]][row["metric"]] = row
    attach_oi_trajectory_points(window_map)
    return window_map


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


def determine_target_stage(oi_summary: dict, price_summary: tuple[str, str, bool, int], volume_summary: tuple[str, str]) -> tuple[int, str]:
    return _determine_target_stage_impl(oi_summary, price_summary, volume_summary)


def apply_stage_guardrails(
    previous_state: dict | None,
    target_stage: int,
    oi_summary: dict,
    price_summary: tuple[str, str, bool, int],
    volume_summary: tuple[str, str],
) -> tuple[int, str]:
    return _apply_stage_guardrails_impl(
        previous_state,
        target_stage,
        oi_summary,
        price_summary,
        volume_summary,
    )


def compute_stage_age(previous_state: dict | None, target_stage: int, cycle_ts: datetime) -> float:
    return _compute_stage_age_impl(previous_state, target_stage, cycle_ts)


def compute_transition_permission(previous_state: dict | None, target_stage: int, stage_age_minutes: float, blocked_by_price: bool) -> str:
    return _compute_transition_permission_impl(previous_state, target_stage, stage_age_minutes, blocked_by_price)


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
    price_summary: tuple[str, str, bool, int],
    volume_summary: tuple[str, str],
    target_stage: int,
    transition_permission: str,
    stage_age_minutes: float,
    cycle_ts: datetime,
    decision_reason: str,
) -> tuple:
    price_state, price_block, blocked_by_price, blocked_stage_max = price_summary
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
        cycle_ts,
    )


def build_stage_history_record(previous_state: dict | None, exchange: str, symbol: str, target_stage: int, transition_permission: str, stage_age_minutes: float, cycle_ts: datetime, reason: str) -> tuple | None:
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
        round(stage_age_minutes, 2),
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
    if smoothness >= 0.85:
        return "очень_гладко"
    if smoothness >= 0.70:
        return "гладко"
    if smoothness >= 0.50:
        return "средне"
    if smoothness >= 0.30:
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
    price_summary: tuple[str, str, bool, int],
    volume_summary: tuple[str, str],
    target_stage: int,
    transition_permission: str,
    stage_age_minutes: float,
    cycle_ts: datetime,
    decision_reason: str,
) -> tuple:
    price_state, price_block, blocked_by_price, blocked_stage_max = price_summary
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
    stage_age_minutes: float,
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
        round(stage_age_minutes, 2),
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

        oi_window_states = [compute_oi_window_state(payload, window) for window in WINDOWS]
        price_window_states = [compute_price_window_state(payload, oi_window_states[idx], window) for idx, window in enumerate(WINDOWS)]
        volume_window_states = [compute_volume_window_state(payload, oi_window_states[idx], window) for idx, window in enumerate(WINDOWS)]

        oi_summary = summarize_oi_window_states(oi_window_states)

        provisional_stage, _ = determine_target_stage(oi_summary, ("нейтральна", "нет", False, 3), ("пустой", "нейтрально"))
        price_summary = summarize_price(price_window_states, provisional_stage)
        volume_summary = summarize_volume(volume_window_states, provisional_stage)
        raw_target_stage, decision_reason = determine_target_stage(oi_summary, price_summary, volume_summary)

        previous_state = previous_state_map.get((exchange, symbol))
        target_stage, guard_reason = apply_stage_guardrails(
            previous_state,
            raw_target_stage,
            oi_summary,
            price_summary,
            volume_summary,
        )
        stage_age_minutes = compute_stage_age(previous_state, target_stage, cycle_ts)
        transition_permission = compute_transition_permission(previous_state, target_stage, stage_age_minutes, price_summary[2])
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
            stage_age_minutes,
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
            stage_age_minutes,
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


def run_autonomous_oi_service(cycle_ts: datetime | None = None) -> int:
    cycle_ts = cycle_ts or datetime.now(timezone.utc)
    core_rows, window_rows, history_rows, next_state_map = compute_autonomous_oi_snapshot(cycle_ts=cycle_ts)
    v2_rows = next_state_map.pop("__v2_rows__", {}) if isinstance(next_state_map, dict) else {}

    replace_oi_core_state(core_rows)
    replace_oi_window_state(window_rows)
    insert_oi_stage_history(history_rows)
    replace_core_state_v2(v2_rows.get("core_rows_v2", []))
    replace_window_state_v2(v2_rows.get("window_rows_v2", []))
    insert_transition_history_v2(v2_rows.get("history_rows_v2", []))
    update_post_stage_analytics(history_rows, cycle_ts)
    log(
        f"autonomous_oi_service ok: symbols={len(core_rows)} "
        f"windows={len(window_rows)} history={len(history_rows)}"
    )
    return len(core_rows)
