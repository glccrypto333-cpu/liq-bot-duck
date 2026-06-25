from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path("/home/alexey/openclaw/apps/liq-bot-duck")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import fetch  # type: ignore
from oi_service import compute_oi_window_state, summarize_oi_window_states  # type: ignore
from price_service import compute_price_window_state, summarize_price  # type: ignore
from autonomous_oi_service import _resolve_growth_trigger_ts  # type: ignore
from phase_service import (  # type: ignore
    compute_stage_age,
    determine_target_stage,
    apply_stage_guardrails,
    compute_transition_permission,
)
import phase_common  # type: ignore


WINDOWS = ["15м", "30м", "1ч", "4ч"]


def parse_ts(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fetch_history(exchange: str, symbol: str, start_ts: datetime, end_ts: datetime) -> list[dict]:
    return fetch(
        """
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
        FROM aggregate_windows_history
        WHERE exchange = %s
          AND symbol = %s
          AND metric IN ('OI', 'PRICE', 'VOLUME')
          AND window_code IN ('15м', '30м', '1ч', '4ч')
          AND source_cycle_ts BETWEEN %s AND %s
        ORDER BY source_cycle_ts, ts_close, metric, window_code
        """,
        (exchange, symbol, start_ts, end_ts),
    )


def build_payload(rows: list[dict], cycle_ts: datetime) -> dict[str, dict[str, dict | None]]:
    latest: dict[tuple[str, str], dict] = {}
    for row in rows:
        row_cycle = row.get("source_cycle_ts")
        if row_cycle is None or row_cycle > cycle_ts:
            continue
        latest[(row["window_code"], row["metric"])] = row

    payload: dict[str, dict[str, dict | None]] = {}
    for window_code in WINDOWS:
        payload[window_code] = {
            "OI": latest.get((window_code, "OI")),
            "PRICE": latest.get((window_code, "PRICE")),
            "VOLUME": latest.get((window_code, "VOLUME")),
        }
    return payload


def replay_case(case: dict) -> dict:
    exchange = case["exchange"]
    symbol = case["symbol"]
    start_ts = parse_ts(case["start_ts"])
    end_ts = parse_ts(case["end_ts"])
    control_points = case["control_points"]

    rows = fetch_history(exchange, symbol, start_ts - timedelta(minutes=5), end_ts + timedelta(minutes=5))
    cycle_points = sorted({row["source_cycle_ts"] for row in rows if row.get("source_cycle_ts")})

    previous_state: dict | None = None
    transitions: list[dict] = []
    snapshots: list[dict] = []

    for cycle_ts in cycle_points:
        payload = build_payload(rows, cycle_ts)
        oi_states = [compute_oi_window_state(payload, window) for window in WINDOWS]
        oi_summary = summarize_oi_window_states(oi_states)
        price_states = [compute_price_window_state(payload, oi_states[0], window) for window in WINDOWS]
        price_summary = summarize_price(price_states, 0)
        volume_summary = ("нет", "нет")

        previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
        previous_stage_age_minutes = compute_stage_age(previous_state, previous_stage, cycle_ts) if previous_state else 0.0
        trigger_ts = _resolve_growth_trigger_ts(previous_state, oi_summary, price_summary, cycle_ts)
        trigger_age_minutes = 0.0
        if trigger_ts is not None:
            trigger_age_minutes = max(0.0, (cycle_ts - trigger_ts).total_seconds() / 60.0)
        target_stage, target_reason = determine_target_stage(oi_summary, price_summary, volume_summary)
        new_stage, guard_reason = apply_stage_guardrails(
            previous_state,
            target_stage,
            oi_summary,
            price_summary,
            volume_summary,
            previous_stage_age_minutes,
            trigger_age_minutes,
        )
        new_age = compute_stage_age(previous_state, new_stage, cycle_ts)
        permission = compute_transition_permission(
            previous_state,
            new_stage,
            new_age,
            oi_summary,
            price_summary,
            trigger_age_minutes,
        )

        if previous_stage != new_stage:
            transitions.append(
                {
                    "cycle_ts": cycle_ts.isoformat(),
                    "from_stage": previous_stage,
                    "to_stage": new_stage,
                    "target_reason": target_reason,
                    "guard_reason": guard_reason,
                    "permission": permission,
                    "oi": {window: oi_summary[f"oi_slope_class_{window.replace('м','m').replace('ч','h')}"] if False else None for window in WINDOWS},
                }
            )

        snapshots.append(
            {
                "cycle_ts": cycle_ts.isoformat(),
                "stage": new_stage,
                "guard_reason": guard_reason,
                "target_reason": target_reason,
                "price_blocked": bool(price_summary[2]),
                "oi_15m": oi_summary["oi_slope_class_15m"],
                "oi_30m": oi_summary["oi_slope_class_30m"],
                "oi_1h": oi_summary["oi_slope_class_1h"],
                "oi_4h": oi_summary["oi_slope_class_4h"],
                "oi_ratio_15m": oi_summary["oi_slope_ratio_15m"],
                "oi_ratio_30m": oi_summary["oi_slope_ratio_30m"],
                "oi_ratio_1h": oi_summary["oi_slope_ratio_1h"],
                "oi_ratio_4h": oi_summary["oi_slope_ratio_4h"],
                "price_state": price_summary[0],
            }
        )

        previous_state = {
            "current_stage": new_stage,
            "oi_stage_age_minutes": new_age,
            "latest_cycle_ts": cycle_ts,
            "growth_trigger_ts": trigger_ts.isoformat() if trigger_ts else None,
            "oi_slope_class_15m": oi_summary["oi_slope_class_15m"],
            "oi_slope_class_30m": oi_summary["oi_slope_class_30m"],
            "oi_slope_class_1h": oi_summary["oi_slope_class_1h"],
            "oi_slope_class_4h": oi_summary["oi_slope_class_4h"],
        }

    evaluations = []
    for cp in control_points:
        ts = parse_ts(cp["ts"])
        latest_snapshot = None
        for snap in snapshots:
            snap_ts = parse_ts(snap["cycle_ts"])
            if snap_ts <= ts:
                latest_snapshot = snap
        evaluations.append(
            {
                "label": cp["label"],
                "ts": cp["ts"],
                "expect": cp["expect"],
                "snapshot": latest_snapshot,
            }
        )

    return {
        "symbol": symbol,
        "exchange": exchange,
        "thresholds": phase_common.OI_SLOPE_THRESHOLDS,
        "evaluations": evaluations,
        "transitions": transitions,
    }


def main() -> None:
    cases = json.loads(Path(sys.argv[1]).read_text())
    result = [replay_case(case) for case in cases]
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
