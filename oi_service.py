from __future__ import annotations

from phase_common import (
    PATTERN_LABELS,
    WINDOW_WEIGHTS,
    classify_oi_slope,
    pullback_ratio_from_ohlc,
    pullback_ratio_from_points,
    retention_ratio_from_ohlc,
    retention_ratio_from_points,
    silent_build_ratio_from_points,
    smoothness_ratio_from_points,
    smoothness_proxy_from_ohlc,
    trajectory_points,
    value_growth_pct,
    value_slope_ratio,
)


def compute_oi_window_state(window_payload: dict[str, dict[str, dict] | None], window_code: str) -> dict:
    row = window_payload[window_code]["OI"]
    slope_ratio = value_slope_ratio(row)
    growth_pct = value_growth_pct(row)
    slope_class = classify_oi_slope(window_code, slope_ratio)
    points = trajectory_points(row)
    retention_ratio = retention_ratio_from_points(points) if points else retention_ratio_from_ohlc(row)
    pullback_ratio = pullback_ratio_from_points(points) if points else pullback_ratio_from_ohlc(row)
    smoothness_proxy = smoothness_ratio_from_points(points) if points else smoothness_proxy_from_ohlc(row)
    silent_build_ratio = silent_build_ratio_from_points(points) if points else slope_ratio
    silent_build_active = (
        bool(points)
        and silent_build_ratio >= 1.05
        and retention_ratio >= 0.70
        and pullback_ratio <= 0.30
        and smoothness_proxy >= 0.45
    )

    if slope_class in {"weak_up", "good_up", "strong_up"}:
        direction = "вверх"
    elif slope_class in {"weak_down", "strong_down"}:
        direction = "вниз"
    else:
        direction = "нейтрально"

    if slope_class in {"flat", "weak_up", "weak_down"}:
        angle = "слабый"
    elif slope_class in {"good_up"}:
        angle = "рабочий"
    else:
        angle = "сильный"

    if smoothness_proxy >= 0.75 and pullback_ratio <= 0.20:
        stability = "высокая"
    elif smoothness_proxy >= 0.50 and pullback_ratio <= 0.50:
        stability = "хорошая"
    else:
        stability = "низкая"

    if retention_ratio >= 0.85:
        retention = "подтвержденное"
    elif retention_ratio >= 0.40:
        retention = "рабочее"
    else:
        retention = "слабое"

    if slope_class == "strong_down":
        breakdown = "поломка"
    elif slope_class == "weak_down":
        breakdown = "риск"
    else:
        breakdown = "нет"

    if slope_class == "strong_down":
        pattern = "поломка_набора"
    elif slope_class == "strong_up":
        pattern = "подтвержденный_набор"
    elif slope_class == "good_up":
        pattern = "развивающийся_набор"
    elif slope_class == "weak_up":
        pattern = "тихое_накопление"
    elif slope_class == "flat":
        pattern = "мертвая_форма"
    elif slope_class == "weak_down":
        pattern = "ложный_всплеск"
    else:
        pattern = "рваный_хаос"

    return {
        "window_code": window_code,
        "oi_direction": direction,
        "oi_angle": angle,
        "oi_stability": stability,
        "oi_retention": retention,
        "oi_breakdown": breakdown,
        "oi_pattern_code": pattern,
        "oi_pattern_label": PATTERN_LABELS[pattern],
        "window_growth_pct": round(growth_pct, 4),
        "window_weight": WINDOW_WEIGHTS[window_code],
        "oi_slope_ratio": round(slope_ratio, 6),
        "oi_slope_class": slope_class,
        "oi_pullback_ratio": round(pullback_ratio, 6),
        "oi_retention_ratio": round(retention_ratio, 6),
        "oi_smoothness_proxy": round(smoothness_proxy, 6),
        "oi_silent_build_ratio": round(silent_build_ratio, 6),
        "oi_silent_build_active": silent_build_active,
        "oi_trajectory_points": len(points),
        "cycle_ts": row.get("source_cycle_ts") if row else None,
    }


def _pick_summary(window_states: list[dict], key: str, priority_windows: list[str]) -> str:
    candidates = [state for state in window_states if state["window_code"] in priority_windows]
    if not candidates:
        candidates = window_states
    return max(candidates, key=lambda item: item["window_weight"]).get(key)


def summarize_oi_window_states(oi_window_states: list[dict]) -> dict:
    by_window = {state["window_code"]: state for state in oi_window_states}
    return {
        "oi_pattern_code": _pick_summary(oi_window_states, "oi_pattern_code", ["1ч", "4ч", "12ч"]),
        "oi_pattern_label": _pick_summary(oi_window_states, "oi_pattern_label", ["1ч", "4ч", "12ч"]),
        "oi_direction_summary": _pick_summary(oi_window_states, "oi_direction", ["30м", "1ч", "4ч"]),
        "oi_angle_summary": _pick_summary(oi_window_states, "oi_angle", ["30м", "1ч", "4ч"]),
        "oi_stability_summary": _pick_summary(oi_window_states, "oi_stability", ["1ч", "4ч"]),
        "oi_retention_summary": _pick_summary(oi_window_states, "oi_retention", ["1ч", "4ч"]),
        "oi_breakdown_summary": _pick_summary(oi_window_states, "oi_breakdown", ["30м", "1ч", "4ч"]),
        "oi_slope_class_15m": (by_window.get("15м") or {}).get("oi_slope_class", "flat"),
        "oi_slope_class_30m": (by_window.get("30м") or {}).get("oi_slope_class", "flat"),
        "oi_slope_class_1h": (by_window.get("1ч") or {}).get("oi_slope_class", "flat"),
        "oi_slope_class_4h": (by_window.get("4ч") or {}).get("oi_slope_class", "flat"),
        "oi_slope_ratio_15m": (by_window.get("15м") or {}).get("oi_slope_ratio", 1.0),
        "oi_slope_ratio_30m": (by_window.get("30м") or {}).get("oi_slope_ratio", 1.0),
        "oi_slope_ratio_1h": (by_window.get("1ч") or {}).get("oi_slope_ratio", 1.0),
        "oi_slope_ratio_4h": (by_window.get("4ч") or {}).get("oi_slope_ratio", 1.0),
        "oi_retention_ratio_1h": (by_window.get("1ч") or {}).get("oi_retention_ratio", 0.0),
        "oi_pullback_ratio_1h": (by_window.get("1ч") or {}).get("oi_pullback_ratio", 1.0),
        "oi_smoothness_proxy_1h": (by_window.get("1ч") or {}).get("oi_smoothness_proxy", 0.0),
        "oi_silent_build_30m": bool((by_window.get("30м") or {}).get("oi_silent_build_active", False)),
        "oi_silent_build_1h": bool((by_window.get("1ч") or {}).get("oi_silent_build_active", False)),
    }
