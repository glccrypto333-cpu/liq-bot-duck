from __future__ import annotations

from phase_common import (
    PATTERN_LABELS,
    WINDOW_WEIGHTS,
    classify_oi_slope,
    value_growth_pct,
    value_slope_ratio,
)


def compute_oi_window_state(window_payload: dict[str, dict[str, dict] | None], window_code: str) -> dict:
    row = window_payload[window_code]["OI"]
    slope_ratio = value_slope_ratio(row)
    growth_pct = value_growth_pct(row)
    slope_class = classify_oi_slope(window_code, slope_ratio)

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
        # Legacy DB columns remain nullable; the discarded interpreter no longer feeds them.
        "oi_stability": None,
        "oi_retention": None,
        "oi_breakdown": breakdown,
        "oi_pattern_code": pattern,
        "oi_pattern_label": PATTERN_LABELS[pattern],
        "window_growth_pct": round(growth_pct, 4),
        "window_weight": WINDOW_WEIGHTS[window_code],
        "oi_slope_ratio": round(slope_ratio, 6),
        "oi_slope_class": slope_class,
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
        "oi_stability_summary": None,
        "oi_retention_summary": None,
        "oi_breakdown_summary": _pick_summary(oi_window_states, "oi_breakdown", ["30м", "1ч", "4ч"]),
        "oi_slope_class_15m": (by_window.get("15м") or {}).get("oi_slope_class", "flat"),
        "oi_slope_class_30m": (by_window.get("30м") or {}).get("oi_slope_class", "flat"),
        "oi_slope_class_1h": (by_window.get("1ч") or {}).get("oi_slope_class", "flat"),
        "oi_slope_class_4h": (by_window.get("4ч") or {}).get("oi_slope_class", "flat"),
        "oi_slope_ratio_15m": (by_window.get("15м") or {}).get("oi_slope_ratio", 1.0),
        "oi_slope_ratio_30m": (by_window.get("30м") or {}).get("oi_slope_ratio", 1.0),
        "oi_slope_ratio_1h": (by_window.get("1ч") or {}).get("oi_slope_ratio", 1.0),
        "oi_slope_ratio_4h": (by_window.get("4ч") or {}).get("oi_slope_ratio", 1.0),
        "oi_growth_pct_15m": (by_window.get("15м") or {}).get("window_growth_pct", 0.0),
        "oi_growth_pct_30m": (by_window.get("30м") or {}).get("window_growth_pct", 0.0),
        "oi_growth_pct_1h": (by_window.get("1ч") or {}).get("window_growth_pct", 0.0),
        "oi_growth_pct_4h": (by_window.get("4ч") or {}).get("window_growth_pct", 0.0),
    }
