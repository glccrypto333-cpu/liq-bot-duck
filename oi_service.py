from __future__ import annotations

from phase_common import (
    PATTERN_LABELS,
    WINDOW_WEIGHTS,
    classify_oi_slope,
    concentration_ratio_from_points,
    flat_tail_ratio_from_points,
    pullback_ratio_from_ohlc,
    pullback_ratio_from_points,
    retention_ratio_from_ohlc,
    retention_ratio_from_points,
    silent_build_ratio_from_points,
    smoothness_ratio_from_points,
    smoothness_proxy_from_ohlc,
    tail_share_from_points,
    trajectory_points,
    value_growth_pct,
    value_slope_ratio,
)


def _classify_hold_text(retention_ratio: float) -> str:
    if retention_ratio >= 0.85:
        return "сильное"
    if retention_ratio >= 0.65:
        return "хорошее"
    if retention_ratio >= 0.40:
        return "слабое"
    if retention_ratio >= 0.0:
        return "плохое"
    return "нет"


def _classify_pullback_text(pullback_ratio: float) -> str:
    if pullback_ratio <= 0.15:
        return "почти_нет"
    if pullback_ratio <= 0.30:
        return "легкий"
    if pullback_ratio <= 0.50:
        return "заметный"
    if pullback_ratio <= 0.80:
        return "глубокий"
    return "срыв"


def _classify_smoothness_text(smoothness_proxy: float) -> str:
    if smoothness_proxy >= 0.90:
        return "очень_гладко"
    if smoothness_proxy >= 0.65:
        return "гладко"
    if smoothness_proxy >= 0.50:
        return "средне"
    if smoothness_proxy >= 0.35:
        return "рвано"
    return "пила"


def _classify_concentration_text(concentration_ratio: float) -> str:
    if concentration_ratio <= 0.20:
        return "почти_нет"
    if concentration_ratio <= 0.35:
        return "слабая"
    if concentration_ratio <= 0.55:
        return "средняя"
    if concentration_ratio <= 0.75:
        return "сильная"
    return "доминирующая"


def _classify_tail_share_text(tail_share: float) -> str:
    if tail_share <= 0.10:
        return "почти_нет"
    if tail_share <= 0.25:
        return "легкий"
    if tail_share <= 0.45:
        return "заметный"
    if tail_share <= 0.65:
        return "сильный"
    return "доминирующий"


def _classify_flat_tail_text(flat_tail_ratio: float) -> str:
    if flat_tail_ratio <= 0.10:
        return "нет"
    if flat_tail_ratio <= 0.22:
        return "слабый"
    if flat_tail_ratio <= 0.38:
        return "заметный"
    if flat_tail_ratio <= 0.55:
        return "сильный"
    return "мертвый"


def _form_score(form_text: str) -> int:
    return {
        "очень_гладко": 5,
        "гладко": 4,
        "средне": 3,
        "рвано": 2,
        "всплеск_с_боковиком": 1,
    }.get(form_text, 1)


def _classify_form_text(
    smoothness_proxy: float,
    concentration_ratio: float,
    pullback_ratio: float,
    tail_share: float,
    flat_tail_ratio: float,
) -> str:
    if concentration_ratio >= 0.65 and tail_share <= 0.12 and flat_tail_ratio >= 0.45:
        return "всплеск_с_боковиком"
    if concentration_ratio >= 0.80 and tail_share <= 0.08:
        return "всплеск_с_боковиком"
    if smoothness_proxy >= 0.90 and concentration_ratio <= 0.45 and tail_share >= 0.20 and pullback_ratio <= 0.12:
        return "очень_гладко"
    if smoothness_proxy >= 0.70 and concentration_ratio <= 0.65 and tail_share >= 0.12 and pullback_ratio <= 0.25:
        return "гладко"
    if smoothness_proxy >= 0.45 and concentration_ratio <= 0.80 and pullback_ratio <= 0.45:
        return "средне"
    return "рвано"


def compute_oi_window_state(window_payload: dict[str, dict[str, dict] | None], window_code: str) -> dict:
    row = window_payload[window_code]["OI"]
    slope_ratio = value_slope_ratio(row)
    growth_pct = value_growth_pct(row)
    slope_class = classify_oi_slope(window_code, slope_ratio)
    points = trajectory_points(row)
    retention_ratio = retention_ratio_from_points(points) if points else retention_ratio_from_ohlc(row)
    pullback_ratio = pullback_ratio_from_points(points) if points else pullback_ratio_from_ohlc(row)
    smoothness_proxy = smoothness_ratio_from_points(points) if points else smoothness_proxy_from_ohlc(row)
    concentration_ratio = concentration_ratio_from_points(points) if points else 1.0
    tail_share = tail_share_from_points(points) if points else 0.0
    flat_tail_ratio = flat_tail_ratio_from_points(points) if points else 0.0
    silent_build_ratio = silent_build_ratio_from_points(points) if points else slope_ratio
    hold_text = _classify_hold_text(retention_ratio)
    pullback_text = _classify_pullback_text(pullback_ratio)
    smoothness_text = _classify_smoothness_text(smoothness_proxy)
    concentration_text = _classify_concentration_text(concentration_ratio)
    tail_share_text = _classify_tail_share_text(tail_share)
    flat_tail_text = _classify_flat_tail_text(flat_tail_ratio)
    form_text = _classify_form_text(
        smoothness_proxy,
        concentration_ratio,
        pullback_ratio,
        tail_share,
        flat_tail_ratio,
    )
    form_score = _form_score(form_text)
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

    if smoothness_proxy >= 0.85 and pullback_ratio <= 0.20:
        stability = "высокая"
    elif smoothness_proxy >= 0.60 and pullback_ratio <= 0.50:
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
        "oi_concentration_ratio": round(concentration_ratio, 6),
        "oi_tail_share": round(tail_share, 6),
        "oi_flat_tail_ratio": round(flat_tail_ratio, 6),
        "oi_silent_build_ratio": round(silent_build_ratio, 6),
        "oi_silent_build_active": silent_build_active,
        "oi_hold_class": hold_text,
        "oi_pullback_class": pullback_text,
        "oi_smoothness_class": smoothness_text,
        "oi_concentration_class": concentration_text,
        "oi_tail_share_class": tail_share_text,
        "oi_flat_tail_class": flat_tail_text,
        "oi_form_class": form_text,
        "oi_form_score": form_score,
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
        "oi_growth_pct_15m": (by_window.get("15м") or {}).get("window_growth_pct", 0.0),
        "oi_growth_pct_30m": (by_window.get("30м") or {}).get("window_growth_pct", 0.0),
        "oi_growth_pct_1h": (by_window.get("1ч") or {}).get("window_growth_pct", 0.0),
        "oi_growth_pct_4h": (by_window.get("4ч") or {}).get("window_growth_pct", 0.0),
        "oi_retention_ratio_1h": (by_window.get("1ч") or {}).get("oi_retention_ratio", 0.0),
        "oi_pullback_ratio_1h": (by_window.get("1ч") or {}).get("oi_pullback_ratio", 1.0),
        "oi_smoothness_proxy_1h": (by_window.get("1ч") or {}).get("oi_smoothness_proxy", 0.0),
        "oi_concentration_ratio_1h": (by_window.get("1ч") or {}).get("oi_concentration_ratio", 1.0),
        "oi_tail_share_1h": (by_window.get("1ч") or {}).get("oi_tail_share", 0.0),
        "oi_tail_share_ratio_1h": (by_window.get("1ч") or {}).get("oi_tail_share", 0.0),
        "oi_flat_tail_ratio_1h": (by_window.get("1ч") or {}).get("oi_flat_tail_ratio", 0.0),
        "oi_hold_class_1h": (by_window.get("1ч") or {}).get("oi_hold_class", "нет"),
        "oi_pullback_class_1h": (by_window.get("1ч") or {}).get("oi_pullback_class", "срыв"),
        "oi_smoothness_class_1h": (by_window.get("1ч") or {}).get("oi_smoothness_class", "пила"),
        "oi_concentration_class_1h": (by_window.get("1ч") or {}).get("oi_concentration_class", "плохая"),
        "oi_tail_share_class_1h": (by_window.get("1ч") or {}).get("oi_tail_share_class", "доминирующий"),
        "oi_flat_tail_class_1h": (by_window.get("1ч") or {}).get("oi_flat_tail_class", "мертвый"),
        "oi_form_class_1h": (by_window.get("1ч") or {}).get("oi_form_class", "рвано"),
        "oi_form_score_1h": (by_window.get("1ч") or {}).get("oi_form_score", 1),
        "oi_silent_build_30m": bool((by_window.get("30м") or {}).get("oi_silent_build_active", False)),
        "oi_silent_build_1h": bool((by_window.get("1ч") or {}).get("oi_silent_build_active", False)),
    }
