from __future__ import annotations

import math

WINDOWS = ["15м", "30м", "1ч", "4ч", "12ч", "24ч"]
WINDOW_WEIGHTS = {
    "15м": 1.0,
    "30м": 1.5,
    "1ч": 2.0,
    "4ч": 2.5,
    "12ч": 2.0,
    "24ч": 1.0,
}

PATTERN_LABELS = {
    "мертвая_форма": "мертвая_форма",
    "тихое_накопление": "тихое_накопление",
    "развивающийся_набор": "развивающийся_набор",
    "подтвержденный_набор": "подтвержденный_набор",
    "ложный_всплеск": "ложный_всплеск",
    "рваный_хаос": "рваный_хаос",
    "поломка_набора": "поломка_набора",
}

OI_SLOPE_THRESHOLDS = {
    "15м": {
        "strong_down": 0.97,
        "weak_down": 0.995,
        "flat_high": 1.0027,
        "weak_up": 1.012,
        "good_up": 1.035,
    },
    "30м": {
        "strong_down": 0.96,
        "weak_down": 0.99,
        "flat_high": 1.0044,
        "weak_up": 1.012,
        "good_up": 1.05,
    },
    "1ч": {
        "strong_down": 0.95,
        "weak_down": 0.99,
        "flat_high": 1.0040,
        "weak_up": 1.015,
        "good_up": 1.05,
    },
    "4ч": {
        "strong_down": 0.94,
        "weak_down": 0.996,
        "flat_high": 1.01,
        "weak_up": 1.035,
        "good_up": 1.095,
    },
    # Пока повторяем 4ч как временный safe fallback до отдельной калибровки 12ч/24ч.
    "12ч": {
        "strong_down": 0.94,
        "weak_down": 0.99,
        "flat_high": 1.018,
        "weak_up": 1.05,
        "good_up": 1.13,
    },
    "24ч": {
        "strong_down": 0.94,
        "weak_down": 0.99,
        "flat_high": 1.018,
        "weak_up": 1.05,
        "good_up": 1.13,
    },
}


def build_symbol_window_payload(window_map: dict[str, dict[str, dict]]) -> dict[str, dict[str, dict] | None]:
    payload = {}
    for window_code in WINDOWS:
        metric_rows = window_map.get(window_code, {})
        payload[window_code] = {
            "OI": metric_rows.get("OI"),
            "PRICE": metric_rows.get("PRICE"),
            "VOLUME": metric_rows.get("VOLUME"),
        }
    return payload


def delta_pct(row: dict | None) -> float:
    if not row:
        return 0.0
    return float(row.get("delta_pct") or 0.0)


def signed_log_delta(delta: float) -> float:
    if delta == 0:
        return 0.0
    return math.copysign(math.log10(1.0 + abs(delta)), delta)


def range_position(row: dict | None) -> float:
    if not row:
        return 0.5
    high = float(row.get("high_value") or 0.0)
    low = float(row.get("low_value") or 0.0)
    close = float(row.get("close_value") or 0.0)
    if high <= low:
        return 0.5
    return max(0.0, min(1.0, (close - low) / (high - low)))


def _safe_ratio(numerator: float, denominator: float, fallback: float = 1.0) -> float:
    if denominator == 0:
        return fallback
    return numerator / denominator


def value_slope_ratio(row: dict | None) -> float:
    if not row:
        return 1.0
    open_value = float(row.get("open_value") or 0.0)
    close_value = float(row.get("close_value") or 0.0)
    return _safe_ratio(close_value, open_value, fallback=1.0)


def value_growth_pct(row: dict | None) -> float:
    slope_ratio = value_slope_ratio(row)
    return (slope_ratio - 1.0) * 100.0


def range_median(row: dict | None) -> float:
    if not row:
        return 0.0
    high_value = float(row.get("high_value") or 0.0)
    low_value = float(row.get("low_value") or 0.0)
    if high_value == 0 and low_value == 0:
        return float(row.get("close_value") or 0.0)
    return (high_value + low_value) / 2.0


def deviation_from_median_pct(row: dict | None) -> float:
    if not row:
        return 0.0
    median = range_median(row)
    close_value = float(row.get("close_value") or 0.0)
    if median == 0:
        return 0.0
    return ((close_value - median) / median) * 100.0


def classify_oi_slope(window_code: str, slope_ratio: float) -> str:
    thresholds = OI_SLOPE_THRESHOLDS.get(window_code, OI_SLOPE_THRESHOLDS["4ч"])
    if slope_ratio < thresholds["strong_down"]:
        return "strong_down"
    if slope_ratio < thresholds["weak_down"]:
        return "weak_down"
    if slope_ratio <= thresholds["flat_high"]:
        return "flat"
    if slope_ratio <= thresholds["weak_up"]:
        return "weak_up"
    if slope_ratio <= thresholds["good_up"]:
        return "good_up"
    return "strong_up"
