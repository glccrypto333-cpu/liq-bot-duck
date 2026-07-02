from __future__ import annotations

from phase_common import classify_oi_slope, value_slope_ratio

PRICE_30M_STAGE3_BLOCK_CLASSES = {"weak_down", "strong_down"}
PRICE_1H_STAGE3_BLOCK_CLASSES = {"weak_down", "strong_down"}
PRICE_4H_HARD_BLOCK_CLASS = "strong_down"
PRICE_4H_STAGE3_BLOCK_CLASS = "weak_down"


def compute_price_window_state(window_payload: dict[str, dict[str, dict] | None], oi_state: dict, window_code: str) -> dict:
    del oi_state

    row = window_payload[window_code]["PRICE"]
    slope_ratio = value_slope_ratio(row)
    slope_class = classify_oi_slope(window_code, slope_ratio)

    if window_code == "4ч" and slope_class == PRICE_4H_HARD_BLOCK_CLASS:
        state = "цена_4ч_сильно_вниз"
        breakdown = "жесткий_блок"
    elif window_code == "4ч" and slope_class == PRICE_4H_STAGE3_BLOCK_CLASS:
        state = "цена_4ч_слабо_вниз"
        breakdown = "блок_3"
    elif window_code == "1ч" and slope_class in PRICE_1H_STAGE3_BLOCK_CLASSES:
        state = "цена_1ч_вниз"
        breakdown = "блок_3"
    elif window_code == "30м" and slope_class in PRICE_30M_STAGE3_BLOCK_CLASSES:
        state = "цена_30м_вниз"
        breakdown = "блок_3"
    else:
        state = "цена_не_блокирует"
        breakdown = "нет"

    return {
        "window_code": window_code,
        "price_direction": slope_class,
        "price_regime": state,
        "price_structure": slope_class,
        "price_displacement": "n/a",
        "price_acceptance": "n/a",
        "price_breakdown": breakdown,
        "price_state_code": state,
        "price_state_label": state,
        "stage_block_level": (
            "жесткий"
            if state == "цена_4ч_сильно_вниз"
            else "стадия_3"
            if state in {"цена_4ч_слабо_вниз", "цена_1ч_вниз", "цена_30м_вниз"}
            else "нет"
        ),
        "price_slope_ratio": round(slope_ratio, 6),
        "price_growth_pct": 0.0,
        "price_deviation_from_median_pct": 0.0,
    }


def summarize_price(window_states: list[dict], target_stage: int) -> tuple[str, str, bool, int, str, str]:
    del target_stage

    states_by_window = {state["window_code"]: state for state in window_states}
    state_4h = states_by_window.get("4ч", {})
    state_1h = states_by_window.get("1ч", {})
    state_30m = states_by_window.get("30м", {})
    state_code_4h = state_4h.get("price_state_code", "цена_не_блокирует")
    state_code_1h = state_1h.get("price_state_code", "цена_не_блокирует")
    state_code_30m = state_30m.get("price_state_code", "цена_не_блокирует")
    price_direction_1h = state_1h.get("price_direction", "flat")
    price_direction_30m = state_30m.get("price_direction", "flat")

    if state_code_4h == "цена_4ч_сильно_вниз":
        return "цена_4ч_сильно_вниз", "жесткий_блок_роста_по_цене_4ч", True, 1, price_direction_30m, price_direction_1h
    if state_code_4h == "цена_4ч_слабо_вниз":
        return "цена_4ч_слабо_вниз", "блок_стадии_3_по_цене_4ч", False, 2, price_direction_30m, price_direction_1h
    if state_code_1h == "цена_1ч_вниз":
        return "цена_1ч_вниз", "блок_стадии_3_по_цене_1ч", False, 2, price_direction_30m, price_direction_1h
    if state_code_30m == "цена_30м_вниз":
        return "цена_30м_вниз", "блок_стадии_3_по_цене_30м", False, 2, price_direction_30m, price_direction_1h

    return "цена_не_блокирует", "нет", False, 3, price_direction_30m, price_direction_1h
