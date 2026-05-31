from __future__ import annotations

from phase_common import (
    deviation_from_median_pct,
    value_growth_pct,
    value_slope_ratio,
)

PRICE_1H_THRESHOLDS = {
    "strong_down": 0.98,
    "down": 0.995,
    "flat_high": 1.005,
    "up": 1.02,
}

PRICE_4H_SIDEWAYS_DEVIATION_PCT = 7.0


def _classify_price_direction_1h(slope_ratio: float) -> str:
    if slope_ratio < PRICE_1H_THRESHOLDS["strong_down"]:
        return "сильно_вниз"
    if slope_ratio < PRICE_1H_THRESHOLDS["down"]:
        return "вниз"
    if slope_ratio <= PRICE_1H_THRESHOLDS["flat_high"]:
        return "боковик"
    if slope_ratio <= PRICE_1H_THRESHOLDS["up"]:
        return "рост"
    return "сильный_рост"


def _classify_price_regime_4h(deviation_pct: float) -> str:
    if deviation_pct < -PRICE_4H_SIDEWAYS_DEVIATION_PCT:
        return "ниже_боковика"
    if deviation_pct > PRICE_4H_SIDEWAYS_DEVIATION_PCT:
        return "выше_боковика"
    return "боковик"


def compute_price_window_state(window_payload: dict[str, dict[str, dict] | None], oi_state: dict, window_code: str) -> dict:
    row = window_payload[window_code]["PRICE"]
    slope_ratio = value_slope_ratio(row)
    growth_pct = value_growth_pct(row)

    if window_code == "1ч":
        direction = _classify_price_direction_1h(slope_ratio)
        regime = "рабочее_окно"
        structure = direction
    elif window_code == "4ч":
        direction = _classify_price_direction_1h(slope_ratio)
        regime = _classify_price_regime_4h(deviation_from_median_pct(row))
        structure = regime
    else:
        direction = "ignored"
        regime = "ignored"
        structure = "ignored"

    if direction in {"сильно_вниз", "вниз"} or regime == "ниже_боковика":
        state = "напряжение"
        breakdown = "риск"
    elif direction in {"рост", "сильный_рост"} or regime == "выше_боковика":
        state = "поддерживает_набор"
        breakdown = "нет"
    else:
        state = "нейтральна"
        breakdown = "нет"

    displacement_abs = abs(growth_pct)
    if displacement_abs < 0.5:
        displacement = "слабое"
    elif displacement_abs < 1.5:
        displacement = "заметное"
    else:
        displacement = "сильное"

    if window_code == "4ч":
        acceptance = "внутри_боковика" if regime == "боковик" else "вне_боковика"
    else:
        acceptance = "n/a"

    return {
        "window_code": window_code,
        "price_direction": direction,
        "price_regime": regime,
        "price_structure": structure,
        "price_displacement": displacement,
        "price_acceptance": acceptance,
        "price_breakdown": breakdown,
        "price_state_code": state,
        "price_state_label": state,
        "stage_block_level": "нет",
        "price_slope_ratio": round(slope_ratio, 6),
        "price_growth_pct": round(growth_pct, 4),
        "price_deviation_from_median_pct": round(deviation_from_median_pct(row), 4),
    }


def summarize_price(window_states: list[dict], target_stage: int) -> tuple[str, str, bool, int]:
    states_by_window = {state["window_code"]: state for state in window_states}
    state_1h = states_by_window.get("1ч", {})
    state_4h = states_by_window.get("4ч", {})

    direction_1h = state_1h.get("price_direction", "ignored")
    regime_4h = state_4h.get("price_regime", "ignored")

    if regime_4h == "ниже_боковика" and direction_1h in {"вниз", "сильно_вниз"}:
        return "ломает_сценарий", "блок_всего_сценария", True, 0

    if direction_1h in {"вниз", "сильно_вниз"}:
        return "напряжение", "нет", False, 3

    if regime_4h == "выше_боковика" or direction_1h in {"рост", "сильный_рост"}:
        return "поддерживает_набор", "нет", False, 3

    return "нейтральна", "нет", False, 3
