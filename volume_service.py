from __future__ import annotations

from phase_common import delta_pct, signed_log_delta


def compute_volume_window_state(window_payload: dict[str, dict[str, dict] | None], oi_state: dict, window_code: str) -> dict:
    row = window_payload[window_code]["VOLUME"]
    raw_delta = delta_pct(row)
    delta = signed_log_delta(raw_delta)
    oi_direction = oi_state["oi_direction"]
    oi_angle = oi_state["oi_angle"]

    if delta > 0.35:
        direction = "растет"
    elif delta < -0.35:
        direction = "снижается"
    else:
        direction = "нейтрально"

    strength = abs(delta)
    if strength < 0.35:
        support = "нет"
    elif strength < 0.75:
        support = "слабая"
    elif strength < 1.35:
        support = "рабочая"
    else:
        support = "сильная"

    if delta >= 2.7:
        impulse = "перегретый"
    elif delta >= 1.8:
        impulse = "подтверждающий"
    elif delta >= 1.0:
        impulse = "локальный"
    else:
        impulse = "нет"

    if delta >= 1.4:
        retention = "удержан"
    elif delta >= 0.6:
        retention = "частично_удержан"
    else:
        retention = "не_удержан"

    if oi_direction == "вверх" and direction == "снижается":
        divergence = "сильное"
    elif oi_direction == "вверх" and support == "нет":
        divergence = "слабое"
    else:
        divergence = "нет"

    if divergence == "сильное":
        state = "расходится"
        effect = "ослабляет"
    elif impulse == "перегретый":
        state = "перегретый"
        effect = "ослабляет"
    elif support == "сильная" and oi_angle in {"рабочий", "сильный"}:
        state = "подтверждающий"
        effect = "усиливает"
    elif support == "рабочая":
        state = "рабочий"
        effect = "усиливает"
    elif support == "слабая":
        state = "слабый"
        effect = "нейтрально"
    else:
        state = "пустой"
        effect = "нейтрально"

    return {
        "window_code": window_code,
        "volume_direction": direction,
        "volume_support": support,
        "volume_impulse": impulse,
        "volume_retention": retention,
        "volume_divergence": divergence,
        "volume_state_code": state,
        "volume_state_label": state,
        "confidence_effect": effect,
    }


def summarize_volume(window_states: list[dict], target_stage: int) -> tuple[str, str]:
    priority_windows = {
        1: ["15м", "30м", "1ч"],
        2: ["30м", "1ч", "4ч"],
        3: ["1ч", "4ч", "12ч"],
    }.get(target_stage, ["15м", "30м", "1ч"])
    candidates = [state for state in window_states if state["window_code"] in priority_windows]

    if target_stage == 3:
        hard_windows = [state for state in candidates if state["window_code"] in {"1ч", "4ч"}]
        soft_windows = [state for state in candidates if state["window_code"] == "12ч"]

        for state in hard_windows:
            if state["volume_state_code"] == "расходится":
                return "расходится", "ослабляет"
        for state in hard_windows:
            if state["volume_state_code"] == "подтверждающий":
                return "подтверждающий", "усиливает"
        for state in hard_windows:
            if state["volume_state_code"] == "рабочий":
                return "рабочий", "усиливает"
        for state in hard_windows:
            if state["volume_state_code"] == "перегретый":
                return "перегретый", "ослабляет"
        for state in soft_windows:
            if state["volume_state_code"] == "расходится":
                return "слабый", "нейтрально"

    for state in candidates:
        if state["volume_state_code"] == "расходится":
            return "расходится", "ослабляет"
    for state in candidates:
        if state["volume_state_code"] == "подтверждающий":
            return "подтверждающий", "усиливает"
    for state in candidates:
        if state["volume_state_code"] == "рабочий":
            return "рабочий", "усиливает"
    for state in candidates:
        if state["volume_state_code"] == "перегретый":
            return "перегретый", "ослабляет"
    for state in candidates:
        if state["volume_state_code"] == "слабый":
            return "слабый", "нейтрально"
    return "пустой", "нейтрально"
