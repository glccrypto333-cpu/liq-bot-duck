from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from phase_service import (
    apply_stage_guardrails,
    compute_transition_permission,
    determine_target_stage,
)


def _oi_summary(
    oi_15m: str = "good_up",
    oi_30m: str = "good_up",
    oi_1h: str = "strong_up",
    oi_4h: str = "good_up",
) -> dict:
    return {
        "oi_slope_class_15m": oi_15m,
        "oi_slope_class_30m": oi_30m,
        "oi_slope_class_1h": oi_1h,
        "oi_slope_class_4h": oi_4h,
    }


def test_weak_down_on_price_4h_caps_stage3_to_stage2() -> None:
    oi_summary = _oi_summary()
    price_summary = ("цена_4ч_слабо_вниз", "блок_стадии_3_по_цене_4ч", False, 2)

    target_stage, reason = determine_target_stage(oi_summary, price_summary, ("пустой", "нейтрально"))

    assert target_stage == 2
    assert "цена_4ч_слабо_вниз" in reason


def test_stage3_holds_on_price_4h_weak_down() -> None:
    previous_state = {"current_stage": 3}
    oi_summary = _oi_summary()
    price_summary = ("цена_4ч_слабо_вниз", "блок_стадии_3_по_цене_4ч", False, 2)

    guarded_stage, guard_reason = apply_stage_guardrails(
        previous_state,
        2,
        oi_summary,
        price_summary,
        ("пустой", "нейтрально"),
        previous_stage_age_minutes=45.0,
        trigger_age_minutes=90.0,
    )
    permission = compute_transition_permission(
        previous_state,
        guarded_stage,
        45.0,
        oi_summary,
        price_summary,
        90.0,
    )

    assert guarded_stage == 3
    assert guard_reason == "удержание_3:только_ручной_или_по_oi_4ч"
    assert permission == "удержание_3"


def test_stage3_holds_on_price_4h_strong_down() -> None:
    previous_state = {"current_stage": 3}
    oi_summary = _oi_summary()
    price_summary = ("цена_4ч_сильно_вниз", "жесткий_блок_роста_по_цене_4ч", True, 1)

    guarded_stage, guard_reason = apply_stage_guardrails(
        previous_state,
        0,
        oi_summary,
        price_summary,
        ("пустой", "нейтрально"),
        previous_stage_age_minutes=45.0,
        trigger_age_minutes=90.0,
    )
    permission = compute_transition_permission(
        previous_state,
        guarded_stage,
        45.0,
        oi_summary,
        price_summary,
        90.0,
    )

    assert guarded_stage == 3
    assert guard_reason == "удержание_3:только_ручной_или_по_oi_4ч"
    assert permission == "удержание_3"
