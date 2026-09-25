from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from phase_service import apply_stage_guardrails, compute_transition_permission, determine_target_stage


PRICE_OK = ("цена_не_блокирует", "нет", False, 3)
VOLUME_DUMMY = ("пустой", "нейтрально")


def summary(oi_15m: str, oi_30m: str, oi_1h: str, oi_4h: str) -> dict:
    return {
        "oi_slope_class_15m": oi_15m,
        "oi_slope_class_30m": oi_30m,
        "oi_slope_class_1h": oi_1h,
        "oi_slope_class_4h": oi_4h,
        "oi_growth_pct_1h": 4.5,
    }


def test_ordinary_2_to_3_waits_until_trigger_is_60_minutes() -> None:
    oi = summary("flat", "good_up", "good_up", "good_up")
    raw_target, _ = determine_target_stage(oi, PRICE_OK, VOLUME_DUMMY)
    assert raw_target == 3

    for trigger_age in (45.0, 55.0):
        stage, reason = apply_stage_guardrails(
            {"current_stage": 2}, raw_target, oi, PRICE_OK, VOLUME_DUMMY, 30.0, trigger_age
        )
        assert stage == 2
        assert reason == "удержание_2:ждем_1ч_от_триггера"

    stage, reason = apply_stage_guardrails(
        {"current_stage": 2}, raw_target, oi, PRICE_OK, VOLUME_DUMMY, 30.0, 60.0
    )
    assert stage == 3
    assert reason == "переход_2_3:30м_зрелое_1ч_подтверждает_силу"


def test_early_2_to_3_rejects_4h_weak_down() -> None:
    oi = summary("good_up", "good_up", "strong_up", "weak_down")
    raw_target, _ = determine_target_stage(oi, PRICE_OK, VOLUME_DUMMY)

    stage, reason = apply_stage_guardrails(
        {"current_stage": 2}, raw_target, oi, PRICE_OK, VOLUME_DUMMY, 10.0, 30.0
    )

    assert stage == 2
    assert reason == "удержание_2:oi_4h_decline:weak_down"


def test_early_2_to_3_still_allows_ideal_case_at_30_minutes() -> None:
    oi = summary("good_up", "good_up", "strong_up", "good_up")
    raw_target, _ = determine_target_stage(oi, PRICE_OK, VOLUME_DUMMY)

    stage, reason = apply_stage_guardrails(
        {"current_stage": 2}, raw_target, oi, PRICE_OK, VOLUME_DUMMY, 10.0, 30.0
    )

    assert stage == 3
    assert reason == "переход_2_3:ранний_выпуск_A_15м_подтвердил_импульс"


def test_transition_permission_uses_the_same_early_and_ordinary_clock() -> None:
    ordinary = summary("flat", "good_up", "good_up", "good_up")
    early = summary("good_up", "good_up", "strong_up", "good_up")

    assert compute_transition_permission(
        {"current_stage": 2}, 3, 30.0, ordinary, PRICE_OK, 55.0
    ) == "ждем_1ч_от_триггера"
    assert compute_transition_permission(
        {"current_stage": 2}, 3, 10.0, early, PRICE_OK, 30.0
    ) == "разрешен_ранний_вход_в_3"
