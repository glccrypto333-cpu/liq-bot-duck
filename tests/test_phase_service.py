from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from phase_service import (
    apply_stage_guardrails,
    compute_transition_permission,
    determine_target_stage,
)


PRICE_OK = ("цена_не_блокирует", "нет", False, 3)
PRICE_BLOCK = ("цена_4ч_сильно_вниз", "жесткий_блок_роста_по_цене_4ч", True, 1)
VOLUME_DUMMY = ("пустой", "нейтрально")


def make_oi_summary(
    oi_15m: str = "flat",
    oi_30m: str = "flat",
    oi_1h: str = "flat",
    oi_4h: str = "flat",
) -> dict:
    return {
        "oi_slope_class_15m": oi_15m,
        "oi_slope_class_30m": oi_30m,
        "oi_slope_class_1h": oi_1h,
        "oi_slope_class_4h": oi_4h,
    }


def test_0_to_1_happens_when_hard_ban_is_removed() -> None:
    target_stage, _ = determine_target_stage(make_oi_summary(), PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(None, target_stage, make_oi_summary(), PRICE_OK, VOLUME_DUMMY, 0.0, 0.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "вход_в_1:жесткий_запрет_снят"


def test_price_block_keeps_stage_0() -> None:
    target_stage, reason = determine_target_stage(make_oi_summary(), PRICE_BLOCK, VOLUME_DUMMY)
    assert target_stage == 0
    assert reason == "цена_4ч_сильно_вниз:цена_4ч_сильно_вниз"


def test_stage_1_does_not_drop_to_0_on_4h_weak_down_alone() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_4h="weak_down")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 0.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "удержание_1:нет_живого_набора"


def test_1_to_2_requires_30m_live_confirmation_and_30_minutes() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_15m="good_up", oi_30m="weak_up", oi_1h="flat", oi_4h="flat")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 29.0, 29.0)
    assert target_stage == 2
    assert stage == 1
    assert reason == "удержание_1:30м_еще_не_зрелое"

    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 30.0, 30.0)
    assert stage == 1
    assert reason == "удержание_1:30м_еще_не_зрелое"


def test_1_to_2_fails_if_15m_does_not_confirm_after_trigger() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_15m="weak_down", oi_30m="weak_up", oi_1h="weak_up", oi_4h="flat")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 45.0, 30.0)
    assert target_stage == 2
    assert stage == 1
    assert reason == "удержание_1:15м_не_подтвердило_старт"


def test_strong_15m_without_30m_confirmation_does_not_create_stage_2() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_15m="strong_up", oi_30m="flat", oi_1h="flat", oi_4h="flat")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 45.0, 15.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "удержание_1:нет_живого_набора"


def test_2_to_3_requires_mature_30m_and_positive_1h() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="flat", oi_30m="good_up", oi_1h="weak_up", oi_4h="weak_down")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 29.0, 59.0)
    assert target_stage == 3
    assert stage == 2
    assert reason == "удержание_2:ждем_30_минут"

    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 30.0, 59.0)
    assert stage == 2
    assert reason == "удержание_2:ждем_1ч_от_триггера"

    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 30.0, 60.0)
    assert stage == 3
    assert reason == "переход_2_3:30м_зрелое_1ч_подтверждает_силу"


def test_2_to_3_allows_15m_weak_down_as_local_pause() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="weak_down", oi_30m="strong_up", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 35.0, 60.0)
    assert target_stage == 3
    assert stage == 3
    assert reason == "переход_2_3:30м_зрелое_1ч_подтверждает_силу"


def test_15m_strong_down_does_not_drop_stage_2_but_blocks_fresh_stage_3() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="strong_down", oi_30m="good_up", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 35.0, 60.0)
    assert target_stage == 2
    assert stage == 2
    assert reason == "удержание_2:15м_локально_слабое"


def test_30m_decline_drops_stage_2_to_1() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="weak_down", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 40.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:oi_30м=weak_down"


def test_1h_decline_drops_stage_2_to_1() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="good_up", oi_1h="weak_down", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 40.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:oi_1ч=weak_down"


def test_stage_2_drops_to_1_when_live_build_is_gone_even_without_explicit_decline() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="flat", oi_30m="flat", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 480.0, 480.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:живой_набор_умер"


def test_4h_strong_down_resets_stage_1_to_0() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_4h="strong_down")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 0.0)
    assert target_stage == 0
    assert stage == 0
    assert reason == "снижение_1_0:oi_4ч=strong_down"


def test_stage_3_degrades_on_strong_price_4h_block() -> None:
    previous_state = {"current_stage": 3}
    stage, reason = apply_stage_guardrails(previous_state, 0, make_oi_summary(oi_4h="good_up"), PRICE_BLOCK, VOLUME_DUMMY, 12.0, 0.0)
    assert stage == 1
    assert reason == "снижение_3_1:цена_4ч=цена_4ч_сильно_вниз"


def test_stage_3_resets_on_4h_oi_weak_down() -> None:
    previous_state = {"current_stage": 3}
    stage, reason = apply_stage_guardrails(previous_state, 0, make_oi_summary(oi_4h="weak_down"), PRICE_BLOCK, VOLUME_DUMMY, 300.0, 0.0)
    assert stage == 0
    assert reason == "сброс_3_0:oi_4ч=weak_down"


def test_transition_permission_uses_new_codes_for_stage_2_wait() -> None:
    previous_state = {"current_stage": 1}
    permission = compute_transition_permission(
        previous_state,
        2,
        10.0,
        make_oi_summary(oi_15m="good_up", oi_30m="weak_up"),
        PRICE_OK,
        10.0,
    )
    assert permission == "удержание_1_по_30м"


def test_1_to_2_requires_strong_1h_when_15m_only_weak_up() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_15m="weak_up", oi_30m="good_up", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 45.0, 45.0)
    assert target_stage == 3
    assert stage == 1
    assert reason == "удержание_1:1ч_еще_не_сильный"


def test_1_to_2_allows_weak_15m_if_30m_is_mature_and_1h_is_strong() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_15m="weak_up", oi_30m="good_up", oi_1h="strong_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 45.0, 45.0)
    assert target_stage == 3
    assert stage == 2
    assert reason == "переход_1_2:30м_подтвердило_живой_набор"


def test_transition_permission_marks_stage_3_hold() -> None:
    previous_state = {"current_stage": 3}
    permission = compute_transition_permission(previous_state, 0, 10.0, make_oi_summary(), PRICE_OK, 0.0)
    assert permission == "удержание_3"


def test_transition_permission_marks_stage_3_reset_by_4h_oi() -> None:
    previous_state = {"current_stage": 3}
    permission = compute_transition_permission(previous_state, 0, 10.0, make_oi_summary(oi_4h="weak_down"), PRICE_OK, 0.0)
    assert permission == "сброс_3_0_по_oi_4ч"
