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


def test_2_to_3_is_blocked_by_4h_oi_weak_down() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="flat", oi_30m="good_up", oi_1h="weak_up", oi_4h="weak_down")
    summary["oi_growth_pct_1h"] = 9.0
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 30.0, 60.0)
    assert target_stage == 2
    assert stage == 2
    assert reason == "удержание_2:oi_4h_decline:weak_down"


def test_2_to_3_requires_mature_30m_and_mature_1h() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="flat", oi_30m="good_up", oi_1h="weak_up", oi_4h="flat")
    summary["oi_growth_pct_1h"] = 9.0
    target_stage, reason = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    assert target_stage == 2
    assert reason == "30м_подтвердило_живой_набор"


def test_2_to_3_allows_15m_weak_down_as_local_pause() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="weak_down", oi_30m="strong_up", oi_1h="good_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 9.0
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 35.0, 60.0)
    assert target_stage == 3
    assert stage == 3
    assert reason == "переход_2_3:30м_зрелое_1ч_подтверждает_силу"


def test_15m_strong_down_does_not_drop_stage_2_but_blocks_fresh_stage_3() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="strong_down", oi_30m="good_up", oi_1h="good_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 9.0
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 35.0, 60.0)
    assert target_stage == 2
    assert stage == 2
    assert reason == "удержание_2:15м_локально_слабое"


def test_30m_strong_down_drops_stage_2_to_1() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="strong_down", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 40.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:oi_30м=strong_down"


def test_30m_weak_down_drops_stage_2_to_1_per_canonical_matrix() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="weak_down", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 40.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:oi_30м=weak_down"


def test_1h_decline_drops_stage_2_to_1_per_canonical_matrix() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="good_up", oi_1h="weak_down", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 40.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:oi_1ч=weak_down"


def test_strong_1h_decline_also_drops_stage_2_to_1() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="good_up", oi_1h="strong_down", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 40.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:oi_1ч=strong_down"


def test_target_stage_below_2_drops_stage_2_to_1_per_canonical_matrix() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="flat", oi_30m="flat", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 480.0, 480.0)
    assert target_stage == 1
    assert stage == 1
    assert reason == "снижение_2_1:target_stage<=1"


def test_4h_hard_decline_drops_stage_2_to_1_per_canonical_matrix() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="good_up", oi_4h="strong_down")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, _ = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 45.0, 60.0)
    assert target_stage == 0
    assert stage == 1


def test_4h_weak_down_does_not_downgrade_stage_2() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_30m="good_up", oi_1h="strong_up", oi_4h="weak_down")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, _ = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 20.0, 40.0)
    assert target_stage == 2
    assert stage == 2


def test_local_price_1h_or_30m_block_does_not_downgrade_stage_2() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="good_up", oi_30m="good_up", oi_1h="good_up", oi_4h="good_up")
    for price_state in ("цена_1ч_вниз", "цена_30м_вниз"):
        price_summary = (price_state, "локальный_блок_новой_3", False, 2)
        target_stage, _ = determine_target_stage(summary, price_summary, VOLUME_DUMMY)
        stage, _ = apply_stage_guardrails(previous_state, target_stage, summary, price_summary, VOLUME_DUMMY, 45.0, 60.0)
        assert target_stage == 2
        assert stage == 2


def test_stage2_hard_price_block_downgrades_to_stage1_per_canonical_matrix() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="good_up", oi_30m="good_up", oi_1h="good_up", oi_4h="good_up")
    stage, reason = apply_stage_guardrails(previous_state, 0, summary, PRICE_BLOCK, VOLUME_DUMMY, 45.0, 60.0)
    permission = compute_transition_permission(previous_state, 0, 45.0, summary, PRICE_BLOCK, 60.0)
    assert stage == 1
    assert reason == "снижение_2_1:цена_4ч=цена_4ч_сильно_вниз"
    assert permission == "снижение_2_1_по_цене_4ч"


def test_transition_permission_matches_canonical_stage2_downgrade_matrix() -> None:
    previous_state = {"current_stage": 2}
    assert compute_transition_permission(
        previous_state, 1, 45.0,
        make_oi_summary(oi_30m="weak_down", oi_1h="good_up", oi_4h="good_up"), PRICE_OK, 60.0
    ) == "снижение_2_1_по_oi_30м"
    assert compute_transition_permission(
        previous_state, 1, 45.0,
        make_oi_summary(oi_30m="good_up", oi_1h="weak_down", oi_4h="good_up"), PRICE_OK, 60.0
    ) == "снижение_2_1_по_oi_1ч"
    assert compute_transition_permission(
        previous_state, 1, 45.0,
        make_oi_summary(oi_30m="good_up", oi_1h="strong_down", oi_4h="good_up"), PRICE_OK, 60.0
    ) == "снижение_2_1_по_oi_1ч"
    assert compute_transition_permission(
        previous_state, 1, 45.0,
        make_oi_summary(oi_30m="flat", oi_1h="good_up", oi_4h="good_up"), PRICE_OK, 60.0
    ) == "снижение_2_1_по_target_stage"
    assert compute_transition_permission(
        previous_state, 0, 45.0,
        make_oi_summary(oi_30m="strong_down", oi_1h="good_up", oi_4h="good_up"), PRICE_OK, 60.0
    ) == "снижение_2_1_по_oi_30м"
    assert compute_transition_permission(
        previous_state, 0, 45.0,
        make_oi_summary(oi_30m="good_up", oi_1h="good_up", oi_4h="strong_down"), PRICE_OK, 60.0
    ) == "снижение_2_1_по_oi_4ч"


def test_2_to_1_guardrail_downgrade_is_reported_consistently() -> None:
    previous_state = {"current_stage": 2}
    summary = make_oi_summary(oi_15m="flat", oi_30m="flat", oi_1h="good_up", oi_4h="good_up")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, guard_reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 480.0, 480.0)
    permission = compute_transition_permission(previous_state, target_stage, 480.0, summary, PRICE_OK, 480.0)
    assert stage == 1
    assert guard_reason == "снижение_2_1:target_stage<=1"
    assert permission == "снижение_2_1_по_target_stage"


def test_4h_strong_down_resets_stage_1_to_0() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_4h="strong_down")
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 12.0, 0.0)
    assert target_stage == 0
    assert stage == 0
    assert reason == "снижение_1_0:oi_4ч=strong_down"


def test_stage_3_holds_on_strong_price_4h_block() -> None:
    previous_state = {"current_stage": 3}
    stage, reason = apply_stage_guardrails(previous_state, 0, make_oi_summary(oi_4h="good_up"), PRICE_BLOCK, VOLUME_DUMMY, 12.0, 0.0)
    assert stage == 3
    assert reason == "удержание_3:только_ручной_или_по_oi_4ч"


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
    summary["oi_growth_pct_1h"] = 9.0
    target_stage, _ = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)
    stage, reason = apply_stage_guardrails(previous_state, target_stage, summary, PRICE_OK, VOLUME_DUMMY, 45.0, 45.0)
    assert target_stage == 3
    assert stage == 1
    assert reason == "удержание_1:1ч_еще_не_сильный"


def test_1_to_2_allows_weak_15m_if_30m_is_mature_and_1h_is_strong() -> None:
    previous_state = {"current_stage": 1}
    summary = make_oi_summary(oi_15m="weak_up", oi_30m="good_up", oi_1h="strong_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 9.0
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


def test_stage_3_holds_on_price_30m_down_when_oi_is_still_mature() -> None:
    previous_state = {"current_stage": 3}
    price_30m_down = ("цена_30м_вниз", "блок_стадии_3_по_цене_30м", False, 2, "weak_down", "flat")
    summary = make_oi_summary(oi_15m="flat", oi_30m="good_up", oi_1h="good_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 9.0

    stage, reason = apply_stage_guardrails(previous_state, 2, summary, price_30m_down, VOLUME_DUMMY, 60.0, 90.0)
    permission = compute_transition_permission(previous_state, 2, 60.0, summary, price_30m_down, 90.0)

    assert stage == 3
    assert reason == "удержание_3:только_ручной_или_по_oi_4ч"
    assert permission == "удержание_3"


def test_stage_3_holds_on_price_1h_down_when_oi_is_still_mature() -> None:
    previous_state = {"current_stage": 3}
    price_1h_down = ("цена_1ч_вниз", "блок_стадии_3_по_цене_1ч", False, 2, "flat", "weak_down")
    summary = make_oi_summary(oi_15m="weak_down", oi_30m="strong_up", oi_1h="good_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 9.0

    stage, reason = apply_stage_guardrails(previous_state, 2, summary, price_1h_down, VOLUME_DUMMY, 60.0, 90.0)
    permission = compute_transition_permission(previous_state, 2, 60.0, summary, price_1h_down, 90.0)

    assert stage == 3
    assert reason == "удержание_3:только_ручной_или_по_oi_4ч"
    assert permission == "удержание_3"


def test_2_to_3_is_blocked_when_good_1h_growth_below_4_5pct() -> None:
    summary = make_oi_summary(oi_15m="good_up", oi_30m="strong_up", oi_1h="good_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 4.4

    target_stage, reason = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    assert target_stage == 2
    assert reason == "рост_oi_1ч_недостаточен_для_умеренного_1ч:выше_2_не_пускаем"


def test_2_to_3_is_allowed_when_good_1h_growth_reaches_4_5pct() -> None:
    summary = make_oi_summary(oi_15m="good_up", oi_30m="strong_up", oi_1h="good_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 4.5

    target_stage, reason = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    assert target_stage == 3
    assert reason == "30м_зрелое_1ч_подтверждает_силу"


def test_2_to_3_allows_strong_1h_even_when_growth_below_old_7pct() -> None:
    summary = make_oi_summary(oi_15m="good_up", oi_30m="strong_up", oi_1h="strong_up", oi_4h="good_up")
    summary["oi_growth_pct_1h"] = 6.0

    target_stage, reason = determine_target_stage(summary, PRICE_OK, VOLUME_DUMMY)

    assert target_stage == 3
    assert reason == "30м_зрелое_1ч_подтверждает_силу"
