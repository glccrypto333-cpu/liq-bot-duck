from __future__ import annotations

from datetime import datetime, timezone


OI_POSITIVE_START = {"weak_up", "good_up", "strong_up"}
OI_WORKING_SET = {"good_up", "strong_up"}
OI_SUPPORTING_4H = {"weak_up", "good_up", "strong_up"}
OI_STRONG_SET = {"strong_up"}
OI_NON_DEGRADING_1H = {"flat", "weak_up", "good_up", "strong_up"}
PRICE_HARD_BAN = "ломает_сценарий"
PHASE1_MIN_AGE_MINUTES = 60.0
PHASE2_MIN_AGE_MINUTES = 30.0


def _oi_classes(oi_summary: dict) -> tuple[str, str]:
    return (
        str(oi_summary.get("oi_slope_class_1h") or "flat"),
        str(oi_summary.get("oi_slope_class_4h") or "flat"),
    )


def determine_target_stage(oi_summary: dict, price_summary: tuple[str, str, bool, int], volume_summary: tuple[str, str]) -> tuple[int, str]:
    del volume_summary
    price_state, _, blocked_by_price, _blocked_stage_max = price_summary
    oi_1h, oi_4h = _oi_classes(oi_summary)

    if blocked_by_price:
        return 0, f"price={price_state}; hard_ban"

    if oi_1h in OI_STRONG_SET and oi_4h in OI_WORKING_SET:
        return 3, f"oi_1h={oi_1h}; oi_4h={oi_4h}; aggressive"
    if oi_1h in OI_WORKING_SET and oi_4h not in {"weak_down", "strong_down"}:
        return 2, f"oi_1h={oi_1h}; oi_4h={oi_4h}; working"
    if oi_1h in OI_POSITIVE_START:
        return 1, f"oi_1h={oi_1h}; early_positive"
    return 0, f"oi_1h={oi_1h}; outside"


def apply_stage_guardrails(
    previous_state: dict | None,
    target_stage: int,
    oi_summary: dict,
    price_summary: tuple[str, str, bool, int],
    volume_summary: tuple[str, str],
) -> tuple[int, str]:
    del volume_summary
    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    previous_age = float(previous_state.get("oi_stage_age_minutes") or 0.0) if previous_state else 0.0
    price_state, _, blocked_by_price, _blocked_stage_max = price_summary
    oi_1h, oi_4h = _oi_classes(oi_summary)

    # Stage 3 is operator-owned: no automatic downgrade by any service.
    if previous_stage == 3:
        return 3, "manual_hold_stage_3"

    if blocked_by_price:
        if previous_stage >= 2:
            return 1, f"price_hard_ban:{price_state}; degrade_2_to_1"
        return 0, f"price_hard_ban:{price_state}; degrade_to_0"

    if target_stage <= 0:
        if previous_stage >= 2 and oi_4h in OI_SUPPORTING_4H and oi_1h in OI_NON_DEGRADING_1H:
            return 1, "degrade_2_to_1_soft"
        if previous_stage == 1 and oi_4h in OI_SUPPORTING_4H and oi_1h in OI_NON_DEGRADING_1H:
            return 1, "hold_phase_1_hysteresis"
        return 0, "outside_phase_model"

    if target_stage == 1:
        if oi_1h in OI_POSITIVE_START:
            return 1, "phase_1_early_positive"
        return 0, "phase_1_not_confirmed"

    if target_stage == 2:
        if previous_stage >= 2:
            return 2, "hold_phase_2"
        if previous_stage >= 1 and previous_age >= PHASE1_MIN_AGE_MINUTES and oi_1h in OI_WORKING_SET and oi_4h not in {"weak_down", "strong_down"}:
            return 2, "promote_1_to_2"
        if oi_1h in OI_POSITIVE_START:
            return 1, "seed_phase_1_before_phase_2"
        return 0, "no_base_for_phase_2"

    if target_stage >= 3:
        if previous_stage >= 2 and previous_age >= PHASE2_MIN_AGE_MINUTES and oi_1h in OI_STRONG_SET and oi_4h in OI_WORKING_SET:
            return 3, "promote_2_to_3"
        if previous_stage >= 2:
            return 2, "hold_phase_2_before_phase_3"
        if previous_stage == 1 and previous_age >= PHASE1_MIN_AGE_MINUTES and oi_4h in OI_WORKING_SET:
            return 2, "promote_1_to_2_before_phase_3"
        if oi_1h in OI_POSITIVE_START:
            return 1, "seed_phase_1_before_phase_3"
        return 0, "no_base_for_phase_3"

    return 0, "fallback_to_0"


def compute_stage_age(previous_state: dict | None, target_stage: int, cycle_ts: datetime) -> float:
    if not previous_state:
        return 0.0
    previous_stage = int(previous_state.get("current_stage") or 0)
    previous_age = float(previous_state.get("oi_stage_age_minutes") or 0.0)
    previous_ts = previous_state.get("latest_cycle_ts")
    if previous_stage != target_stage or previous_ts is None:
        return 0.0
    if isinstance(previous_ts, str):
        previous_ts = datetime.fromisoformat(previous_ts)
    if previous_ts.tzinfo is None:
        previous_ts = previous_ts.replace(tzinfo=timezone.utc)
    delta_minutes = max(0.0, (cycle_ts - previous_ts).total_seconds() / 60.0)
    return previous_age + delta_minutes


def compute_transition_permission(previous_state: dict | None, target_stage: int, stage_age_minutes: float, blocked_by_price: bool) -> str:
    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    previous_age = float(previous_state.get("oi_stage_age_minutes") or 0.0) if previous_state else 0.0

    if previous_stage == 3:
        return "manual_only_stage_3"
    if blocked_by_price:
        return "price_hard_ban"
    if target_stage <= 0:
        return "phase_0"
    if target_stage == 1:
        return "phase_1_allowed"
    if target_stage == 2:
        if previous_stage < 1:
            return "need_phase_1_seed"
        if previous_stage == 1 and previous_age < PHASE1_MIN_AGE_MINUTES:
            return "need_1h_in_phase_1"
        return "phase_2_allowed"
    if target_stage >= 3:
        if previous_stage < 2:
            return "need_phase_2_base"
        if previous_stage == 2 and previous_age < PHASE2_MIN_AGE_MINUTES:
            return "need_15m_in_phase_2"
        return "phase_3_allowed"
    return "unknown"
