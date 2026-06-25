from __future__ import annotations

from datetime import datetime, timezone


OI_GROWTH_SET = {"weak_up", "good_up", "strong_up"}
OI_MATURE_SET = {"good_up", "strong_up"}
OI_DECLINE_SET = {"weak_down", "strong_down"}
PRICE_HARD_BAN = "цена_4ч_сильно_вниз"
PRICE_STAGE3_BAN = "цена_4ч_слабо_вниз"
PHASE1_MIN_AGE_MINUTES = 30.0
PHASE2_MIN_AGE_MINUTES = 30.0
TRIGGER_TO_STAGE2_MINUTES = 30.0
TRIGGER_TO_STAGE3_MINUTES = 60.0


def _oi_classes(oi_summary: dict) -> tuple[str, str, str, str]:
    return (
        str(oi_summary.get("oi_slope_class_15m") or "flat"),
        str(oi_summary.get("oi_slope_class_30m") or "flat"),
        str(oi_summary.get("oi_slope_class_1h") or "flat"),
        str(oi_summary.get("oi_slope_class_4h") or "flat"),
    )


def _is_growth(slope_class: str) -> bool:
    return slope_class in OI_GROWTH_SET


def _is_mature_growth(slope_class: str) -> bool:
    return slope_class in OI_MATURE_SET


def _is_decline(slope_class: str) -> bool:
    return slope_class in OI_DECLINE_SET


def _is_4h_hard_decline(slope_class: str) -> bool:
    return slope_class == "strong_down"


def _is_15m_hard_negative_for_fresh_stage3(slope_class: str) -> bool:
    return slope_class == "strong_down"


def _has_live_build(oi_summary: dict) -> bool:
    _oi_15m, oi_30m, oi_1h, oi_4h = _oi_classes(oi_summary)
    return (
        _is_growth(oi_30m)
        and not _is_decline(oi_1h)
        and not _is_4h_hard_decline(oi_4h)
    )


def _has_mature_build(oi_summary: dict) -> bool:
    oi_15m, oi_30m, oi_1h, oi_4h = _oi_classes(oi_summary)
    return (
        _is_mature_growth(oi_30m)
        and _is_growth(oi_1h)
        and not _is_15m_hard_negative_for_fresh_stage3(oi_15m)
        and not _is_4h_hard_decline(oi_4h)
    )


def _price_block(price_summary: tuple[str, str, bool, int]) -> tuple[str, bool]:
    price_state, _price_block, blocked_by_price, _blocked_stage_max = price_summary
    return price_state, bool(blocked_by_price)


def _price_stage3_block(price_state: str) -> bool:
    return price_state == PRICE_STAGE3_BAN


def determine_target_stage(
    oi_summary: dict,
    price_summary: tuple[str, str, bool, int],
    volume_summary: tuple[str, str],
) -> tuple[int, str]:
    del volume_summary

    oi_15m, oi_30m, oi_1h, oi_4h = _oi_classes(oi_summary)
    price_state, blocked_by_price = _price_block(price_summary)

    if blocked_by_price:
        return 0, f"{PRICE_HARD_BAN}:{price_state}"
    if _is_4h_hard_decline(oi_4h):
        return 0, f"oi_4ч={oi_4h}; жесткий_старший_блок"
    if _price_stage3_block(price_state):
        if _has_live_build(oi_summary):
            return 2, "цена_4ч_слабо_вниз:выше_2_не_пускаем"
        return 1, "цена_4ч_слабо_вниз:ждем_живой_набор"
    if _has_mature_build(oi_summary):
        return 3, "30м_зрелое_1ч_подтверждает_силу"
    if _has_live_build(oi_summary):
        return 2, "30м_подтвердило_живой_набор"
    return 1, "жесткий_запрет_снят"


def apply_stage_guardrails(
    previous_state: dict | None,
    target_stage: int,
    oi_summary: dict,
    price_summary: tuple[str, str, bool, int],
    volume_summary: tuple[str, str],
    previous_stage_age_minutes: float,
    trigger_age_minutes: float = 0.0,
) -> tuple[int, str]:
    del volume_summary

    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    effective_previous_age = float(previous_stage_age_minutes or 0.0)
    effective_trigger_age = float(trigger_age_minutes or 0.0)
    oi_15m, oi_30m, oi_1h, oi_4h = _oi_classes(oi_summary)
    price_state, blocked_by_price = _price_block(price_summary)

    if previous_stage == 3:
        if _is_decline(oi_4h):
            return 0, f"сброс_3_0:oi_4ч={oi_4h}"
        if blocked_by_price:
            return 1, f"снижение_3_1:цена_4ч={price_state}"
        if _price_stage3_block(price_state):
            return 2, f"снижение_3_2:цена_4ч={price_state}"
        return 3, "удержание_3:только_ручной_или_по_oi_4ч"

    if blocked_by_price:
        if previous_stage >= 2:
            return 1, f"снижение_2_1:цена_4ч={price_state}"
        return 0, f"снижение_1_0:цена_4ч={price_state}"

    if _is_4h_hard_decline(oi_4h):
        if previous_stage >= 2:
            return 1, f"снижение_2_1:oi_4ч={oi_4h}"
        return 0, f"снижение_1_0:oi_4ч={oi_4h}"

    if _is_decline(oi_30m):
        if previous_stage >= 2:
            return 1, f"снижение_2_1:oi_30м={oi_30m}"
        return 1 if previous_stage == 1 else 1, "удержание_1:нет_живого_набора"

    if _is_decline(oi_1h):
        if previous_stage >= 2:
            return 1, f"снижение_2_1:oi_1ч={oi_1h}"
        return 1 if previous_stage == 1 else 1, "удержание_1:нет_живого_набора"

    if previous_stage <= 0:
        return 1, "вход_в_1:жесткий_запрет_снят"

    if previous_stage == 1:
        if target_stage < 2:
            return 1, "удержание_1:нет_живого_набора"
        if not _is_growth(oi_15m):
            return 1, "удержание_1:15м_не_подтвердило_старт"
        if not _is_mature_growth(oi_30m):
            return 1, "удержание_1:30м_еще_не_зрелое"
        if oi_15m == "weak_up" and oi_1h != "strong_up":
            return 1, "удержание_1:1ч_еще_не_сильный"
        if effective_trigger_age < TRIGGER_TO_STAGE2_MINUTES:
            return 1, "удержание_1:ждем_30_минут_от_триггера"
        if effective_previous_age < PHASE1_MIN_AGE_MINUTES:
            return 1, "удержание_1:ждем_30_минут"
        return 2, "переход_1_2:30м_подтвердило_живой_набор"

    if previous_stage == 2:
        if _price_stage3_block(price_state):
            return 2, f"удержание_2:цена_4ч={price_state}"
        if target_stage <= 1:
            return 1, "снижение_2_1:живой_набор_умер"
        if target_stage == 2:
            if _is_15m_hard_negative_for_fresh_stage3(oi_15m):
                return 2, "удержание_2:15м_локально_слабое"
            return 2, "удержание_2:30м_или_1ч_еще_не_созрели"
        if effective_previous_age < PHASE2_MIN_AGE_MINUTES:
            return 2, "удержание_2:ждем_30_минут"
        if effective_trigger_age < TRIGGER_TO_STAGE3_MINUTES:
            return 2, "удержание_2:ждем_1ч_от_триггера"
        return 3, "переход_2_3:30м_зрелое_1ч_подтверждает_силу"

    return 1, "fallback_в_1"


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


def compute_transition_permission(
    previous_state: dict | None,
    target_stage: int,
    stage_age_minutes: float,
    oi_summary: dict,
    price_summary: tuple[str, str, bool, int],
    trigger_age_minutes: float = 0.0,
) -> str:
    previous_stage = int(previous_state.get("current_stage") or 0) if previous_state else 0
    effective_age = float(stage_age_minutes or 0.0)
    effective_trigger_age = float(trigger_age_minutes or 0.0)
    oi_15m, oi_30m, oi_1h, oi_4h = _oi_classes(oi_summary)
    _price_state, blocked_by_price = _price_block(price_summary)

    if previous_stage == 3:
        if target_stage == 0 and _is_decline(oi_4h):
            return "сброс_3_0_по_oi_4ч"
        if target_stage == 1 and blocked_by_price:
            return "снижение_3_1_по_цене_4ч"
        if target_stage == 2 and _price_stage3_block(_price_state):
            return "снижение_3_2_по_цене_4ч"
        return "удержание_3"

    if blocked_by_price:
        return "блок_цены_4ч"
    if previous_stage >= 2 and _price_stage3_block(_price_state):
        return "блок_3_по_цене_4ч"
    if _is_4h_hard_decline(oi_4h):
        return "блок_oi_4ч"
    if previous_stage >= 2 and _is_decline(oi_30m):
        return "снижение_2_1_по_30м"
    if previous_stage >= 2 and _is_decline(oi_1h):
        return "снижение_2_1_по_1ч"
    if previous_stage == 1 and target_stage < 2:
        return "удержание_1"
    if previous_stage == 1 and not _is_growth(oi_15m):
        return "удержание_1_по_15м"
    if previous_stage == 1 and not _is_mature_growth(oi_30m):
        return "удержание_1_по_30м"
    if previous_stage == 1 and oi_15m == "weak_up" and oi_1h != "strong_up":
        return "удержание_1_по_1ч"
    if previous_stage == 1 and effective_trigger_age < TRIGGER_TO_STAGE2_MINUTES:
        return "ждем_30_минут_от_триггера"
    if previous_stage == 1 and effective_age < PHASE1_MIN_AGE_MINUTES:
        return "ждем_30_минут_в_1"
    if previous_stage == 2 and target_stage <= 2:
        if _is_15m_hard_negative_for_fresh_stage3(oi_15m):
            return "удержание_2_по_15м"
        return "удержание_2"
    if previous_stage == 2 and effective_trigger_age < TRIGGER_TO_STAGE3_MINUTES:
        return "ждем_1ч_от_триггера"
    if previous_stage == 2 and effective_age < PHASE2_MIN_AGE_MINUTES:
        return "ждем_30_минут_в_2"
    if target_stage == 1:
        return "разрешен_вход_в_1"
    if target_stage == 2:
        return "разрешен_вход_в_2"
    if target_stage == 3:
        return "разрешен_вход_в_3"
    return "неизвестно"
