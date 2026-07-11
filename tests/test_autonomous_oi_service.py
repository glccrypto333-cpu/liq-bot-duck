from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_service import _resolve_growth_trigger_ts, build_core_record


PRICE_OK = ("цена_не_блокирует", "нет", False, 3)


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


def make_persistent_oi_summary(
    oi_15m: str = "weak_up",
    oi_30m: str = "good_up",
    oi_1h: str = "strong_up",
    oi_4h: str = "good_up",
) -> dict:
    return {
        "oi_pattern_label": "подтвержденный_набор",
        "oi_pattern_code": "подтвержденный_набор",
        "oi_direction_summary": "вверх",
        "oi_angle_summary": "сильный",
        "oi_stability_summary": "хорошая",
        "oi_retention_summary": "подтвержденное",
        "oi_breakdown_summary": "нет",
        "oi_slope_class_15m": oi_15m,
        "oi_slope_class_30m": oi_30m,
        "oi_slope_class_1h": oi_1h,
        "oi_slope_class_4h": oi_4h,
    }


def test_stage_2_without_saved_trigger_does_not_restore_ancient_trigger_from_age() -> None:
    cycle_ts = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)
    previous_state = {
        "current_stage": 2,
        "oi_stage_age_minutes": 855.14,
        "oi_slope_class_15m": "flat",
        "growth_trigger_ts": None,
    }
    summary = make_oi_summary(oi_15m="good_up", oi_30m="good_up", oi_1h="good_up", oi_4h="weak_up")
    trigger_ts = _resolve_growth_trigger_ts(previous_state, summary, PRICE_OK, cycle_ts)
    assert trigger_ts is None


def test_build_core_record_persists_trigger_and_latest_oi_slopes_for_next_cycle() -> None:
    cycle_ts = datetime(2026, 6, 23, 20, 20, tzinfo=timezone.utc)
    trigger_ts = datetime(2026, 6, 23, 19, 55, tzinfo=timezone.utc)
    row = build_core_record(
        "BYBIT",
        "BASEDUSDT",
        make_persistent_oi_summary(),
        ("цена_не_блокирует", "нет", False, 3),
        ("пустой", "нейтрально"),
        1,
        "удержание_1",
        414.05,
        cycle_ts,
        "30м_зрелое_1ч_подтверждает_силу; guard=удержание_1:ждем_30_минут_от_триггера",
        trigger_ts,
    )
    assert row[-6] == trigger_ts
    assert row[-5:] == ("weak_up", "good_up", "strong_up", "good_up", cycle_ts)
