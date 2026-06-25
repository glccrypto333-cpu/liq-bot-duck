from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/home/alexey/openclaw/apps/liq-bot-duck")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_service import _resolve_growth_trigger_ts


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


def test_trigger_does_not_start_from_weak_up_even_if_30m_is_also_alive() -> None:
    cycle_ts = datetime(2026, 6, 22, 12, 30, tzinfo=timezone.utc)
    previous_state = {
        "current_stage": 1,
        "oi_slope_class_15m": "flat",
        "growth_trigger_ts": None,
    }
    summary = make_oi_summary(oi_15m="weak_up", oi_30m="weak_up", oi_1h="flat", oi_4h="flat")
    trigger_ts = _resolve_growth_trigger_ts(previous_state, summary, PRICE_OK, cycle_ts)
    assert trigger_ts is None


def test_trigger_does_not_start_from_lonely_weak_up_without_30m_support() -> None:
    cycle_ts = datetime(2026, 6, 22, 12, 30, tzinfo=timezone.utc)
    previous_state = {
        "current_stage": 1,
        "oi_slope_class_15m": "flat",
        "growth_trigger_ts": None,
    }
    summary = make_oi_summary(oi_15m="weak_up", oi_30m="flat", oi_1h="flat", oi_4h="flat")
    trigger_ts = _resolve_growth_trigger_ts(previous_state, summary, PRICE_OK, cycle_ts)
    assert trigger_ts is None
