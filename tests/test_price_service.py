from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from price_service import compute_price_window_state, summarize_price


def make_payload(window_code: str, close_value: float, open_value: float = 100.0) -> dict:
    row = {
        "open_value": open_value,
        "high_value": max(open_value, close_value),
        "low_value": min(open_value, close_value),
        "close_value": close_value,
    }
    return {window_code: {"PRICE": row}}


def test_only_4h_strong_down_price_blocks_growth() -> None:
    state_4h = compute_price_window_state(make_payload("4ч", 93.0), {}, "4ч")
    summary = summarize_price([state_4h], 3)
    assert summary == ("цена_4ч_сильно_вниз", "жесткий_блок_роста_по_цене_4ч", True, 1, "flat", "flat")


def test_1h_downward_price_caps_stage3() -> None:
    state_1h = compute_price_window_state(make_payload("1ч", 96.0), {}, "1ч")
    state_4h = compute_price_window_state(make_payload("4ч", 101.0), {}, "4ч")
    summary = summarize_price([state_1h, state_4h], 3)
    assert summary == ("цена_1ч_вниз", "блок_стадии_3_по_цене_1ч", False, 2, "flat", "weak_down")


def test_4h_weak_down_caps_stage3_but_is_not_hard_block() -> None:
    state_4h = compute_price_window_state(make_payload("4ч", 96.0), {}, "4ч")
    summary = summarize_price([state_4h], 3)
    assert summary == ("цена_4ч_слабо_вниз", "блок_стадии_3_по_цене_4ч", False, 2, "flat", "flat")


def test_30m_downward_price_also_caps_stage3() -> None:
    state_30m = compute_price_window_state(make_payload("30м", 96.0), {}, "30м")
    state_1h = compute_price_window_state(make_payload("1ч", 101.0), {}, "1ч")
    state_4h = compute_price_window_state(make_payload("4ч", 101.0), {}, "4ч")
    summary = summarize_price([state_30m, state_1h, state_4h], 3)
    assert summary == ("цена_30м_вниз", "блок_стадии_3_по_цене_30м", False, 2, "weak_down", "weak_up")
