from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from phase_service import determine_target_stage
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


def test_any_negative_1h_price_blocks_stage3_including_aevo_move() -> None:
    # AEVO's 1h price move was -0.4648%, but the OI-derived cutoff called it flat.
    state_1h = compute_price_window_state(make_payload("1ч", 99.5352), {}, "1ч")
    state_4h = compute_price_window_state(make_payload("4ч", 100.1169), {}, "4ч")

    summary = summarize_price([state_1h, state_4h], 3)

    assert state_1h["price_direction"] == "weak_down"
    assert summary[0] == "цена_1ч_вниз"
    assert summary[2:4] == (False, 2)


def test_aevo_negative_1h_price_blocks_phase_transition_end_to_end() -> None:
    price_1h = compute_price_window_state(make_payload("1ч", 99.5352), {}, "1ч")
    price_4h = compute_price_window_state(make_payload("4ч", 100.1169), {}, "4ч")
    price_summary = summarize_price([price_1h, price_4h], 3)
    oi_summary = {
        "oi_slope_class_15m": "good_up",
        "oi_slope_class_30m": "strong_up",
        "oi_slope_class_1h": "strong_up",
        "oi_slope_class_4h": "good_up",
        "oi_growth_pct_1h": 8.0,
    }

    target_stage, reason = determine_target_stage(oi_summary, price_summary, ("пустой", "нейтрально"))

    assert target_stage == 2
    assert reason == "цена_1ч_вниз:выше_2_не_пускаем"


def test_any_negative_30m_price_blocks_stage3() -> None:
    state_30m = compute_price_window_state(make_payload("30м", 99.9), {}, "30м")
    state_4h = compute_price_window_state(make_payload("4ч", 100.1), {}, "4ч")

    summary = summarize_price([state_30m, state_4h], 3)

    assert state_30m["price_direction"] == "weak_down"
    assert summary[0] == "цена_30м_вниз"
    assert summary[2:4] == (False, 2)


def test_zero_or_positive_30m_price_is_not_a_downward_block() -> None:
    zero = compute_price_window_state(make_payload("30м", 100.0), {}, "30м")
    positive = compute_price_window_state(make_payload("30м", 100.0001), {}, "30м")

    assert zero["price_state_code"] == "цена_не_блокирует"
    assert positive["price_state_code"] == "цена_не_блокирует"


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
