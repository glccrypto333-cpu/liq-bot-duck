from __future__ import annotations

import telegram_bot


def test_enrich_stage3_decision_snapshot_from_core_json():
    row = {
        "exchange": "BYBIT",
        "symbol": "TESTUSDT",
        "oi_summary": {
            "oi_pattern_code": "strong_up",
            "oi_pattern_label": "подтверждённый набор",
        },
        "price_summary": {
            "price_state": "цена_не_блокирует",
        },
        "volume_summary": {
            "volume_state": "рабочий",
        },
        "phase_reason": "канон_2_3",
    }

    result = telegram_bot._enrich_stage3_decision_snapshot(row)

    assert result["oi_pattern_code"] == "strong_up"
    assert result["oi_pattern_label"] == "подтверждённый набор"
    assert result["price_state_summary"] == "цена_не_блокирует"
    assert result["volume_state_summary"] == "рабочий"
    assert result["decision_reason"] == "канон_2_3"
