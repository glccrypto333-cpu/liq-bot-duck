from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from oi_service import compute_oi_window_state, summarize_oi_window_states


def test_summary_exposes_15m_class() -> None:
    summary = summarize_oi_window_states(
        [
            {"window_code": "15м", "window_weight": 1.0, "oi_slope_class": "good_up"},
            {"window_code": "30м", "window_weight": 1.5, "oi_slope_class": "good_up"},
            {"window_code": "1ч", "window_weight": 2.0, "oi_slope_class": "good_up"},
            {"window_code": "4ч", "window_weight": 2.5, "oi_slope_class": "good_up"},
            {"window_code": "12ч", "window_weight": 2.0, "oi_pattern_code": "подтвержденный_набор", "oi_pattern_label": "подтвержденный_набор"},
        ]
    )
    assert summary["oi_slope_class_15m"] == "good_up"


def test_window_state_does_not_expose_removed_oi_interpreter_fields() -> None:
    payload = {
        "1ч": {
            "OI": {
                "open_value": 100.0,
                "close_value": 110.0,
                "high_value": 110.0,
                "low_value": 100.0,
                "trajectory_points": [100.0, 103.0, 106.0, 110.0],
            }
        }
    }

    state = compute_oi_window_state(payload, "1ч")

    removed_prefixes = (
        "oi_form",
        "oi_hold",
        "oi_pullback",
        "oi_smoothness",
        "oi_concentration",
        "oi_tail",
        "oi_flat_tail",
        "oi_retention_ratio",
    )
    assert not any(key.startswith(removed_prefixes) for key in state)
