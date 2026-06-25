from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from oi_service import summarize_oi_window_states


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
