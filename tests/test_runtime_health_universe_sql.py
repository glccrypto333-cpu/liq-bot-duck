from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import UNIVERSE_HEALTH_LATEST_ROWS_SQL
from main import _canonical_global_block_reason
from main import _classify_universe_problem


def test_universe_health_latest_rows_keeps_senior_windows_beyond_latest_source_cycle():
    assert "source_cycle_ts >= %s::timestamptz - interval '35 minutes'" not in UNIVERSE_HEALTH_LATEST_ROWS_SQL
    assert UNIVERSE_HEALTH_LATEST_ROWS_SQL.count("%s::timestamptz") == 1


def test_universe_problem_marks_only_stale_4h_context_as_info():
    verdict = _classify_universe_problem(
        {"missing_cnt": 0, "stale_list": "OI:4ч=1350м, PRICE:4ч=1350м", "max_lag_min": 1350},
        {"exchange_latest_ts": None},
    )

    assert verdict["reason_code"] == "старший_фон_4ч_восстановление"
    assert verdict["reason_level"] == "info"


def test_canonical_global_block_ignores_raw_stale_summary_when_quality_is_ok():
    reason = _canonical_global_block_reason(
        {
            "cycle_health": "ok",
            "duck_universe_health": "ok",
            "data_quality_state": "ok",
            "symbols_total": 1180,
            "duck_universe_summary": {
                "universe": 1180,
                "incomplete_pairs": 0,
                "no_windows_pairs": 0,
                "stale30_pairs": 1180,
                "stale60_pairs": 1180,
                "stale180_pairs": 1180,
            },
        },
        {"cycle_health": "ok", "stop_reason": "ok", "stop_severity": "ok"},
    )

    assert reason is None
