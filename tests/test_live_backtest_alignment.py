from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_service import compute_autonomous_oi_snapshot_incremental_to_cycle
from db import fetch
from tools.backtest.back_test import parse_user_ts


def _load_history_cycles(start_ts, end_ts):
    rows = fetch(
        """
        SELECT source_cycle_ts
        FROM aggregate_windows_history
        WHERE source_cycle_ts IS NOT NULL
          AND source_cycle_ts >= %s
          AND source_cycle_ts <= %s
        GROUP BY source_cycle_ts
        ORDER BY source_cycle_ts ASC
        """,
        (start_ts, end_ts),
    )
    return [row["source_cycle_ts"] for row in rows]


def _select_sparse_wall_cycles(all_cycles, step_minutes: int):
    chosen = [all_cycles[0]]
    last = all_cycles[0]
    for cycle in all_cycles[1:]:
        if cycle >= last + timedelta(minutes=step_minutes):
            chosen.append(cycle)
            last = cycle
    if chosen[-1] != all_cycles[-1]:
        chosen.append(all_cycles[-1])
    return chosen


def test_incremental_live_processing_stays_aligned_with_ctr_replay() -> None:
    start = parse_user_ts("2026-06-24 01:00:00")
    end = start + timedelta(hours=4)
    all_cycles = _load_history_cycles(start, end)
    if not all_cycles:
        pytest.skip("Нет исторических cycle_ts в aggregate_windows_history для окна CTRUSDT")
    wall_cycles = _select_sparse_wall_cycles(all_cycles, step_minutes=7)

    state_map = {}
    last_source_cycle_ts = None
    first_stage_2 = None
    first_stage_3 = None

    for wall_cycle_ts in wall_cycles:
        _core_rows, _window_rows, _history_rows, state_map, last_source_cycle_ts = compute_autonomous_oi_snapshot_incremental_to_cycle(
            cycle_ts=wall_cycle_ts,
            previous_state_map=state_map,
            last_source_cycle_ts=last_source_cycle_ts,
            tracked_pairs=[("BYBIT", "CTRUSDT")],
            window_source="history",
        )
        state = state_map.get(("BYBIT", "CTRUSDT"))
        assert state is not None
        stage = int(state.get("current_stage") or 0)
        if stage >= 2 and first_stage_2 is None:
            first_stage_2 = wall_cycle_ts
        if stage >= 3 and first_stage_3 is None:
            first_stage_3 = wall_cycle_ts

    assert first_stage_2 is not None
    assert first_stage_3 is None
