from __future__ import annotations

from datetime import datetime, timezone

import autonomous_oi_service as svc


def test_incremental_aggregates_v2_history_across_all_inner_cycles(monkeypatch) -> None:
    prev_cycle = datetime(2026, 6, 24, 11, 0, tzinfo=timezone.utc)
    cycle1 = datetime(2026, 6, 24, 11, 5, tzinfo=timezone.utc)
    cycle2 = datetime(2026, 6, 24, 11, 10, tzinfo=timezone.utc)
    cycle3 = datetime(2026, 6, 24, 11, 15, tzinfo=timezone.utc)

    monkeypatch.setattr(svc, "load_source_cycle_timestamps", lambda *args, **kwargs: [cycle1, cycle2, cycle3])
    monkeypatch.setattr(svc, "load_latest_window_map", lambda *args, **kwargs: {})
    monkeypatch.setattr(svc, "load_window_updates_by_cycle", lambda *args, **kwargs: {})

    def fake_snapshot(_latest_window_map, cycle_ts=None, previous_state_map=None):
        history_row = ("BYBIT", "TQQQUSDT", 1, 2 if cycle_ts == cycle1 else 3, "reason", True, 10.0, cycle_ts)
        state_map = {
            ("BYBIT", "TQQQUSDT"): {
                "exchange": "BYBIT",
                "symbol": "TQQQUSDT",
                "current_stage": 2 if cycle_ts == cycle1 else 3,
            },
            "__v2_rows__": {
                "core_rows_v2": [("core", cycle_ts)],
                "window_rows_v2": [("window", cycle_ts)],
                "history_rows_v2": [("BYBIT", "TQQQUSDT", 1, 2 if cycle_ts == cycle1 else 3, cycle_ts, True, 10.0, "reason")],
            },
        }
        return [("core_legacy", cycle_ts)], [("window_legacy", cycle_ts)], [history_row], state_map

    monkeypatch.setattr(svc, "compute_autonomous_oi_snapshot_from_latest_window_map", fake_snapshot)

    _core_rows, _window_rows, history_rows, state_map, last_source_cycle_ts = svc.compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=cycle3,
        previous_state_map={},
        last_source_cycle_ts=prev_cycle,
        tracked_pairs=[("BYBIT", "TQQQUSDT")],
        window_source="history",
    )

    assert last_source_cycle_ts == cycle3
    assert [row[-1] for row in history_rows] == [cycle1, cycle2, cycle3]

    v2_rows = state_map["__v2_rows__"]
    assert v2_rows["core_rows_v2"] == [("core", cycle3)]
    assert v2_rows["window_rows_v2"] == [("window", cycle3)]
    assert [row[4] for row in v2_rows["history_rows_v2"]] == [cycle1, cycle2, cycle3]
