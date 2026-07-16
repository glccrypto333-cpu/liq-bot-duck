from __future__ import annotations

from datetime import datetime, timedelta, timezone

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


def test_incremental_rechecks_stage2_pair_from_history_at_next_global_cycle(monkeypatch) -> None:
    cycle = datetime(2026, 7, 16, 6, 20, tzinfo=timezone.utc)
    flock = ("BINANCE", "FLOCKUSDT")
    history_map = {
        flock: {"15м": {"OI": {"source_cycle_ts": cycle}}},
    }
    calls = []

    monkeypatch.setattr(svc, "load_source_cycle_timestamps", lambda *args, **kwargs: [cycle])
    monkeypatch.setattr(svc, "load_window_updates_by_cycle", lambda *args, **kwargs: {})

    def fake_load_latest(cycle_ts, window_source="hot", tracked_pairs=None):
        calls.append((cycle_ts, window_source, tracked_pairs))
        return history_map if window_source == "history" else {}

    monkeypatch.setattr(svc, "load_latest_window_map", fake_load_latest)

    def fake_snapshot(latest_window_map, cycle_ts=None, previous_state_map=None):
        assert latest_window_map == history_map
        state_map = {
            flock: {"exchange": "BINANCE", "symbol": "FLOCKUSDT", "current_stage": 3},
            "__v2_rows__": {"core_rows_v2": [], "window_rows_v2": [], "history_rows_v2": []},
        }
        return [], [], [], state_map

    monkeypatch.setattr(svc, "compute_autonomous_oi_snapshot_from_latest_window_map", fake_snapshot)

    svc.compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=cycle,
        previous_state_map={flock: {"current_stage": 2}},
        last_source_cycle_ts=cycle - timedelta(minutes=5),
    )

    assert (cycle, "history", [flock]) in calls
