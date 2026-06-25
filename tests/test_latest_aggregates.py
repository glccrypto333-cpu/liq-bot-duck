from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from aggregation_engine import build_latest_aggregate_rows, rebuild_latest_aggregate_windows


def _oi_row(ts_open: datetime, exchange: str, symbol: str, value: float) -> dict:
    return {
        "ts_open": ts_open,
        "ts_close": ts_open + timedelta(minutes=5),
        "exchange": exchange,
        "symbol": symbol,
        "oi_open": value,
        "oi_high": value + 1.0,
        "oi_low": value - 1.0,
        "oi_close": value + 0.5,
    }


def test_build_latest_aggregate_rows_keeps_only_latest_cycle(monkeypatch) -> None:
    source_cycle_ts = datetime(2026, 6, 24, 16, 0)
    starts = [
        datetime(2026, 6, 24, 15, 45),
        datetime(2026, 6, 24, 15, 50),
        datetime(2026, 6, 24, 15, 55),
    ]
    oi_rows = [_oi_row(ts_open, "BYBIT", "TESTUSDT", float(index + 1)) for index, ts_open in enumerate(starts)]

    def fake_fetch(table, select_sql, alias, active_only, cycle_ts):
        assert cycle_ts == source_cycle_ts
        if table == "oi_raw":
            return oi_rows
        return []

    monkeypatch.setattr("aggregation_engine._fetch_recent_metric_rows", fake_fetch)

    rows, stats = build_latest_aggregate_rows(
        source_cycle_ts,
        selected_windows=("15м",),
        selected_metrics=("OI",),
        active_only=True,
    )

    assert stats["aggregates"] == 1
    assert len(rows) == 1
    metric, window_code, ts_open, ts_close, exchange, symbol, *_rest = rows[0]
    assert metric == "OI"
    assert window_code == "15м"
    assert ts_open == starts[0]
    assert ts_close == source_cycle_ts
    assert exchange == "BYBIT"
    assert symbol == "TESTUSDT"


def test_rebuild_latest_aggregate_windows_uses_common_anchor(monkeypatch) -> None:
    requested_cycle_ts = datetime(2026, 6, 24, 16, 14, 40)
    anchor_ts = datetime(2026, 6, 24, 16, 15)
    captured = {}

    def fake_resolve_anchor():
        return anchor_ts

    def fake_build_latest(source_cycle_ts, selected_windows=None, selected_metrics=None, active_only=True):
        captured["source_cycle_ts"] = source_cycle_ts
        return ([(
            "OI", "15м",
            datetime(2026, 6, 24, 16, 0),
            anchor_ts,
            "BYBIT", "TESTUSDT",
            1.0, 2.0, 0.5, 1.5,
            None, None, 50.0, 3,
        )], {
            "raw_oi": 3,
            "raw_price": 0,
            "raw_volume": 0,
            "aggregates": 1,
            "skipped_non_contiguous": 0,
        })

    monkeypatch.setattr("aggregation_engine.resolve_latest_common_ts_close", fake_resolve_anchor)
    monkeypatch.setattr("aggregation_engine.build_latest_aggregate_rows", fake_build_latest)
    monkeypatch.setattr("aggregation_engine.upsert_aggregate_hot_rows", lambda rows: len(rows))
    monkeypatch.setattr("aggregation_engine.sync_aggregate_history_from_rows", lambda rows: (0, 0))

    result = rebuild_latest_aggregate_windows(requested_cycle_ts)

    assert result == 1
    assert captured["source_cycle_ts"] == anchor_ts
