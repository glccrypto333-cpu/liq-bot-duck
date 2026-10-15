from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from aggregation_engine import build_latest_aggregate_rows
from exchange_clients import fetch_binance_oi_5m


def _snapshot(timestamp: datetime, value: float) -> dict:
    return {
        "ts_open": timestamp - timedelta(minutes=5),
        "ts_close": timestamp,
        "exchange": "BINANCE",
        "symbol": "TESTUSDT",
        "oi_open": value,
        "oi_high": value,
        "oi_low": value,
        "oi_close": value,
    }


def test_binance_oi_snapshots_are_bucketed_and_latest_sample_wins(monkeypatch) -> None:
    anchor = datetime(2026, 10, 15, 16, 0, tzinfo=timezone.utc)
    points = [
        _snapshot(anchor - timedelta(minutes=15) + timedelta(seconds=4), 10.0),
        _snapshot(anchor - timedelta(minutes=10) - timedelta(seconds=1), 11.0),
        _snapshot(anchor - timedelta(minutes=10) + timedelta(seconds=2), 12.0),
        _snapshot(anchor - timedelta(minutes=5) - timedelta(seconds=1), 13.0),
        _snapshot(anchor - timedelta(minutes=5) + timedelta(seconds=7), 14.0),
        _snapshot(anchor - timedelta(seconds=1), 15.0),
    ]

    def fake_fetch(table, *_args, **_kwargs):
        return points if table == "oi_raw" else []

    monkeypatch.setattr(
        "aggregation_engine._resolve_symbol_anchor_map_from_db",
        lambda *_args, **_kwargs: {("BINANCE", "TESTUSDT"): anchor},
    )
    monkeypatch.setattr("aggregation_engine._fetch_recent_metric_rows", fake_fetch)

    rows, _stats = build_latest_aggregate_rows(
        anchor,
        selected_windows=("15\u043c",),
        selected_metrics=("OI",),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row[2] == anchor - timedelta(minutes=15)
    assert row[3] == anchor
    assert row[6] == 11.0
    assert row[9] == 15.0
    assert row[13] == 3


def test_binance_oi_collector_returns_latest_point_per_closed_five_minute_bucket(monkeypatch) -> None:
    base = datetime(2026, 10, 15, 16, 0, tzinfo=timezone.utc)
    timestamps = [
        base + timedelta(seconds=10),
        base + timedelta(minutes=4, seconds=50),
        base + timedelta(minutes=5, seconds=10),
    ]
    response = [
        {"timestamp": int(timestamp.timestamp() * 1000), "sumOpenInterest": str(index + 1)}
        for index, timestamp in enumerate(timestamps)
    ]
    captured = {}

    def fake_get(_url, params):
        captured.update(params)
        return response

    monkeypatch.setattr("exchange_clients._get", fake_get)
    monkeypatch.setattr("exchange_clients._closed", lambda _close: True)

    rows = fetch_binance_oi_5m("TESTUSDT", limit=500)

    assert captured["limit"] == 500
    assert [(row[0], row[1], row[7]) for row in rows] == [
        (base, base + timedelta(minutes=5), 2.0),
        (base + timedelta(minutes=5), base + timedelta(minutes=10), 3.0),
    ]
