from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from aggregation_engine import build_aggregate_rows


def test_full_rebuild_normalizes_legacy_binance_oi_snapshots(monkeypatch) -> None:
    anchor = datetime(2026, 10, 15, 16, 0, tzinfo=timezone.utc)
    points = []
    for offset, value in ((15, 10.0), (10, 11.0), (5, 12.0)):
        close = anchor - timedelta(minutes=offset) + timedelta(seconds=10)
        points.append({
            "ts_open": close - timedelta(minutes=5),
            "ts_close": close,
            "exchange": "BINANCE",
            "symbol": "TESTUSDT",
            "oi_open": value,
            "oi_high": value,
            "oi_low": value,
            "oi_close": value,
        })

    monkeypatch.setattr(
        "aggregation_engine._fetch_metric_rows",
        lambda table, *_args, **_kwargs: points if table == "oi_raw" else [],
    )

    rows, _stats = build_aggregate_rows(
        window_hours=1,
        selected_windows=("15\u043c",),
        selected_metrics=("OI",),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row[0:6] == (
        "OI",
        "15\u043c",
        anchor - timedelta(minutes=15),
        anchor,
        "BINANCE",
        "TESTUSDT",
    )
    assert row[6] == 10.0
    assert row[9] == 12.0
    assert row[13] == 3
    assert _stats["skipped_non_contiguous"] == 0
