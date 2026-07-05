from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

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


def _price_row(ts_open: datetime, exchange: str, symbol: str, open_value: float, close_value: float) -> dict:
    high_value = max(open_value, close_value) + 1.0
    low_value = min(open_value, close_value) - 1.0
    return {
        "ts_open": ts_open,
        "ts_close": ts_open + timedelta(minutes=5),
        "exchange": exchange,
        "symbol": symbol,
        "price_open": open_value,
        "price_high": high_value,
        "price_low": low_value,
        "price_close": close_value,
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

    monkeypatch.setattr(
        "aggregation_engine._resolve_symbol_anchor_map_from_db",
        lambda *args, **kwargs: {("BYBIT", "TESTUSDT"): source_cycle_ts},
    )
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


def test_build_latest_aggregate_rows_anchors_oi_open_to_previous_close(monkeypatch) -> None:
    source_cycle_ts = datetime(2026, 6, 24, 16, 0)
    starts = [
        datetime(2026, 6, 24, 15, 40),
        datetime(2026, 6, 24, 15, 45),
        datetime(2026, 6, 24, 15, 50),
        datetime(2026, 6, 24, 15, 55),
    ]
    oi_rows = [_oi_row(ts_open, "BYBIT", "TESTUSDT", 100.0 + index * 10.0) for index, ts_open in enumerate(starts)]

    def fake_fetch(table, select_sql, alias, active_only, cycle_ts):
        assert cycle_ts == source_cycle_ts
        if table == "oi_raw":
            return oi_rows
        return []

    monkeypatch.setattr(
        "aggregation_engine._resolve_symbol_anchor_map_from_db",
        lambda *args, **kwargs: {("BYBIT", "TESTUSDT"): source_cycle_ts},
    )
    monkeypatch.setattr("aggregation_engine._fetch_recent_metric_rows", fake_fetch)

    rows, stats = build_latest_aggregate_rows(
        source_cycle_ts,
        selected_windows=("15м",),
        selected_metrics=("OI",),
        active_only=True,
    )

    assert stats["aggregates"] == 1
    row = rows[0]
    assert row[6] == oi_rows[0]["oi_close"]
    assert row[9] == oi_rows[-1]["oi_close"]
    assert row[12] == pytest.approx(29.850746268656717)
    assert row[14] == [oi_rows[0]["oi_close"], oi_rows[1]["oi_close"], oi_rows[2]["oi_close"], oi_rows[3]["oi_close"]]


def test_build_latest_aggregate_rows_uses_price_first_open_and_last_close(monkeypatch) -> None:
    source_cycle_ts = datetime(2026, 7, 2, 6, 0)
    starts = [
        datetime(2026, 7, 2, 5, 45),
        datetime(2026, 7, 2, 5, 50),
        datetime(2026, 7, 2, 5, 55),
    ]
    price_rows = [
        _price_row(starts[0], "BINANCE", "TESTUSDT", 10.0, 12.0),
        _price_row(starts[1], "BINANCE", "TESTUSDT", 12.0, 11.0),
        _price_row(starts[2], "BINANCE", "TESTUSDT", 11.0, 15.0),
    ]

    def fake_fetch(table, select_sql, alias, active_only, cycle_ts):
        assert cycle_ts == source_cycle_ts
        if table == "price_raw":
            return price_rows
        return []

    monkeypatch.setattr(
        "aggregation_engine._resolve_symbol_anchor_map_from_db",
        lambda *args, **kwargs: {("BINANCE", "TESTUSDT"): source_cycle_ts},
    )
    monkeypatch.setattr("aggregation_engine._fetch_recent_metric_rows", fake_fetch)

    rows, stats = build_latest_aggregate_rows(
        source_cycle_ts,
        selected_windows=("15м",),
        selected_metrics=("PRICE",),
        active_only=True,
    )

    assert stats["aggregates"] == 1
    row = rows[0]
    assert row[0] == "PRICE"
    assert row[6] == 10.0
    assert row[7] == 16.0
    assert row[8] == 9.0
    assert row[9] == 15.0
    assert row[12] == pytest.approx(50.0)
    assert row[14] is None


def test_build_latest_aggregate_rows_uses_symbol_anchor_not_global_anchor(monkeypatch) -> None:
    requested_cycle_ts = datetime(2026, 6, 24, 16, 25)
    starts = [
        datetime(2026, 6, 24, 16, 5),
        datetime(2026, 6, 24, 16, 10),
        datetime(2026, 6, 24, 16, 15),
        datetime(2026, 6, 24, 16, 20),
    ]
    oi_rows = [_oi_row(ts_open, "BYBIT", "TESTUSDT", float(index + 1)) for index, ts_open in enumerate(starts)]
    price_rows = [_oi_row(ts_open, "BYBIT", "TESTUSDT", float(index + 10)) for index, ts_open in enumerate(starts)]
    volume_rows = [_oi_row(ts_open, "BYBIT", "TESTUSDT", float(index + 20)) for index, ts_open in enumerate(starts[:3])]

    def fake_fetch(table, select_sql, alias, active_only, cycle_ts):
        assert cycle_ts == requested_cycle_ts
        if table == "oi_raw":
            return oi_rows
        if table == "price_raw":
            remapped = []
            for row in price_rows:
                remapped.append({
                    "ts_open": row["ts_open"],
                    "ts_close": row["ts_close"],
                    "exchange": row["exchange"],
                    "symbol": row["symbol"],
                    "price_open": row["oi_open"],
                    "price_high": row["oi_high"],
                    "price_low": row["oi_low"],
                    "price_close": row["oi_close"],
                })
            return remapped
        if table == "volume_raw":
            remapped = []
            for row in volume_rows:
                remapped.append({
                    "ts_open": row["ts_open"],
                    "ts_close": row["ts_close"],
                    "exchange": row["exchange"],
                    "symbol": row["symbol"],
                    "volume": row["oi_close"],
                })
            return remapped
        return []

    monkeypatch.setattr(
        "aggregation_engine._resolve_symbol_anchor_map_from_db",
        lambda *args, **kwargs: {("BYBIT", "TESTUSDT"): datetime(2026, 6, 24, 16, 20)},
    )
    monkeypatch.setattr("aggregation_engine._fetch_recent_metric_rows", fake_fetch)

    rows, stats = build_latest_aggregate_rows(
        requested_cycle_ts,
        selected_windows=("15м",),
        selected_metrics=("OI", "PRICE", "VOLUME"),
        active_only=True,
    )

    assert stats["aggregates"] == 3
    assert stats["symbol_anchors"] == 1
    assert all(row[3] == datetime(2026, 6, 24, 16, 20) for row in rows)


def test_rebuild_latest_aggregate_windows_uses_requested_cycle_as_upper_bound(monkeypatch) -> None:
    requested_cycle_ts = datetime(2026, 6, 24, 16, 14, 40)
    captured = {}

    def fake_build_latest(source_cycle_ts, selected_windows=None, selected_metrics=None, active_only=True):
        captured["source_cycle_ts"] = source_cycle_ts
        return ([(
            "OI", "15м",
            datetime(2026, 6, 24, 16, 0),
            requested_cycle_ts,
            "BYBIT", "TESTUSDT",
            1.0, 2.0, 0.5, 1.5,
            None, None, 50.0, 3,
        )], {
            "raw_oi": 3,
            "raw_price": 0,
            "raw_volume": 0,
            "aggregates": 1,
            "skipped_non_contiguous": 0,
            "symbol_anchors": 1,
        })

    monkeypatch.setattr("aggregation_engine.build_latest_aggregate_rows", fake_build_latest)
    monkeypatch.setattr("aggregation_engine.upsert_aggregate_hot_rows", lambda rows: len(rows))
    monkeypatch.setattr("aggregation_engine.sync_aggregate_history_from_rows", lambda rows: (0, 0))

    result = rebuild_latest_aggregate_windows(requested_cycle_ts)

    assert result == 1
    assert captured["source_cycle_ts"] == requested_cycle_ts
