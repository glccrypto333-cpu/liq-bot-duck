from __future__ import annotations

from datetime import datetime, timezone

from autonomous_oi_service import WINDOWS, load_latest_window_map


def test_latest_window_map_uses_bounded_latest_lookup(monkeypatch):
    cycle_ts = datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)
    expected_row = {
        "metric": "OI",
        "window_code": "15м",
        "ts_open": cycle_ts,
        "ts_close": cycle_ts,
        "exchange": "BYBIT",
        "symbol": "ABCUSDT",
        "open_value": 100.0,
        "high_value": 101.0,
        "low_value": 99.0,
        "close_value": 100.5,
        "sum_value": None,
        "avg_value": None,
        "delta_pct": 0.5,
        "unique_candles": 3,
        "source_cycle_ts": cycle_ts,
        "built_at": cycle_ts,
    }
    captured = {}

    def fake_fetch(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [expected_row]

    monkeypatch.setattr("autonomous_oi_service.fetch", fake_fetch)

    result = load_latest_window_map(
        cycle_ts=cycle_ts,
        window_source="hot",
        tracked_pairs=[("BYBIT", "ABCUSDT")],
    )

    assert result[("BYBIT", "ABCUSDT")]["15м"]["OI"] == expected_row
    sql = " ".join(captured["sql"].split()).upper()
    assert "CROSS JOIN LATERAL" in sql
    assert "ORDER BY AW.TS_CLOSE DESC, AW.UNIQUE_CANDLES DESC NULLS LAST, AW.TS_OPEN ASC, AW.BUILT_AT DESC LIMIT 1" in sql
    assert "AW.SOURCE_CYCLE_TS <= %S OR AW.SOURCE_CYCLE_TS IS NULL" in sql
    assert "ACTIVE_SYMBOL_UNIVERSE" in sql
    assert "DATA_QUALITY_QUARANTINE" in sql
    assert "AGGREGATE_WINDOWS" in sql
    assert captured["params"][:2] == ("BYBIT", "ABCUSDT")
    assert captured["params"][2] == WINDOWS
    assert captured["params"][3] == cycle_ts


def test_latest_window_map_history_table_without_optional_filters(monkeypatch):
    captured = {}

    def fake_fetch(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr("autonomous_oi_service.fetch", fake_fetch)

    assert load_latest_window_map(window_source="history") == {}

    sql = " ".join(captured["sql"].split()).lower()
    assert "from aggregate_windows_history aw" in sql
    assert "cross join lateral" in sql
    assert "source_cycle_ts <=" not in sql
    assert "active_symbol_universe" in sql
    assert captured["params"] == (WINDOWS,)
