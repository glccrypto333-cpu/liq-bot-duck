from __future__ import annotations

from datetime import datetime, timezone

import db


class _FakeCursor:
    def __init__(self, captured):
        self.captured = captured

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def executemany(self, sql, rows):
        self.captured["rows"] = rows


class _FakeConn:
    def __init__(self, captured):
        self.captured = captured

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self.captured)


def test_insert_oi_stage_history_skips_batch_and_existing_duplicates(monkeypatch) -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    duplicate = ("BINANCE", "FLOCKUSDT", 2, 3, "early", True, 45.0, cycle_ts)
    fresh = ("BINANCE", "FRESHUSDT", 2, 3, "early", True, 45.0, cycle_ts)
    captured = {}

    monkeypatch.setattr(db, "DATABASE_URL", "postgres://test")
    monkeypatch.setattr(db, "_conn", lambda: _FakeConn(captured))
    monkeypatch.setattr(
        db,
        "fetch",
        lambda sql, params=(): [
            {
                "exchange": "BINANCE",
                "symbol": "FLOCKUSDT",
                "from_stage": 2,
                "to_stage": 3,
                "cycle_ts": cycle_ts,
                "transition_reason": "early",
            }
        ],
    )

    db.insert_oi_stage_history([duplicate, duplicate, fresh])

    assert captured["rows"] == [fresh]


def test_insert_oi_stage_history_returns_before_executemany_when_all_rows_are_duplicates(monkeypatch) -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    duplicate = ("BINANCE", "FLOCKUSDT", 2, 3, "early", True, 45.0, cycle_ts)
    captured = {}

    monkeypatch.setattr(db, "DATABASE_URL", "postgres://test")
    monkeypatch.setattr(db, "_conn", lambda: _FakeConn(captured))
    monkeypatch.setattr(
        db,
        "fetch",
        lambda sql, params=(): [
            {
                "exchange": "BINANCE",
                "symbol": "FLOCKUSDT",
                "from_stage": 2,
                "to_stage": 3,
                "cycle_ts": cycle_ts,
                "transition_reason": "early",
            }
        ],
    )

    db.insert_oi_stage_history([duplicate, duplicate])

    assert "rows" not in captured


def test_insert_transition_history_v2_skips_batch_and_existing_duplicates(monkeypatch) -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    duplicate = ("BINANCE", "FLOCKUSDT", 2, 3, cycle_ts, True, 45.0, "early")
    fresh = ("BINANCE", "FRESHUSDT", 2, 3, cycle_ts, True, 45.0, "early")
    captured = {}

    monkeypatch.setattr(db, "DATABASE_URL", "postgres://test")
    monkeypatch.setattr(db, "_conn", lambda: _FakeConn(captured))
    monkeypatch.setattr(
        db,
        "fetch",
        lambda sql, params=(): [
            {
                "exchange": "BINANCE",
                "symbol": "FLOCKUSDT",
                "from_stage": 2,
                "to_stage": 3,
                "cycle_ts": cycle_ts,
                "reason": "early",
            }
        ],
    )

    db.insert_transition_history_v2([duplicate, duplicate, fresh])

    assert captured["rows"] == [fresh]


def test_insert_transition_history_v2_returns_before_executemany_when_all_rows_are_duplicates(monkeypatch) -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    duplicate = ("BINANCE", "FLOCKUSDT", 2, 3, cycle_ts, True, 45.0, "early")
    captured = {}

    monkeypatch.setattr(db, "DATABASE_URL", "postgres://test")
    monkeypatch.setattr(db, "_conn", lambda: _FakeConn(captured))
    monkeypatch.setattr(
        db,
        "fetch",
        lambda sql, params=(): [
            {
                "exchange": "BINANCE",
                "symbol": "FLOCKUSDT",
                "from_stage": 2,
                "to_stage": 3,
                "cycle_ts": cycle_ts,
                "reason": "early",
            }
        ],
    )

    db.insert_transition_history_v2([duplicate, duplicate])

    assert "rows" not in captured
