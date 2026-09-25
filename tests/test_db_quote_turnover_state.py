from datetime import datetime, timedelta, timezone

import db


class _Cursor:
    def __init__(self):
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, rows))


class _Connection:
    def __init__(self, cursor):
        self.cursor_value = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self.cursor_value


def test_refresh_persists_latest_per_pair_without_historical_growth_table(monkeypatch):
    source_cycle_ts = datetime(2026, 9, 22, 16, 0, tzinfo=timezone.utc)
    rows = [
        {
            "exchange": "BINANCE",
            "symbol": "TESTUSDT",
            "ts_open": source_cycle_ts - timedelta(minutes=5 * (96 - index)),
            "ts_close": source_cycle_ts - timedelta(minutes=5 * (95 - index)),
            "quote_turnover": 100.0 if index < 48 else 300.0,
        }
        for index in range(96)
    ]
    cursor = _Cursor()
    monkeypatch.setattr(db, "DATABASE_URL", "postgres://test")
    monkeypatch.setattr(db, "fetch", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(db, "_conn", lambda: _Connection(cursor))

    result = db.refresh_quote_turnover_state(source_cycle_ts)

    assert result["ready"] == 1
    assert result["degraded"] == 0
    written = cursor.executemany_calls[0][1][0]
    assert written[0:2] == ("BINANCE", "TESTUSDT")
    assert written[4:7] == (4800.0, 14400.0, 200.0)
    assert written[10:13] == (48, 48, 0.0)
    assert written[13:15] == (True, "ready")


def test_backfill_targets_are_bounded_and_only_for_non_ready_pairs(monkeypatch):
    captured = {}

    def fake_fetch(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [{"exchange": "BYBIT", "symbol": "NEWUSDT"}]

    monkeypatch.setattr(db, "DATABASE_URL", "postgres://test")
    monkeypatch.setattr(db, "fetch", fake_fetch)

    assert db.select_quote_turnover_backfill_targets(24) == {("BYBIT", "NEWUSDT")}
    assert captured["params"] == (24,)
    assert "quote_turnover_state" in captured["sql"]
