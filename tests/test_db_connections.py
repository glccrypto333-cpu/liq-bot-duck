from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.executed.append((sql, params))

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        self.executemany_calls.append((sql, rows))

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.autocommit = True
        self.closed = False
        self.cursor_obj = _FakeCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_replace_aggregate_layers_atomically_uses_dedicated_connection(monkeypatch) -> None:
    fake_conn = _FakeConn()

    monkeypatch.setattr(db, "DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(db, "_derived_retention_hours", lambda: 26)

    def _boom():
        raise AssertionError("shared _conn must not be used here")

    monkeypatch.setattr(db, "_conn", _boom)
    monkeypatch.setattr(db, "_fresh_conn", lambda: fake_conn)

    rows = [
        ("OI", "15m", "2026-06-23 20:00:00+00", "2026-06-23 20:15:00+00", "BYBIT", "TESTUSDT", 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 3),
    ]

    db.replace_aggregate_layers_atomically(rows)

    assert fake_conn.committed is True
    assert fake_conn.rolled_back is False
    assert any("pg_advisory_xact_lock" in sql for sql, _ in fake_conn.cursor_obj.executed)
    assert len(fake_conn.cursor_obj.executemany_calls) == 1
    insert_sql, _ = fake_conn.cursor_obj.executemany_calls[0]
    assert "ON CONFLICT (metric, window_code, exchange, symbol, ts_open)" in insert_sql
