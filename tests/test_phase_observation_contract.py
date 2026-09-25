from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db
from autonomous_oi_service import build_phase_decision_observation_row


class FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        self.sql = sql
        self.rows = rows

    def execute(self, _sql: str) -> None:
        return None


class FakeConn:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self._cursor


def test_phase_observation_persists_explicit_pre_and_post_transition_ages(monkeypatch) -> None:
    cursor = FakeCursor()
    monkeypatch.setattr(db, "DATABASE_URL", "postgresql://test")
    monkeypatch.setattr(db, "_conn", lambda: FakeConn(cursor))
    row = (
        "BINANCE", "TESTUSDT", "2026-09-14T00:00:00+00:00", 2, 3,
        0.0, 30.0, 30.0, 0.0, "good_up", "good_up", "strong_up", "good_up",
        "decision", "guard", "разрешен_ранний_вход_в_3",
    )

    db.insert_phase_decision_observations([row])

    assert "stage_age_before_transition" in cursor.sql
    assert "stage_age_after_transition" in cursor.sql
    assert "transition_permission_pre" in cursor.sql
    assert cursor.rows == [row]


def test_phase_observation_row_keeps_pre_age_separate_from_new_stage_age() -> None:
    row = build_phase_decision_observation_row(
        exchange="BINANCE",
        symbol="TESTUSDT",
        cycle_ts="2026-09-14T00:00:00+00:00",
        previous_stage=2,
        target_stage=3,
        stage_age_before_transition=30.0,
        stage_age_after_transition=0.0,
        trigger_age_minutes=60.0,
        oi_summary={
            "oi_slope_class_15m": "flat",
            "oi_slope_class_30m": "good_up",
            "oi_slope_class_1h": "good_up",
            "oi_slope_class_4h": "good_up",
        },
        decision_reason="decision",
        guard_reason="guard",
        transition_permission_pre="разрешен_вход_в_3",
    )

    assert row[5:9] == (0.0, 30.0, 0.0, 60.0)
    assert row[-1] == "разрешен_вход_в_3"
