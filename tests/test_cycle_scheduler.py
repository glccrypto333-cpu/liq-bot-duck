from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import main


def test_aligned_cycle_sleep_seconds_aligns_to_next_boundary(monkeypatch) -> None:
    monkeypatch.setenv("CYCLE_ALIGN_OFFSET_SECONDS", "5")
    monkeypatch.setattr(main, "ИНТЕРВАЛ_ЦИКЛА_СЕК", 300)
    monkeypatch.setattr(main.time, "time", lambda: 1_700_000_385.0)

    sleep_seconds = main._aligned_cycle_sleep_seconds(118.0)

    assert sleep_seconds == 20.0


def test_aligned_cycle_sleep_seconds_works_after_boundary(monkeypatch) -> None:
    monkeypatch.setenv("CYCLE_ALIGN_OFFSET_SECONDS", "5")
    monkeypatch.setattr(main, "ИНТЕРВАЛ_ЦИКЛА_СЕК", 300)
    monkeypatch.setattr(main.time, "time", lambda: 1_700_000_406.0)

    sleep_seconds = main._aligned_cycle_sleep_seconds(125.0)

    assert sleep_seconds == 299.0
