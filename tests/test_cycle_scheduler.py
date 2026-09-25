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


def test_cycle_step_budget_blocks_validate_when_reserve_is_gone(monkeypatch) -> None:
    monkeypatch.setattr(main, "ИНТЕРВАЛ_ЦИКЛА_СЕК", 300)

    assert main._cycle_step_fits_budget(
        elapsed_seconds=298.88,
        expected_seconds=12.0,
        reserve_seconds=10.0,
    ) is False


def test_cycle_step_budget_allows_validate_with_room(monkeypatch) -> None:
    monkeypatch.setattr(main, "ИНТЕРВАЛ_ЦИКЛА_СЕК", 300)

    assert main._cycle_step_fits_budget(
        elapsed_seconds=250.0,
        expected_seconds=12.0,
        reserve_seconds=10.0,
    ) is True


def test_cleanup_old_budget_skips_when_cycle_is_already_tight(monkeypatch) -> None:
    monkeypatch.setattr(main, "ИНТЕРВАЛ_ЦИКЛА_СЕК", 300)
    monkeypatch.delenv("CLEANUP_OLD_EXPECTED_SECONDS", raising=False)
    monkeypatch.delenv("CLEANUP_OLD_RESERVE_SECONDS", raising=False)

    assert main._cleanup_old_fits_budget(266.7) is False


def test_cleanup_old_budget_runs_when_cycle_has_real_headroom(monkeypatch) -> None:
    monkeypatch.setattr(main, "ИНТЕРВАЛ_ЦИКЛА_СЕК", 300)
    monkeypatch.delenv("CLEANUP_OLD_EXPECTED_SECONDS", raising=False)
    monkeypatch.delenv("CLEANUP_OLD_RESERVE_SECONDS", raising=False)

    assert main._cleanup_old_fits_budget(220.0) is True


def test_aggregates_validate_treats_12h_24h_as_optional(monkeypatch) -> None:
    rows = []
    for metric in ("OI", "PRICE", "VOLUME"):
        for window in ("15м", "30м", "1ч", "4ч"):
            rows.append({"metric": metric, "window_code": window, "row_count": 100})
    spans = [
        {"metric": "OI", "row_count": 1000, "span_minutes": 780.0},
        {"metric": "PRICE", "row_count": 1000, "span_minutes": 780.0},
        {"metric": "VOLUME", "row_count": 1000, "span_minutes": 780.0},
    ]

    def fake_fetch(sql):
        if "GROUP BY metric, window_code" in sql:
            return rows
        if "UNION ALL" in sql:
            return spans
        return []

    monkeypatch.setattr(main, "fetch", fake_fetch)
    monkeypatch.setattr(main, "log", lambda message: None)

    result = main.validate_aggregate_windows()

    assert "OI:12ч:optional_missing" in result["warmup_pending"]
    assert "PRICE:12ч:optional_missing" in result["warmup_pending"]
    assert "VOLUME:12ч:optional_missing" in result["warmup_pending"]
