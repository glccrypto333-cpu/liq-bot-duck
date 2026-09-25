from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
import db


def test_raw_retention_days_default_is_3(monkeypatch) -> None:
    monkeypatch.delenv("RAW_RETENTION_DAYS", raising=False)
    importlib.reload(config)

    assert config.RAW_RETENTION_DAYS == 3


def test_legacy_retention_days_default_follows_raw_default(monkeypatch) -> None:
    monkeypatch.delenv("RAW_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("RETENTION_DAYS", raising=False)
    importlib.reload(config)

    assert config.ДНЕЙ_ХРАНЕНИЯ == 3


def test_legacy_retention_days_falls_back_to_raw_retention_days(monkeypatch) -> None:
    monkeypatch.setenv("RAW_RETENTION_DAYS", "5")
    monkeypatch.delenv("RETENTION_DAYS", raising=False)
    importlib.reload(config)

    assert config.ДНЕЙ_ХРАНЕНИЯ == 5


def test_legacy_retention_days_respects_explicit_retention_days(monkeypatch) -> None:
    monkeypatch.setenv("RAW_RETENTION_DAYS", "5")
    monkeypatch.setenv("RETENTION_DAYS", "9")
    importlib.reload(config)

    assert config.ДНЕЙ_ХРАНЕНИЯ == 9


def test_derived_retention_hours_default_is_36(monkeypatch) -> None:
    monkeypatch.delenv("DERIVED_RETENTION_HOURS", raising=False)

    assert db._derived_retention_hours() == 36


def test_history_retention_hours_default_is_72(monkeypatch) -> None:
    monkeypatch.delenv("AGGREGATE_HISTORY_RETENTION_HOURS", raising=False)

    assert db.history_retention_hours() == 72
