from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from raw_validate import validate_collected_raw


def _build_batch(total_symbols: int, missing_symbols: int) -> tuple[dict, list[str], list[str]]:
    symbols_bybit = [f"SYM{i:04d}USDT" for i in range(total_symbols)]
    symbols_binance: list[str] = []

    oi_rows = []
    price_rows = []
    volume_rows = []

    for idx, symbol in enumerate(symbols_bybit):
        ts_open = __import__("datetime").datetime(2026, 6, 24, 16, 0)
        ts_close = __import__("datetime").datetime(2026, 6, 24, 16, 5)
        oi_rows.append((ts_open, ts_close, "BYBIT", symbol, 1.0, 1.0, 1.0, 1.0))
        if idx >= missing_symbols:
            price_rows.append((ts_open, ts_close, "BYBIT", symbol, 1.0, 1.0, 1.0, 1.0))
            volume_rows.append((ts_open, ts_close, "BYBIT", symbol, 1.0))

    batch = {
        "oi_rows": oi_rows,
        "price_rows": price_rows,
        "volume_rows": volume_rows,
        "failures": [],
    }
    return batch, symbols_bybit, symbols_binance


def _drop_oi_rows(batch: dict, symbols: set[str]) -> None:
    batch["oi_rows"] = [row for row in batch["oi_rows"] if row[3] not in symbols]


def test_raw_validate_error_shows_exchange_breakdown(monkeypatch) -> None:
    monkeypatch.setenv("RAW_VALIDATE_TOLERATED_PRICE_VOLUME_MISSING", "10")
    monkeypatch.setenv("RAW_VALIDATE_TOLERATED_PRICE_VOLUME_MISSING_PCT", "0.04")
    batch, bybit, binance = _build_batch(total_symbols=1122, missing_symbols=83)

    with pytest.raises(RuntimeError) as exc:
        validate_collected_raw(batch, bybit, binance)

    message = str(exc.value)
    assert "missing_exchange_counts=" in message
    assert "BYBIT:83" in message


def test_raw_validate_tolerates_83_price_volume_missing_with_8pct(monkeypatch) -> None:
    monkeypatch.setenv("RAW_VALIDATE_TOLERATED_PRICE_VOLUME_MISSING", "10")
    monkeypatch.setenv("RAW_VALIDATE_TOLERATED_PRICE_VOLUME_MISSING_PCT", "0.08")
    batch, bybit, binance = _build_batch(total_symbols=1122, missing_symbols=83)

    result = validate_collected_raw(batch, bybit, binance)

    assert result is batch


def test_raw_validate_tolerates_two_missing_oi_pairs(monkeypatch) -> None:
    monkeypatch.setenv("RAW_VALIDATE_TOLERATED_OI_MISSING", "2")
    batch, bybit, binance = _build_batch(total_symbols=4, missing_symbols=0)
    _drop_oi_rows(batch, {"SYM0001USDT", "SYM0002USDT"})

    result = validate_collected_raw(batch, bybit, binance)

    assert result is batch


def test_raw_validate_rejects_three_missing_oi_pairs(monkeypatch) -> None:
    monkeypatch.setenv("RAW_VALIDATE_TOLERATED_OI_MISSING", "2")
    batch, bybit, binance = _build_batch(total_symbols=4, missing_symbols=0)
    _drop_oi_rows(batch, {"SYM0001USDT", "SYM0002USDT", "SYM0003USDT"})

    with pytest.raises(RuntimeError, match="missing_oi=3") as exc:
        validate_collected_raw(batch, bybit, binance)

    assert "missing_oi_exchange_counts=BYBIT:3" in str(exc.value)
