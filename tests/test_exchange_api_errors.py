from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import exchange_clients
from exchange_clients import ExchangeAPIError
from main import collect
from raw_validate import validate_collected_raw


class _Response:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


def test_bybit_invalid_symbol_is_counted_and_not_retried(monkeypatch) -> None:
    calls = []
    def get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response({"retCode": 10001, "retMsg": "symbol invalid", "result": {"list": []}})

    monkeypatch.setattr(exchange_clients.requests, "get", get)
    monkeypatch.setattr(exchange_clients.time, "sleep", lambda _seconds: None)
    exchange_clients.reset_request_stats()

    with pytest.raises(ExchangeAPIError) as exc:
        exchange_clients._get(
            "https://api.bybit.com/v5/market/open-interest",
            {"symbol": "DRVUSDT"},
        )

    assert exc.value.ret_code == 10001
    assert exc.value.is_symbol_unavailable
    assert len(calls) == 1
    stats = exchange_clients.get_request_stats()["bybit.com/v5/market/open-interest"]
    assert stats["count"] == 1
    assert stats["errors"] == 1
    assert stats["retries"] == 0


def test_other_bybit_retcode_is_retried_and_reported_as_api_error(monkeypatch) -> None:
    calls = []
    def get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response({"retCode": 10006, "retMsg": "too many visits", "result": {"list": []}})

    monkeypatch.setattr(exchange_clients.requests, "get", get)
    monkeypatch.setattr(exchange_clients.time, "sleep", lambda _seconds: None)
    exchange_clients.reset_request_stats()

    with pytest.raises(ExchangeAPIError) as exc:
        exchange_clients._get(
            "https://api.bybit.com/v5/market/open-interest",
            {"symbol": "GOODUSDT"},
            retries=2,
        )

    assert exc.value.ret_code == 10006
    assert not exc.value.is_symbol_unavailable
    assert len(calls) == 2
    stats = exchange_clients.get_request_stats()["bybit.com/v5/market/open-interest"]
    assert stats["errors"] == 2
    assert stats["retries"] == 1


def test_collect_isolates_bybit_invalid_symbol_but_keeps_healthy_pair(monkeypatch) -> None:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts_close = now - timedelta(minutes=5)
    ts_open = ts_close - timedelta(minutes=5)

    def bybit_oi(symbol: str, _limit: int):
        if symbol == "DRVUSDT":
            raise ExchangeAPIError("bybit.com/v5/market/open-interest", 10001, "symbol invalid", {})
        return [(ts_open, ts_close, "BYBIT", symbol, 2.0, 2.0, 2.0, 2.0)]

    def bybit_kline(symbol: str, _limit: int):
        assert symbol != "DRVUSDT", "do not make a second request for an invalid symbol"
        price = [(ts_open, ts_close, "BYBIT", symbol, 1.0, 2.0, 0.5, 1.5)]
        volume = [(ts_open, ts_close, "BYBIT", symbol, 10.0, 12.0)]
        return price, volume

    monkeypatch.setattr("main.fetch_bybit_oi_5m", bybit_oi)
    monkeypatch.setattr("main.fetch_bybit_kline_5m", bybit_kline)
    monkeypatch.setattr("main.fetch_binance_oi_5m", lambda *_args: [])
    monkeypatch.setattr("main.fetch_binance_kline_5m", lambda *_args: ([], []))

    batch = collect(["DRVUSDT", "GOODUSDT"], [])

    assert batch["failures"] == []
    assert batch["symbol_unavailable"] == [
        ("BYBIT", "DRVUSDT", 10001, "symbol invalid")
    ]
    assert {row[3] for row in batch["oi_rows"]} == {"GOODUSDT"}
    assert {row[3] for row in batch["price_rows"]} == {"GOODUSDT"}
    assert validate_collected_raw(batch, ["DRVUSDT", "GOODUSDT"], []) is batch


def test_symbol_unavailable_limit_still_fails_closed() -> None:
    ts_open = datetime(2026, 1, 1)
    ts_close = datetime(2026, 1, 1, 0, 5)
    batch = {
        "oi_rows": [(ts_open, ts_close, "BYBIT", "GOODUSDT", 1, 1, 1, 1)],
        "price_rows": [(ts_open, ts_close, "BYBIT", "GOODUSDT", 1, 1, 1, 1)],
        "volume_rows": [(ts_open, ts_close, "BYBIT", "GOODUSDT", 1, 1)],
        "symbol_unavailable": [
            ("BYBIT", "DRV1USDT", 10001, "symbol invalid"),
            ("BYBIT", "DRV2USDT", 10001, "symbol invalid"),
            ("BYBIT", "DRV3USDT", 10001, "symbol invalid"),
        ],
        "failures": [],
    }

    with pytest.raises(RuntimeError, match="too many unavailable symbols"):
        validate_collected_raw(
            batch,
            ["GOODUSDT", "DRV1USDT", "DRV2USDT", "DRV3USDT"],
            [],
        )


def test_unavailable_symbol_tolerance_does_not_hide_other_request_failures() -> None:
    batch = {
        "oi_rows": [(datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 5), "BYBIT", "GOODUSDT", 1, 1, 1, 1)],
        "price_rows": [(datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 5), "BYBIT", "GOODUSDT", 1, 1, 1, 1)],
        "volume_rows": [(datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 5), "BYBIT", "GOODUSDT", 1, 1)],
        "symbol_unavailable": [("BYBIT", "DRVUSDT", 10001, "symbol invalid")],
        "failures": [(datetime.now(timezone.utc), "BYBIT", "GOODUSDT", "OI", "Timeout", "timeout")],
    }

    with pytest.raises(RuntimeError, match="request failures"):
        validate_collected_raw(batch, ["DRVUSDT", "GOODUSDT"], [])
