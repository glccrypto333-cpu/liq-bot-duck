from datetime import datetime, timezone

import exchange_clients


def test_binance_five_minute_collector_keeps_native_quote_turnover(monkeypatch):
    monkeypatch.setattr(
        exchange_clients,
        "_get",
        lambda *_args, **_kwargs: [[0, "1", "2", "0.5", "1.5", "10", 299999, "123.45"]],
    )
    monkeypatch.setattr(exchange_clients, "_closed", lambda _close: True)

    _, volume_rows = exchange_clients.fetch_binance_kline_5m("TESTUSDT", 1)

    assert volume_rows == [
        (datetime(1970, 1, 1, tzinfo=timezone.utc), datetime(1970, 1, 1, 0, 5, tzinfo=timezone.utc), "BINANCE", "TESTUSDT", 10.0, 123.45)
    ]


def test_bybit_five_minute_collector_keeps_native_quote_turnover(monkeypatch):
    monkeypatch.setattr(
        exchange_clients,
        "_get",
        lambda *_args, **_kwargs: {"result": {"list": [["0", "1", "2", "0.5", "1.5", "10", "456.78"]]}},
    )
    monkeypatch.setattr(exchange_clients, "_closed", lambda _close: True)

    _, volume_rows = exchange_clients.fetch_bybit_kline_5m("TESTUSDT", 1)

    assert volume_rows == [
        (datetime(1970, 1, 1, tzinfo=timezone.utc), datetime(1970, 1, 1, 0, 5, tzinfo=timezone.utc), "BYBIT", "TESTUSDT", 10.0, 456.78)
    ]
