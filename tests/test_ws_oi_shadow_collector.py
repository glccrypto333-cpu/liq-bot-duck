from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tools.ws_oi_shadow_collector import normalize_bybit_ticker_message


def test_normalize_bybit_ticker_message_extracts_open_interest() -> None:
    record = normalize_bybit_ticker_message(
        {
            "topic": "tickers.MAGMAUSDT",
            "ts": 1780000000000,
            "data": {"symbol": "MAGMAUSDT", "openInterest": "123.45"},
        },
        received_at="2026-07-07T10:00:00+00:00",
    )

    assert record == {
        "exchange": "BYBIT",
        "symbol": "MAGMAUSDT",
        "received_at": "2026-07-07T10:00:00+00:00",
        "event_ts_ms": 1780000000000,
        "open_interest": 123.45,
        "raw_topic": "tickers.MAGMAUSDT",
    }


def test_normalize_bybit_ticker_message_ignores_missing_open_interest() -> None:
    record = normalize_bybit_ticker_message(
        {"topic": "tickers.MAGMAUSDT", "data": {"symbol": "MAGMAUSDT"}},
        received_at="2026-07-07T10:00:00+00:00",
    )

    assert record is None
