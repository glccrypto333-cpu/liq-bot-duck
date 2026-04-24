from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from db import fetch, replace_bot_aggregates
from metrics import изменение_в_процентах
from logger import log

WINDOWS = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}
WINDOW_MINUTES = {"15м": 15, "30м": 30, "1ч": 60, "4ч": 240}


def _groups(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["exchange"], row["symbol"])].append(row)

    for key in grouped:
        grouped[key].sort(key=lambda item: item["ts_open"])

    return grouped


def _window_close(ts_open, timeframe: str):
    return ts_open + timedelta(minutes=WINDOW_MINUTES[timeframe])


def rebuild_bot_aggregates() -> int:
    rows_out = []

    oi_rows = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые
        WHERE ts_close <= NOW() - interval '30 seconds'
        ORDER BY exchange, symbol, ts_open
    """)

    for (exchange, symbol), items in _groups(oi_rows).items():
        for timeframe, need in WINDOWS.items():
            if len(items) < need:
                continue
            for i in range(need - 1, len(items)):
                chunk = items[i - need + 1:i + 1]
                rows_out.append((
                    "OI", timeframe,
                    chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                    exchange, symbol,
                    chunk[0]["oi_open"],
                    max(x["oi_high"] for x in chunk),
                    min(x["oi_low"] for x in chunk),
                    chunk[-1]["oi_close"],
                    None, None,
                    изменение_в_процентах(chunk[0]["oi_open"], chunk[-1]["oi_close"]),
                    len(chunk),
                ))

    price_rows = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые
        WHERE ts_close <= NOW() - interval '30 seconds'
        ORDER BY exchange, symbol, ts_open
    """)

    for (exchange, symbol), items in _groups(price_rows).items():
        for timeframe, need in WINDOWS.items():
            if len(items) < need:
                continue
            for i in range(need - 1, len(items)):
                chunk = items[i - need + 1:i + 1]
                rows_out.append((
                    "PRICE", timeframe,
                    chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                    exchange, symbol,
                    chunk[0]["price_open"],
                    max(x["price_high"] for x in chunk),
                    min(x["price_low"] for x in chunk),
                    chunk[-1]["price_close"],
                    None, None,
                    изменение_в_процентах(chunk[0]["price_open"], chunk[-1]["price_close"]),
                    len(chunk),
                ))

    volume_rows = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые
        WHERE ts_close <= NOW() - interval '30 seconds'
        ORDER BY exchange, symbol, ts_open
    """)

    for (exchange, symbol), items in _groups(volume_rows).items():
        for timeframe, need in WINDOWS.items():
            if len(items) < need:
                continue
            for i in range(need - 1, len(items)):
                chunk = items[i - need + 1:i + 1]
                values = [x["volume"] for x in chunk]
                rows_out.append((
                    "VOLUME", timeframe,
                    chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                    exchange, symbol,
                    None,
                    max(values),
                    min(values),
                    None,
                    sum(values),
                    sum(values) / len(values),
                    None,
                    len(chunk),
                ))

    replace_bot_aggregates(rows_out)
    log(
        f"aggregates rebuilt: raw_oi={len(oi_rows)} "
        f"raw_price={len(price_rows)} raw_volume={len(volume_rows)} "
        f"aggregates={len(rows_out)}"
    )
    return len(rows_out)
