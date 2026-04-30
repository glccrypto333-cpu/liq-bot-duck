from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from db import fetch, replace_bot_aggregates, insert_bot_aggregates, active_universe_sql
from metrics import изменение_в_процентах
from logger import log

WINDOWS = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}
WINDOW_MINUTES = {"15м": 15, "30м": 30, "1ч": 60, "4ч": 240}
FIVE_MINUTES = timedelta(minutes=5)


def _groups(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["exchange"], row["symbol"])].append(row)

    for key in grouped:
        grouped[key].sort(key=lambda item: item["ts_open"])

    return grouped


def _window_close(ts_open, timeframe: str):
    return ts_open + timedelta(minutes=WINDOW_MINUTES[timeframe])


def _is_contiguous_5m(chunk) -> bool:
    """
    v3.5.4 data-quality rule.

    Биржа отдаёт native 5m candles. Мы НЕ реконструируем 5m из 1m.

    Но если в базе есть дырка:
        10:00, 10:05, 10:15
    такое окно нельзя считать валидным 15m окном.

    Aggregate создаётся только если каждая следующая свеча идёт
    строго через 5 минут после предыдущей.
    """
    if not chunk:
        return False

    for prev, current in zip(chunk, chunk[1:]):
        if current["ts_open"] - prev["ts_open"] != FIVE_MINUTES:
            return False

    return True


def _flush_aggregates(rows_out: list[tuple]) -> int:
    if not rows_out:
        return 0
    insert_bot_aggregates(rows_out)
    return len(rows_out)


def rebuild_bot_aggregates() -> int:
    replace_bot_aggregates([])
    rows_out = []
    total_out = 0
    flush_size = 5000
    skipped_non_contiguous = 0

    oi_rows = fetch(f"""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые x
        WHERE ts_close <= NOW() - interval '30 seconds'
          AND ts_close >= NOW() - interval '30 hours'
          AND {active_universe_sql("x")}
        ORDER BY exchange, symbol, ts_open
    """)

    for (exchange, symbol), items in _groups(oi_rows).items():
        for timeframe, need in WINDOWS.items():
            if len(items) < need:
                continue
            for i in range(need - 1, len(items)):
                chunk = items[i - need + 1:i + 1]
                if not _is_contiguous_5m(chunk):
                    skipped_non_contiguous += 1
                    continue
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
                if len(rows_out) >= flush_size:
                    total_out += _flush_aggregates(rows_out)
                    rows_out.clear()

    price_rows = fetch(f"""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые x
        WHERE ts_close <= NOW() - interval '30 seconds'
          AND ts_close >= NOW() - interval '30 hours'
          AND {active_universe_sql("x")}
        ORDER BY exchange, symbol, ts_open
    """)

    for (exchange, symbol), items in _groups(price_rows).items():
        for timeframe, need in WINDOWS.items():
            if len(items) < need:
                continue
            for i in range(need - 1, len(items)):
                chunk = items[i - need + 1:i + 1]
                if not _is_contiguous_5m(chunk):
                    skipped_non_contiguous += 1
                    continue
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
                if len(rows_out) >= flush_size:
                    total_out += _flush_aggregates(rows_out)
                    rows_out.clear()

    volume_rows = fetch(f"""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые x
        WHERE ts_close <= NOW() - interval '30 seconds'
          AND ts_close >= NOW() - interval '30 hours'
          AND {active_universe_sql("x")}
        ORDER BY exchange, symbol, ts_open
    """)

    for (exchange, symbol), items in _groups(volume_rows).items():
        for timeframe, need in WINDOWS.items():
            if len(items) < need:
                continue
            for i in range(need - 1, len(items)):
                chunk = items[i - need + 1:i + 1]
                if not _is_contiguous_5m(chunk):
                    skipped_non_contiguous += 1
                    continue
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
                if len(rows_out) >= flush_size:
                    total_out += _flush_aggregates(rows_out)
                    rows_out.clear()

    total_out += _flush_aggregates(rows_out)
    rows_out.clear()

    log(
        f"aggregates rebuilt: raw_oi={len(oi_rows)} "
        f"raw_price={len(price_rows)} raw_volume={len(volume_rows)} "
        f"aggregates={total_out} "
        f"skipped_non_contiguous={skipped_non_contiguous}"
    )
    return total_out
