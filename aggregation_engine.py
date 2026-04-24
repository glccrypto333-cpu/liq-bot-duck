from __future__ import annotations
from collections import defaultdict
from db import fetch_rows, bulk_insert, execute_sql
from metrics import изменение_в_процентах

МНОЖИТЕЛИ = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}

def _dedupe(rows: list[dict]) -> dict:
    grouped = defaultdict(dict)
    for r in rows:
        grouped[(r["exchange"], r["symbol"])][r["ts_open"]] = r
    return grouped

def rebuild_oi_aggregates() -> None:
    rows = fetch_rows("""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые
        WHERE ts_open >= NOW() - interval '2 days'
        ORDER BY exchange, symbol, ts_open
    """)
    grouped = _dedupe(rows)
    out = []
    for (exchange, symbol), mp in grouped.items():
        items = [mp[k] for k in sorted(mp.keys())]
        for окно, n in МНОЖИТЕЛИ.items():
            for i in range(n - 1, len(items), n):
                bucket = items[i - n + 1 : i + 1]
                out.append((
                    окно, bucket[0]["ts_open"], bucket[-1]["ts_close"], exchange, symbol,
                    bucket[0]["oi_open"], max(x["oi_high"] for x in bucket), min(x["oi_low"] for x in bucket),
                    bucket[-1]["oi_close"], изменение_в_процентах(bucket[0]["oi_open"], bucket[-1]["oi_close"])
                ))
    execute_sql("TRUNCATE TABLE oi_агрегаты")
    if out:
        bulk_insert("oi_агрегаты", out)

def rebuild_price_aggregates() -> None:
    rows = fetch_rows("""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые
        WHERE ts_open >= NOW() - interval '2 days'
        ORDER BY exchange, symbol, ts_open
    """)
    grouped = _dedupe(rows)
    out = []
    for (exchange, symbol), mp in grouped.items():
        items = [mp[k] for k in sorted(mp.keys())]
        for окно, n in МНОЖИТЕЛИ.items():
            for i in range(n - 1, len(items), n):
                bucket = items[i - n + 1 : i + 1]
                out.append((
                    окно, bucket[0]["ts_open"], bucket[-1]["ts_close"], exchange, symbol,
                    bucket[0]["price_open"], max(x["price_high"] for x in bucket), min(x["price_low"] for x in bucket),
                    bucket[-1]["price_close"], изменение_в_процентах(bucket[0]["price_open"], bucket[-1]["price_close"])
                ))
    execute_sql("TRUNCATE TABLE price_агрегаты")
    if out:
        bulk_insert("price_агрегаты", out)

def rebuild_volume_aggregates() -> None:
    rows = fetch_rows("""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые
        WHERE ts_open >= NOW() - interval '2 days'
        ORDER BY exchange, symbol, ts_open
    """)
    grouped = _dedupe(rows)
    out = []
    for (exchange, symbol), mp in grouped.items():
        items = [mp[k] for k in sorted(mp.keys())]
        for окно, n in МНОЖИТЕЛИ.items():
            for i in range(n - 1, len(items), n):
                bucket = items[i - n + 1 : i + 1]
                vals = [x["volume"] for x in bucket]
                out.append((окно, bucket[0]["ts_open"], bucket[-1]["ts_close"], exchange, symbol, sum(vals), sum(vals) / len(vals), max(vals), min(vals)))
    execute_sql("TRUNCATE TABLE volume_агрегаты")
    if out:
        bulk_insert("volume_агрегаты", out)
