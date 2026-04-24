from __future__ import annotations
from collections import defaultdict
from db import fetch, replace_bot_aggregates
from metrics import изменение_в_процентах
from logger import log

WINDOWS = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}

def _groups(rows):
    g = defaultdict(list)
    for r in rows:
        g[(r["exchange"], r["symbol"])].append(r)
    for k in g:
        g[k].sort(key=lambda x: x["ts_open"])
    return g

def rebuild_bot_aggregates() -> int:
    rows_out = []

    oi_rows = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые
        WHERE ts_close <= NOW() - interval '30 seconds'
        ORDER BY exchange, symbol, ts_open
    """)
    for (exchange, symbol), items in _groups(oi_rows).items():
        for tf, n in WINDOWS.items():
            if len(items) < n:
                continue
            for i in range(n - 1, len(items)):
                b = items[i - n + 1:i + 1]
                rows_out.append(("OI", tf, b[0]["ts_open"], b[-1]["ts_close"], exchange, symbol, b[0]["oi_open"], max(x["oi_high"] for x in b), min(x["oi_low"] for x in b), b[-1]["oi_close"], None, None, изменение_в_процентах(b[0]["oi_open"], b[-1]["oi_close"]), len(b)))

    price_rows = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые
        WHERE ts_close <= NOW() - interval '30 seconds'
        ORDER BY exchange, symbol, ts_open
    """)
    for (exchange, symbol), items in _groups(price_rows).items():
        for tf, n in WINDOWS.items():
            if len(items) < n:
                continue
            for i in range(n - 1, len(items)):
                b = items[i - n + 1:i + 1]
                rows_out.append(("PRICE", tf, b[0]["ts_open"], b[-1]["ts_close"], exchange, symbol, b[0]["price_open"], max(x["price_high"] for x in b), min(x["price_low"] for x in b), b[-1]["price_close"], None, None, изменение_в_процентах(b[0]["price_open"], b[-1]["price_close"]), len(b)))

    vol_rows = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые
        WHERE ts_close <= NOW() - interval '30 seconds'
        ORDER BY exchange, symbol, ts_open
    """)
    for (exchange, symbol), items in _groups(vol_rows).items():
        for tf, n in WINDOWS.items():
            if len(items) < n:
                continue
            for i in range(n - 1, len(items)):
                b = items[i - n + 1:i + 1]
                vals = [x["volume"] for x in b]
                rows_out.append(("VOLUME", tf, b[0]["ts_open"], b[-1]["ts_close"], exchange, symbol, None, max(vals), min(vals), None, sum(vals), sum(vals)/len(vals), None, len(b)))

    replace_bot_aggregates(rows_out)
    log(f"aggregates rebuilt: raw_oi={len(oi_rows)} raw_price={len(price_rows)} raw_volume={len(vol_rows)} aggregates={len(rows_out)}")
    return len(rows_out)
