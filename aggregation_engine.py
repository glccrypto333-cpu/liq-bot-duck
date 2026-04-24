from __future__ import annotations
from collections import defaultdict
from db import fetch, execute, upsert_bot_aggregates
from metrics import изменение_в_процентах

WINDOWS = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}

def _groups(rows):
    g = defaultdict(list)
    for r in rows:
        g[(r["exchange"], r["symbol"])].append(r)
    for k in g:
        g[k].sort(key=lambda x: x["ts_open"])
    return g

def rebuild_bot_aggregates() -> None:
    execute("TRUNCATE TABLE bot_aggregates")
    rows_out = []

    oi_rows = fetch("SELECT * FROM oi_5m_сырые WHERE ts_close <= NOW() - interval '30 seconds' AND ts_open >= NOW() - interval '30 days' ORDER BY exchange, symbol, ts_open")
    for (exchange, symbol), items in _groups(oi_rows).items():
        for tf, n in WINDOWS.items():
            for i in range(n - 1, len(items)):
                b = items[i - n + 1:i + 1]
                rows_out.append(("OI", tf, b[0]["ts_open"], b[-1]["ts_close"], exchange, symbol, b[0]["oi_open"], max(x["oi_high"] for x in b), min(x["oi_low"] for x in b), b[-1]["oi_close"], None, None, изменение_в_процентах(b[0]["oi_open"], b[-1]["oi_close"]), len(b)))

    price_rows = fetch("SELECT * FROM price_5m_сырые WHERE ts_close <= NOW() - interval '30 seconds' AND ts_open >= NOW() - interval '30 days' ORDER BY exchange, symbol, ts_open")
    for (exchange, symbol), items in _groups(price_rows).items():
        for tf, n in WINDOWS.items():
            for i in range(n - 1, len(items)):
                b = items[i - n + 1:i + 1]
                rows_out.append(("PRICE", tf, b[0]["ts_open"], b[-1]["ts_close"], exchange, symbol, b[0]["price_open"], max(x["price_high"] for x in b), min(x["price_low"] for x in b), b[-1]["price_close"], None, None, изменение_в_процентах(b[0]["price_open"], b[-1]["price_close"]), len(b)))

    vol_rows = fetch("SELECT * FROM volume_5m_сырые WHERE ts_close <= NOW() - interval '30 seconds' AND ts_open >= NOW() - interval '30 days' ORDER BY exchange, symbol, ts_open")
    for (exchange, symbol), items in _groups(vol_rows).items():
        for tf, n in WINDOWS.items():
            for i in range(n - 1, len(items)):
                b = items[i - n + 1:i + 1]
                vals = [x["volume"] for x in b]
                rows_out.append(("VOLUME", tf, b[0]["ts_open"], b[-1]["ts_close"], exchange, symbol, None, max(vals), min(vals), None, sum(vals), sum(vals)/len(vals), None, len(b)))

    upsert_bot_aggregates(rows_out)
