from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone
from db import fetch_rows, replace_table
from metrics import изменение_в_процентах, абсолютное_расхождение, относительное_расхождение_pct

МНОЖИТЕЛИ = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}

def _integrity_from_rows(metric: str, rows: list[dict]) -> list[tuple]:
    now = datetime.now(timezone.utc)
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["exchange"], r["symbol"])].append(r)
    out = []
    for (exchange, symbol), items in grouped.items():
        items = sorted(items, key=lambda x: x["ts_open"])
        total = len(items)
        unique_count = len({x["ts_open"] for x in items})
        duplicates_found = max(0, total - unique_count)
        invalid_timestamps = 0
        missing_candles = 0
        empty_rows = 0
        prev = None
        for x in items:
            if prev is not None:
                diff = (x["ts_open"] - prev).total_seconds()
                if diff <= 0:
                    invalid_timestamps += 1
                elif diff > 300:
                    missing_candles += int(diff // 300) - 1
            prev = x["ts_open"]
            for key, value in x.items():
                if key not in ("ts_open", "ts_close", "exchange", "symbol") and value is None:
                    empty_rows += 1
        integrity_score = max(0.0, min(100.0, 100.0 - duplicates_found * 3.0 - invalid_timestamps * 10.0 - missing_candles * 2.0 - empty_rows * 5.0))
        out.append((now, metric, exchange, symbol, duplicates_found, missing_candles, invalid_timestamps, empty_rows, integrity_score))
    return out

def rebuild_raw_integrity_report() -> None:
    oi_rows = fetch_rows("SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close FROM oi_5m_сырые WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, ts_open")
    price_rows = fetch_rows("SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close FROM price_5m_сырые WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, ts_open")
    volume_rows = fetch_rows("SELECT ts_open, ts_close, exchange, symbol, volume FROM volume_5m_сырые WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, ts_open")
    out = _integrity_from_rows("OI", oi_rows) + _integrity_from_rows("PRICE", price_rows) + _integrity_from_rows("VOLUME", volume_rows)
    replace_table("raw_integrity_report", out)

def _dedupe(rows: list[dict]) -> dict:
    grouped = defaultdict(dict)
    for r in rows:
        grouped[(r["exchange"], r["symbol"])][r["ts_open"]] = r
    return grouped

def _status_from_unique(unique_candles: int, needed: int, drift: float | None, metric: str) -> str:
    if unique_candles < needed:
        return "недостаточно_данных"
    if drift is None:
        return "ошибка_агрегации_бота"
    if metric == "OI":
        if drift >= 0.50:
            return "расхождение_выше_допуска"
        return "валидно"
    if metric == "PRICE":
        if drift >= 0.05:
            return "расхождение_выше_допуска"
        return "валидно"
    if metric == "VOLUME":
        if drift >= 3.0:
            return "расхождение_выше_допуска"
        return "валидно"
    return "валидно"

def rebuild_audit_oi() -> None:
    now = datetime.now(timezone.utc)
    raw_rows = fetch_rows("SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close FROM oi_5m_сырые WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, ts_open")
    bot_rows = fetch_rows("SELECT окно, ts_open, exchange, symbol, oi_open, oi_close, oi_изменение_pct FROM oi_агрегаты WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, окно, ts_open")
    raw_group = _dedupe(raw_rows)
    bot_map = {(r["exchange"], r["symbol"], r["окно"]): r for r in bot_rows}
    out = []
    for (exchange, symbol), mp in raw_group.items():
        items = [mp[k] for k in sorted(mp.keys())]
        for окно, needed in МНОЖИТЕЛИ.items():
            bucket = items[-needed:] if len(items) >= needed else items[:]
            unique_candles = len(bucket)
            audit_open = audit_close = audit_delta = None
            if unique_candles >= needed:
                audit_open = bucket[0]["oi_open"]
                audit_close = bucket[-1]["oi_close"]
                audit_delta = изменение_в_процентах(audit_open, audit_close)
            bot = bot_map.get((exchange, symbol, окно))
            bot_open = bot["oi_open"] if bot else None
            bot_close = bot["oi_close"] if bot else None
            bot_delta = bot["oi_изменение_pct"] if bot else None
            drift = абсолютное_расхождение(bot_delta, audit_delta)
            out.append((now, symbol, exchange, окно, bot_open, audit_open, bot_close, audit_close, bot_delta, audit_delta, drift, unique_candles, _status_from_unique(unique_candles, needed, drift, "OI")))
    replace_table("аудит_ои", out)

def rebuild_audit_price() -> None:
    now = datetime.now(timezone.utc)
    raw_rows = fetch_rows("SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close FROM price_5m_сырые WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, ts_open")
    bot_rows = fetch_rows("SELECT окно, ts_open, exchange, symbol, price_open, price_close, price_изменение_pct FROM price_агрегаты WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, окно, ts_open")
    raw_group = _dedupe(raw_rows)
    bot_map = {(r["exchange"], r["symbol"], r["окно"]): r for r in bot_rows}
    out = []
    for (exchange, symbol), mp in raw_group.items():
        items = [mp[k] for k in sorted(mp.keys())]
        for окно, needed in МНОЖИТЕЛИ.items():
            bucket = items[-needed:] if len(items) >= needed else items[:]
            unique_candles = len(bucket)
            audit_open = audit_close = audit_delta = None
            if unique_candles >= needed:
                audit_open = bucket[0]["price_open"]
                audit_close = bucket[-1]["price_close"]
                audit_delta = изменение_в_процентах(audit_open, audit_close)
            bot = bot_map.get((exchange, symbol, окно))
            bot_open = bot["price_open"] if bot else None
            bot_close = bot["price_close"] if bot else None
            bot_delta = bot["price_изменение_pct"] if bot else None
            drift = абсолютное_расхождение(bot_delta, audit_delta)
            out.append((now, symbol, exchange, окно, bot_open, audit_open, bot_close, audit_close, bot_delta, audit_delta, drift, unique_candles, _status_from_unique(unique_candles, needed, drift, "PRICE")))
    replace_table("аудит_цены", out)

def rebuild_audit_volume() -> None:
    now = datetime.now(timezone.utc)
    raw_rows = fetch_rows("SELECT ts_open, ts_close, exchange, symbol, volume FROM volume_5m_сырые WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, ts_open")
    bot_rows = fetch_rows("SELECT окно, ts_open, exchange, symbol, volume_sum, volume_avg FROM volume_агрегаты WHERE ts_open >= NOW() - interval '2 days' ORDER BY exchange, symbol, окно, ts_open")
    raw_group = _dedupe(raw_rows)
    bot_map = {(r["exchange"], r["symbol"], r["окно"]): r for r in bot_rows}
    out = []
    for (exchange, symbol), mp in raw_group.items():
        items = [mp[k] for k in sorted(mp.keys())]
        for окно, needed in МНОЖИТЕЛИ.items():
            bucket = items[-needed:] if len(items) >= needed else items[:]
            unique_candles = len(bucket)
            audit_sum = audit_avg = None
            if unique_candles >= needed:
                vals = [x["volume"] for x in bucket]
                audit_sum = sum(vals)
                audit_avg = sum(vals) / len(vals)
            bot = bot_map.get((exchange, symbol, окно))
            bot_sum = bot["volume_sum"] if bot else None
            bot_avg = bot["volume_avg"] if bot else None
            drift = относительное_расхождение_pct(bot_sum, audit_sum)
            out.append((now, symbol, exchange, окно, bot_sum, audit_sum, bot_avg, audit_avg, drift, unique_candles, _status_from_unique(unique_candles, needed, drift, "VOLUME")))
    replace_table("аудит_объёма", out)

def rebuild_all_audits() -> None:
    rebuild_raw_integrity_report()
    rebuild_audit_oi()
    rebuild_audit_price()
    rebuild_audit_volume()
