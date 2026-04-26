from __future__ import annotations

from datetime import datetime, timezone

from db import fetch, replace_price_state
from logger import log


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _price_state(price_delta, range_width):
    abs_price = abs(price_delta)

    if range_width <= 7 and abs_price <= 3:
        return 0, "слабый боковик", "цена сжата, направленного движения нет"

    if range_width <= 10 and abs_price <= 5:
        return 1, "широкий боковик", "цена внутри диапазона, волатильность умеренная"

    if price_delta > 5:
        return 2, "наклон вверх", "цена показывает направленное движение вверх"

    if price_delta < -5:
        return -2, "наклон вниз", "цена показывает направленное движение вниз"

    return 0, "нейтрально", "цена без явного режима"


def rebuild_price_state() -> int:
    rows = fetch("""
        SELECT
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            price_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        FROM market_research
        ORDER BY exchange, symbol, timeframe, ts_close
    """)

    out = []
    calculated_at = datetime.now(timezone.utc)

    for r in rows:
        price_delta = _f(r["price_delta_pct"])
        range_width = _f(r["range_width_pct"])

        state, state_name, reason = _price_state(price_delta, range_width)

        out.append((
            calculated_at,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            state,
            state_name,
            reason,
            price_delta,
            range_width,
            r["market_state"],
            r["invalid_reason"],
        ))

    replace_price_state(out)

    counts = {}
    for row in out:
        counts[row[6]] = counts.get(row[6], 0) + 1

    log(f"price state rebuilt: rows={len(out)} {counts}")
    return len(out)
