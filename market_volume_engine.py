from __future__ import annotations

from datetime import datetime, timezone

from db import fetch, replace_volume_state
from logger import log


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _volume_state(volume_delta):
    """
    Объем НЕ управляет стадией.
    Объем описывает активность вокруг ОИ.
    """

    if volume_delta <= -30:
        return -1, "объем падает", "активность снижается"

    if -30 < volume_delta < 20:
        return 0, "обычный объем", "объем без явного всплеска"

    if 20 <= volume_delta < 80:
        return 1, "объем растет", "активность приходит"

    if 80 <= volume_delta < 250:
        return 2, "всплеск объема", "сильный приход активности"

    return 3, "аномальный объем", "экстремальный объем, возможен шум или разовый выброс"


def rebuild_volume_state() -> int:
    rows = fetch("""
        SELECT
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            volume_delta_pct,
            market_state,
            invalid_reason
        FROM market_research
        ORDER BY exchange, symbol, timeframe, ts_close
    """)

    out = []
    calculated_at = datetime.now(timezone.utc)

    for r in rows:
        volume_delta = _f(r["volume_delta_pct"])
        state, state_name, reason = _volume_state(volume_delta)

        out.append((
            calculated_at,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            state,
            state_name,
            reason,
            volume_delta,
            r["market_state"],
            r["invalid_reason"],
        ))

    replace_volume_state(out)

    counts = {}
    for row in out:
        counts[row[6]] = counts.get(row[6], 0) + 1

    log(f"volume state rebuilt: rows={len(out)} {counts}")
    return len(out)
