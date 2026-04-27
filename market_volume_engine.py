from __future__ import annotations

from datetime import datetime, timezone
import math

from db import fetch, replace_volume_state
from logger import log


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default




def _safe_log_volume(v):
    v = max(float(v or 0.0), 0.0)
    return math.log1p(v)


def _volume_percentile(volume_delta, history):
    """
    Настоящий исторический процентиль объема по своей монете.

    Сравниваем текущий volume_delta_pct только с прошлой историей:
    exchange + symbol + timeframe.

    Если истории мало, возвращаем 30 как нейтральное значение,
    чтобы не рисовать ложные всплески.
    """

    clean_history = [float(x) for x in history if x is not None]

    if len(clean_history) < 20:
        return 30

    current = float(volume_delta or 0.0)
    less_or_equal = sum(1 for x in clean_history if x <= current)

    return int(round((less_or_equal / len(clean_history)) * 100))


def _noise_state(range_width, volume_delta, oi_delta):
    """
    Noise filter.
    """

    if volume_delta >= 80 and range_width <= 2 and abs(oi_delta) <= 0.5:
        return "шум"

    if volume_delta >= 150 and abs(oi_delta) <= 0.3:
        return "аномальный шум"

    return "не шум"



def _volume_state_by_percentile(volume_delta, percentile):
    """
    Объем НЕ является сигналом.
    Он описывает участие рынка.
    """

    if percentile >= 99:
        return 4, "аномальный объем", "экстремальный всплеск участия"

    if percentile >= 95:
        return 3, "всплеск объема", "участие рынка резко выросло"

    if percentile >= 75:
        return 2, "объем растет", "активность рынка расширяется"

    if volume_delta <= -20:
        return -1, "объем падает", "интерес участников снижается"

    return 0, "обычный объем", "объем без аномалий"


def rebuild_volume_state() -> int:
    rows = fetch("""
        SELECT
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            volume_delta_pct,
            oi_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        FROM market_research
        ORDER BY exchange, symbol, timeframe, ts_close
    """)

    out = []
    calculated_at = datetime.now(timezone.utc)
    history_by_key = {}

    for r in rows:
        volume_delta = _f(r["volume_delta_pct"])
        oi_delta = _f(r.get("oi_delta_pct"))
        range_width = _f(r.get("range_width_pct"))
        key = (r["exchange"], r["symbol"], r["timeframe"])
        history = history_by_key.get(key, [])

        normalized_volume = _safe_log_volume(volume_delta)
        percentile = _volume_percentile(volume_delta, history)
        noise_state = _noise_state(range_width, volume_delta, oi_delta)

        state, state_name, reason = _volume_state_by_percentile(volume_delta, percentile)

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
            normalized_volume,
            percentile,
            noise_state,
            r["market_state"],
            r["invalid_reason"],
        ))

        history_by_key.setdefault(key, []).append(volume_delta)

    replace_volume_state(out)

    counts = {}
    for row in out:
        counts[row[6]] = counts.get(row[6], 0) + 1

    log(f"volume state rebuilt: rows={len(out)} {counts}")
    return len(out)
