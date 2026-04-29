from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
import math

from db import fetch, replace_volume_state
from logger import log


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _safe_log_volume(v):
    return math.log1p(max(float(v or 0.0), 0.0))


def _volume_percentile(volume_delta, history):
    clean = [float(x) for x in history if x is not None]
    if len(clean) < 20:
        return 30
    return int(round((sum(1 for x in clean if x <= float(volume_delta or 0.0)) / len(clean)) * 100))


def _volume_structure(volume_delta: float, percentile: int, hold_state: str) -> str:
    if volume_delta <= -20:
        return "объем падает"
    if percentile < 75:
        return "обычный объем"
    if percentile >= 99 and hold_state == "удержание":
        return "аномальный объем"
    if percentile >= 95 and hold_state in ("удержание", "попытка удержания"):
        return "всплеск объема"
    if percentile >= 75:
        return "объем растет"
    return "обычный объем"


def _volume_quality(structure: str, noise_state: str) -> str:
    if noise_state != "не шум":
        return noise_state
    if structure == "аномальный объем":
        return "аномальное устойчивое участие"
    if structure == "всплеск объема":
        return "устойчивый всплеск"
    if structure == "объем растет":
        return "активность растет"
    if structure == "объем падает":
        return "интерес снижается"
    return "нет аномалии"


def _hold_state(series: list[float], percentile_series: list[int]) -> str:
    recent_p = percentile_series[-3:]
    recent_v = series[-3:]

    if len(recent_p) < 3:
        return "недостаточно данных"
    if all(p >= 95 for p in recent_p):
        return "удержание"
    if recent_p[-1] >= 95 and recent_p[-2] >= 75:
        return "попытка удержания"
    if recent_p[-1] >= 95 and recent_p[-2] < 75:
        return "одиночный всплеск"
    if all(v <= -20 for v in recent_v):
        return "устойчивое падение"
    return "нет удержания"


def _noise_state(range_width, volume_delta, oi_delta):
    if volume_delta >= 80 and range_width <= 2 and abs(oi_delta) <= 0.5:
        return "шум"
    if volume_delta >= 150 and abs(oi_delta) <= 0.3:
        return "аномальный шум"
    return "не шум"


def _legacy_state(structure: str) -> tuple[int, str]:
    mapping = {
        "аномальный объем": (4, "аномальный объем"),
        "всплеск объема": (3, "всплеск объема"),
        "объем растет": (2, "объем растет"),
        "объем падает": (-1, "объем падает"),
        "обычный объем": (0, "обычный объем"),
    }
    return mapping.get(structure, (0, "обычный объем"))


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
        WHERE ts_close >= (
            SELECT MAX(ts_close) - '24 hours'::interval
            FROM market_research
        )
        ORDER BY exchange, symbol, timeframe, ts_close
    """)

    out = []
    calculated_at = datetime.now(timezone.utc)
    history_by_key = {}
    percentile_by_key = {}

    for r in rows:
        key = (r["exchange"], r["symbol"], r["timeframe"])

        volume_delta = _f(r["volume_delta_pct"])
        oi_delta = _f(r.get("oi_delta_pct"))
        range_width = _f(r.get("range_width_pct"))

        history = history_by_key.get(key, [])
        percentiles = percentile_by_key.get(key, [])

        normalized_volume = _safe_log_volume(volume_delta)
        percentile = _volume_percentile(volume_delta, history)
        noise_state = _noise_state(range_width, volume_delta, oi_delta)

        tmp_series = history + [volume_delta]
        tmp_percentiles = percentiles + [percentile]

        volume_baseline_24h = mean(history[-24:]) if history else 0.0
        volume_hold_state = _hold_state(tmp_series, tmp_percentiles)
        volume_structure = _volume_structure(volume_delta, percentile, volume_hold_state)
        volume_quality = _volume_quality(volume_structure, noise_state)

        volume_reason = (
            f"structure={volume_structure}; quality={volume_quality}; "
            f"hold={volume_hold_state}; percentile={percentile}; "
            f"volume_delta={volume_delta:.2f}; baseline_24h={volume_baseline_24h:.2f}; "
            f"noise={noise_state}"
        )

        state, state_name = _legacy_state(volume_structure)

        out.append((
            calculated_at,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            state,
            state_name,
            volume_structure,
            volume_quality,
            volume_baseline_24h,
            volume_hold_state,
            volume_reason,
            volume_reason,
            volume_delta,
            normalized_volume,
            percentile,
            noise_state,
            r["market_state"],
            r["invalid_reason"],
        ))

        history_by_key.setdefault(key, []).append(volume_delta)
        percentile_by_key.setdefault(key, []).append(percentile)

    replace_volume_state(out)

    counts = {}
    for row in out:
        counts[row[6]] = counts.get(row[6], 0) + 1

    log(f"volume state rebuilt: rows={len(out)} {counts}")
    return len(out)
