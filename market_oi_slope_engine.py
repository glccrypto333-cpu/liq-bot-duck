from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean

from db import fetch, replace_oi_slope
from logger import log


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default



def _bucket_oi_delta(v: float) -> str:
    if v < 0:
        return "отрицательный"
    if v < 3:
        return "0-3%"
    if v < 5:
        return "3-5%"
    if v < 10:
        return "5-10%"
    if v < 15:
        return "10-15%"
    return "15%+"


def _bucket_acceleration(v: float) -> str:
    if v < 0:
        return "отрицательное"
    if v < 0.5:
        return "нейтральное"
    if v < 2:
        return "растет"
    return "резко растет"


def _trend_from_delta(delta: float) -> str:
    if delta < 0:
        return "снижение"
    if delta < 1:
        return "боковик"
    if delta < 3:
        return "плавный рост"
    if delta < 6:
        return "устойчивый рост"
    return "агрессивный рост"


def _oi_structure(oi_delta: float, acceleration: float, prev_avg: float) -> str:
    if oi_delta < -1:
        return "нисходящий OI"
    if oi_delta < 0.3 and abs(acceleration) < 0.5:
        return "тишина"
    if oi_delta < 1 and abs(acceleration) < 0.5:
        return "спокойный боковик"
    if oi_delta >= 15:
        return "перегрев"
    if oi_delta >= 10 and acceleration < 0:
        return "распределение"
    if oi_delta >= 6 and acceleration < -1:
        return "всплеск без удержания"
    if oi_delta >= 6 and acceleration >= 2:
        return "агрессивный набор"
    if oi_delta >= 4 and acceleration >= 1:
        return "ускорение"
    if oi_delta >= 2 and prev_avg >= 1:
        return "ступенчатый набор"
    if oi_delta >= 1 and acceleration >= 0:
        return "плавный набор"
    if oi_delta > 0 and acceleration < 0:
        return "пила"
    return "пила"


def _oi_priority(structure: str, quality: str) -> int:
    if quality in ("нет качества", "пила"):
        return 0
    if structure == "плавный набор":
        return 1
    if structure == "ступенчатый набор":
        return 2
    if structure == "ускорение":
        return 3
    if structure == "агрессивный набор":
        return 4
    if structure == "перегрев":
        return 2
    return 1


def _hold_state(series: list[float]) -> str:
    recent = series[-4:]
    if len(recent) < 4:
        return "недостаточно данных"
    if all(v > 0 for v in recent):
        return "удержание"
    if recent[-1] > 0 and recent[-2] <= 0:
        return "попытка удержания"
    if recent[-1] <= 0:
        return "нет удержания"
    return "нестабильно"


def _stage_from_oi(priority: int, hold_state: str) -> tuple[int, str]:
    if priority <= 0:
        return 0, "нет сигнала"
    if priority == 1:
        return 1, "наблюдение"
    if priority in (2, 3):
        return 2, "возня"
    if priority >= 4 and hold_state in ("удержание", "попытка удержания"):
        return 3, "подтверждение"
    return 2, "возня"


def rebuild_oi_slope() -> int:
    rows = fetch("""
        SELECT
            r.calculated_at,
            r.ts_close,
            r.exchange,
            r.symbol,
            r.timeframe,
            r.oi_delta_pct,
            r.price_delta_pct,
            r.volume_delta_pct,
            r.range_width_pct,
            s.stage AS silence_stage
        FROM market_research r
        LEFT JOIN market_silence s
          ON s.exchange = r.exchange
         AND s.symbol = r.symbol
         AND s.timeframe = r.timeframe
         AND s.ts_close = r.ts_close
        ORDER BY r.exchange, r.symbol, r.timeframe, r.ts_close
    """)

    history = {}
    out = []
    calculated_at = datetime.now(timezone.utc)

    for r in rows:
        key = (r["exchange"], r["symbol"], r["timeframe"])
        series = history.setdefault(key, [])

        oi_delta = _f(r["oi_delta_pct"])
        price_delta = _f(r["price_delta_pct"])
        volume_delta = _f(r["volume_delta_pct"])
        range_width = _f(r["range_width_pct"])

        series.append(oi_delta)

        prev_avg = mean(series[-4:-1]) if len(series) >= 4 else 0.0
        acceleration = oi_delta - prev_avg

        oi_structure = _oi_structure(oi_delta, acceleration, prev_avg)
        oi_priority = _oi_priority(oi_structure, "")
        oi_hold_state = _hold_state(series)

        oi_delta_bucket = _bucket_oi_delta(oi_delta)
        oi_acceleration_bucket = _bucket_acceleration(acceleration)
        oi_trend_15m = _trend_from_delta(mean(series[-3:]) if len(series) >= 3 else oi_delta)
        oi_trend_30m = _trend_from_delta(mean(series[-6:]) if len(series) >= 6 else oi_delta)
        oi_trend_1h = _trend_from_delta(mean(series[-12:]) if len(series) >= 12 else oi_delta)
        oi_trend_4h = _trend_from_delta(mean(series[-48:]) if len(series) >= 48 else prev_avg)
        oi_trend_24h = "ожидает отдельного окна"

        stage, stage_name = _stage_from_oi(oi_priority, oi_hold_state)

        oi_reason = (
            f"structure={oi_structure}; "
            f"priority={oi_priority}; hold={oi_hold_state}; "
            f"delta_bucket={oi_delta_bucket}; acceleration_bucket={oi_acceleration_bucket}; "
            f"trend15m={oi_trend_15m}; trend30m={oi_trend_30m}; "
            f"trend1h={oi_trend_1h}; trend4h={oi_trend_4h}; "
            f"oi_delta={oi_delta:.2f}; acceleration={acceleration:.2f}"
        )

        out.append((
            calculated_at,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            stage,
            stage_name,
            oi_structure,
            oi_priority,
            oi_hold_state,
            oi_trend_15m,
            oi_trend_30m,
            oi_trend_1h,
            oi_trend_4h,
            oi_trend_24h,
            oi_reason,
            oi_reason,
            oi_delta,
            acceleration,
            prev_avg,
            price_delta,
            volume_delta,
            range_width,
            int(r["silence_stage"] or -1),
        ))

    replace_oi_slope(out)

    counts = {}
    for row in out:
        counts[row[6]] = counts.get(row[6], 0) + 1

    log(f"oi slope rebuilt: rows={len(out)} {counts}")
    return len(out)
