from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean

from db import fetch, replace_oi_slope
from logger import log


TIMEFRAMES = {"15m", "30m", "1h", "4h"}


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _quality_from_shape(oi_delta, acceleration, price_delta, volume_delta, range_width):
    if oi_delta >= 3.0 and acceleration > 0.5 and volume_delta >= 35 and price_delta > 1.0:
        return "сильный наклон"
    if oi_delta >= 1.5 and acceleration > 0 and volume_delta >= 20 and abs(price_delta) <= 7:
        return "рабочий наклон"
    if oi_delta >= 0.8 and volume_delta >= 10 and abs(price_delta) <= 7:
        return "ранний наклон"
    if oi_delta >= 2.0 and volume_delta < 10:
        return "подозрительный ОИ без объема"
    if abs(price_delta) > 7 and oi_delta < 1.0:
        return "цена убежала без ОИ"
    return "нет наклона"




def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _normalize_strength(raw_strength):
    """
    Убираем saturation.
    Strength должен быть полезен для ранжирования,
    а не постоянно упираться в 100.
    """

    if raw_strength <= 0:
        return 0.0

    # мягкое логарифмическое сглаживание
    normalized = (raw_strength ** 0.72) * 8.5

    return round(_clamp(normalized, 0.0, 100.0), 2)


def _oi_quality(oi_delta, acceleration, price_delta):
    """
    Качество наклона ОИ.
    """

    if oi_delta >= 4 and acceleration >= 1:
        if abs(price_delta) <= 2:
            return "рост без цены"

    if oi_delta >= 3 and price_delta < -3:
        return "рост против цены"

    if oi_delta >= 4 and acceleration >= 2:
        return "агрессивный набор"

    if oi_delta >= 1 and acceleration > 0:
        return "плавный набор"

    return "нет качества"



def _stage_from_slope(silence_stage, oi_delta, price_delta, volume_delta, acceleration, *_):
    """
    ОИ — основа.
    Объем — подтверждающий фактор, но не главный.
    Цена не обязана сразу расти на стадии 1, но не должна показывать явный слив для подтверждения.
    """

    clean_volume = min(max(volume_delta, 0.0), 80.0)

    if silence_stage in (0, 1) and oi_delta >= 0.6 and acceleration > 0 and abs(price_delta) <= 7:
        return 1, "наблюдение", "ранний рост ОИ из спокойного рынка", "ранний наклон"

    if oi_delta >= 1.5 and acceleration > 0 and clean_volume >= 12 and price_delta > -4:
        return 2, "возня", "наклон ОИ усиливается, объем подтверждает активность", "рабочий наклон"

    if oi_delta >= 3.0 and acceleration > 0 and clean_volume >= 25 and price_delta > 0.8:
        return 3, "подтверждение", "ОИ, объем и цена подтверждают расширение", "сильный наклон"

    return 0, "нет сигнала", "условия наклона ОИ не собраны", "нет качества"


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
            s.stage AS silence_stage,
            s.stage_name AS silence_stage_name
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
        price_delta = price_delta
        volume_delta = volume_delta
        series.append(oi_delta)

        prev_avg = mean(series[-4:-1]) if len(series) >= 4 else 0.0
        acceleration = oi_delta - prev_avg

        stage, stage_name, quality, reason = _stage_from_slope(
            int(r["silence_stage"] or -1),
            oi_delta,
            price_delta,
            volume_delta,
            acceleration,
            _f(r["range_width_pct"]),
        )

        clean_volume = min(max(volume_delta, 0.0), 80.0)

        raw_strength = (
            oi_delta * 18
            + acceleration * 14
            + clean_volume * 0.08
        )

        strength = _normalize_strength(raw_strength)

        oi_quality = _oi_quality(
            oi_delta,
            acceleration,
            price_delta,
        )

        out.append((
            calculated_at,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            stage,
            stage_name,
            strength,
            reason + " / " + quality,
            oi_delta,
            acceleration,
            prev_avg,
            price_delta,
            volume_delta,
            _f(r["range_width_pct"]),
            int(r["silence_stage"] or -1),
            r["silence_stage_name"] or "нет данных",
        ))

    replace_oi_slope(out)

    counts = {}
    for row in out:
        counts[row[6]] = counts.get(row[6], 0) + 1

    log(f"oi slope rebuilt: rows={len(out)} {counts}")
    return len(out)
