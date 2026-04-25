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


def _stage_from_slope(silence_stage, oi_delta, price_delta, volume_delta, acceleration, range_width):
    quality = _quality_from_shape(oi_delta, acceleration, price_delta, volume_delta, range_width)

    if silence_stage == 0 and quality == "ранний наклон":
        return 1, "наблюдение", quality, "рост ОИ из тишины"

    if quality == "рабочий наклон":
        return 2, "возня", quality, "наклон ОИ усиливается, цена еще не убежала"

    if quality == "сильный наклон":
        return 3, "подтверждение", quality, "ОИ, объем и цена подтверждают наклон"

    return 0, "нет сигнала", quality, "условия наклона ОИ не собраны"


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
        WHERE r.timeframe IN ('15m','30m','1h','4h')
        ORDER BY r.exchange, r.symbol, r.timeframe, r.ts_close
    """)

    history = {}
    out = []
    calculated_at = datetime.now(timezone.utc)

    for r in rows:
        key = (r["exchange"], r["symbol"], r["timeframe"])
        series = history.setdefault(key, [])
        oi_delta = _f(r["oi_delta_pct"])
        series.append(oi_delta)

        prev_avg = mean(series[-4:-1]) if len(series) >= 4 else 0.0
        acceleration = oi_delta - prev_avg

        stage, stage_name, quality, reason = _stage_from_slope(
            int(r["silence_stage"] or -1),
            oi_delta,
            _f(r["price_delta_pct"]),
            _f(r["volume_delta_pct"]),
            acceleration,
            _f(r["range_width_pct"]),
        )

        strength = max(0.0, min(100.0, oi_delta * 12 + acceleration * 8 + _f(r["volume_delta_pct"]) * 0.7))

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
            _f(r["price_delta_pct"]),
            _f(r["volume_delta_pct"]),
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
