from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean

from db import fetch, execute, _conn
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

    if structure in {"нисходящий OI", "тишина", "пила", "всплеск без удержания"}:
        return 0

    if structure in {"распределение", "спокойный боковик", "перегрев"}:
        return 4

    if structure == "плавный набор":
        return 3

    if structure in {"ступенчатый набор", "ускорение"}:
        return 2

    if structure in {"агрессивный набор", "удержание после роста"}:
        return 1

    return 5


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



def _insert_oi_slope_rows(rows: list[tuple]) -> None:
    if not rows:
        return

    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_oi_slope(
            calculated_at, ts_close, exchange, symbol, timeframe,
            stage, stage_name, oi_structure, oi_priority, oi_hold_state,
            oi_trend_15m, oi_trend_30m, oi_trend_1h, oi_trend_4h, oi_trend_24h,
            oi_reason, reason, oi_delta_pct, oi_acceleration, oi_prev_avg,
            price_delta_pct, volume_delta_pct, range_width_pct, silence_stage
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def _rebuild_oi_slope_symbol_batch(symbols: list[tuple[str, str]]) -> tuple[int, dict]:
    if not symbols:
        return 0, {}

    values_sql = ",".join(["(%s,%s)"] * len(symbols))
    params = []
    for exchange, symbol in symbols:
        params.extend([exchange, symbol])

    rows = fetch(
        f"""
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
        WHERE (r.exchange, r.symbol) IN ({values_sql})
          AND r.ts_close >= (
            SELECT MAX(ts_close) - '24 hours'::interval
            FROM market_research
          )
        ORDER BY r.exchange, r.symbol, r.timeframe, r.ts_close
        """,
        tuple(params),
    )

    history = {}
    out = []
    counts = {}
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

        counts[stage_name] = counts.get(stage_name, 0) + 1

    _insert_oi_slope_rows(out)
    return len(out), counts


def rebuild_oi_slope() -> int:
    execute("""
        DELETE FROM market_oi_slope
        WHERE ts_close >= (
            SELECT MAX(ts_close) - '24 hours'::interval
            FROM market_research
        )
    """)

    symbols = [
        (r["exchange"], r["symbol"])
        for r in fetch("""
            SELECT DISTINCT exchange, symbol
            FROM market_research
            WHERE ts_close >= (
                SELECT MAX(ts_close) - '24 hours'::interval
                FROM market_research
            )
            ORDER BY exchange, symbol
        """)
    ]

    total_rows = 0
    total_counts = {}
    batch_size = 25

    for i in range(0, len(symbols), batch_size):
        rows_count, counts = _rebuild_oi_slope_symbol_batch(symbols[i:i + batch_size])
        total_rows += rows_count
        for k, v in counts.items():
            total_counts[k] = total_counts.get(k, 0) + v

    log(f"oi slope rebuilt: rows={total_rows} {total_counts}")
    return total_rows
