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


def _price_structure(price_delta: float, range_width: float, prev_avg: float) -> str:
    abs_price = abs(price_delta)

    if range_width <= 3 and abs_price <= 1.2:
        return "сжатие"
    if range_width <= 7 and abs_price <= 3:
        return "спокойный боковик"
    if range_width <= 12 and abs_price <= 6:
        return "широкий боковик"
    if abs_price <= 2 and range_width >= 10:
        return "возврат внутрь диапазона"
    if price_delta >= 6 and range_width >= 8:
        return "расширение вверх"
    if price_delta <= -6 and range_width >= 8:
        return "расширение вниз"
    if price_delta > 0 and prev_avg > 0:
        return "ползущий рост"
    if price_delta < 0 and prev_avg < 0:
        return "ползущий слив"
    return "нейтрально"


def _price_quality(structure: str, price_delta: float, range_width: float) -> str:
    if structure in ("сжатие", "спокойный боковик"):
        return "чистый диапазон"
    if structure == "широкий боковик":
        return "расширенный диапазон"
    if structure == "возврат внутрь диапазона":
        return "возврат"
    if structure in ("расширение вверх", "расширение вниз"):
        if abs(price_delta) >= 12:
            return "импульсный выброс"
        return "направленное расширение"
    if structure in ("ползущий рост", "ползущий слив"):
        return "медленная наклонка"
    return "нет качества"


def _slope_state(price_delta: float, prev_avg: float) -> str:
    if price_delta < -6:
        return "резко вниз"
    if price_delta < -2:
        return "вниз"
    if abs(price_delta) <= 2 and abs(prev_avg) <= 2:
        return "плоско"
    if price_delta > 6:
        return "резко вверх"
    if price_delta > 2:
        return "вверх"
    return "нестабильно"


def _legacy_state(structure: str) -> tuple[int, str]:
    mapping = {
        "сжатие": (0, "сжатие"),
        "спокойный боковик": (1, "спокойный боковик"),
        "широкий боковик": (2, "широкий боковик"),
        "возврат внутрь диапазона": (4, "возврат"),
        "расширение вверх": (3, "импульс вверх"),
        "расширение вниз": (-3, "импульс вниз"),
    }
    return mapping.get(structure, (0, "нейтрально"))



def _insert_price_state_rows(rows: list[tuple]) -> None:
    if not rows:
        return

    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_price_state(
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            price_state,
            price_state_name,
            price_structure,
            price_quality,
            price_slope_state,
            price_trend_24h,
            price_range_from_median_pct,
            price_reason,
            reason,
            price_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def _rebuild_price_state_symbol_batch(symbols: list[tuple[str, str]]) -> tuple[int, dict]:
    if not symbols:
        return 0, {}

    values_sql = ",".join(["(%s,%s)"] * len(symbols))
    params = []
    for exchange, symbol in symbols:
        params.extend([exchange, symbol])

    rows = fetch(
        f"""
        SELECT
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            price_delta_pct,
            volume_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        FROM market_research
        WHERE (exchange, symbol) IN ({values_sql})
          AND ts_close >= (
            SELECT MAX(ts_close) - '24 hours'::interval
            FROM market_research
          )
        ORDER BY exchange, symbol, timeframe, ts_close
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

        price_delta = _f(r["price_delta_pct"])
        range_width = _f(r["range_width_pct"])

        series.append(price_delta)
        prev_avg = mean(series[-4:-1]) if len(series) >= 4 else 0.0

        price_structure = _price_structure(price_delta, range_width, prev_avg)
        price_quality = _price_quality(price_structure, price_delta, range_width)
        price_slope_state = _slope_state(price_delta, prev_avg)
        price_trend_24h = "ожидает отдельного окна"
        price_range_from_median_pct = range_width

        price_reason = (
            f"structure={price_structure}; quality={price_quality}; "
            f"slope={price_slope_state}; price_delta={price_delta:.2f}; "
            f"prev_avg={prev_avg:.2f}; range_width={range_width:.2f}"
        )

        state, state_name = _legacy_state(price_structure)

        out.append((
            calculated_at,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            state,
            state_name,
            price_structure,
            price_quality,
            price_slope_state,
            price_trend_24h,
            price_range_from_median_pct,
            price_reason,
            price_reason,
            price_delta,
            range_width,
            r["market_state"],
            r["invalid_reason"],
        ))

        counts[state_name] = counts.get(state_name, 0) + 1

    _insert_price_state_rows(out)
    return len(out), counts


def rebuild_price_state() -> int:
    execute("""
        DELETE FROM market_price_state
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
        rows_count, counts = _rebuild_price_state_symbol_batch(symbols[i:i + batch_size])
        total_rows += rows_count
        for k, v in counts.items():
            total_counts[k] = total_counts.get(k, 0) + v

    log(f"price state rebuilt: rows={total_rows} {total_counts}")
    return total_rows
