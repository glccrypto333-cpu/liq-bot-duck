from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from db import fetch, execute
from logger import log


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _pct_change(current: float | None, previous: float | None) -> float:
    if current is None or previous is None or previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


def init_research_schema() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS market_research(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,

            oi_delta_pct DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,

            oi_velocity DOUBLE PRECISION,
            oi_acceleration DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,

            continuation_score DOUBLE PRECISION,
            exhaustion_score DOUBLE PRECISION,
            compression_score DOUBLE PRECISION,

            market_state TEXT NOT NULL
        )
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_market_research_main ON market_research(exchange, symbol, timeframe, ts_close)")
    execute("CREATE INDEX IF NOT EXISTS idx_market_research_state ON market_research(market_state, timeframe, ts_close)")


def _score_continuation(oi_delta: float, price_delta: float, volume_delta: float) -> float:
    oi_part = min(40.0, max(0.0, oi_delta) * 12.0)
    price_part = min(35.0, abs(price_delta) * 18.0)
    volume_part = min(25.0, max(0.0, volume_delta) * 1.8)
    return max(0.0, min(100.0, oi_part + price_part + volume_part))


def _score_exhaustion(oi_delta: float, price_delta: float, volume_delta: float) -> float:
    impulse_part = min(45.0, abs(price_delta) * 20.0)
    volume_part = min(35.0, max(0.0, volume_delta) * 2.0)
    oi_drop_part = min(20.0, max(0.0, -oi_delta) * 10.0)
    return max(0.0, min(100.0, impulse_part + volume_part + oi_drop_part))


def _score_compression(oi_delta: float, price_delta: float, volume_delta: float, range_width: float) -> float:
    narrow_range = max(0.0, 45.0 - range_width * 35.0)
    quiet_price = max(0.0, 25.0 - abs(price_delta) * 35.0)
    oi_build = min(20.0, max(0.0, oi_delta) * 8.0)
    quiet_volume = max(0.0, 10.0 - abs(volume_delta) * 1.5)
    return max(0.0, min(100.0, narrow_range + quiet_price + oi_build + quiet_volume))


def _classify_state(
    oi_delta: float,
    price_delta: float,
    range_width: float,
    continuation_score: float,
    exhaustion_score: float,
    compression_score: float,
) -> str:
    if compression_score >= 65:
        return "сжатие"
    if exhaustion_score >= 65:
        return "выдох"
    if continuation_score >= 60:
        return "продолжение"
    if abs(price_delta) <= 0.35 and abs(oi_delta) <= 0.60 and range_width <= 1.20:
        return "диапазон"
    return "нейтрально"


def rebuild_market_research() -> int:
    """
    Исследовательский слой.
    Источник истины: bot_aggregates.
    Fake rows: нет.
    Сигналы: нет.
    """
    init_research_schema()

    rows = fetch(
        """
        SELECT
            metric,
            timeframe,
            ts_open,
            ts_close,
            exchange,
            symbol,
            open_value,
            high_value,
            low_value,
            close_value,
            sum_value,
            avg_value,
            delta_pct,
            unique_candles
        FROM bot_aggregates
        ORDER BY exchange, symbol, timeframe, ts_close
        """
    )

    metric_map: dict[tuple, dict] = {}
    keys = set()

    for r in rows:
        key_base = (r["exchange"], r["symbol"], r["timeframe"], r["ts_close"])
        keys.add(key_base)
        metric_map[key_base + (r["metric"],)] = r

    sorted_keys = sorted(keys, key=lambda x: (x[0], x[1], x[2], x[3]))

    oi_history: dict[tuple, list[tuple]] = defaultdict(list)
    volume_history: dict[tuple, list[tuple]] = defaultdict(list)

    for exchange, symbol, timeframe, ts_close in sorted_keys:
        base = (exchange, symbol, timeframe, ts_close)
        group = (exchange, symbol, timeframe)

        oi = metric_map.get(base + ("OI",))
        volume = metric_map.get(base + ("VOLUME",))

        if oi:
            oi_history[group].append((ts_close, _safe_float(oi.get("delta_pct"))))
        if volume:
            volume_history[group].append((ts_close, _safe_float(volume.get("sum_value"))))

    calculated_at = datetime.now(timezone.utc)
    out = []

    for exchange, symbol, timeframe, ts_close in sorted_keys:
        base = (exchange, symbol, timeframe, ts_close)
        group = (exchange, symbol, timeframe)

        oi = metric_map.get(base + ("OI",))
        price = metric_map.get(base + ("PRICE",))
        volume = metric_map.get(base + ("VOLUME",))

        if not oi or not price or not volume:
            continue

        oi_delta = _safe_float(oi.get("delta_pct"))
        price_delta = _safe_float(price.get("delta_pct"))

        volume_delta = 0.0
        volume_series = volume_history[group]
        for idx, (t, current_sum) in enumerate(volume_series):
            if t == ts_close:
                previous_sum = volume_series[idx - 1][1] if idx > 0 else None
                volume_delta = _pct_change(current_sum, previous_sum)
                break

        oi_velocity = 0.0
        oi_acceleration = 0.0
        oi_series = oi_history[group]

        for idx, (t, current_delta) in enumerate(oi_series):
            if t == ts_close:
                previous_delta = oi_series[idx - 1][1] if idx > 0 else None
                previous_previous_delta = oi_series[idx - 2][1] if idx > 1 else None

                if previous_delta is not None:
                    oi_velocity = current_delta - previous_delta

                if previous_delta is not None and previous_previous_delta is not None:
                    previous_velocity = previous_delta - previous_previous_delta
                    oi_acceleration = oi_velocity - previous_velocity
                break

        price_high = _safe_float(price.get("high_value"))
        price_low = _safe_float(price.get("low_value"))
        price_close = _safe_float(price.get("close_value"))

        range_width = ((price_high - price_low) / price_close) * 100.0 if price_close else 0.0

        continuation_score = _score_continuation(oi_delta, price_delta, volume_delta)
        exhaustion_score = _score_exhaustion(oi_delta, price_delta, volume_delta)
        compression_score = _score_compression(oi_delta, price_delta, volume_delta, range_width)

        market_state = _classify_state(
            oi_delta=oi_delta,
            price_delta=price_delta,
            range_width=range_width,
            continuation_score=continuation_score,
            exhaustion_score=exhaustion_score,
            compression_score=compression_score,
        )

        out.append(
            (
                calculated_at,
                ts_close,
                exchange,
                symbol,
                timeframe,
                oi_delta,
                price_delta,
                volume_delta,
                oi_velocity,
                oi_acceleration,
                range_width,
                continuation_score,
                exhaustion_score,
                compression_score,
                market_state,
            )
        )

    execute("TRUNCATE TABLE market_research")

    if out:
        from db import _conn

        with _conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO market_research(
                    calculated_at,
                    ts_close,
                    exchange,
                    symbol,
                    timeframe,
                    oi_delta_pct,
                    price_delta_pct,
                    volume_delta_pct,
                    oi_velocity,
                    oi_acceleration,
                    range_width_pct,
                    continuation_score,
                    exhaustion_score,
                    compression_score,
                    market_state
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                out,
            )

    log(f"market research rebuilt: rows={len(out)}")
    return len(out)
