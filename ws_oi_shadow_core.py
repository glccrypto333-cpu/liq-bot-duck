from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class OiTick:
    ts: datetime
    value: float


@dataclass(frozen=True)
class OiShadowCandle:
    exchange: str
    symbol: str
    ts_open: datetime
    ts_close: datetime
    oi_open: float | None
    oi_high: float | None
    oi_low: float | None
    oi_close: float | None
    points_count: int
    max_gap_seconds: float | None
    close_lag_seconds: float | None
    usable: bool
    quality_reason: str


def build_oi_5m_candle(
    exchange: str,
    symbol: str,
    ts_open: datetime,
    ticks: list[OiTick],
    *,
    min_points: int = 3,
    max_gap_seconds_allowed: float = 75.0,
    max_close_lag_seconds_allowed: float = 75.0,
) -> OiShadowCandle:
    """Build a diagnostic OI OHLC candle from raw stream ticks.

    This is intentionally a shadow-only primitive: it does not infer missing
    data and does not write into production aggregate tables.
    """

    ts_close = ts_open + timedelta(minutes=5)
    window_ticks = sorted(
        (tick for tick in ticks if ts_open <= tick.ts < ts_close),
        key=lambda tick: tick.ts,
    )

    if not window_ticks:
        return OiShadowCandle(
            exchange=exchange,
            symbol=symbol,
            ts_open=ts_open,
            ts_close=ts_close,
            oi_open=None,
            oi_high=None,
            oi_low=None,
            oi_close=None,
            points_count=0,
            max_gap_seconds=None,
            close_lag_seconds=None,
            usable=False,
            quality_reason="no_ticks",
        )

    values = [tick.value for tick in window_ticks]
    max_gap = _max_gap_seconds(window_ticks)
    close_lag = (ts_close - window_ticks[-1].ts).total_seconds()

    reason = "ok"
    usable = True
    if len(window_ticks) < min_points:
        reason = "too_few_points"
        usable = False
    elif close_lag > max_close_lag_seconds_allowed:
        reason = "late_close_tick"
        usable = False
    elif max_gap is not None and max_gap > max_gap_seconds_allowed:
        reason = "stream_gap"
        usable = False

    return OiShadowCandle(
        exchange=exchange,
        symbol=symbol,
        ts_open=ts_open,
        ts_close=ts_close,
        oi_open=values[0],
        oi_high=max(values),
        oi_low=min(values),
        oi_close=values[-1],
        points_count=len(window_ticks),
        max_gap_seconds=max_gap,
        close_lag_seconds=close_lag,
        usable=usable,
        quality_reason=reason,
    )


def _max_gap_seconds(ticks: list[OiTick]) -> float | None:
    if len(ticks) < 2:
        return None
    return max(
        (right.ts - left.ts).total_seconds()
        for left, right in zip(ticks, ticks[1:])
    )
