from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ws_oi_shadow_core import OiTick, build_oi_5m_candle


UTC = timezone.utc


def test_build_oi_5m_candle_uses_first_tick_open_and_last_tick_close() -> None:
    ts_open = datetime(2026, 7, 7, 10, 0, tzinfo=UTC)
    ticks = [
        OiTick(ts=ts_open + timedelta(seconds=10), value=100.0),
        OiTick(ts=ts_open + timedelta(seconds=120), value=110.0),
        OiTick(ts=ts_open + timedelta(seconds=290), value=108.0),
    ]

    candle = build_oi_5m_candle("BYBIT", "TESTUSDT", ts_open, ticks)

    assert candle.exchange == "BYBIT"
    assert candle.symbol == "TESTUSDT"
    assert candle.ts_open == ts_open
    assert candle.ts_close == ts_open + timedelta(minutes=5)
    assert candle.oi_open == 100.0
    assert candle.oi_high == 110.0
    assert candle.oi_low == 100.0
    assert candle.oi_close == 108.0
    assert candle.points_count == 3


def test_build_oi_5m_candle_reports_gaps_and_close_lag() -> None:
    ts_open = datetime(2026, 7, 7, 10, 0, tzinfo=UTC)
    ticks = [
        OiTick(ts=ts_open + timedelta(seconds=5), value=100.0),
        OiTick(ts=ts_open + timedelta(seconds=95), value=105.0),
        OiTick(ts=ts_open + timedelta(seconds=180), value=106.0),
    ]

    candle = build_oi_5m_candle("BYBIT", "TESTUSDT", ts_open, ticks)

    assert candle.max_gap_seconds == 90.0
    assert candle.close_lag_seconds == 120.0
    assert candle.usable is False
    assert candle.quality_reason == "late_close_tick"


def test_build_oi_5m_candle_marks_dense_stream_usable() -> None:
    ts_open = datetime(2026, 7, 7, 10, 0, tzinfo=UTC)
    ticks = [
        OiTick(ts=ts_open + timedelta(seconds=5), value=100.0),
        OiTick(ts=ts_open + timedelta(seconds=45), value=101.0),
        OiTick(ts=ts_open + timedelta(seconds=105), value=102.0),
        OiTick(ts=ts_open + timedelta(seconds=165), value=103.0),
        OiTick(ts=ts_open + timedelta(seconds=235), value=104.0),
        OiTick(ts=ts_open + timedelta(seconds=292), value=105.0),
    ]

    candle = build_oi_5m_candle("BYBIT", "TESTUSDT", ts_open, ticks)

    assert candle.usable is True
    assert candle.quality_reason == "ok"
