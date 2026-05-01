from __future__ import annotations

import os
from db import execute, fetch


WINDOW_HOURS = max(1, int(os.getenv("DERIVED_WINDOW_HOURS", "1")))


def init_market_phase_source() -> None:
    execute("""
        CREATE TABLE IF NOT EXISTS market_phase_source (
            calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,

            market_state TEXT,
            invalid_reason TEXT,

            oi_delta_pct DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,

            price_structure TEXT,
            price_quality TEXT,
            price_slope_state TEXT,

            volume_structure TEXT,
            volume_quality TEXT,
            volume_hold_state TEXT,

            oi_slope_state TEXT,
            oi_slope_quality TEXT,
            oi_hold_state TEXT,
            oi_trend_15m TEXT,
            oi_trend_30m TEXT,
            oi_trend_1h TEXT,
            oi_trend_4h TEXT,
            oi_structure TEXT,
            oi_priority INT,

            silence_state TEXT,
            silence_reason TEXT,

            PRIMARY KEY (exchange, symbol, timeframe, ts_close)
        )
    """)


def rebuild_market_phase_source() -> int:
    init_market_phase_source()

    print(f"MARKET_PHASE_SOURCE_INCREMENTAL window_hours={WINDOW_HOURS} delete_before_insert=0")

    execute("""
        INSERT INTO market_phase_source (
            calculated_at,
            exchange, symbol, timeframe, ts_close,
            market_state, invalid_reason,
            oi_delta_pct, price_delta_pct, volume_delta_pct, range_width_pct,
            price_structure, price_quality, price_slope_state,
            volume_structure, volume_quality, volume_hold_state,
            oi_slope_state, oi_slope_quality, oi_hold_state,
            oi_trend_15m, oi_trend_30m, oi_trend_1h, oi_trend_4h,
            oi_structure, oi_priority,
            silence_state, silence_reason
        )
        SELECT
            NOW(),
            r.exchange, r.symbol, r.timeframe, r.ts_close,
            r.market_state, r.invalid_reason,
            r.oi_delta_pct, r.price_delta_pct, r.volume_delta_pct, r.range_width_pct,
            p.price_structure, p.price_quality, p.price_slope_state,
            v.volume_structure, v.volume_quality, v.volume_hold_state,
            o.stage_name, o.reason, o.oi_hold_state,
            o.oi_trend_15m, o.oi_trend_30m, o.oi_trend_1h, o.oi_trend_4h,
            o.oi_structure, o.oi_priority,
            s.stage_name, s.reason
        FROM market_research r
        LEFT JOIN market_price_state p
          ON p.exchange = r.exchange
         AND p.symbol = r.symbol
         AND p.timeframe = r.timeframe
         AND p.ts_close = r.ts_close
        LEFT JOIN market_volume_state v
          ON v.exchange = r.exchange
         AND v.symbol = r.symbol
         AND v.timeframe = r.timeframe
         AND v.ts_close = r.ts_close
        LEFT JOIN market_oi_slope o
          ON o.exchange = r.exchange
         AND o.symbol = r.symbol
         AND o.timeframe = r.timeframe
         AND o.ts_close = r.ts_close
        LEFT JOIN market_silence s
          ON s.exchange = r.exchange
         AND s.symbol = r.symbol
         AND s.timeframe = r.timeframe
         AND s.ts_close = r.ts_close
        WHERE r.ts_close >= (
            SELECT MAX(ts_close) - (%s || ' hours')::interval
            FROM market_research
        )
        ON CONFLICT (exchange, symbol, timeframe, ts_close)
        DO UPDATE SET
            calculated_at = EXCLUDED.calculated_at,
            market_state = EXCLUDED.market_state,
            invalid_reason = EXCLUDED.invalid_reason,
            oi_delta_pct = EXCLUDED.oi_delta_pct,
            price_delta_pct = EXCLUDED.price_delta_pct,
            volume_delta_pct = EXCLUDED.volume_delta_pct,
            range_width_pct = EXCLUDED.range_width_pct,
            price_structure = EXCLUDED.price_structure,
            price_quality = EXCLUDED.price_quality,
            price_slope_state = EXCLUDED.price_slope_state,
            volume_structure = EXCLUDED.volume_structure,
            volume_quality = EXCLUDED.volume_quality,
            volume_hold_state = EXCLUDED.volume_hold_state,
            oi_slope_state = EXCLUDED.oi_slope_state,
            oi_slope_quality = EXCLUDED.oi_slope_quality,
            oi_hold_state = EXCLUDED.oi_hold_state,
            oi_trend_15m = EXCLUDED.oi_trend_15m,
            oi_trend_30m = EXCLUDED.oi_trend_30m,
            oi_trend_1h = EXCLUDED.oi_trend_1h,
            oi_trend_4h = EXCLUDED.oi_trend_4h,
            oi_structure = EXCLUDED.oi_structure,
            oi_priority = EXCLUDED.oi_priority,
            silence_state = EXCLUDED.silence_state,
            silence_reason = EXCLUDED.silence_reason
    """, (WINDOW_HOURS,))

    rows = fetch("""
        SELECT COUNT(*) AS rows
        FROM market_phase_source
        WHERE ts_close >= (
            SELECT MAX(ts_close) - (%s || ' hours')::interval
            FROM market_phase_source
        )
    """, (WINDOW_HOURS,))

    count = int(rows[0]["rows"]) if rows else 0
    print(f"MARKET_PHASE_SOURCE_OK rows={count} window_hours={WINDOW_HOURS}")
    return count


def main() -> None:
    rebuild_market_phase_source()


if __name__ == "__main__":
    main()
