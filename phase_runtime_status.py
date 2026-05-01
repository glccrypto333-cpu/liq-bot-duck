from __future__ import annotations

from db import fetch


def main() -> None:
    rows = fetch("""
        SELECT
            'raw_oi' AS table_name,
            COUNT(*) AS rows,
            MAX(ts_close) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT AS lag_seconds
        FROM oi_5m_сырые

        UNION ALL

        SELECT
            'bot_aggregates',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM bot_aggregates

        UNION ALL

        SELECT
            'market_research',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM market_research

        UNION ALL

        SELECT
            'market_price_state',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM market_price_state

        UNION ALL

        SELECT
            'market_volume_state',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM market_volume_state

        UNION ALL

        SELECT
            'market_oi_slope',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM market_oi_slope

        UNION ALL

        SELECT
            'market_silence',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM market_silence

        UNION ALL

        SELECT
            'market_phase',
            COUNT(*),
            MAX(phase_updated_at),
            EXTRACT(EPOCH FROM (NOW() - MAX(phase_updated_at)))::BIGINT
        FROM market_phase

        UNION ALL

        SELECT
            'market_phase_source',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM market_phase_source
    """)

    print("PHASE_RUNTIME_STATUS")
    for r in rows:
        print(dict(r))

    print("PHASE_RUNTIME_STATUS_OK")


if __name__ == "__main__":
    main()
