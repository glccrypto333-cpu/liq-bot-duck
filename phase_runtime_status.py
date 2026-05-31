from __future__ import annotations

from db import fetch


def main() -> None:
    rows = fetch("""
        SELECT
            'raw_oi' AS table_name,
            COUNT(*) AS rows,
            MAX(ts_close) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT AS lag_seconds
        FROM oi_raw

        UNION ALL

        SELECT
            'aggregate_windows',
            COUNT(*),
            MAX(ts_close),
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close)))::BIGINT
        FROM aggregate_windows

        UNION ALL

        SELECT
            'oi_core_state',
            COUNT(*),
            MAX(latest_cycle_ts),
            EXTRACT(EPOCH FROM (NOW() - MAX(latest_cycle_ts)))::BIGINT
        FROM oi_core_state

        UNION ALL

        SELECT
            'oi_window_state',
            COUNT(*),
            MAX(cycle_ts),
            EXTRACT(EPOCH FROM (NOW() - MAX(cycle_ts)))::BIGINT
        FROM oi_window_state

        UNION ALL

        SELECT
            'oi_stage_history',
            COUNT(*),
            MAX(cycle_ts),
            EXTRACT(EPOCH FROM (NOW() - MAX(cycle_ts)))::BIGINT
        FROM oi_stage_history

        UNION ALL

        SELECT
            'core_state_v2',
            COUNT(*),
            MAX(latest_cycle_ts),
            EXTRACT(EPOCH FROM (NOW() - MAX(latest_cycle_ts)))::BIGINT
        FROM core_state_v2

        UNION ALL

        SELECT
            'window_state_v2',
            COUNT(*),
            MAX(cycle_ts),
            EXTRACT(EPOCH FROM (NOW() - MAX(cycle_ts)))::BIGINT
        FROM window_state_v2

        UNION ALL

        SELECT
            'transition_history_v2',
            COUNT(*),
            MAX(cycle_ts),
            EXTRACT(EPOCH FROM (NOW() - MAX(cycle_ts)))::BIGINT
        FROM transition_history_v2
    """)

    print("PHASE_RUNTIME_STATUS")
    for r in rows:
        print(dict(r))

    sync_rows = fetch("""
        SELECT
            (SELECT COUNT(*) FROM oi_core_state) AS legacy_core_rows,
            (SELECT COUNT(*) FROM core_state_v2) AS v2_core_rows,
            (SELECT COUNT(*) FROM oi_window_state) AS legacy_window_rows,
            (SELECT COUNT(*) FROM window_state_v2) AS v2_window_rows
    """)
    print("PHASE_RUNTIME_SYNC")
    for r in sync_rows:
        print(dict(r))

    print("PHASE_RUNTIME_STATUS_OK")


if __name__ == "__main__":
    main()
