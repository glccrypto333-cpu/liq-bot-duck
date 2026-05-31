from __future__ import annotations

"""
Product-grade verification tool for canonical MightyDuck DB health.

This script reports freshness, row counts, canonical indexes, legacy-table
presence, and relation sizes for the OI-only database surface.
"""

import argparse

from db import fetch


def dump(title: str, query: str) -> None:
    print(title)
    for row in fetch(query):
        print(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify canonical MightyDuck DB health")
    parser.add_argument("--skip-indexes", action="store_true")
    parser.add_argument("--skip-legacy", action="store_true")
    parser.add_argument("--skip-sizes", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    dump(
        "freshness_summary",
        """
        select 'oi_raw' as table_name, max(ts_close) as latest_ts from oi_raw
        union all
        select 'price_raw' as table_name, max(ts_close) as latest_ts from price_raw
        union all
        select 'volume_raw' as table_name, max(ts_close) as latest_ts from volume_raw
        union all
        select 'aggregate_windows' as table_name, max(ts_close) as latest_ts from aggregate_windows
        union all
        select 'oi_core_state' as table_name, max(latest_cycle_ts) as latest_ts from oi_core_state
        union all
        select 'oi_window_state' as table_name, max(cycle_ts) as latest_ts from oi_window_state
        union all
        select 'oi_stage_history' as table_name, max(cycle_ts) as latest_ts from oi_stage_history
        order by table_name
        """,
    )

    dump(
        "row_counts",
        """
        select 'aggregate_windows' as table_name, count(*) as row_count from aggregate_windows
        union all
        select 'oi_core_state' as table_name, count(*) as row_count from oi_core_state
        union all
        select 'oi_window_state' as table_name, count(*) as row_count from oi_window_state
        union all
        select 'oi_stage_history' as table_name, count(*) as row_count from oi_stage_history
        order by table_name
        """,
    )

    if not args.skip_indexes:
        dump(
            "canonical_indexes",
            """
            select tablename, indexname
            from pg_indexes
            where schemaname = 'public'
              and tablename in (
                'oi_raw',
                'price_raw',
                'volume_raw',
                'aggregate_windows',
                'oi_core_state',
                'oi_window_state',
                'oi_stage_history'
              )
            order by tablename, indexname
            """,
        )

    if not args.skip_legacy:
        dump(
            "legacy_tables_present",
            """
            select table_name
            from information_schema.tables
            where table_schema = 'public'
              and table_name in (
                'oi_5m_сырые',
                'price_5m_сырые',
                'volume_5m_сырые',
                'bot_aggregates'
              )
            order by table_name
            """,
        )

    if not args.skip_sizes:
        dump(
            "table_sizes",
            """
            select relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) as total_size
            from pg_catalog.pg_statio_user_tables
            where schemaname = 'public'
              and relname in (
                'oi_raw',
                'price_raw',
                'volume_raw',
                'aggregate_windows',
                'oi_core_state',
                'oi_window_state',
                'oi_stage_history'
              )
            order by relname
            """,
        )


if __name__ == "__main__":
    main()
