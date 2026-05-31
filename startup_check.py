from __future__ import annotations

import os
import sys
from db import fetch, execute


REQUIRED_TABLES = [
    "oi_raw",
    "price_raw",
    "volume_raw",
    "active_symbol_universe",
    "aggregate_windows",
    "oi_core_state",
    "oi_window_state",
    "oi_stage_history",
]

REQUIRED_COLUMNS = {
    "oi_core_state": [
        "exchange", "symbol",
        "current_stage",
        "oi_pattern_code",
        "oi_transition_permission",
        "oi_stage_age_minutes",
        "latest_cycle_ts",
        "decision_reason",
    ],
    "oi_window_state": [
        "exchange", "symbol", "window_code",
        "oi_pattern_code",
        "price_state_code",
        "volume_state_code",
        "cycle_ts",
    ],
    "oi_stage_history": [
        "exchange", "symbol",
        "from_stage", "to_stage",
        "transition_reason",
        "transition_allowed",
        "cycle_ts",
    ],
}

LEGACY_COLUMNS_ABSENT = {
    "oi_core_state": ["timeframe", "phase", "phase_name"],
}


def fail(msg: str) -> None:
    print(f"STARTUP_CHECK_FAIL {msg}")
    sys.exit(1)


def table_exists(table: str) -> bool:
    r = fetch("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name=%s
        ) AS ok
    """, (table,))
    return bool(r and r[0]["ok"])


def columns(table: str) -> set[str]:
    r = fetch("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name=%s
    """, (table,))
    return {x["column_name"] for x in r}


def main() -> None:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        fail("DATABASE_URL is not set")

    if "postgres.railway.internal" in db_url and os.getenv("RAILWAY_ENVIRONMENT") is None:
        fail("DATABASE_URL uses internal Railway host outside Railway runtime")

    execute("SET statement_timeout = '10s'")

    print("STARTUP_CHECK database_url=SET")

    missing_tables = [t for t in REQUIRED_TABLES if not table_exists(t)]
    if missing_tables:
        fail(f"missing_tables={missing_tables}")

    print(f"STARTUP_CHECK tables_ok count={len(REQUIRED_TABLES)}")

    for table, need_cols in REQUIRED_COLUMNS.items():
        have = columns(table)
        missing = [c for c in need_cols if c not in have]
        if missing:
            fail(f"table={table} missing_columns={missing}")
        print(f"STARTUP_CHECK columns_ok table={table}")

    for table, bad_cols in LEGACY_COLUMNS_ABSENT.items():
        have = columns(table)
        present = [c for c in bad_cols if c in have]
        if present:
            fail(f"table={table} legacy_columns_present={present}")
        print(f"STARTUP_CHECK legacy_absent_ok table={table}")

    aggregates = fetch("""
        SELECT
            COUNT(*) AS rows,
            MAX(ts_close) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX(ts_close))) / 60.0 AS age_minutes
        FROM aggregate_windows
    """)
    print(f"STARTUP_CHECK aggregate_windows {dict(aggregates[0]) if aggregates else None}")
    if not aggregates or int(aggregates[0].get("rows") or 0) <= 0:
        fail("aggregate_windows_empty")

    core_state = fetch("""
        SELECT
            COUNT(*) AS rows,
            MAX(latest_cycle_ts) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX(latest_cycle_ts))) / 60.0 AS age_minutes
        FROM oi_core_state
    """)
    print(f"STARTUP_CHECK oi_core_state {dict(core_state[0]) if core_state else None}")
    if not core_state or int(core_state[0].get("rows") or 0) <= 0:
        fail("oi_core_state_empty")

    window_state = fetch("""
        SELECT
            COUNT(*) AS rows,
            MAX(cycle_ts) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX(cycle_ts))) / 60.0 AS age_minutes
        FROM oi_window_state
    """)
    print(f"STARTUP_CHECK oi_window_state {dict(window_state[0]) if window_state else None}")
    if not window_state or int(window_state[0].get("rows") or 0) <= 0:
        fail("oi_window_state_empty")

    stage_history = fetch("""
        SELECT
            COUNT(*) AS rows,
            MAX(cycle_ts) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX(cycle_ts))) / 60.0 AS age_minutes
        FROM oi_stage_history
    """)
    print(f"STARTUP_CHECK oi_stage_history {dict(stage_history[0]) if stage_history else None}")

    print("STARTUP_CHECK_OK")


if __name__ == "__main__":
    main()
