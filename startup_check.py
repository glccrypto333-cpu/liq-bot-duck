from __future__ import annotations

import os
import sys
from db import fetch, execute


REQUIRED_TABLES = [
    "oi_5m_сырые",
    "price_5m_сырые",
    "volume_5m_сырые",
    "active_symbol_universe",
    "bot_aggregates",
    "market_research",
    "market_price_state",
    "market_volume_state",
    "market_oi_slope",
    "market_silence",
    "market_phase_source",
    "market_phase",
    "market_phase_history",
]

REQUIRED_COLUMNS = {
    "market_phase": [
        "exchange", "symbol", "timeframe",
        "phase", "phase_name",
        "phase_updated_at",
        "stage1_started_at",
        "stage2_started_at",
        "stage3_started_at",
    ],
    "market_phase_source": [
        "exchange", "symbol", "timeframe", "ts_close",
        "oi_structure", "oi_priority", "oi_hold_state",
        "oi_trend_15m", "oi_trend_30m", "oi_trend_1h", "oi_trend_4h",
        "price_structure", "volume_structure",
    ],
}

LEGACY_COLUMNS_ABSENT = {
    "market_phase": ["ts_close"],
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

    phase = fetch("""
        SELECT
            COUNT(*) AS rows,
            MAX(phase_updated_at) AS latest
        FROM market_phase
    """)
    print(f"STARTUP_CHECK market_phase {dict(phase[0]) if phase else None}")

    source = fetch("""
        SELECT
            COUNT(*) AS rows,
            MAX(ts_close) AS latest
        FROM market_phase_source
    """)
    print(f"STARTUP_CHECK market_phase_source {dict(source[0]) if source else None}")

    print("STARTUP_CHECK_OK")


if __name__ == "__main__":
    main()
