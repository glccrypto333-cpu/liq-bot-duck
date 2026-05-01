from __future__ import annotations

import os
import time
from db import execute


DERIVED_TABLES = [
    ("bot_aggregates", "ts_close"),
    ("market_research", "ts_close"),
    ("market_oi_slope", "ts_close"),
    ("market_price_state", "ts_close"),
    ("market_volume_state", "ts_close"),
    ("market_silence", "ts_close"),
    ("market_phase_source", "ts_close"),
    ("market_oi_slope_staging", "ts_close"),
]


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def cleanup_derived_windows() -> int:
    retention_hours = _env_int("DERIVED_RETENTION_HOURS", 72)

    total_deleted = 0
    started = time.monotonic()

    for table, ts_col in DERIVED_TABLES:
        t0 = time.monotonic()

        try:
            rows = execute(
                f"""
                DELETE FROM {table}
                WHERE {ts_col} < NOW() - (%s || ' hours')::interval
                """,
                (retention_hours,),
            )
            deleted = int(rows or 0)
            total_deleted += deleted

            print(
                "DERIVED_CLEANUP_TABLE "
                f"table={table} ts_col={ts_col} "
                f"rows_deleted={deleted} "
                f"retention_hours={retention_hours} "
                f"seconds={time.monotonic() - t0:.2f}"
            )

        except Exception as e:
            print(
                "DERIVED_CLEANUP_TABLE_ERROR "
                f"table={table} ts_col={ts_col} "
                f"retention_hours={retention_hours} "
                f"error={type(e).__name__}: {e}"
            )

    print(
        "DERIVED_WINDOWS_CLEANUP_OK "
        f"rows_deleted={total_deleted} "
        f"retention_hours={retention_hours} "
        f"seconds={time.monotonic() - started:.2f}"
    )

    return total_deleted


if __name__ == "__main__":
    cleanup_derived_windows()
