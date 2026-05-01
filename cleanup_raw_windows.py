from __future__ import annotations

import os
import time
from db import execute

RAW_TABLES = [
    ("oi_5m_сырые", "ts_close"),
    ("price_5m_сырые", "ts_close"),
    ("volume_5m_сырые", "ts_close"),
]

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def cleanup_raw_windows() -> int:
    retention_hours = _env_int("RAW_RETENTION_HOURS", 72)
    total_deleted = 0
    started = time.monotonic()

    for table, ts_col in RAW_TABLES:
        t0 = time.monotonic()
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
            "RAW_CLEANUP_TABLE "
            f"table={table} ts_col={ts_col} rows_deleted={deleted} "
            f"retention_hours={retention_hours} seconds={time.monotonic() - t0:.2f}"
        )

    print(
        "RAW_WINDOWS_CLEANUP_OK "
        f"rows_deleted={total_deleted} retention_hours={retention_hours} "
        f"seconds={time.monotonic() - started:.2f}"
    )
    return total_deleted

if __name__ == "__main__":
    cleanup_raw_windows()
