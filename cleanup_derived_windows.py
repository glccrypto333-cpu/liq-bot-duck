from __future__ import annotations

import os

from db import execute


RETENTION_HOURS = int(os.getenv("DERIVED_RETENTION_HOURS", "6"))


TABLES = [
    "bot_aggregates",
    "market_research",
    "market_price_state",
    "market_volume_state",
    "market_oi_slope",
    "market_silence",
]


def cleanup_derived_windows() -> None:
    for table in TABLES:
        execute(
            f"""
            DELETE FROM {table}
            WHERE ts_close < NOW() - (%s || ' hours')::interval
            """,
            (RETENTION_HOURS,),
        )

    print(f"DERIVED_WINDOWS_CLEANUP_OK retention_hours={RETENTION_HOURS}")


def main() -> None:
    cleanup_derived_windows()


if __name__ == "__main__":
    main()
