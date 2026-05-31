from __future__ import annotations

from db import execute


RETENTION_DAYS = 30


def cleanup_phase_snapshot() -> None:
    execute(
        """
        DELETE FROM market_phase_snapshot
        WHERE snapshot_at < NOW() - (%s || ' days')::interval
        """,
        (RETENTION_DAYS,),
    )

    print(f"PHASE_SNAPSHOT_CLEANUP_OK retention_days={RETENTION_DAYS}")


def main() -> None:
    cleanup_phase_snapshot()


if __name__ == "__main__":
    main()
