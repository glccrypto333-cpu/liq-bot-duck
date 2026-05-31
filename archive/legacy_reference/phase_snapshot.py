from __future__ import annotations

from datetime import datetime, timezone

from db import execute, fetch


ZERO_ROW = {
    "total_rows": 0,
    "stage0_rows": 0,
    "stage1_rows": 0,
    "stage2_rows": 0,
    "stage3_rows": 0,
    "p1_rows": 0,
    "p2_rows": 0,
    "p3_rows": 0,
    "stage3_manual_required": 0,
    "hard_price_stage3": 0,
}


def init_phase_snapshot() -> None:
    execute("""
        CREATE TABLE IF NOT EXISTS market_phase_snapshot (
            snapshot_at TIMESTAMPTZ NOT NULL,
            total_rows BIGINT NOT NULL,
            stage0_rows BIGINT NOT NULL,
            stage1_rows BIGINT NOT NULL,
            stage2_rows BIGINT NOT NULL,
            stage3_rows BIGINT NOT NULL,
            p1_rows BIGINT NOT NULL,
            p2_rows BIGINT NOT NULL,
            p3_rows BIGINT NOT NULL,
            stage3_manual_required BIGINT NOT NULL,
            hard_price_stage3 BIGINT NOT NULL
        )
    """)


def insert_phase_snapshot() -> None:
    init_phase_snapshot()
    now = datetime.now(timezone.utc)

    rows = fetch("""
        SELECT
            COALESCE(COUNT(*), 0) AS total_rows,
            COALESCE(COUNT(*) FILTER (WHERE phase = 0), 0) AS stage0_rows,
            COALESCE(COUNT(*) FILTER (WHERE phase = 1), 0) AS stage1_rows,
            COALESCE(COUNT(*) FILTER (WHERE phase = 2), 0) AS stage2_rows,
            COALESCE(COUNT(*) FILTER (WHERE phase = 3), 0) AS stage3_rows,
            COALESCE(COUNT(*) FILTER (WHERE priority = 'P1'), 0) AS p1_rows,
            COALESCE(COUNT(*) FILTER (WHERE priority = 'P2'), 0) AS p2_rows,
            COALESCE(COUNT(*) FILTER (WHERE priority = 'P3'), 0) AS p3_rows,
            COALESCE(COUNT(*) FILTER (WHERE phase = 3 AND manual_reset_required = TRUE), 0) AS stage3_manual_required,
            COALESCE(COUNT(*) FILTER (
                WHERE phase = 3
                  AND price_structure IN ('импульс вниз','расширение вниз','дамп')
            ), 0) AS hard_price_stage3
        FROM market_phase
    """)

    row = dict(rows[0]) if rows else ZERO_ROW

    execute(
        """
        INSERT INTO market_phase_snapshot (
            snapshot_at,
            total_rows,
            stage0_rows,
            stage1_rows,
            stage2_rows,
            stage3_rows,
            p1_rows,
            p2_rows,
            p3_rows,
            stage3_manual_required,
            hard_price_stage3
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            now,
            row["total_rows"],
            row["stage0_rows"],
            row["stage1_rows"],
            row["stage2_rows"],
            row["stage3_rows"],
            row["p1_rows"],
            row["p2_rows"],
            row["p3_rows"],
            row["stage3_manual_required"],
            row["hard_price_stage3"],
        ),
    )

    print("PHASE_SNAPSHOT_OK", row)


def main() -> None:
    insert_phase_snapshot()


if __name__ == "__main__":
    main()
