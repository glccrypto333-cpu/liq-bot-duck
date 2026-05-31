from __future__ import annotations

from db import fetch


MAX_STUCK_STAGE3 = 25
MAX_HARD_PRICE_STAGE3 = 50


def main() -> None:
    rows = fetch("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE phase = 0) AS stage0,
            COUNT(*) FILTER (WHERE phase = 1) AS stage1,
            COUNT(*) FILTER (WHERE phase = 2) AS stage2,
            COUNT(*) FILTER (WHERE phase = 3) AS stage3,
            COUNT(*) FILTER (
                WHERE phase = 3
                  AND manual_reset_required = TRUE
            ) AS stuck_stage3,
            COUNT(*) FILTER (
                WHERE phase = 3
                  AND price_structure IN ('импульс вниз','расширение вниз','дамп')
            ) AS hard_price_stage3
        FROM market_phase
    """)

    row = dict(rows[0]) if rows else {
        "total": 0,
        "stage0": 0,
        "stage1": 0,
        "stage2": 0,
        "stage3": 0,
        "stuck_stage3": 0,
        "hard_price_stage3": 0,
    }

    print("PHASE_HEALTHCHECK", row)

    if row.get("stuck_stage3", 0) > MAX_STUCK_STAGE3:
        raise SystemExit(f"TOO_MANY_STUCK_STAGE3 count={row.get('stuck_stage3')} limit={MAX_STUCK_STAGE3}")

    if row.get("hard_price_stage3", 0) > MAX_HARD_PRICE_STAGE3:
        raise SystemExit(f"TOO_MANY_HARD_PRICE_STAGE3 count={row.get('hard_price_stage3')} limit={MAX_HARD_PRICE_STAGE3}")

    print("PHASE_HEALTHCHECK_OK")


if __name__ == "__main__":
    main()
