from __future__ import annotations

"""
Product-grade verification tool for compact stage-state summaries.

This script prints a short snapshot of stage distribution and the most common
price/volume blockers around stage-2 candidates on the canonical OI surface.
"""

import argparse

from db import fetch


def dump(title: str, query: str) -> None:
    print(title)
    for row in fetch(query):
        print(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print compact stage-state summaries for the canonical OI surface")
    parser.add_argument("--skip-window-blocks", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    dump(
        "stage_summary",
        """
        select current_stage, count(*) as c
        from oi_core_state
        group by current_stage
        order by current_stage
        """,
    )

    dump(
        "stage2_price_volume",
        """
        select price_state_summary, volume_state_summary, count(*) as c
        from oi_core_state
        where current_stage = 2
        group by price_state_summary, volume_state_summary
        order by c desc
        """,
    )

    if not args.skip_window_blocks:
        dump(
            "stage2_window_price_blocks",
            """
            select w.window_code, w.price_state_code, w.price_block_level, count(*) as c
            from oi_window_state w
            join oi_core_state c
              on c.exchange = w.exchange and c.symbol = w.symbol
            where c.current_stage = 2
            group by w.window_code, w.price_state_code, w.price_block_level
            order by w.window_code, c desc
            """,
        )


if __name__ == "__main__":
    main()
