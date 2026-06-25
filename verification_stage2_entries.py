from __future__ import annotations

"""
Product-grade verification tool for scanning stage-2 entries by replay cycle.

This script replays autonomous OI snapshots and prints compact stage-2 rows for
each cycle so stage-2 candidates can be scanned before deeper compare/trace.
"""

import argparse
from collections import defaultdict

from autonomous_oi_replay import _parse_ts, load_cycle_timestamps, load_latest_window_map_for_replay, load_batch_window_updates
from autonomous_oi_service import (
    compute_autonomous_oi_snapshot_from_latest_window_map,
)


CORE_EXCHANGE_IDX = 0
CORE_SYMBOL_IDX = 1
CORE_STAGE_IDX = 2
CORE_PATTERN_IDX = 4
CORE_PRICE_IDX = 16
CORE_VOLUME_IDX = 18
CORE_AGE_IDX = 20
CORE_PERMISSION_IDX = 21
CORE_REASON_IDX = 25


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan stage-2 entries across replay cycles")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit-cycles", type=int, default=60)
    parser.add_argument("--to-ts")
    parser.add_argument("--exchange", default="ALL", help="Filter exchange for displayed rows; default ALL")
    parser.add_argument("--symbols", nargs="*", default=[], help="Optional symbol filter")
    parser.add_argument("--max-rows", type=int, default=10, help="Maximum rows per cycle")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cycles, resolved_window_source = load_cycle_timestamps(args.hours, args.limit_cycles, to_ts=_parse_ts(args.to_ts))
    if not cycles:
        print("NO_CYCLES")
        return

    tracked = {symbol.upper() for symbol in args.symbols}
    exchange_filter = (args.exchange or "ALL").upper()
    latest_window_map = load_latest_window_map_for_replay(cycles[0], window_source=resolved_window_source)
    updates_by_cycle = load_batch_window_updates(cycles, window_source=resolved_window_source)
    state_map = {}

    for cycle_ts in cycles:
        for row in updates_by_cycle.get(cycle_ts, []):
            symbol_key = (row["exchange"], row["symbol"])
            latest_window_map.setdefault(symbol_key, {})
            latest_window_map[symbol_key].setdefault(row["window_code"], {})
            latest_window_map[symbol_key][row["window_code"]][row["metric"]] = row

        core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=cycle_ts,
            previous_state_map=state_map,
        )

        stage2_rows = []
        for row in core_rows:
            if int(row[CORE_STAGE_IDX] or 0) != 2:
                continue
            exchange = row[CORE_EXCHANGE_IDX]
            symbol = row[CORE_SYMBOL_IDX]
            if tracked and symbol not in tracked:
                continue
            if exchange_filter != "ALL" and exchange != exchange_filter:
                continue
            stage2_rows.append(row)

        if not stage2_rows:
            continue

        stage2_rows.sort(key=lambda row: (row[CORE_EXCHANGE_IDX], row[CORE_SYMBOL_IDX]))
        print(f"CYCLE {cycle_ts.isoformat()} stage2_count={len(stage2_rows)}")
        for row in stage2_rows[: max(args.max_rows, 0)]:
            print(
                "  "
                f"{row[CORE_EXCHANGE_IDX]}:{row[CORE_SYMBOL_IDX]} "
                f"pattern={row[CORE_PATTERN_IDX]} "
                f"price={row[CORE_PRICE_IDX]} "
                f"volume={row[CORE_VOLUME_IDX]} "
                f"age={row[CORE_AGE_IDX]} "
                f"perm={row[CORE_PERMISSION_IDX]} "
                f"reason={row[CORE_REASON_IDX]}"
            )


if __name__ == "__main__":
    main()
