from __future__ import annotations

"""
Product-grade verification tool for following stage-3 entries.

This script replays autonomous OI snapshots, detects fresh stage-3 entries, and
prints how each entry behaves over the next N replay cycles.
"""

import argparse
from collections import defaultdict

from autonomous_oi_replay import _parse_ts, load_cycle_timestamps
from autonomous_oi_service import (
    WINDOWS,
    compute_autonomous_oi_snapshot_from_latest_window_map,
    load_latest_window_map,
)
from db import fetch


CORE_EXCHANGE_IDX = 0
CORE_SYMBOL_IDX = 1
CORE_STAGE_IDX = 2
CORE_PATTERN_IDX = 4
CORE_PRICE_IDX = 16
CORE_VOLUME_IDX = 18
CORE_REASON_IDX = 25


def build_updates(cycles):
    if not cycles:
        return defaultdict(list)

    updates = fetch(
        """
        SELECT
            metric,
            window_code,
            ts_open,
            ts_close,
            exchange,
            symbol,
            open_value,
            high_value,
            low_value,
            close_value,
            sum_value,
            avg_value,
            delta_pct,
            unique_candles,
            source_cycle_ts,
            built_at
        FROM aggregate_windows
        WHERE metric IN ('OI', 'PRICE', 'VOLUME')
          AND window_code = ANY(%s)
          AND source_cycle_ts IS NOT NULL
          AND source_cycle_ts > %s
          AND source_cycle_ts <= %s
        ORDER BY source_cycle_ts ASC, metric, window_code, exchange, symbol
        """,
        (WINDOWS, cycles[0], cycles[-1]),
    )
    updates_by_cycle = defaultdict(list)
    for row in updates:
        updates_by_cycle[row["source_cycle_ts"]].append(row)
    return updates_by_cycle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Follow fresh stage-3 entries over subsequent replay cycles")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit-cycles", type=int, default=60)
    parser.add_argument("--to-ts")
    parser.add_argument("--follow-steps", type=int, default=3)
    parser.add_argument("--exchange", default="ALL", help="Filter exchange for displayed rows; default ALL")
    parser.add_argument("--symbols", nargs="*", default=[], help="Optional symbol filter")
    return parser.parse_args()


def row_view(row: tuple) -> dict[str, object]:
    return {
        "stage": int(row[CORE_STAGE_IDX] or 0),
        "pattern": row[CORE_PATTERN_IDX],
        "price": row[CORE_PRICE_IDX],
        "volume": row[CORE_VOLUME_IDX],
        "reason": row[CORE_REASON_IDX],
    }


def main() -> None:
    args = parse_args()
    cycles = load_cycle_timestamps(args.hours, args.limit_cycles, to_ts=_parse_ts(args.to_ts))
    if not cycles:
        print("NO_CYCLES")
        return

    tracked = {symbol.upper() for symbol in args.symbols}
    exchange_filter = (args.exchange or "ALL").upper()
    latest_window_map = load_latest_window_map(cycles[0])
    updates_by_cycle = build_updates(cycles)
    state_map = {}
    snapshots = []

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
        snapshot = {}
        for row in core_rows:
            exchange = row[CORE_EXCHANGE_IDX]
            symbol = row[CORE_SYMBOL_IDX]
            if tracked and symbol not in tracked:
                continue
            if exchange_filter != "ALL" and exchange != exchange_filter:
                continue
            snapshot[(exchange, symbol)] = row_view(row)
        snapshots.append((cycle_ts, snapshot))

    seen = False
    for idx, (cycle_ts, snapshot) in enumerate(snapshots):
        for key in sorted(snapshot):
            row = snapshot[key]
            prev_stage = 0
            if idx > 0:
                prev_stage = int(snapshots[idx - 1][1].get(key, {}).get("stage", 0) or 0)
            if row["stage"] != 3 or prev_stage == 3:
                continue

            seen = True
            exchange, symbol = key
            print(
                f"ENTRY {cycle_ts.isoformat()} {exchange}:{symbol} "
                f"stage=3 pattern={row['pattern']} price={row['price']} volume={row['volume']}"
            )
            print(f"  step0 reason={row['reason']}")
            for step in range(1, max(args.follow_steps, 0) + 1):
                if idx + step >= len(snapshots):
                    break
                next_ts, next_snapshot = snapshots[idx + step]
                next_row = next_snapshot.get(key)
                if not next_row:
                    print(f"  step{step} ts={next_ts.isoformat()} missing")
                    continue
                print(
                    f"  step{step} ts={next_ts.isoformat()} "
                    f"stage={next_row['stage']} pattern={next_row['pattern']} "
                    f"price={next_row['price']} volume={next_row['volume']}"
                )
                print(f"    reason={next_row['reason']}")

    if not seen:
        print("NO_STAGE3_ENTRIES")


if __name__ == "__main__":
    main()
