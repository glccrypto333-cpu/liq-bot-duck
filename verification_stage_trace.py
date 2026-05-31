from __future__ import annotations

"""
Product-grade verification tool for cycle-by-cycle stage trace output.

This script replays autonomous OI snapshots and prints a compact trace for
tracked symbols across replay cycles.
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


CORE_SYMBOL_IDX = 1
CORE_STAGE_IDX = 2
CORE_PATTERN_IDX = 4
CORE_PRICE_IDX = 16
CORE_VOLUME_IDX = 18
CORE_AGE_IDX = 20
CORE_PERMISSION_IDX = 21
CORE_BLOCKED_IDX = 22
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Trace stage decisions for tracked symbols across replay cycles")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit-cycles", type=int, default=60)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--exchange", default="ALL", help="Filter exchange for displayed rows; default ALL")
    parser.add_argument("--to-ts")
    args = parser.parse_args()

    cycles = load_cycle_timestamps(args.hours, args.limit_cycles, to_ts=_parse_ts(args.to_ts))
    if not cycles:
        print("NO_CYCLES")
        return

    tracked = {symbol.upper() for symbol in args.symbols}
    exchange_filter = (args.exchange or "ALL").upper()
    latest_window_map = load_latest_window_map(cycles[0])
    updates_by_cycle = build_updates(cycles)
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

        by_symbol = {}
        for row in core_rows:
            exchange = row[0]
            symbol = row[CORE_SYMBOL_IDX]
            if symbol not in tracked:
                continue
            if exchange_filter != "ALL" and exchange != exchange_filter:
                continue
            by_symbol[(exchange, symbol)] = row

        if exchange_filter == "ALL":
            keys = sorted(by_symbol)
        else:
            keys = [(exchange_filter, symbol) for symbol in sorted(tracked)]

        for key in keys:
            exchange, symbol = key
            row = by_symbol.get(key)
            if not row:
                print(f"{cycle_ts.isoformat()} exchange={exchange} symbol={symbol} missing")
                continue
            print(
                f"{cycle_ts.isoformat()} "
                f"exchange={exchange} "
                f"symbol={symbol} "
                f"stage={row[CORE_STAGE_IDX]} "
                f"pattern={row[CORE_PATTERN_IDX]} "
                f"price={row[CORE_PRICE_IDX]} "
                f"volume={row[CORE_VOLUME_IDX]} "
                f"age={row[CORE_AGE_IDX]} "
                f"perm={row[CORE_PERMISSION_IDX]} "
                f"blocked={row[CORE_BLOCKED_IDX]} "
                f"reason={row[CORE_REASON_IDX]}"
            )


if __name__ == "__main__":
    main()
