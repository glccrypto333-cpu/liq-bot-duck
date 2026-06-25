from __future__ import annotations

"""
Product-grade verification tool for cycle-by-cycle stage comparison.

This script replays autonomous OI snapshots for a selected period and prints
core state details for tracked symbols, with optional per-window breakdown.
"""

import argparse

from autonomous_oi_replay import (
    _parse_ts,
    load_batch_window_updates,
    load_cycle_timestamps,
    load_latest_window_map_for_replay,
)
from autonomous_oi_service import (
    compute_autonomous_oi_snapshot_from_latest_window_map,
)

def main() -> None:
    parser = argparse.ArgumentParser(description="Compare stage snapshots for tracked symbols across replay cycles")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit-cycles", type=int, default=60)
    parser.add_argument("--from-ts", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--exchange", default="BYBIT", help="Filter exchange for displayed rows; use ALL for every exchange")
    parser.add_argument("--show-windows", action="store_true")
    parser.add_argument("--only-cycles", nargs="*", default=[])
    parser.add_argument("--to-ts")
    parser.add_argument("--window-source", choices=["auto", "hot", "history"], default="auto")
    args = parser.parse_args()

    tracked = {symbol.upper() for symbol in args.symbols}
    only_cycles = set(args.only_cycles)
    exchange_filter = (args.exchange or "BYBIT").upper()
    cycles, resolved_window_source = load_cycle_timestamps(
        args.hours,
        args.limit_cycles,
        to_ts=_parse_ts(args.to_ts),
        window_source=args.window_source,
    )
    if not cycles:
        print("NO_CYCLES")
        return

    latest_window_map = load_latest_window_map_for_replay(cycles[0], window_source=resolved_window_source)
    updates_by_cycle = load_batch_window_updates(cycles, window_source=resolved_window_source)
    state_map = {}

    for cycle_ts in cycles:
        if cycle_ts.isoformat() < args.from_ts:
            for row in updates_by_cycle.get(cycle_ts, []):
                latest_window_map[(row["exchange"], row["symbol"])][row["window_code"]][row["metric"]] = row
            _core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
                latest_window_map,
                cycle_ts=cycle_ts,
                previous_state_map=state_map,
            )
            continue

        for row in updates_by_cycle.get(cycle_ts, []):
            latest_window_map[(row["exchange"], row["symbol"])][row["window_code"]][row["metric"]] = row

        core_rows, window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=cycle_ts,
            previous_state_map=state_map,
        )
        if only_cycles and cycle_ts.isoformat() not in only_cycles:
            continue

        selected = []
        for row in core_rows:
            exchange = row[0]
            symbol = row[1]
            if symbol not in tracked:
                continue
            if exchange_filter != "ALL" and exchange != exchange_filter:
                continue
            selected.append(row)

        selected.sort(key=lambda row: (row[0], row[1]))
        if not selected:
            continue

        print(f"CYCLE {cycle_ts.isoformat()}")
        for row in selected:
            print(
                "  "
                f"{row[0]}:{row[1]} "
                f"stage={row[2]} "
                f"pattern={row[4]} "
                f"price={row[16]} "
                f"volume={row[18]} "
                f"age={row[20]} "
                f"perm={row[21]} "
                f"blocked={row[22]} "
                f"reason={row[25]}"
            )
            if args.show_windows:
                symbol_windows = [item for item in window_rows if item[0] == row[0] and item[1] == row[1]]
                symbol_windows.sort(key=lambda item: WINDOWS.index(item[2]))
                for item in symbol_windows:
                    print(
                        "    "
                        f"{item[2]} "
                        f"pattern={item[9]} "
                        f"price={item[11]} "
                        f"price_block={item[13]} "
                        f"volume={item[14]} "
                        f"volume_effect={item[16]}"
                    )


if __name__ == "__main__":
    main()
