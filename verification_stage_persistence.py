from __future__ import annotations

"""
Product-grade verification tool for stage persistence.

This script replays autonomous OI snapshots over historical aggregate cycles and
measures sustained stage-2 / stage-3 streaks for tracked symbols.
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


def build_updates(cycles):
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


def finalize_streak(
    streaks: list[tuple[int, str, str]],
    current_len: int,
    start_ts: str | None,
    end_ts: str | None,
) -> None:
    if current_len <= 0 or start_ts is None or end_ts is None:
        return
    streaks.append((current_len, start_ts, end_ts))


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify sustained stage-2/stage-3 persistence over replay cycles")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit-cycles", type=int, default=60)
    parser.add_argument("--to-ts")
    parser.add_argument("--symbols", nargs="+", required=True)
    args = parser.parse_args()

    cycles = load_cycle_timestamps(args.hours, args.limit_cycles, to_ts=_parse_ts(args.to_ts))
    if not cycles:
        print("NO_CYCLES")
        return

    tracked = {symbol.upper() for symbol in args.symbols}
    latest_window_map = load_latest_window_map(cycles[0])
    updates_by_cycle = build_updates(cycles)
    state_map = {}
    per_symbol_rows: dict[tuple[str, str], list[tuple]] = defaultdict(list)

    for cycle_ts in cycles:
        for row in updates_by_cycle.get(cycle_ts, []):
            latest_window_map[(row["exchange"], row["symbol"])][row["window_code"]][row["metric"]] = row

        core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=cycle_ts,
            previous_state_map=state_map,
        )
        for row in core_rows:
            symbol = row[1]
            if symbol in tracked:
                exchange = row[0]
                per_symbol_rows[(exchange, symbol)].append((cycle_ts.isoformat(), row))

    found_keys = sorted(per_symbol_rows)
    missing_symbols = tracked - {symbol for _, symbol in found_keys}
    for symbol in sorted(missing_symbols):
        print(f"SYMBOL {symbol} missing")

    for exchange, symbol in found_keys:
        rows = per_symbol_rows[(exchange, symbol)]
        if not rows:
            continue

        stage2_streaks: list[tuple[int, str, str]] = []
        stage3_streaks: list[tuple[int, str, str]] = []
        current_stage2_len = 0
        current_stage2_start = None
        current_stage3_len = 0
        current_stage3_start = None
        first_stage2 = None
        first_stage3 = None
        last_row = rows[-1][1]

        prev_cycle_ts = None
        for cycle_ts, row in rows:
            stage = int(row[2] or 0)

            if stage == 2:
                if current_stage2_len == 0:
                    current_stage2_start = cycle_ts
                current_stage2_len += 1
                if first_stage2 is None:
                    first_stage2 = cycle_ts
            else:
                finalize_streak(stage2_streaks, current_stage2_len, current_stage2_start, prev_cycle_ts if current_stage2_len else None)
                current_stage2_len = 0
                current_stage2_start = None

            if stage == 3:
                if current_stage3_len == 0:
                    current_stage3_start = cycle_ts
                current_stage3_len += 1
                if first_stage3 is None:
                    first_stage3 = cycle_ts
            else:
                finalize_streak(stage3_streaks, current_stage3_len, current_stage3_start, prev_cycle_ts if current_stage3_len else None)
                current_stage3_len = 0
                current_stage3_start = None

            prev_cycle_ts = cycle_ts

        if current_stage2_len:
            finalize_streak(stage2_streaks, current_stage2_len, current_stage2_start, rows[-1][0])
        if current_stage3_len:
            finalize_streak(stage3_streaks, current_stage3_len, current_stage3_start, rows[-1][0])

        max_stage2 = max((length for length, _, _ in stage2_streaks), default=0)
        max_stage3 = max((length for length, _, _ in stage3_streaks), default=0)

        print(
            f"SYMBOL {exchange}:{symbol} "
            f"first_stage2={first_stage2 or '-'} "
            f"first_stage3={first_stage3 or '-'} "
            f"max_stage2_streak={max_stage2} "
            f"max_stage3_streak={max_stage3} "
            f"last_stage={last_row[2]} "
            f"last_pattern={last_row[4]} "
            f"last_price={last_row[16]} "
            f"last_volume={last_row[18]}"
        )

        for length, start_ts, end_ts in stage2_streaks:
            print(f"  stage2_streak len={length} start={start_ts} end={end_ts}")
        for length, start_ts, end_ts in stage3_streaks:
            print(f"  stage3_streak len={length} start={start_ts} end={end_ts}")


if __name__ == "__main__":
    main()
