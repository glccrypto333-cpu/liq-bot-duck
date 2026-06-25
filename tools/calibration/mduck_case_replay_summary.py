from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path("/home/alexey/openclaw/apps/liq-bot-duck")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_replay import _parse_ts, load_cycle_timestamps
from autonomous_oi_service import compute_autonomous_oi_snapshot_from_latest_window_map, load_latest_window_map
from autonomous_oi_service import WINDOWS
from db import fetch


def load_updates_by_cycle(cycles):
    if not cycles:
        return {}
    window_list = ", ".join("'" + w + "'" for w in WINDOWS)
    sql = f"""
        SELECT metric, window_code, ts_open, ts_close, exchange, symbol,
               open_value, high_value, low_value, close_value,
               sum_value, avg_value, delta_pct, unique_candles,
               source_cycle_ts, built_at
        FROM aggregate_windows
        WHERE metric IN ('OI', 'PRICE', 'VOLUME')
          AND window_code IN ({window_list})
          AND source_cycle_ts IS NOT NULL
          AND source_cycle_ts > %s
          AND source_cycle_ts <= %s
        ORDER BY source_cycle_ts ASC, metric, window_code, exchange, symbol
    """
    rows = fetch(sql, (cycles[0], cycles[-1]))
    updates_by_cycle = {}
    for row in rows:
        updates_by_cycle.setdefault(row["source_cycle_ts"], []).append(row)
    return updates_by_cycle


def fmt(value):
    if value is None:
        return "-"
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--limit-cycles", type=int, default=240)
    parser.add_argument("--to-ts", required=True)
    parser.add_argument("--cases", nargs="+", required=True, help="EXCHANGE:SYMBOL")
    args = parser.parse_args()

    cases = []
    for item in args.cases:
        exchange, symbol = item.split(":", 1)
        cases.append((exchange.upper(), symbol.upper()))

    tracked = set(cases)
    cycles = load_cycle_timestamps(args.hours, args.limit_cycles, to_ts=_parse_ts(args.to_ts))
    if isinstance(cycles, tuple):
        cycles = cycles[0]
    if not cycles:
        print("NO_CYCLES")
        return

    updates_by_cycle = load_updates_by_cycle(cycles)
    latest_window_map = load_latest_window_map(cycles[0])
    state_map = {}
    results = {
        case: {"first2": None, "first3": None, "last_stage": None, "last_reason": None, "last_cycle": None}
        for case in cases
    }

    for cycle_ts in cycles:
        for row in updates_by_cycle.get(cycle_ts, []):
            key = (row["exchange"], row["symbol"])
            latest_window_map.setdefault(key, {})
            latest_window_map[key].setdefault(row["window_code"], {})
            latest_window_map[key][row["window_code"]][row["metric"]] = row

        core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=cycle_ts,
            previous_state_map=state_map,
        )

        for row in core_rows:
            key = (row[0], row[1])
            if key not in tracked:
                continue
            stage = int(row[2] or 0)
            rec = results[key]
            rec["last_stage"] = stage
            rec["last_reason"] = row[25]
            rec["last_cycle"] = cycle_ts
            if stage >= 2 and rec["first2"] is None:
                rec["first2"] = cycle_ts
            if stage >= 3 and rec["first3"] is None:
                rec["first3"] = cycle_ts

    print("EXCHANGE\tSYMBOL\tFIRST2\tFIRST3\tLAST_STAGE\tLAST_CYCLE\tLAST_REASON")
    for exchange, symbol in cases:
        rec = results[(exchange, symbol)]
        print(
            f"{exchange}\t{symbol}\t{fmt(rec['first2'])}\t{fmt(rec['first3'])}\t"
            f"{fmt(rec['last_stage'])}\t{fmt(rec['last_cycle'])}\t{fmt(rec['last_reason'])}"
        )


if __name__ == "__main__":
    main()
