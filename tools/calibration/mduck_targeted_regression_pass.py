from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path("/home/alexey/openclaw/apps/liq-bot-duck")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_replay import _parse_ts, load_cycle_timestamps
from autonomous_oi_service import WINDOWS, compute_autonomous_oi_snapshot_from_latest_window_map, load_latest_window_map
from db import fetch


CASES = [
    {
        "exchange": "BYBIT",
        "symbol": "HIGHUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "positive-anchor",
    },
    {
        "exchange": "BINANCE",
        "symbol": "AGTUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "improved-but-risky anchor",
    },
    {
        "exchange": "BYBIT",
        "symbol": "BIOUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "valid watch-case",
    },
    {
        "exchange": "BYBIT",
        "symbol": "CUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "timing-case",
    },
    {
        "exchange": "BYBIT",
        "symbol": "BRETTUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "positive-anchor",
    },
    {
        "exchange": "BINANCE",
        "symbol": "PRLUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "card-only",
    },
    {
        "exchange": "BYBIT",
        "symbol": "MITOUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": True,
        "note": "likely fixed by current canon",
    },
    {
        "exchange": "BYBIT",
        "symbol": "TACUSDT",
        "to_ts": "2026-06-17T21:30:00+02:00",
        "hours": 36,
        "expect_stage3": False,
        "note": "likely fixed by current canon; expect at least stage2",
    },
]


def _load_updates_by_cycle(cycle_list):
    if not cycle_list:
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
    rows = fetch(sql, (cycle_list[0], cycle_list[-1]))
    by_cycle = {}
    for row in rows:
        by_cycle.setdefault(row["source_cycle_ts"], []).append(row)
    return by_cycle


def _run_case(case):
    cycles = load_cycle_timestamps(case["hours"], 240, to_ts=_parse_ts(case["to_ts"]))
    if isinstance(cycles, tuple):
        cycles = cycles[0]
    if not cycles:
        return {"error": "NO_CYCLES"}

    updates_by_cycle = _load_updates_by_cycle(cycles)
    latest_window_map = load_latest_window_map(cycles[0])
    state_map = {}
    first2 = None
    first3 = None
    last_stage = None
    last_reason = None
    last_cycle = None

    tracked_key = (case["exchange"], case["symbol"])

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
            if key != tracked_key:
                continue
            stage = int(row[2] or 0)
            last_stage = stage
            last_reason = row[25]
            last_cycle = cycle_ts
            if stage >= 2 and first2 is None:
                first2 = cycle_ts
            if stage >= 3 and first3 is None:
                first3 = cycle_ts

    return {
        "first2": first2,
        "first3": first3,
        "last_stage": last_stage,
        "last_reason": last_reason,
        "last_cycle": last_cycle,
    }


def _fmt(value):
    if value is None:
        return "-"
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def main():
    print("EXCHANGE\tSYMBOL\tEXPECT_F3\tFIRST2\tFIRST3\tLAST_STAGE\tLAST_CYCLE\tLAST_REASON\tNOTE")
    for case in CASES:
        result = _run_case(case)
        print(
            f"{case['exchange']}\t{case['symbol']}\t{case['expect_stage3']}\t"
            f"{_fmt(result.get('first2'))}\t{_fmt(result.get('first3'))}\t"
            f"{_fmt(result.get('last_stage'))}\t{_fmt(result.get('last_cycle'))}\t"
            f"{_fmt(result.get('last_reason'))}\t{case['note']}"
        )


if __name__ == "__main__":
    main()
