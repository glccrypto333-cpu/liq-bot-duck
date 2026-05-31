from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone

from db import fetch
from autonomous_oi_service import (
    WINDOWS,
    compute_autonomous_oi_snapshot,
    compute_autonomous_oi_snapshot_from_latest_window_map,
    load_latest_window_map,
)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def load_cycle_timestamps(hours: int, limit: int | None, to_ts: datetime | None = None) -> list[datetime]:
    upper_bound = to_ts or datetime.now(timezone.utc)
    rows = fetch(
        """
        SELECT source_cycle_ts
        FROM aggregate_windows
        WHERE source_cycle_ts IS NOT NULL
          AND source_cycle_ts <= %s
          AND source_cycle_ts >= %s - (%s || ' hours')::interval
        GROUP BY source_cycle_ts
        ORDER BY source_cycle_ts ASC
        """,
        (upper_bound, upper_bound, hours),
    )
    cycle_ts = [row["source_cycle_ts"] for row in rows]
    if limit is not None and limit > 0:
        cycle_ts = cycle_ts[-limit:]
    return cycle_ts


def load_batch_window_updates(cycle_timestamps: list[datetime]) -> dict[datetime, list[dict]]:
    if not cycle_timestamps:
        return {}

    window_start = cycle_timestamps[0]
    window_end = cycle_timestamps[-1]
    rows = fetch(
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
        (WINDOWS, window_start, window_end),
    )

    by_cycle_ts: dict[datetime, list[dict]] = defaultdict(list)
    for row in rows:
        by_cycle_ts[row["source_cycle_ts"]].append(row)
    return by_cycle_ts


def replay_batch_snapshots(cycle_timestamps: list[datetime]) -> list[tuple[datetime, Counter, Counter]]:
    initial_cycle_ts = cycle_timestamps[0]
    updates_by_cycle = load_batch_window_updates(cycle_timestamps)
    baseline_window_map = load_latest_window_map(initial_cycle_ts)
    latest_window_map: dict[tuple[str, str], dict[str, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
    for symbol_key, window_map in baseline_window_map.items():
        for window_code, metric_rows in window_map.items():
            latest_window_map[symbol_key][window_code] = dict(metric_rows)
    state_map: dict[tuple[str, str], dict] = {}
    cycle_summaries: list[tuple[datetime, Counter, Counter]] = []

    for cycle_ts in cycle_timestamps:
        for row in updates_by_cycle.get(cycle_ts, []):
            symbol_key = (row["exchange"], row["symbol"])
            latest_window_map[symbol_key][row["window_code"]][row["metric"]] = row

        core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=cycle_ts,
            previous_state_map=state_map,
        )
        stage_counts = summarize_stage_counts(core_rows)
        pattern_counts = summarize_pattern_counts(core_rows)
        cycle_summaries.append((cycle_ts, stage_counts, pattern_counts))

    return cycle_summaries


def summarize_stage_counts(core_rows: list[tuple]) -> Counter:
    counter: Counter = Counter()
    for row in core_rows:
        stage = int(row[2] or 0)
        counter[stage] += 1
    return counter


def summarize_pattern_counts(core_rows: list[tuple]) -> Counter:
    counter: Counter = Counter()
    for row in core_rows:
        pattern = row[4] or "unknown"
        counter[pattern] += 1
    return counter


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay autonomous OI stages over historical cycle timestamps")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours for aggregate_windows.source_cycle_ts")
    parser.add_argument("--limit-cycles", type=int, default=60, help="Limit to the latest N cycle timestamps after lookback filtering; 60 keeps enough warmup for stage-2/stage-3 replay semantics")
    parser.add_argument("--show-cycles", type=int, default=8, help="How many cycle summaries to print")
    parser.add_argument("--mode", choices=["batch", "sql"], default="batch", help="Replay mode: batch preload or per-cycle SQL fallback")
    parser.add_argument("--to-ts", help="Replay upper bound in ISO format instead of current NOW()")
    args = parser.parse_args()

    limit = args.limit_cycles or None
    cycle_timestamps = load_cycle_timestamps(args.hours, limit, to_ts=_parse_ts(args.to_ts))
    if not cycle_timestamps:
        print("REPLAY_EMPTY no cycle timestamps found")
        return

    if args.mode == "batch":
        cycle_summaries = replay_batch_snapshots(cycle_timestamps)
    else:
        state_map: dict[tuple[str, str], dict] = {}
        cycle_summaries = []
        for cycle_ts in cycle_timestamps:
            core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot(
                cycle_ts=cycle_ts,
                previous_state_map=state_map,
            )
            stage_counts = summarize_stage_counts(core_rows)
            pattern_counts = summarize_pattern_counts(core_rows)
            cycle_summaries.append((cycle_ts, stage_counts, pattern_counts))

    mature_stage2_seen = any(stage_counts.get(2, 0) > 0 for _, stage_counts, _ in cycle_summaries)
    mature_stage3_seen = any(stage_counts.get(3, 0) > 0 for _, stage_counts, _ in cycle_summaries)

    print(
        f"REPLAY_OK mode={args.mode} cycles={len(cycle_summaries)} "
        f"from={cycle_summaries[0][0].isoformat()} "
        f"to={cycle_summaries[-1][0].isoformat()}"
    )

    for cycle_ts, stage_counts, pattern_counts in cycle_summaries[-args.show_cycles:]:
        print(
            "CYCLE "
            f"ts={cycle_ts.isoformat()} "
            f"stage0={stage_counts.get(0, 0)} "
            f"stage1={stage_counts.get(1, 0)} "
            f"stage2={stage_counts.get(2, 0)} "
            f"stage3={stage_counts.get(3, 0)} "
            f"confirmed={pattern_counts.get('подтвержденный_набор', 0)} "
            f"developing={pattern_counts.get('развивающийся_набор', 0)}"
        )

    final_ts, final_stage_counts, final_pattern_counts = cycle_summaries[-1]
    print(
        "FINAL "
        f"ts={final_ts.isoformat()} "
        f"stage0={final_stage_counts.get(0, 0)} "
        f"stage1={final_stage_counts.get(1, 0)} "
        f"stage2={final_stage_counts.get(2, 0)} "
        f"stage3={final_stage_counts.get(3, 0)} "
        f"confirmed={final_pattern_counts.get('подтвержденный_набор', 0)} "
        f"developing={final_pattern_counts.get('развивающийся_набор', 0)}"
    )

    print(
        "MATURITY "
        f"stage2_seen={mature_stage2_seen} "
        f"stage3_seen={mature_stage3_seen}"
    )


if __name__ == "__main__":
    main()
