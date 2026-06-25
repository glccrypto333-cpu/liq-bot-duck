from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from db import active_universe_sql, fetch
from autonomous_oi_service import (
    WINDOWS,
    attach_oi_trajectory_points,
    compute_autonomous_oi_snapshot,
    compute_autonomous_oi_snapshot_from_latest_window_map,
)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _query_cycle_timestamps(hours: int, limit: int | None, upper_bound: datetime, table_name: str) -> list[datetime]:
    lower_bound = upper_bound - timedelta(hours=int(hours))
    rows = fetch(
        f"""
        SELECT source_cycle_ts
        FROM {table_name}
        WHERE source_cycle_ts IS NOT NULL
          AND source_cycle_ts <= %s
          AND source_cycle_ts >= %s
        GROUP BY source_cycle_ts
        ORDER BY source_cycle_ts ASC
        """,
        (upper_bound, lower_bound),
    )
    cycle_ts = [row["source_cycle_ts"] for row in rows]
    if limit is not None and limit > 0:
        cycle_ts = cycle_ts[-limit:]
    return cycle_ts


def load_cycle_timestamps(
    hours: int,
    limit: int | None,
    to_ts: datetime | None = None,
    window_source: str = "auto",
) -> tuple[list[datetime], str]:
    upper_bound = to_ts or datetime.now(timezone.utc)
    if window_source == "hot":
        return _query_cycle_timestamps(hours, limit, upper_bound, "aggregate_windows"), "hot"
    if window_source == "history":
        return _query_cycle_timestamps(hours, limit, upper_bound, "aggregate_windows_history"), "history"

    hot_cycles = _query_cycle_timestamps(hours, limit, upper_bound, "aggregate_windows")
    if hot_cycles:
        return hot_cycles, "hot"
    return _query_cycle_timestamps(hours, limit, upper_bound, "aggregate_windows_history"), "history"


def _symbol_filter_sql(tracked_pairs: Iterable[tuple[str, str]] | None) -> tuple[str, tuple]:
    tracked = list(tracked_pairs or [])
    if not tracked:
        return "", ()
    clauses = []
    params: list[str] = []
    for exchange, symbol in tracked:
        clauses.append("(exchange = %s AND symbol = %s)")
        params.extend([exchange, symbol])
    return "AND (" + " OR ".join(clauses) + ")", tuple(params)


def load_latest_window_map_for_replay(
    cycle_ts: datetime | None = None,
    window_source: str = "hot",
    tracked_pairs: Iterable[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], dict[str, dict[str, dict]]]:
    table_name = "aggregate_windows_history" if window_source == "history" else "aggregate_windows"
    params: tuple = ()
    cycle_filter = ""
    if cycle_ts is not None:
        cycle_filter = "AND (source_cycle_ts <= %s OR source_cycle_ts IS NULL)"
        params = (cycle_ts,)
    symbol_filter_sql, symbol_filter_params = _symbol_filter_sql(tracked_pairs)

    rows = fetch(
        f"""
        WITH ranked AS (
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
                built_at,
                ROW_NUMBER() OVER (
                    PARTITION BY metric, window_code, exchange, symbol
                    ORDER BY ts_close DESC
                ) AS rn
            FROM {table_name}
            WHERE metric IN ('OI', 'PRICE', 'VOLUME')
              AND window_code = ANY(%s)
              AND {active_universe_sql()}
              {symbol_filter_sql}
              {cycle_filter}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        """,
        (WINDOWS, *symbol_filter_params, *params),
    )

    window_map: dict[tuple[str, str], dict[str, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        key = (row["exchange"], row["symbol"])
        window_map[key][row["window_code"]][row["metric"]] = row
    attach_oi_trajectory_points(window_map)
    return window_map


def load_batch_window_updates(
    cycle_timestamps: list[datetime],
    window_source: str = "hot",
    tracked_pairs: Iterable[tuple[str, str]] | None = None,
) -> dict[datetime, list[dict]]:
    if not cycle_timestamps:
        return {}

    window_start = cycle_timestamps[0]
    window_end = cycle_timestamps[-1]
    table_name = "aggregate_windows_history" if window_source == "history" else "aggregate_windows"
    symbol_filter_sql, symbol_filter_params = _symbol_filter_sql(tracked_pairs)
    rows = fetch(
        f"""
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
        FROM {table_name}
        WHERE metric IN ('OI', 'PRICE', 'VOLUME')
          AND window_code = ANY(%s)
          AND source_cycle_ts IS NOT NULL
          {symbol_filter_sql}
          AND source_cycle_ts > %s
          AND source_cycle_ts <= %s
        ORDER BY source_cycle_ts ASC, metric, window_code, exchange, symbol
        """,
        (WINDOWS, *symbol_filter_params, window_start, window_end),
    )

    by_cycle_ts: dict[datetime, list[dict]] = defaultdict(list)
    for row in rows:
        by_cycle_ts[row["source_cycle_ts"]].append(row)
    return by_cycle_ts


def replay_batch_snapshots(
    cycle_timestamps: list[datetime],
    window_source: str = "hot",
) -> list[tuple[datetime, Counter, Counter]]:
    initial_cycle_ts = cycle_timestamps[0]
    updates_by_cycle = load_batch_window_updates(cycle_timestamps, window_source=window_source)
    baseline_window_map = load_latest_window_map_for_replay(initial_cycle_ts, window_source=window_source)
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
    parser.add_argument("--window-source", choices=["auto", "hot", "history"], default="auto", help="Which aggregate contour to use for replay windows")
    args = parser.parse_args()

    limit = args.limit_cycles or None
    cycle_timestamps, resolved_window_source = load_cycle_timestamps(
        args.hours,
        limit,
        to_ts=_parse_ts(args.to_ts),
        window_source=args.window_source,
    )
    if not cycle_timestamps:
        print("REPLAY_EMPTY no cycle timestamps found")
        return

    if args.mode == "batch":
        cycle_summaries = replay_batch_snapshots(cycle_timestamps, window_source=resolved_window_source)
    else:
        if resolved_window_source != "hot":
            raise SystemExit("SQL mode supports only hot aggregate_windows; use --mode batch for history replay")
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
        f"REPLAY_OK mode={args.mode} window_source={resolved_window_source} cycles={len(cycle_summaries)} "
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
