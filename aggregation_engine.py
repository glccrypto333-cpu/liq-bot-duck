from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import gc
import os

from db import (
    fetch,
    replace_aggregate_layers_atomically,
    upsert_aggregate_hot_rows,
    active_universe_sql,
    upsert_aggregate_history_rows,
    prune_aggregate_history,
    history_retention_hours,
)
from metrics import изменение_в_процентах
from logger import log

WINDOWS = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48, "12ч": 144, "24ч": 288}
WINDOW_MINUTES = {"15м": 15, "30м": 30, "1ч": 60, "4ч": 240, "12ч": 720, "24ч": 1440}
HISTORY_WINDOWS = ("15м", "30м", "1ч", "4ч")
FIVE_MINUTES = timedelta(minutes=5)


def _groups(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["exchange"], row["symbol"])].append(row)

    for key in grouped:
        grouped[key].sort(key=lambda item: item["ts_open"])

    return grouped


def _window_close(ts_open, timeframe: str):
    return ts_open + timedelta(minutes=WINDOW_MINUTES[timeframe])


def _is_contiguous_5m(chunk) -> bool:
    if not chunk:
        return False

    for prev, current in zip(chunk, chunk[1:]):
        if current["ts_open"] - prev["ts_open"] != FIVE_MINUTES:
            return False

    return True


def _select_window_chunk(items, anchor_ts: datetime, timeframe: str, need: int):
    """Return a strict 5m chunk, or tolerate one missing 5m candle on senior windows."""
    strict_chunk = [item for item in items if item["ts_close"] <= anchor_ts][-need:]
    if len(strict_chunk) == need and _is_contiguous_5m(strict_chunk):
        return strict_chunk

    if need < 12:
        return None

    window_start = anchor_ts - timedelta(minutes=WINDOW_MINUTES[timeframe])
    window_items = [
        item
        for item in items
        if item["ts_close"] > window_start and item["ts_close"] <= anchor_ts
    ]
    if len(window_items) < need - 1:
        return None

    missing_slots = 0
    for prev, current in zip(window_items, window_items[1:]):
        delta_slots = int((current["ts_open"] - prev["ts_open"]) / FIVE_MINUTES)
        if delta_slots < 1:
            return None
        missing_slots += max(0, delta_slots - 1)
        if missing_slots > 1:
            return None

    return window_items


def _previous_contiguous_item(items: list[dict], first_item: dict) -> dict | None:
    expected_close = first_item["ts_open"]
    for item in reversed(items):
        if item["ts_close"] == expected_close:
            return item
        if item["ts_close"] < expected_close:
            return None
    return None


def _oi_window_values(chunk: list[dict], previous_item: dict | None) -> tuple[float, float, float, float, float | None, list[float]]:
    # OI raw is point-based, so the real window open is the previous closed point.
    open_value = previous_item["oi_close"] if previous_item is not None else chunk[0]["oi_open"]
    close_value = chunk[-1]["oi_close"]
    trajectory = ([open_value] if previous_item is not None else []) + [x["oi_close"] for x in chunk]
    return (
        open_value,
        max([open_value, *[x["oi_high"] for x in chunk]]),
        min([open_value, *[x["oi_low"] for x in chunk]]),
        close_value,
        изменение_в_процентах(open_value, close_value),
        trajectory,
    )


def _window_items(selected_windows: tuple[str, ...] | None):
    window_names = selected_windows or tuple(WINDOWS.keys())
    return [(timeframe, WINDOWS[timeframe]) for timeframe in window_names]


def _active_clause(alias: str, active_only: bool) -> str:
    if not active_only:
        return ""
    return f"\n          AND {active_universe_sql(alias, include_data_quality_quarantine=False)}"


def _tracked_pairs_clause(
    alias: str,
    tracked_pairs: Iterable[tuple[str, str]] | None,
) -> tuple[str, tuple]:
    tracked = list(tracked_pairs or [])
    if not tracked:
        return "", ()
    placeholders = ",".join(["(%s,%s)"] * len(tracked))
    sql = f"\n          AND ({alias}.exchange, {alias}.symbol) IN ({placeholders})"
    params: list[str] = []
    for exchange, symbol in tracked:
        params.extend([exchange, symbol])
    return sql, tuple(params)


def _fetch_metric_rows(
    table: str,
    select_sql: str,
    alias: str,
    active_only: bool,
    window_hours: int | None = None,
    start_ts=None,
    end_ts=None,
    tracked_pairs: Iterable[tuple[str, str]] | None = None,
):
    active_clause = _active_clause(alias, active_only)
    tracked_clause, tracked_params = _tracked_pairs_clause(alias, tracked_pairs)
    if window_hours is not None:
        return fetch(f"""
            SELECT {select_sql}
            FROM {table} {alias}
            WHERE ts_close <= NOW() - interval '30 seconds'
              AND ts_close >= NOW() - (%s || ' hours')::interval{active_clause}{tracked_clause}
            ORDER BY exchange, symbol, ts_open
        """, (window_hours, *tracked_params))

    if start_ts is None or end_ts is None:
        raise RuntimeError("_fetch_metric_rows requires window_hours or start_ts/end_ts")

    return fetch(f"""
        SELECT {select_sql}
        FROM {table} {alias}
        WHERE ts_close >= %s
          AND ts_close < %s{active_clause}{tracked_clause}
        ORDER BY exchange, symbol, ts_open
    """, (start_ts, end_ts, *tracked_params))


def _fetch_recent_metric_rows(
    table: str,
    select_sql: str,
    alias: str,
    active_only: bool,
    source_cycle_ts: datetime,
):
    active_clause = _active_clause(alias, active_only)
    max_window = max(WINDOWS.values())
    lookback = source_cycle_ts - FIVE_MINUTES * max_window
    return fetch(f"""
        SELECT {select_sql}
        FROM {table} {alias}
        WHERE ts_close > %s
          AND ts_close <= %s{active_clause}
        ORDER BY exchange, symbol, ts_open
    """, (lookback, source_cycle_ts))


def _fetch_latest_ts_map(
    table: str,
    alias: str,
    active_only: bool,
    source_cycle_ts: datetime,
) -> dict[tuple[str, str], datetime]:
    active_clause = _active_clause(alias, active_only)
    rows = fetch(f"""
        SELECT exchange, symbol, max(ts_close) AS latest_ts
        FROM {table} {alias}
        WHERE ts_close <= %s{active_clause}
        GROUP BY exchange, symbol
    """, (source_cycle_ts,))
    return {
        (row["exchange"], row["symbol"]): row["latest_ts"]
        for row in rows
        if row.get("latest_ts") is not None
    }


def _resolve_symbol_anchor_map_from_db(
    metric_set: set[str],
    active_only: bool,
    source_cycle_ts: datetime,
) -> dict[tuple[str, str], datetime]:
    metric_tables = {
        "OI": ("oi_raw", "oi"),
        "PRICE": ("price_raw", "price"),
        "VOLUME": ("volume_raw", "volume"),
    }
    latest_by_metric = {
        metric: _fetch_latest_ts_map(table, alias, active_only, source_cycle_ts)
        for metric, (table, alias) in metric_tables.items()
        if metric in metric_set
    }
    if not latest_by_metric:
        return {}

    common_keys: set[tuple[str, str]] | None = None
    for latest_map in latest_by_metric.values():
        keys = set(latest_map.keys())
        common_keys = keys if common_keys is None else common_keys & keys
    if not common_keys:
        return {}

    return {
        key: min(latest_by_metric[metric][key] for metric in latest_by_metric)
        for key in common_keys
    }


def _resolve_symbol_anchor_map(
    grouped_by_metric: dict[str, dict[tuple[str, str], list[dict]]],
    metric_set: set[str],
) -> dict[tuple[str, str], datetime]:
    anchors: dict[tuple[str, str], datetime] = {}
    symbol_keys: set[tuple[str, str]] = set()
    for grouped in grouped_by_metric.values():
        symbol_keys.update(grouped.keys())

    for key in symbol_keys:
        latest_closes: list[datetime] = []
        for metric_name in metric_set:
            metric_rows = grouped_by_metric.get(metric_name, {}).get(key, [])
            if not metric_rows:
                latest_closes = []
                break
            latest_closes.append(metric_rows[-1]["ts_close"])
        if latest_closes:
            anchors[key] = min(latest_closes)
    return anchors


def build_aggregate_rows(
    window_hours: int | None = None,
    selected_windows: tuple[str, ...] | None = None,
    selected_metrics: tuple[str, ...] | None = None,
    active_only: bool = True,
    start_ts=None,
    end_ts=None,
    tracked_pairs: Iterable[tuple[str, str]] | None = None,
) -> tuple[list[tuple], dict]:
    rows_out: list[tuple] = []
    skipped_non_contiguous = 0
    window_items = _window_items(selected_windows)
    metric_set = set(selected_metrics or ("OI", "PRICE", "VOLUME"))

    oi_rows = []
    if "OI" in metric_set:
        oi_rows = _fetch_metric_rows(
            "oi_raw",
            "ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close",
            "x",
            active_only,
            window_hours=window_hours,
            start_ts=start_ts,
            end_ts=end_ts,
            tracked_pairs=tracked_pairs,
        )

        for (exchange, symbol), items in _groups(oi_rows).items():
            for timeframe, need in window_items:
                if len(items) < need:
                    continue
                for i in range(need - 1, len(items)):
                    chunk = items[i - need + 1:i + 1]
                    if not _is_contiguous_5m(chunk):
                        skipped_non_contiguous += 1
                        continue
                    oi_open, oi_high, oi_low, oi_close, delta_pct, trajectory = _oi_window_values(
                        chunk,
                        items[i - need] if i - need >= 0 and items[i - need]["ts_close"] == chunk[0]["ts_open"] else None,
                    )
                    rows_out.append((
                        "OI", timeframe,
                        chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                        exchange, symbol,
                        oi_open,
                        oi_high,
                        oi_low,
                        oi_close,
                        None, None,
                        delta_pct,
                        len(chunk),
                        trajectory,
                    ))

    price_rows = []
    if "PRICE" in metric_set:
        price_rows = _fetch_metric_rows(
            "price_raw",
            "ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close",
            "x",
            active_only,
            window_hours=window_hours,
            start_ts=start_ts,
            end_ts=end_ts,
            tracked_pairs=tracked_pairs,
        )

        for (exchange, symbol), items in _groups(price_rows).items():
            for timeframe, need in window_items:
                if len(items) < need:
                    continue
                for i in range(need - 1, len(items)):
                    chunk = items[i - need + 1:i + 1]
                    if not _is_contiguous_5m(chunk):
                        skipped_non_contiguous += 1
                        continue
                    rows_out.append((
                        "PRICE", timeframe,
                        chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                        exchange, symbol,
                        chunk[0]["price_open"],
                        max(x["price_high"] for x in chunk),
                        min(x["price_low"] for x in chunk),
                        chunk[-1]["price_close"],
                        None, None,
                        изменение_в_процентах(chunk[0]["price_open"], chunk[-1]["price_close"]),
                        len(chunk),
                        None,
                    ))

    volume_rows = []
    if "VOLUME" in metric_set:
        volume_rows = _fetch_metric_rows(
            "volume_raw",
            "ts_open, ts_close, exchange, symbol, volume",
            "x",
            active_only,
            window_hours=window_hours,
            start_ts=start_ts,
            end_ts=end_ts,
            tracked_pairs=tracked_pairs,
        )

        for (exchange, symbol), items in _groups(volume_rows).items():
            for timeframe, need in window_items:
                if len(items) < need:
                    continue
                for i in range(need - 1, len(items)):
                    chunk = items[i - need + 1:i + 1]
                    if not _is_contiguous_5m(chunk):
                        skipped_non_contiguous += 1
                        continue
                    values = [x["volume"] for x in chunk]
                    rows_out.append((
                        "VOLUME", timeframe,
                        chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                        exchange, symbol,
                        None,
                        max(values),
                        min(values),
                        None,
                        sum(values),
                        sum(values) / len(values),
                        изменение_в_процентах(chunk[0]["volume"], chunk[-1]["volume"]),
                        len(chunk),
                        None,
                    ))

    stats = {
        "raw_oi": len(oi_rows),
        "raw_price": len(price_rows),
        "raw_volume": len(volume_rows),
        "aggregates": len(rows_out),
        "skipped_non_contiguous": skipped_non_contiguous,
        "window_hours": window_hours,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "selected_metrics": ",".join(sorted(metric_set)),
        "selected_windows": ",".join(selected_windows or tuple(WINDOWS.keys())),
        "active_only": active_only,
    }
    return rows_out, stats


def build_latest_aggregate_rows(
    source_cycle_ts: datetime,
    selected_windows: tuple[str, ...] | None = None,
    selected_metrics: tuple[str, ...] | None = None,
    active_only: bool = True,
) -> tuple[list[tuple], dict]:
    rows_out: list[tuple] = []
    skipped_non_contiguous = 0
    window_items = _window_items(selected_windows)
    metric_set = set(selected_metrics or ("OI", "PRICE", "VOLUME"))
    symbol_anchor_map = _resolve_symbol_anchor_map_from_db(metric_set, active_only, source_cycle_ts)
    raw_oi_count = 0
    raw_price_count = 0
    raw_volume_count = 0

    oi_rows = []
    if "OI" in metric_set:
        oi_rows = _fetch_recent_metric_rows(
            "oi_raw",
            "ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close",
            "x",
            active_only,
            source_cycle_ts,
        )
        raw_oi_count = len(oi_rows)
        for (exchange, symbol), items in _groups(oi_rows).items():
            anchor_ts = symbol_anchor_map.get((exchange, symbol))
            if anchor_ts is None:
                continue
            anchored_items = [item for item in items if item["ts_close"] <= anchor_ts]
            if not anchored_items or anchored_items[-1]["ts_close"] != anchor_ts:
                continue
            for timeframe, need in window_items:
                if len(anchored_items) < need - 1:
                    continue
                chunk = _select_window_chunk(anchored_items, anchor_ts, timeframe, need)
                if chunk is None:
                    skipped_non_contiguous += 1
                    continue
                oi_open, oi_high, oi_low, oi_close, delta_pct, trajectory = _oi_window_values(
                    chunk,
                    _previous_contiguous_item(anchored_items, chunk[0]),
                )
                rows_out.append((
                    "OI", timeframe,
                    chunk[0]["ts_open"], chunk[-1]["ts_close"],
                    exchange, symbol,
                    oi_open,
                    oi_high,
                    oi_low,
                    oi_close,
                    None, None,
                    delta_pct,
                    len(chunk),
                    trajectory,
                ))
        del oi_rows
        gc.collect()

    price_rows = []
    if "PRICE" in metric_set:
        price_rows = _fetch_recent_metric_rows(
            "price_raw",
            "ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close",
            "x",
            active_only,
            source_cycle_ts,
        )
        raw_price_count = len(price_rows)
        for (exchange, symbol), items in _groups(price_rows).items():
            anchor_ts = symbol_anchor_map.get((exchange, symbol))
            if anchor_ts is None:
                continue
            anchored_items = [item for item in items if item["ts_close"] <= anchor_ts]
            if not anchored_items or anchored_items[-1]["ts_close"] != anchor_ts:
                continue
            for timeframe, need in window_items:
                if len(anchored_items) < need - 1:
                    continue
                chunk = _select_window_chunk(anchored_items, anchor_ts, timeframe, need)
                if chunk is None:
                    skipped_non_contiguous += 1
                    continue
                rows_out.append((
                    "PRICE", timeframe,
                    chunk[0]["ts_open"], chunk[-1]["ts_close"],
                    exchange, symbol,
                    chunk[0]["price_open"],
                    max(x["price_high"] for x in chunk),
                    min(x["price_low"] for x in chunk),
                    chunk[-1]["price_close"],
                    None, None,
                    изменение_в_процентах(chunk[0]["price_open"], chunk[-1]["price_close"]),
                    len(chunk),
                    None,
                ))
        del price_rows
        gc.collect()

    volume_rows = []
    if "VOLUME" in metric_set:
        volume_rows = _fetch_recent_metric_rows(
            "volume_raw",
            "ts_open, ts_close, exchange, symbol, volume",
            "x",
            active_only,
            source_cycle_ts,
        )
        raw_volume_count = len(volume_rows)
        for (exchange, symbol), items in _groups(volume_rows).items():
            anchor_ts = symbol_anchor_map.get((exchange, symbol))
            if anchor_ts is None:
                continue
            anchored_items = [item for item in items if item["ts_close"] <= anchor_ts]
            if not anchored_items or anchored_items[-1]["ts_close"] != anchor_ts:
                continue
            for timeframe, need in window_items:
                if len(anchored_items) < need - 1:
                    continue
                chunk = _select_window_chunk(anchored_items, anchor_ts, timeframe, need)
                if chunk is None:
                    skipped_non_contiguous += 1
                    continue
                values = [x["volume"] for x in chunk]
                rows_out.append((
                    "VOLUME", timeframe,
                    chunk[0]["ts_open"], chunk[-1]["ts_close"],
                    exchange, symbol,
                    None,
                    max(values),
                    min(values),
                    None,
                    sum(values),
                    sum(values) / len(values),
                    изменение_в_процентах(chunk[0]["volume"], chunk[-1]["volume"]),
                    len(chunk),
                    None,
                ))
        del volume_rows
        gc.collect()

    stats = {
        "raw_oi": raw_oi_count,
        "raw_price": raw_price_count,
        "raw_volume": raw_volume_count,
        "aggregates": len(rows_out),
        "skipped_non_contiguous": skipped_non_contiguous,
        "source_cycle_ts": source_cycle_ts,
        "symbol_anchors": len(symbol_anchor_map),
        "selected_metrics": ",".join(sorted(metric_set)),
        "selected_windows": ",".join(selected_windows or tuple(WINDOWS.keys())),
        "active_only": active_only,
    }
    return rows_out, stats


def sync_aggregate_history_from_rows(rows: list[tuple]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    sync_hours = int(os.getenv("AGGREGATE_HISTORY_SYNC_HOURS", "6"))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=sync_hours)
    history_rows = [
        row for row in rows
        if row[1] in HISTORY_WINDOWS and row[3] >= cutoff
    ]
    synced = upsert_aggregate_history_rows(history_rows)
    pruned = prune_aggregate_history()
    return synced, pruned


def rebuild_aggregate_windows() -> int:
    window_hours = int(os.getenv("AGGREGATES_WINDOW_HOURS", "30"))
    rows_out, stats = build_aggregate_rows(window_hours, active_only=True)

    if not rows_out:
        raise RuntimeError("aggregates rebuild failed: empty rows_out")

    replace_aggregate_layers_atomically(rows_out)
    history_synced, history_pruned = sync_aggregate_history_from_rows(rows_out)

    log(
        f"aggregates rebuilt: raw_oi={stats['raw_oi']} "
        f"raw_price={stats['raw_price']} raw_volume={stats['raw_volume']} "
        f"aggregates={stats['aggregates']} "
        f"skipped_non_contiguous={stats['skipped_non_contiguous']} "
        f"history_synced={history_synced} history_pruned={history_pruned}"
    )
    return len(rows_out)


def rebuild_latest_aggregate_windows(source_cycle_ts: datetime) -> int:
    rows_out, stats = build_latest_aggregate_rows(source_cycle_ts, active_only=True)
    if not rows_out:
        raise RuntimeError(
            "aggregates latest rebuild failed: empty rows_out "
            f"source_cycle_ts={source_cycle_ts.isoformat()}"
        )

    upsert_aggregate_hot_rows(rows_out)
    history_synced, history_pruned = sync_aggregate_history_from_rows(rows_out)

    log(
        f"aggregates latest rebuilt: source_cycle_ts={source_cycle_ts.isoformat()} "
        f"raw_oi={stats['raw_oi']} raw_price={stats['raw_price']} raw_volume={stats['raw_volume']} "
        f"aggregates={stats['aggregates']} "
        f"symbol_anchors={stats['symbol_anchors']} "
        f"skipped_non_contiguous={stats['skipped_non_contiguous']} "
        f"history_synced={history_synced} history_pruned={history_pruned}"
    )
    return len(rows_out)


def backfill_aggregate_history(window_hours: int | None = None, active_only: bool | None = None) -> dict:
    history_hours = int(window_hours or os.getenv("AGGREGATE_HISTORY_BACKFILL_HOURS", str(history_retention_hours())))
    chunk_hours = int(os.getenv("AGGREGATE_HISTORY_BACKFILL_CHUNK_HOURS", "24"))
    if active_only is None:
        active_only = os.getenv("AGGREGATE_HISTORY_BACKFILL_ACTIVE_ONLY", "0") == "1"

    end_ts = datetime.now(timezone.utc) - timedelta(seconds=30)
    history_start = end_ts - timedelta(hours=history_hours)
    context_hours = max(WINDOW_MINUTES[name] for name in HISTORY_WINDOWS) // 60

    total_rows = 0
    total_synced = 0
    chunks = 0
    cursor = history_start
    metrics = ("OI", "PRICE", "VOLUME")
    while cursor < end_ts:
        next_cursor = min(cursor + timedelta(hours=chunk_hours), end_ts)
        fetch_start = cursor - timedelta(hours=context_hours)
        for metric_name in metrics:
            rows_out, stats = build_aggregate_rows(
                selected_windows=HISTORY_WINDOWS,
                selected_metrics=(metric_name,),
                active_only=active_only,
                start_ts=fetch_start,
                end_ts=next_cursor,
            )
            chunk_rows = [row for row in rows_out if cursor <= row[3] < next_cursor]
            total_rows += len(chunk_rows)
            total_synced += upsert_aggregate_history_rows(chunk_rows)
        chunks += 1
        cursor = next_cursor

    pruned = prune_aggregate_history()

    result = {
        "history_hours": history_hours,
        "chunk_hours": chunk_hours,
        "chunks": chunks,
        "active_only": active_only,
        "history_rows": total_rows,
        "history_synced": total_synced,
        "history_pruned": pruned,
        "selected_windows": list(HISTORY_WINDOWS),
    }
    log(
        "aggregate history backfill: "
        f"hours={history_hours} chunk_hours={chunk_hours} active_only={active_only} "
        f"history_rows={total_rows} synced={total_synced} pruned={pruned} chunks={chunks}"
    )
    return result
