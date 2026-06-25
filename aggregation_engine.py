from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

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


def _window_items(selected_windows: tuple[str, ...] | None):
    window_names = selected_windows or tuple(WINDOWS.keys())
    return [(timeframe, WINDOWS[timeframe]) for timeframe in window_names]


def _active_clause(alias: str, active_only: bool) -> str:
    if not active_only:
        return ""
    return f"\n          AND {active_universe_sql(alias)}"


def _fetch_metric_rows(
    table: str,
    select_sql: str,
    alias: str,
    active_only: bool,
    window_hours: int | None = None,
    start_ts=None,
    end_ts=None,
):
    active_clause = _active_clause(alias, active_only)
    if window_hours is not None:
        return fetch(f"""
            SELECT {select_sql}
            FROM {table} {alias}
            WHERE ts_close <= NOW() - interval '30 seconds'
              AND ts_close >= NOW() - (%s || ' hours')::interval{active_clause}
            ORDER BY exchange, symbol, ts_open
        """, (window_hours,))

    if start_ts is None or end_ts is None:
        raise RuntimeError("_fetch_metric_rows requires window_hours or start_ts/end_ts")

    return fetch(f"""
        SELECT {select_sql}
        FROM {table} {alias}
        WHERE ts_close >= %s
          AND ts_close < %s{active_clause}
        ORDER BY exchange, symbol, ts_open
    """, (start_ts, end_ts))


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


def resolve_latest_common_ts_close() -> datetime | None:
    rows = fetch("""
        SELECT LEAST(
            (SELECT MAX(ts_close) FROM oi_raw),
            (SELECT MAX(ts_close) FROM price_raw),
            (SELECT MAX(ts_close) FROM volume_raw)
        ) AS anchor_ts
    """)
    if not rows:
        return None
    return rows[0]["anchor_ts"]


def build_aggregate_rows(
    window_hours: int | None = None,
    selected_windows: tuple[str, ...] | None = None,
    selected_metrics: tuple[str, ...] | None = None,
    active_only: bool = True,
    start_ts=None,
    end_ts=None,
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
                    rows_out.append((
                        "OI", timeframe,
                        chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                        exchange, symbol,
                        chunk[0]["oi_open"],
                        max(x["oi_high"] for x in chunk),
                        min(x["oi_low"] for x in chunk),
                        chunk[-1]["oi_close"],
                        None, None,
                        изменение_в_процентах(chunk[0]["oi_open"], chunk[-1]["oi_close"]),
                        len(chunk),
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

    oi_rows = []
    if "OI" in metric_set:
        oi_rows = _fetch_recent_metric_rows(
            "oi_raw",
            "ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close",
            "x",
            active_only,
            source_cycle_ts,
        )
        for (exchange, symbol), items in _groups(oi_rows).items():
            if not items or items[-1]["ts_close"] != source_cycle_ts:
                continue
            for timeframe, need in window_items:
                if len(items) < need:
                    continue
                chunk = items[-need:]
                if not _is_contiguous_5m(chunk):
                    skipped_non_contiguous += 1
                    continue
                rows_out.append((
                    "OI", timeframe,
                    chunk[0]["ts_open"], _window_close(chunk[0]["ts_open"], timeframe),
                    exchange, symbol,
                    chunk[0]["oi_open"],
                    max(x["oi_high"] for x in chunk),
                    min(x["oi_low"] for x in chunk),
                    chunk[-1]["oi_close"],
                    None, None,
                    изменение_в_процентах(chunk[0]["oi_open"], chunk[-1]["oi_close"]),
                    len(chunk),
                ))

    price_rows = []
    if "PRICE" in metric_set:
        price_rows = _fetch_recent_metric_rows(
            "price_raw",
            "ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close",
            "x",
            active_only,
            source_cycle_ts,
        )
        for (exchange, symbol), items in _groups(price_rows).items():
            if not items or items[-1]["ts_close"] != source_cycle_ts:
                continue
            for timeframe, need in window_items:
                if len(items) < need:
                    continue
                chunk = items[-need:]
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
                ))

    volume_rows = []
    if "VOLUME" in metric_set:
        volume_rows = _fetch_recent_metric_rows(
            "volume_raw",
            "ts_open, ts_close, exchange, symbol, volume",
            "x",
            active_only,
            source_cycle_ts,
        )
        for (exchange, symbol), items in _groups(volume_rows).items():
            if not items or items[-1]["ts_close"] != source_cycle_ts:
                continue
            for timeframe, need in window_items:
                if len(items) < need:
                    continue
                chunk = items[-need:]
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
                ))

    stats = {
        "raw_oi": len(oi_rows),
        "raw_price": len(price_rows),
        "raw_volume": len(volume_rows),
        "aggregates": len(rows_out),
        "skipped_non_contiguous": skipped_non_contiguous,
        "source_cycle_ts": source_cycle_ts,
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
    anchor_ts = resolve_latest_common_ts_close()
    if anchor_ts is None:
        raise RuntimeError("aggregates latest rebuild failed: empty common anchor_ts")

    rows_out, stats = build_latest_aggregate_rows(anchor_ts, active_only=True)
    if not rows_out:
        raise RuntimeError(
            "aggregates latest rebuild failed: empty rows_out "
            f"source_cycle_ts={source_cycle_ts.isoformat()} "
            f"anchor_ts={anchor_ts.isoformat()}"
        )

    upsert_aggregate_hot_rows(rows_out)
    history_synced, history_pruned = sync_aggregate_history_from_rows(rows_out)

    log(
        f"aggregates latest rebuilt: source_cycle_ts={source_cycle_ts.isoformat()} "
        f"anchor_ts={anchor_ts.isoformat()} "
        f"raw_oi={stats['raw_oi']} raw_price={stats['raw_price']} raw_volume={stats['raw_volume']} "
        f"aggregates={stats['aggregates']} "
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
