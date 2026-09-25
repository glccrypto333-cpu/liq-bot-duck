"""Pure, DB-agnostic validation for the Duck quote-turnover evidence window."""

from __future__ import annotations

from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any, Iterable


POINTS_PER_4H = 48
POINTS_REQUIRED = POINTS_PER_4H * 2
FIVE_MINUTES = timedelta(minutes=5)
MAX_FRESHNESS = timedelta(minutes=10)


def stage3_price_veto_reason(
    *,
    price_30m_class: str | None,
    price_1h_class: str | None,
    price_cycle_ts: datetime | None,
    transition_ts: datetime | None,
    volume_unlocked_at: datetime | None,
    volume_unlock_cycle_ts: datetime | None,
) -> str | None:
    """Veto only on falling price in the frozen first volume-unlock cycle."""
    if (
        transition_ts is None
        or volume_unlocked_at is None
        or volume_unlock_cycle_ts is None
        or price_cycle_ts is None
        or price_cycle_ts != volume_unlock_cycle_ts
        or price_cycle_ts <= transition_ts
    ):
        return None

    if str(price_30m_class or "").lower() in {"weak_down", "strong_down"}:
        return "blocked:price_30m_down_at_volume_unlock"
    if str(price_1h_class or "").lower() in {"weak_down", "strong_down"}:
        return "blocked:price_1h_down_at_volume_unlock"
    return None

def build_current_4h_distribution(
    quote_values: Iterable[Any],
    *,
    ts_opens: Iterable[datetime] | None = None,
    expected_latest_close: datetime | None = None,
) -> dict[str, Any] | None:
    """Summarize hourly buckets and candle concentration for the latest 48 closed 5m candles."""
    from math import isfinite

    try:
        values = [float(value) for value in quote_values]
    except (TypeError, ValueError):
        return None
    if len(values) != POINTS_PER_4H or any(not isfinite(value) or value < 0 for value in values):
        return None
    if ts_opens is not None:
        opens = list(ts_opens)
        try:
            if len(opens) != POINTS_PER_4H or any(
                current - previous != FIVE_MINUTES
                for previous, current in zip(opens, opens[1:])
            ):
                return None
            if expected_latest_close is not None and (
                opens[-1] + FIVE_MINUTES != expected_latest_close
                or opens[0] + POINTS_PER_4H * FIVE_MINUTES != expected_latest_close
            ):
                return None
        except (AttributeError, IndexError, TypeError):
            return None
    total = sum(values)
    if total <= 0:
        return None
    top = sorted(values, reverse=True)
    points_per_hour = POINTS_PER_4H // 4
    return {
        "hourly_quote_totals": [
            sum(values[index:index + points_per_hour])
            for index in range(0, POINTS_PER_4H, points_per_hour)
        ],
        "largest_5m_share_pct": top[0] * 100.0 / total,
        "top3_5m_share_pct": sum(top[:3]) * 100.0 / total,
    }


def _empty(reason: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "ready": False,
        "reason": reason,
        "previous_4h_quote": None,
        "current_4h_quote": None,
        "growth_4h_pct": None,
        "current_4h_distribution": None,
        "previous_1h_quote": None,
        "current_1h_quote": None,
        "growth_1h_pct": None,
        "previous_4h_points": min(len(rows), POINTS_PER_4H),
        "current_4h_points": max(0, len(rows) - POINTS_PER_4H),
    }


def build_quote_turnover_snapshot(
    rows: Iterable[dict[str, Any]],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Build a strict, two-window 4h quote-turnover snapshot.

    Rows must be closed native 5-minute candles.  This deliberately rejects
    a gap or an incomplete 96-point history: incomplete evidence is warm-up,
    never a judgement that the volume itself was weak.
    """
    ordered = sorted(rows, key=lambda row: row["ts_open"])
    for previous, current in zip(ordered, ordered[1:]):
        if current["ts_open"] - previous["ts_open"] != FIVE_MINUTES:
            return _empty("non_contiguous", ordered)
    if len(ordered) < POINTS_REQUIRED:
        return _empty("warming_up", ordered)
    selected = ordered[-POINTS_REQUIRED:]
    if any(row.get("quote_turnover") is None for row in selected):
        return _empty("warming_up_quote_history", selected)
    from math import isfinite

    try:
        selected_turnover = [float(row["quote_turnover"]) for row in selected]
    except (KeyError, TypeError, ValueError):
        return _empty("invalid_quote_turnover", selected)
    if any(not isfinite(value) or value < 0 for value in selected_turnover):
        return _empty("invalid_quote_turnover", selected)
    previous_total = sum(selected_turnover[:POINTS_PER_4H])
    current_total = sum(selected_turnover[POINTS_PER_4H:])
    if previous_total <= 0:
        return _empty("invalid_previous_window", selected)
    freshness_seconds = None
    if as_of is not None:
        freshness_seconds = max(0.0, (as_of - selected[-1]["ts_close"]).total_seconds())
    previous_1h = sum(float(row["quote_turnover"]) for row in selected[-24:-12])
    current_1h = sum(float(row["quote_turnover"]) for row in selected[-12:])
    if previous_1h <= 0:
        return _empty("invalid_previous_window", selected)
    result = {
        "ready": True,
        "reason": "ready",
        "previous_4h_quote": previous_total,
        "current_4h_quote": current_total,
        "growth_4h_pct": (current_total - previous_total) * 100.0 / previous_total,
        "current_4h_distribution": build_current_4h_distribution(
            (row.get("quote_turnover") for row in selected[POINTS_PER_4H:]),
            ts_opens=(row.get("ts_open") for row in selected[POINTS_PER_4H:]),
            expected_latest_close=selected[-1].get("ts_close"),
        ),
        "previous_1h_quote": previous_1h,
        "current_1h_quote": current_1h,
        "growth_1h_pct": (current_1h - previous_1h) * 100.0 / previous_1h,
        "previous_4h_points": POINTS_PER_4H,
        "current_4h_points": POINTS_PER_4H,
        "freshness_seconds": freshness_seconds,
    }
    if freshness_seconds is not None and freshness_seconds > MAX_FRESHNESS.total_seconds():
        result["ready"] = False
        result["reason"] = "stale"
    return result


def build_quote_turnover_state_rows(
    rows: Iterable[dict[str, Any]],
    *,
    source_cycle_ts: datetime,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Evaluate source-native quote-turnover readiness per exchange and symbol."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["exchange"]), str(row["symbol"]))].append(row)
    return {
        key: build_quote_turnover_snapshot(items, as_of=source_cycle_ts)
        for key, items in grouped.items()
    }



def evaluate_stage3_volume_candidate(
    *,
    current_stage: int,
    ready: bool,
    growth_4h_pct: float | None,
    observed_at: datetime | None = None,
    previous_status: str | None = None,
    volume_unlocked_at: datetime | None = None,
) -> dict[str, Any]:
    """Keep a Stage-3 signal pending until its first valid >=100% DB volume window.

    A pending signal is invalidated only by a canonical phase exit. Once volume
    unlocks, its first observation timestamp is preserved through retries.
    """
    status = str(previous_status or "")
    if status == "sent":
        return {"status": "sent", "volume_unlocked_at": volume_unlocked_at}
    if int(current_stage) != 3:
        return {"status": "invalidated", "volume_unlocked_at": volume_unlocked_at}
    if status == "unlocked" or volume_unlocked_at is not None:
        return {"status": "unlocked", "volume_unlocked_at": volume_unlocked_at}
    try:
        qualifies = bool(ready) and growth_4h_pct is not None and float(growth_4h_pct) >= 100.0
    except (TypeError, ValueError):
        qualifies = False
    if qualifies and observed_at is not None:
        return {"status": "unlocked", "volume_unlocked_at": observed_at}
    return {"status": "waiting_volume", "volume_unlocked_at": None}
