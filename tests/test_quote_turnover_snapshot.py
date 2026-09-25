from datetime import datetime, timedelta, timezone

import pytest


def _rows(count: int = 96, quote: float = 100.0):
    base = datetime(2026, 9, 22, 8, 0, tzinfo=timezone.utc)
    return [
        {
            "ts_open": base + timedelta(minutes=5 * index),
            "ts_close": base + timedelta(minutes=5 * (index + 1)),
            "quote_turnover": quote if index < 48 else quote * 3,
        }
        for index in range(count)
    ]


def test_two_contiguous_four_hour_quote_windows_are_ready():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    snapshot = build_quote_turnover_snapshot(_rows())

    assert snapshot["ready"] is True
    assert snapshot["reason"] == "ready"
    assert snapshot["previous_4h_quote"] == 4800.0
    assert snapshot["current_4h_quote"] == 14400.0
    assert snapshot["growth_4h_pct"] == 200.0
    assert snapshot["previous_1h_quote"] == 3600.0
    assert snapshot["current_1h_quote"] == 3600.0
    assert snapshot["growth_1h_pct"] == 0.0
    assert snapshot["previous_4h_points"] == 48
    assert snapshot["current_4h_points"] == 48
    assert snapshot["current_4h_distribution"]["hourly_quote_totals"] == [3600.0] * 4
    assert snapshot["current_4h_distribution"]["largest_5m_share_pct"] == pytest.approx(100.0 / 48.0)


def test_gap_in_closed_five_minute_rows_is_not_battle_ready():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    rows = _rows()
    del rows[63]
    snapshot = build_quote_turnover_snapshot(rows)

    assert snapshot["ready"] is False
    assert snapshot["reason"] == "non_contiguous"


def test_less_than_two_windows_is_warming_up_not_weak_volume():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    snapshot = build_quote_turnover_snapshot(_rows(95))

    assert snapshot["ready"] is False
    assert snapshot["reason"] == "warming_up"
    assert snapshot["growth_4h_pct"] is None


def test_stale_complete_history_is_degraded_not_a_volume_verdict():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    rows = _rows()
    snapshot = build_quote_turnover_snapshot(
        rows,
        as_of=rows[-1]["ts_close"] + timedelta(minutes=11),
    )

    assert snapshot["ready"] is False
    assert snapshot["reason"] == "stale"
    assert snapshot["growth_4h_pct"] == 200.0


def test_bulk_state_keeps_each_exchange_symbol_independent():
    from quote_turnover_snapshot import build_quote_turnover_state_rows

    base_rows = _rows()
    source_cycle_ts = base_rows[-1]["ts_close"]
    raw_rows = [
        {**row, "exchange": "BINANCE", "symbol": "GOODUSDT"}
        for row in base_rows
    ] + [
        {**row, "exchange": "BYBIT", "symbol": "WARMUSDT"}
        for row in _rows(95)
    ]

    states = build_quote_turnover_state_rows(raw_rows, source_cycle_ts=source_cycle_ts)

    assert states[("BINANCE", "GOODUSDT")]["ready"] is True
    assert states[("BYBIT", "WARMUSDT")]["ready"] is False
    assert states[("BYBIT", "WARMUSDT")]["reason"] == "warming_up"


def test_legacy_base_volume_without_native_quote_is_quote_history_warmup():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    rows = _rows()
    rows[0]["quote_turnover"] = None

    snapshot = build_quote_turnover_snapshot(rows)

    assert snapshot["ready"] is False
    assert snapshot["reason"] == "warming_up_quote_history"


def test_one_hour_metrics_compare_the_last_closed_hour_to_the_previous_hour():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    rows = _rows()
    for row in rows[-12:]:
        row["quote_turnover"] *= 2
    snapshot = build_quote_turnover_snapshot(rows)

    assert snapshot["previous_1h_quote"] == 3600.0
    assert snapshot["current_1h_quote"] == 7200.0
    assert snapshot["growth_1h_pct"] == 100.0

def test_current_four_hour_distribution_distinguishes_single_spike_from_smooth_volume():
    from quote_turnover_snapshot import build_current_4h_distribution

    smooth = build_current_4h_distribution([2000.0] * 48)
    spike = build_current_4h_distribution([49000.0] + [1000.0] * 47)
    late_spike = build_current_4h_distribution([1000.0] * 47 + [49000.0])

    assert smooth["hourly_quote_totals"] == [24000.0] * 4
    assert smooth["largest_5m_share_pct"] == pytest.approx(100.0 / 48.0)
    assert smooth["top3_5m_share_pct"] == 6.25
    assert spike["hourly_quote_totals"] == [60000.0, 12000.0, 12000.0, 12000.0]
    assert spike["largest_5m_share_pct"] == pytest.approx(49000.0 * 100.0 / 96000.0)
    assert spike["top3_5m_share_pct"] == pytest.approx(51000.0 * 100.0 / 96000.0)
    assert late_spike["hourly_quote_totals"] == [12000.0, 12000.0, 12000.0, 60000.0]
    assert late_spike["top3_5m_share_pct"] == spike["top3_5m_share_pct"]


def test_current_four_hour_distribution_rejects_incomplete_or_invalid_bars():
    from quote_turnover_snapshot import build_current_4h_distribution

    assert build_current_4h_distribution([1000.0] * 47) is None
    assert build_current_4h_distribution([1000.0] * 47 + [None]) is None
    assert build_current_4h_distribution([1000.0] * 47 + [-1.0]) is None


def test_current_four_hour_distribution_requires_contiguous_candle_opens_when_supplied():
    from quote_turnover_snapshot import build_current_4h_distribution

    start = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    opens = [start + timedelta(minutes=5 * index) for index in range(48)]
    gap_opens = list(opens)
    gap_opens[24] += timedelta(minutes=5)

    assert build_current_4h_distribution([1000.0] * 48, ts_opens=opens) is not None
    assert build_current_4h_distribution(
        [1000.0] * 48, ts_opens=opens, expected_latest_close=start + timedelta(hours=4)
    ) is not None
    assert build_current_4h_distribution(
        [1000.0] * 48, ts_opens=opens, expected_latest_close=start + timedelta(hours=4, minutes=5)
    ) is None
    assert build_current_4h_distribution([1000.0] * 48, ts_opens=gap_opens) is None


def test_non_finite_or_negative_quote_turnover_is_not_a_ready_volume_window():
    from quote_turnover_snapshot import build_quote_turnover_snapshot

    for bad_value in (float("nan"), float("inf"), -1.0):
        rows = _rows()
        rows[60]["quote_turnover"] = bad_value
        snapshot = build_quote_turnover_snapshot(rows)
        assert snapshot["ready"] is False
        assert snapshot["reason"] == "invalid_quote_turnover"
        assert snapshot["growth_4h_pct"] is None
