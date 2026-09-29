from datetime import datetime, timedelta, timezone


def test_stage3_price_veto_accepts_exact_or_previous_fresh_closed_window():
    from quote_turnover_snapshot import stage3_price_veto_reason

    transition = datetime(2026, 9, 24, 6, 0, tzinfo=timezone.utc)
    unlock = datetime(2026, 9, 24, 7, 0, tzinfo=timezone.utc)

    for down_class in ("weak_down", "strong_down"):
        assert stage3_price_veto_reason(
            price_30m_class=down_class,
            price_1h_class="good_up",
            price_30m_cycle_ts=unlock,
            price_1h_cycle_ts=unlock - timedelta(minutes=5),
            transition_ts=transition,
            volume_unlocked_at=unlock,
            volume_unlock_cycle_ts=unlock,
        ) == "blocked:price_30m_down_at_volume_unlock"
        assert stage3_price_veto_reason(
            price_30m_class="flat",
            price_1h_class=down_class,
            price_30m_cycle_ts=unlock - timedelta(minutes=5),
            price_1h_cycle_ts=unlock,
            transition_ts=transition,
            volume_unlocked_at=unlock,
            volume_unlock_cycle_ts=unlock,
        ) == "blocked:price_1h_down_at_volume_unlock"

    assert stage3_price_veto_reason(
        price_30m_class="good_up",
        price_1h_class="flat",
        price_30m_cycle_ts=unlock,
        price_1h_cycle_ts=unlock - timedelta(minutes=5),
        transition_ts=transition,
        volume_unlocked_at=unlock,
        volume_unlock_cycle_ts=unlock,
    ) is None

    for stale_or_future in (unlock - timedelta(minutes=10), unlock + timedelta(minutes=5), None):
        assert stage3_price_veto_reason(
            price_30m_class="strong_down",
            price_1h_class="flat",
            price_30m_cycle_ts=unlock,
            price_1h_cycle_ts=stale_or_future,
            transition_ts=transition,
            volume_unlocked_at=unlock,
            volume_unlock_cycle_ts=unlock,
        ) == "blocked:missing_fresh_price_at_volume_unlock"



def test_stage3_price_veto_fails_closed_when_unlock_anchor_is_missing():
    from quote_turnover_snapshot import stage3_price_veto_reason

    transition = datetime(2026, 9, 24, 6, 0, tzinfo=timezone.utc)
    unlocked_at = datetime(2026, 9, 24, 7, 0, tzinfo=timezone.utc)
    assert stage3_price_veto_reason(
        price_30m_class="good_up",
        price_1h_class="good_up",
        price_30m_cycle_ts=unlocked_at,
        price_1h_cycle_ts=unlocked_at,
        transition_ts=transition,
        volume_unlocked_at=unlocked_at,
        volume_unlock_cycle_ts=None,
    ) == "blocked:missing_fresh_price_at_volume_unlock"

def test_stage3_price_snapshot_uses_latest_closed_cycle_not_future_data():
    from quote_turnover_snapshot import build_stage3_price_snapshot

    transition = datetime(2026, 9, 24, 6, 0, tzinfo=timezone.utc)
    unlock = datetime(2026, 9, 24, 7, 0, tzinfo=timezone.utc)
    rows = [
        {"window_code": "30м", "ts_close": unlock - timedelta(minutes=5), "open_value": 100, "close_value": 99},
        {"window_code": "30м", "ts_close": unlock, "open_value": 100, "close_value": 101},
        {"window_code": "30м", "ts_close": unlock + timedelta(minutes=5), "open_value": 100, "close_value": 80},
        {"window_code": "1ч", "ts_close": unlock - timedelta(minutes=5), "open_value": 100, "close_value": 99},
    ]

    snapshot = build_stage3_price_snapshot(
        rows, transition_ts=transition, volume_unlock_cycle_ts=unlock,
    )
    assert snapshot["price_30m_cycle_ts"] == unlock
    assert snapshot["price_30m_class"] in {"weak_up", "good_up", "strong_up"}
    assert snapshot["price_1h_cycle_ts"] == unlock - timedelta(minutes=5)
    assert snapshot["price_1h_class"] == "weak_down"
    assert snapshot["price_data_error"] is None


def test_stage3_price_snapshot_reports_missing_fresh_window_as_data_error():
    from quote_turnover_snapshot import build_stage3_price_snapshot

    transition = datetime(2026, 9, 24, 6, 0, tzinfo=timezone.utc)
    unlock = datetime(2026, 9, 24, 7, 0, tzinfo=timezone.utc)
    snapshot = build_stage3_price_snapshot(
        [{"window_code": "30м", "ts_close": unlock, "open_value": 100, "close_value": 99}],
        transition_ts=transition,
        volume_unlock_cycle_ts=unlock,
    )
    assert snapshot["price_data_error"] == "missing_fresh_closed_window:1ч"
    assert snapshot["price_1h_cycle_ts"] is None


def test_stage3_candidate_waits_below_threshold_without_expiry():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=70.0,
        observed_at=datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "waiting_volume"
    assert result["volume_unlocked_at"] is None


def test_stage3_candidate_unlocks_once_and_preserves_first_crossing_time():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    first = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    later = datetime(2026, 9, 23, 8, 10, tzinfo=timezone.utc)
    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=105.0,
        observed_at=later, volume_unlocked_at=first,
    )
    assert result["status"] == "unlocked"
    assert result["volume_unlocked_at"] == first


def test_stage3_candidate_is_invalidated_by_phase_exit_before_volume_unlock():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    result = evaluate_stage3_volume_candidate(
        current_stage=1, ready=True, growth_4h_pct=150.0,
        observed_at=datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc),
    )
    assert result["status"] == "invalidated"
    assert result["volume_unlocked_at"] is None


def test_unlocked_stage3_candidate_is_invalidated_after_phase_exit():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    unlocked_at = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    result = evaluate_stage3_volume_candidate(
        current_stage=1, ready=False, growth_4h_pct=None,
        previous_status="unlocked", volume_unlocked_at=unlocked_at,
    )
    assert result["status"] == "invalidated"
    assert result["volume_unlocked_at"] == unlocked_at


def test_stage3_candidate_waits_when_window_is_not_ready():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=False, growth_4h_pct=None,
        observed_at=datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "waiting_volume"
    assert result["volume_unlocked_at"] is None


def test_stage3_candidate_waits_when_ready_window_has_no_growth_value():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=None,
        observed_at=datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "waiting_volume"
    assert result["volume_unlocked_at"] is None


def test_unlocked_stage3_candidate_stays_unlocked_during_volume_warmup():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    unlocked_at = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=False, growth_4h_pct=None,
        previous_status="unlocked", volume_unlocked_at=unlocked_at,
    )
    assert result["status"] == "unlocked"
    assert result["volume_unlocked_at"] == unlocked_at


def test_unlocked_stage3_candidate_stays_unlocked_if_ready_window_falls_below_threshold():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    unlocked_at = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=92.0,
        observed_at=datetime(2026, 9, 23, 8, 10, tzinfo=timezone.utc),
        previous_status="unlocked", volume_unlocked_at=unlocked_at,
    )
    assert result["status"] == "unlocked"
    assert result["volume_unlocked_at"] == unlocked_at


def test_sent_stage3_candidate_stays_sent_after_later_phase_exit():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    unlocked_at = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    result = evaluate_stage3_volume_candidate(
        current_stage=1, ready=False, growth_4h_pct=None,
        previous_status="sent",
        volume_unlocked_at=unlocked_at,
    )
    assert result["status"] == "sent"
    assert result["volume_unlocked_at"] == unlocked_at


def test_stage3_candidate_unlocks_at_exact_100_percent():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    observed_at = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=100.0, observed_at=observed_at,
    )
    assert result["status"] == "unlocked"
    assert result["volume_unlocked_at"] == observed_at


def test_stage3_candidate_does_not_unlock_without_observation_timestamp():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=150.0, observed_at=None,
    )
    assert result["status"] == "waiting_volume"
    assert result["volume_unlocked_at"] is None


def test_stage3_candidate_does_not_unlock_just_below_100_percent():
    from quote_turnover_snapshot import evaluate_stage3_volume_candidate

    result = evaluate_stage3_volume_candidate(
        current_stage=3, ready=True, growth_4h_pct=99.9,
        observed_at=datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "waiting_volume"
    assert result["volume_unlocked_at"] is None


def test_stage3_price_veto_uses_volume_cycle_not_collection_timestamp():
    from quote_turnover_snapshot import stage3_price_veto_reason

    transition = datetime(2026, 9, 25, 10, 15, tzinfo=timezone.utc)
    volume_cycle = datetime(2026, 9, 25, 11, 55, tzinfo=timezone.utc)
    collection_time = volume_cycle + timedelta(seconds=21)

    assert stage3_price_veto_reason(
        price_30m_class="strong_down",
        price_1h_class="weak_down",
        price_30m_cycle_ts=volume_cycle,
        price_1h_cycle_ts=volume_cycle,
        transition_ts=transition,
        volume_unlocked_at=collection_time,
        volume_unlock_cycle_ts=volume_cycle,
    ) == "blocked:price_30m_down_at_volume_unlock"

    assert stage3_price_veto_reason(
        price_30m_class="strong_down",
        price_1h_class="weak_down",
        price_30m_cycle_ts=volume_cycle - timedelta(minutes=5),
        price_1h_cycle_ts=volume_cycle - timedelta(minutes=5),
        transition_ts=transition,
        volume_unlocked_at=collection_time,
        volume_unlock_cycle_ts=volume_cycle,
    ) == "blocked:price_30m_down_at_volume_unlock"

    # A future cycle cannot replace the frozen unlock snapshot; if the historical
    # unlock-cycle window is absent, fail closed as a data error.
    later_cycle = volume_cycle + timedelta(minutes=5)
    assert stage3_price_veto_reason(
        price_30m_class="strong_down",
        price_1h_class="weak_down",
        price_30m_cycle_ts=later_cycle,
        price_1h_cycle_ts=later_cycle,
        transition_ts=transition,
        volume_unlocked_at=collection_time,
        volume_unlock_cycle_ts=volume_cycle,
    ) == "blocked:missing_fresh_price_at_volume_unlock"
