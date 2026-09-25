from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_service import (
    AUTONOMOUS_OI_PROGRESS_PATH,
    _apply_price_freshness_guard,
    _record_price_freshness_guard_block,
    _record_transition_observation,
    build_stage_chain_continuity_report,
    build_window_freshness_by_kind,
    collect_stage1_near_maturity_diagnostics,
    get_runtime_observability_metrics,
    _resolve_growth_trigger_ts,
    build_core_record,
    compute_autonomous_oi_snapshot_incremental_to_cycle,
    reconcile_core_state_integrity,
    reset_runtime_observability_metrics,
    run_autonomous_oi_service,
    run_post_stage_analytics_tail,
    save_autonomous_oi_progress,
    update_post_stage_analytics,
)


PRICE_OK = ("цена_не_блокирует", "нет", False, 3)


def make_price_payload(cycle_ts: datetime, *, fresh: bool = True) -> dict:
    ts = cycle_ts if fresh else datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc)
    return {
        "30м": {"PRICE": {"source_cycle_ts": ts}},
        "1ч": {"PRICE": {"source_cycle_ts": ts}},
        "4ч": {"PRICE": {"source_cycle_ts": ts}},
    }


def make_oi_summary(
    oi_15m: str = "flat",
    oi_30m: str = "flat",
    oi_1h: str = "flat",
    oi_4h: str = "flat",
) -> dict:
    return {
        "oi_slope_class_15m": oi_15m,
        "oi_slope_class_30m": oi_30m,
        "oi_slope_class_1h": oi_1h,
        "oi_slope_class_4h": oi_4h,
    }


def make_persistent_oi_summary(
    oi_15m: str = "weak_up",
    oi_30m: str = "good_up",
    oi_1h: str = "strong_up",
    oi_4h: str = "good_up",
) -> dict:
    return {
        "oi_pattern_label": "подтвержденный_набор",
        "oi_pattern_code": "подтвержденный_набор",
        "oi_direction_summary": "вверх",
        "oi_angle_summary": "сильный",
        "oi_stability_summary": "хорошая",
        "oi_retention_summary": "подтвержденное",
        "oi_breakdown_summary": "нет",
        "oi_slope_class_15m": oi_15m,
        "oi_slope_class_30m": oi_30m,
        "oi_slope_class_1h": oi_1h,
        "oi_slope_class_4h": oi_4h,
    }


def test_runtime_observability_records_price_freshness_guard_blocks():
    reset_runtime_observability_metrics()

    _record_price_freshness_guard_block(
        exchange="BINANCE",
        symbol="TESTUSDT",
        previous_stage=2,
        requested_stage=3,
        held_stage=2,
        reason="удержание:нет_свежей_цены_30м_1ч_4ч",
    )

    metrics = get_runtime_observability_metrics()
    guard = metrics["price_freshness_guard"]
    assert guard["blocked_promotions_total"] == 1
    assert guard["blocked_2_to_3"] == 1
    assert guard["blocked_1_to_2"] == 0
    assert guard["sample"][0]["pair"] == "BINANCE:TESTUSDT"


def test_runtime_observability_records_transition_and_degrade_reasons():
    reset_runtime_observability_metrics()

    _record_transition_observation(
        previous_stage=3,
        target_stage=0,
        decision_reason="stage3_reset",
        guard_reason="oi_4h=weak_down",
        oi_summary=make_oi_summary(oi_4h="weak_down"),
        exchange="BYBIT",
        symbol="DROPUSDT",
    )

    metrics = get_runtime_observability_metrics()
    assert metrics["transition_metrics"]["by_transition"]["3_to_0"] == 1
    assert metrics["degrade_reasons"]["by_reason"]["oi_4h_weak_down"] == 1
    assert metrics["degrade_reasons"]["sample"][0]["pair"] == "BYBIT:DROPUSDT"


def test_window_freshness_by_kind_counts_problem_pairs_by_metric_and_window():
    summary = build_window_freshness_by_kind(
        [
            {
                "exchange": "BINANCE",
                "symbol": "ALLUSDT",
                "missing_list": "PRICE:1ч, OI:30м",
                "stale_list": "OI:4ч=240м, PRICE:30м=35м",
            },
            {
                "exchange": "BYBIT",
                "symbol": "GAPUSDT",
                "missing_list": "VOLUME:15м",
                "stale_list": "OI:30м=40м",
            },
        ]
    )

    assert summary["missing"]["PRICE"]["1h"] == 1
    assert summary["missing"]["OI"]["30m"] == 1
    assert summary["missing"]["VOLUME"]["15m"] == 1
    assert summary["stale"]["OI"]["30m"] == 1
    assert summary["stale"]["OI"]["4h"] == 1
    assert summary["stale"]["PRICE"]["30m"] == 1


def test_stage_chain_continuity_report_flags_silent_stage_reset():
    report = build_stage_chain_continuity_report(
        [
            {
                "exchange": "BYBIT",
                "symbol": "NFLXUSDT",
                "cycle_ts_msk": "2026-07-21 13:55",
                "prev_to": 3,
                "from_stage": 0,
                "to_stage": 1,
            },
            {
                "exchange": "BINANCE",
                "symbol": "OKUSDT",
                "cycle_ts_msk": "2026-07-21 14:00",
                "prev_to": 1,
                "from_stage": 1,
                "to_stage": 2,
            },
        ],
        lookback_hours=24,
        recent_minutes=30,
    )

    assert report["total"] == 1
    assert report["recent_total"] == 1
    assert report["health"] == "critical"
    assert report["sample"][0]["pair"] == "BYBIT:NFLXUSDT"
    assert report["sample"][0]["expected_from_stage"] == 3
    assert report["sample"][0]["actual_from_stage"] == 0


def test_stage_2_without_saved_trigger_does_not_restore_ancient_trigger_from_age() -> None:
    cycle_ts = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)
    previous_state = {
        "current_stage": 2,
        "oi_stage_age_minutes": 855.14,
        "oi_slope_class_15m": "flat",
        "growth_trigger_ts": None,
    }
    summary = make_oi_summary(oi_15m="good_up", oi_30m="good_up", oi_1h="good_up", oi_4h="weak_up")
    trigger_ts = _resolve_growth_trigger_ts(previous_state, summary, PRICE_OK, cycle_ts)
    assert trigger_ts is None


def test_build_core_record_persists_trigger_and_latest_oi_slopes_for_next_cycle() -> None:
    cycle_ts = datetime(2026, 6, 23, 20, 20, tzinfo=timezone.utc)
    trigger_ts = datetime(2026, 6, 23, 19, 55, tzinfo=timezone.utc)
    row = build_core_record(
        "BYBIT",
        "BASEDUSDT",
        make_persistent_oi_summary(),
        ("цена_не_блокирует", "нет", False, 3),
        ("пустой", "нейтрально"),
        1,
        "удержание_1",
        414.05,
        cycle_ts,
        "30м_зрелое_1ч_подтверждает_силу; guard=удержание_1:ждем_30_минут_от_триггера",
        trigger_ts,
    )
    assert row[-6] == trigger_ts
    assert row[-5:] == ("weak_up", "good_up", "strong_up", "good_up", cycle_ts)


def test_integrity_guard_restores_stage3_when_mutable_core_row_is_lost() -> None:
    state_map = {
        ("BINANCE", "SXTUSDT"): {
            "exchange": "BINANCE",
            "symbol": "SXTUSDT",
            "current_stage": 2,
            "oi_stage_age_minutes": 5.0,
        }
    }
    recovered = reconcile_core_state_integrity(
        state_map,
        [
            {
                "exchange": "BINANCE",
                "symbol": "SXTUSDT",
                "current_stage": 3,
                "stage_age_minutes": 35.0,
                "latest_cycle_ts": datetime(2026, 7, 14, 8, 20, tzinfo=timezone.utc),
            }
        ],
    )
    assert recovered == [("BINANCE", "SXTUSDT")]
    assert state_map[("BINANCE", "SXTUSDT")]["current_stage"] == 3
    assert state_map[("BINANCE", "SXTUSDT")]["oi_stage_age_minutes"] == 35.0


def test_integrity_guard_restores_stage1_when_mutable_core_row_is_lost() -> None:
    state_map: dict[tuple[str, str], dict] = {}

    recovered = reconcile_core_state_integrity(
        state_map,
        [
            {
                "exchange": "BINANCE",
                "symbol": "AAVEUSDT",
                "current_stage": 1,
                "stage_age_minutes": 45.0,
                "latest_cycle_ts": datetime(2026, 7, 24, 9, 10, tzinfo=timezone.utc),
            }
        ],
    )

    assert recovered == [("BINANCE", "AAVEUSDT")]
    assert state_map[("BINANCE", "AAVEUSDT")]["current_stage"] == 1
    assert state_map[("BINANCE", "AAVEUSDT")]["oi_stage_age_minutes"] == 45.0


def test_incremental_rechecks_stage2_pair_from_history_when_hot_cycle_misses_it(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)
    previous_state = {
        ("BINANCE", "FLOCKUSDT"): {
            "current_stage": 2,
            "growth_trigger_ts": datetime(2026, 7, 16, 8, 10, tzinfo=timezone.utc),
        }
    }
    calls: list[tuple] = []

    def fake_load_source_cycles(*args, **kwargs):
        return [source_cycle]

    def fake_load_latest_window_map(cycle_ts, window_source="hot", tracked_pairs=None):
        calls.append((cycle_ts, window_source, tuple(tracked_pairs or ())))
        if window_source == "history":
            return {("BINANCE", "FLOCKUSDT"): {"15м": {"OI": {"ts_close": source_cycle}}}}
        return {}

    def fake_compute(window_map, cycle_ts=None, previous_state_map=None):
        assert ("BINANCE", "FLOCKUSDT") in window_map
        return [("core",)], [("window",)], [], previous_state_map or {}

    monkeypatch.setattr("autonomous_oi_service.load_source_cycle_timestamps", fake_load_source_cycles)
    monkeypatch.setattr("autonomous_oi_service.load_latest_window_map", fake_load_latest_window_map)
    monkeypatch.setattr("autonomous_oi_service.load_window_updates_by_cycle", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        "autonomous_oi_service.compute_autonomous_oi_snapshot_from_latest_window_map",
        fake_compute,
    )

    core_rows, _, _, _, _ = compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=source_cycle,
        previous_state_map=previous_state,
        last_source_cycle_ts=datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
    )

    assert core_rows == [("core",)]
    assert calls == [
        (source_cycle, "hot", ()),
        (source_cycle, "history", (("BINANCE", "FLOCKUSDT"),)),
    ]


def test_incremental_rechecks_stage1_pair_from_history_when_near_maturity(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)
    previous_state = {
        ("BINANCE", "FLOCKUSDT"): {
            "current_stage": 1,
            "growth_trigger_ts": datetime(2026, 7, 16, 8, 25, tzinfo=timezone.utc),
            "latest_cycle_ts": datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
            "oi_transition_permission": "ждем_30_минут_от_триггера",
            "oi_slope_class_15m": "good_up",
            "oi_slope_class_30m": "good_up",
        }
    }
    calls: list[tuple] = []
    monkeypatch.setenv("ENABLE_STAGE1_HISTORY_RECHECK", "1")

    def fake_load_source_cycles(*args, **kwargs):
        return [source_cycle]

    def fake_load_latest_window_map(cycle_ts, window_source="hot", tracked_pairs=None):
        calls.append((cycle_ts, window_source, tuple(tracked_pairs or ())))
        if window_source == "history":
            return {("BINANCE", "FLOCKUSDT"): {"15м": {"OI": {"ts_close": source_cycle}}}}
        return {}

    def fake_compute(window_map, cycle_ts=None, previous_state_map=None):
        assert ("BINANCE", "FLOCKUSDT") in window_map
        return [("core",)], [("window",)], [], previous_state_map or {}

    monkeypatch.setattr("autonomous_oi_service.load_source_cycle_timestamps", fake_load_source_cycles)
    monkeypatch.setattr("autonomous_oi_service.load_latest_window_map", fake_load_latest_window_map)
    monkeypatch.setattr("autonomous_oi_service.load_window_updates_by_cycle", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        "autonomous_oi_service.compute_autonomous_oi_snapshot_from_latest_window_map",
        fake_compute,
    )

    core_rows, _, _, _, _ = compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=source_cycle,
        previous_state_map=previous_state,
        last_source_cycle_ts=datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
    )

    assert core_rows == [("core",)]
    assert calls == [
        (source_cycle, "hot", ()),
        (source_cycle, "history", (("BINANCE", "FLOCKUSDT"),)),
    ]


def test_incremental_skips_stage1_history_recheck_by_default(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)
    previous_state = {
        ("BINANCE", "FLOCKUSDT"): {
            "current_stage": 1,
            "growth_trigger_ts": datetime(2026, 7, 16, 8, 25, tzinfo=timezone.utc),
            "latest_cycle_ts": datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
            "oi_transition_permission": "ждем_30_минут_от_триггера",
            "oi_slope_class_15m": "good_up",
            "oi_slope_class_30m": "good_up",
        }
    }
    calls: list[tuple] = []

    def fake_load_latest_window_map(cycle_ts, window_source="hot", tracked_pairs=None):
        calls.append((cycle_ts, window_source, tuple(tracked_pairs or ())))
        if window_source == "history":
            raise AssertionError("stage1 history overlay must be opt-in")
        return {}

    monkeypatch.setattr("autonomous_oi_service.load_source_cycle_timestamps", lambda *args, **kwargs: [source_cycle])
    monkeypatch.setattr("autonomous_oi_service.load_latest_window_map", fake_load_latest_window_map)
    monkeypatch.setattr("autonomous_oi_service.load_window_updates_by_cycle", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        "autonomous_oi_service.compute_autonomous_oi_snapshot_from_latest_window_map",
        lambda window_map, cycle_ts=None, previous_state_map=None: ([('core',)], [], [], previous_state_map or {}),
    )

    core_rows, _, _, _, _ = compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=source_cycle,
        previous_state_map=previous_state,
        last_source_cycle_ts=datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
    )

    assert core_rows == [("core",)]
    assert calls == [(source_cycle, "hot", ())]


def test_incremental_does_not_history_recheck_stage1_pair_when_not_near_maturity(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)
    previous_state = {
        ("BINANCE", "FLOCKUSDT"): {
            "current_stage": 1,
            "growth_trigger_ts": None,
            "latest_cycle_ts": datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
            "oi_transition_permission": "удержание_1",
            "oi_slope_class_15m": "flat",
            "oi_slope_class_30m": "flat",
            "oi_stage_age_minutes": 5.0,
        }
    }
    calls: list[tuple] = []

    def fake_load_source_cycles(*args, **kwargs):
        return [source_cycle]

    def fake_load_latest_window_map(cycle_ts, window_source="hot", tracked_pairs=None):
        calls.append((cycle_ts, window_source, tuple(tracked_pairs or ())))
        if window_source == "history":
            raise AssertionError("stage1 history overlay should stay bounded")
        return {}

    def fake_compute(window_map, cycle_ts=None, previous_state_map=None):
        assert window_map == {}
        return [], [], [], previous_state_map or {}

    monkeypatch.setattr("autonomous_oi_service.load_source_cycle_timestamps", fake_load_source_cycles)
    monkeypatch.setattr("autonomous_oi_service.load_latest_window_map", fake_load_latest_window_map)
    monkeypatch.setattr("autonomous_oi_service.load_window_updates_by_cycle", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        "autonomous_oi_service.compute_autonomous_oi_snapshot_from_latest_window_map",
        fake_compute,
    )

    core_rows, _, _, _, _ = compute_autonomous_oi_snapshot_incremental_to_cycle(
        cycle_ts=source_cycle,
        previous_state_map=previous_state,
        last_source_cycle_ts=datetime(2026, 7, 16, 8, 55, tzinfo=timezone.utc),
    )

    assert core_rows == []
    assert calls == [(source_cycle, "hot", ())]


def test_collect_stage1_near_maturity_diagnostics_marks_missing_hot_map(monkeypatch) -> None:
    def fake_fetch(sql, params=()):
        assert "FROM oi_core_state" in sql
        return [
            {
                "exchange": "BINANCE",
                "symbol": "READYUSDT",
                "oi_stage_age_minutes": 29.0,
                "trigger_age_minutes": 28.0,
                "oi_transition_permission": "ждем_30_минут_от_триггера",
                "hot_map_present": True,
                "oi_slope_class_15m": "good_up",
                "oi_slope_class_30m": "strong_up",
                "oi_slope_class_1h": "weak_up",
                "oi_slope_class_4h": "flat",
            },
            {
                "exchange": "BYBIT",
                "symbol": "MISSEDUSDT",
                "oi_stage_age_minutes": 31.0,
                "trigger_age_minutes": 31.0,
                "oi_transition_permission": "ждем_30_минут_в_1",
                "hot_map_present": False,
                "oi_slope_class_15m": "strong_up",
                "oi_slope_class_30m": "good_up",
                "oi_slope_class_1h": "flat",
                "oi_slope_class_4h": "weak_up",
            },
        ]

    monkeypatch.setattr("autonomous_oi_service.fetch", fake_fetch)

    diagnostics = collect_stage1_near_maturity_diagnostics()

    assert diagnostics["total"] == 2
    assert diagnostics["absent_in_hot_map"] == 1
    assert diagnostics["sample"][0]["symbol"] == "READYUSDT"
    assert diagnostics["sample"][1]["hot_map_present"] is False


def test_collect_stage1_near_maturity_diagnostics_is_safe_on_query_error(monkeypatch) -> None:
    def fake_fetch(*args, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr("autonomous_oi_service.fetch", fake_fetch)

    diagnostics = collect_stage1_near_maturity_diagnostics()

    assert diagnostics["total"] == 0
    assert diagnostics["query_error"] == "RuntimeError"


def test_price_freshness_guard_blocks_stage1_promotion_when_price_is_stale() -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)

    target_stage, reason = _apply_price_freshness_guard(
        previous_stage=1,
        target_stage=2,
        payload=make_price_payload(cycle_ts, fresh=False),
        cycle_ts=cycle_ts,
    )

    assert target_stage == 1
    assert reason == "удержание:нет_свежей_цены_30м_1ч_4ч"


def test_price_freshness_guard_does_not_block_oi_degrade() -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)

    target_stage, reason = _apply_price_freshness_guard(
        previous_stage=2,
        target_stage=1,
        payload=make_price_payload(cycle_ts, fresh=False),
        cycle_ts=cycle_ts,
    )

    assert target_stage == 1
    assert reason is None


def test_price_freshness_guard_allows_stage2_promotion_with_current_price() -> None:
    cycle_ts = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)

    target_stage, reason = _apply_price_freshness_guard(
        previous_stage=2,
        target_stage=3,
        payload=make_price_payload(cycle_ts, fresh=True),
        cycle_ts=cycle_ts,
    )

    assert target_stage == 3
    assert reason is None


def test_save_autonomous_oi_progress_writes_through_temp_file(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "autonomous_oi_progress.json"
    monkeypatch.setattr("autonomous_oi_service.RUNTIME_DIR", tmp_path)
    monkeypatch.setattr("autonomous_oi_service.AUTONOMOUS_OI_PROGRESS_PATH", progress_path)

    original_write_text = type(progress_path).write_text

    def fail_if_writing_final_path(self, *args, **kwargs):
        if self == progress_path:
            raise AssertionError("progress must not be written directly to final path")
        return original_write_text(self, *args, **kwargs)

    monkeypatch.setattr(type(progress_path), "write_text", fail_if_writing_final_path)

    save_autonomous_oi_progress(datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc))

    assert "2026-07-16T09:05:00+00:00" in progress_path.read_text(encoding="utf-8")


def test_run_autonomous_oi_service_can_defer_post_stage_analytics(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    calls: list[str] = []

    monkeypatch.setattr("autonomous_oi_service.load_autonomous_oi_progress", lambda: None)
    monkeypatch.setattr(
        "autonomous_oi_service.compute_autonomous_oi_snapshot_incremental_to_cycle",
        lambda **kwargs: ([("core",)], [("window",)], [("history",)], {"__v2_rows__": {}}, source_cycle),
    )
    monkeypatch.setattr("autonomous_oi_service.prune_inactive_state_rows", lambda: {})
    monkeypatch.setattr("autonomous_oi_service.replace_oi_core_state", lambda rows: calls.append("core"))
    monkeypatch.setattr("autonomous_oi_service.replace_oi_window_state", lambda rows: calls.append("windows"))
    monkeypatch.setattr("autonomous_oi_service.insert_oi_stage_history", lambda rows: calls.append("history"))
    monkeypatch.setattr("autonomous_oi_service.replace_core_state_v2", lambda rows: calls.append("core_v2"))
    monkeypatch.setattr("autonomous_oi_service.replace_window_state_v2", lambda rows: calls.append("windows_v2"))
    monkeypatch.setattr("autonomous_oi_service.insert_transition_history_v2", lambda rows: calls.append("history_v2"))
    monkeypatch.setattr("autonomous_oi_service.save_autonomous_oi_progress", lambda ts: calls.append("progress"))
    monkeypatch.setattr("autonomous_oi_service.update_post_stage_analytics", lambda rows, ts: calls.append("post_stage"))

    assert run_autonomous_oi_service(cycle_ts=source_cycle, run_post_stage_analytics=False) == 1
    assert "post_stage" not in calls


def test_run_post_stage_analytics_tail_uses_saved_progress_cycle_and_empty_live_rows(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    fallback_cycle = datetime(2026, 7, 16, 9, 10, tzinfo=timezone.utc)
    captured: list[tuple] = []

    monkeypatch.setattr("autonomous_oi_service.load_autonomous_oi_progress", lambda: source_cycle)
    monkeypatch.setattr(
        "autonomous_oi_service.update_post_stage_analytics",
        lambda rows, ts: captured.append((rows, ts)),
    )

    run_post_stage_analytics_tail(cycle_ts=fallback_cycle)

    assert captured == [([], source_cycle)]


def test_update_post_stage_analytics_backfills_from_history_when_live_rows_are_empty(monkeypatch) -> None:
    source_cycle = datetime(2026, 7, 16, 9, 5, tzinfo=timezone.utc)
    fetch_calls: list[str] = []
    execute_calls: list[str] = []

    def fake_fetch(sql, params=()):
        fetch_calls.append(sql)
        if "FROM oi_stage_history" in sql:
            return [
                {
                    "exchange": "BINANCE",
                    "symbol": "FLOCKUSDT",
                    "from_stage": 2,
                    "to_stage": 3,
                    "transition_reason": "early",
                    "transition_allowed": True,
                    "stage_age_before_transition": 45.0,
                    "cycle_ts": source_cycle,
                }
            ]
        if "FROM oi_post_stage_analytics" in sql:
            return []
        return []

    def fake_execute(sql, params=(), *extra):
        execute_calls.append(sql)

    monkeypatch.setattr("autonomous_oi_service.fetch", fake_fetch)
    monkeypatch.setattr("autonomous_oi_service.execute", fake_execute)
    monkeypatch.setattr(
        "autonomous_oi_service._raw_value_at_or_before",
        lambda table, column, exchange, symbol, ts: 1.23 if table == "price_raw" else 456.0,
    )

    update_post_stage_analytics([], source_cycle)

    assert any("FROM oi_stage_history" in sql for sql in fetch_calls)
    assert any("INSERT INTO oi_post_stage_analytics" in sql for sql in execute_calls)
