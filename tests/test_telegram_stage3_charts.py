from __future__ import annotations

from datetime import datetime, timezone

import pytest
import telegram_bot
from asset_universe_guard import UniverseDecision


def _png(tmp_path, name: str) -> str:
    path = tmp_path / name
    path.write_bytes(b"fake-png")
    return str(path)


def _delivery(mode: str = "text") -> telegram_bot.TelegramDeliveryResult:
    return telegram_bot.TelegramDeliveryResult(
        ok=True,
        message_id=123,
        chat_id="456",
        delivered_at=datetime.now(timezone.utc),
        attempts=1,
        delivery_mode=mode,
    )


def _mock_stage3_volume_queue(monkeypatch, growth: float):
    unlocked_at = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    state = {
        "source": "BINANCE",
        "symbol": "TESTUSDT",
        "ready": True,
        "quality_reason": "ready",
        "growth_4h_pct": growth,
        "growth_1h_pct": 25.0,
        "current_1h_quote": 1500.0,
        "current_4h_quote": 6000.0,
        "previous_1h_quote": 1200.0,
        "previous_4h_quote": 3000.0,
        "source_cycle_ts": unlocked_at,
    }
    monkeypatch.setattr(telegram_bot, "_db_quote_turnover_state", lambda _symbol, *_args: dict(state))

    def fake_sync(candidates):
        records = {}
        waiting = unlocked = 0
        for candidate in candidates:
            record = dict(candidate)
            if candidate.get("status") == "blocked_universe":
                record["status"] = "blocked_universe"
                record["volume_snapshot"] = None
            elif growth >= 100.0:
                record["status"] = "unlocked"
                record["volume_unlocked_at"] = unlocked_at
                record["volume_snapshot"] = {
                    "source": "BINANCE",
                    "source_cycle_ts": unlocked_at.isoformat(),
                    "current_1h_quote": 1500.0,
                    "growth_1h_pct": 25.0,
                    "current_4h_quote": 6000.0,
                    "growth_4h_pct": growth,
                }
                unlocked += 1
            else:
                record["status"] = "waiting_volume"
                record["volume_snapshot"] = None
                waiting += 1
            records[(candidate["exchange"], candidate["symbol"])] = record
        return {"candidates": records, "waiting": waiting, "unlocked": unlocked}

    monkeypatch.setattr(telegram_bot, "sync_stage3_volume_queue", fake_sync)
    monkeypatch.setattr(telegram_bot, "mark_stage3_volume_queue_sent", lambda *_args: None)


def _chart(paths: list[str], captured: tuple[str, ...]) -> dict:
    return {
        "requested": True,
        "paths": paths,
        "requested_timeframes": ("5m", "4H"),
        "captured_timeframes": captured,
        "capture_seconds": 1.25,
        "timeframe_seconds": {"5m": 0.5, "4H": 0.75},
        "timeframe_verification_failures": tuple(
            tf for tf in ("5m", "4H") if tf not in captured
        ),
        "failure_reason": None if paths else "capture_failed",
    }


def test_stage3_charts_two_frames_send_album(monkeypatch, tmp_path):
    paths = [_png(tmp_path, "5m.png"), _png(tmp_path, "4h.png")]
    monkeypatch.setattr(telegram_bot, "_try_chart_screenshot", lambda row: _chart(paths, ("5m", "4H")))
    monkeypatch.setattr(telegram_bot, "_send_media_group_result", lambda *a, **k: _delivery("album"))
    monkeypatch.setattr(telegram_bot, "_send_photo_result", lambda *a, **k: (_ for _ in ()).throw(AssertionError("photo called")))
    monkeypatch.setattr(telegram_bot, "send_message_result", lambda *a, **k: (_ for _ in ()).throw(AssertionError("text called")))

    result = telegram_bot.send_stage3_alert_result({"symbol": "ABCUSDT", "exchange": "BYBIT"}, "text")

    assert result.ok is True
    assert result.delivery_mode == "album"
    assert result.chart_requested is True
    assert result.chart_captured is True
    assert result.chart_captured_timeframes == ("5m", "4H")


def test_stage3_charts_only_5m_sends_photo_not_text(monkeypatch, tmp_path):
    paths = [_png(tmp_path, "5m.png")]
    monkeypatch.setattr(telegram_bot, "_try_chart_screenshot", lambda row: _chart(paths, ("5m",)))
    monkeypatch.setattr(telegram_bot, "_send_photo_result", lambda *a, **k: _delivery("photo"))
    monkeypatch.setattr(telegram_bot, "_send_media_group_result", lambda *a, **k: (_ for _ in ()).throw(AssertionError("album called")))
    monkeypatch.setattr(telegram_bot, "send_message_result", lambda *a, **k: (_ for _ in ()).throw(AssertionError("text called")))

    result = telegram_bot.send_stage3_alert_result({"symbol": "ABCUSDT", "exchange": "BYBIT"}, "text")

    assert result.ok is True
    assert result.delivery_mode == "photo"
    assert result.chart_captured_timeframes == ("5m",)
    assert result.chart_timeframe_verification_failures == ("4H",)


def test_stage3_charts_no_valid_frames_falls_back_to_text(monkeypatch):
    monkeypatch.setattr(telegram_bot, "_try_chart_screenshot", lambda row: _chart([], ()))
    monkeypatch.setattr(telegram_bot, "send_message_result", lambda *a, **k: _delivery("text"))

    result = telegram_bot.send_stage3_alert_result({"symbol": "ABCUSDT", "exchange": "BYBIT"}, "text")

    assert result.ok is True
    assert result.delivery_mode == "text"
    assert result.chart_captured is False
    assert result.chart_capture_failure_reason == "capture_failed"


def test_stage3_charts_media_error_does_not_lose_signal(monkeypatch, tmp_path):
    paths = [_png(tmp_path, "5m.png"), _png(tmp_path, "4h.png")]
    monkeypatch.setattr(telegram_bot, "_try_chart_screenshot", lambda row: _chart(paths, ("5m", "4H")))

    def fail_album(*args, **kwargs):
        raise RuntimeError("telegram media failed")

    monkeypatch.setattr(telegram_bot, "_send_media_group_result", fail_album)
    monkeypatch.setattr(telegram_bot, "send_message_result", lambda *a, **k: _delivery("text"))

    result = telegram_bot.send_stage3_alert_result({"symbol": "ABCUSDT", "exchange": "BYBIT"}, "text")

    assert result.ok is True
    assert result.delivery_mode == "text"
    assert result.chart_delivery_failure_reason == "RuntimeError"
    assert "график_не_доставлен:RuntimeError" in result.media_alerts


def test_stage3_alerts_limit_new_sends_per_cycle(monkeypatch):
    rows = [
        {
            "exchange": "BYBIT",
            "symbol": f"SYM{i}USDT",
            "stage3_transition_ts": f"2026-07-25T10:0{i}:00+00:00",
            "stage3_transition_age_minutes": 1,
        }
        for i in range(3)
    ]
    sent_symbols = []

    def fake_rows(query, *args, **kwargs):
        if "FROM core_state_v2 c" in query:
            return rows
        if "SELECT COUNT(*) AS cnt" in query:
            return [{"cnt": 0}]
        return []

    def fake_send(row, *args, **kwargs):
        sent_symbols.append(row["symbol"])
        return _delivery("text")

    monkeypatch.setenv("STAGE3_ALERTS_MAX_NEW_PER_CYCLE", "2")
    monkeypatch.setattr(telegram_bot, "_safe_rows", fake_rows)
    monkeypatch.setattr(telegram_bot, "_read_stage3_alerted_keys", lambda: set())
    _mock_stage3_volume_queue(monkeypatch, 150.0)
    monkeypatch.setattr(telegram_bot, "_live_market_metrics", lambda *_args: {"klines": {"vol_pct_4h": 100.0}, "volume_source": "BINANCE"})
    monkeypatch.setattr(telegram_bot, "_build_stage3_alert_text", lambda row: "text")
    monkeypatch.setattr(telegram_bot, "send_stage3_alert_result", fake_send)
    monkeypatch.setattr(telegram_bot, "_append_stage3_alert_history", lambda *a, **k: None)
    monkeypatch.setattr(telegram_bot, "_append_stage3_delivery_history", lambda *a, **k: None)
    monkeypatch.setattr(telegram_bot, "_asset_universe_decide", lambda exchange, symbol: UniverseDecision(True, "test_fixture", "enforce", exchange, symbol))
    monkeypatch.setattr(
        telegram_bot,
        "_stage3_cycle_budget_state",
        lambda: {"is_thin_reserve": False, "cycle_latency_class": "normal", "cycle_reserve_pct": 50},
    )

    result = telegram_bot.check_stage3_alerts()

    assert result["sent_count"] == 2
    assert sent_symbols == ["SYM0USDT", "SYM1USDT"]
    assert result["signals_waiting_confirmation"] == 0


def test_db_quote_shadow_never_blocks_the_existing_live_gate(monkeypatch):
    monkeypatch.setattr(telegram_bot, "_resolve_binance_symbol", lambda _symbol: "TESTUSDT")
    monkeypatch.setattr(
        telegram_bot,
        "_safe_rows",
        lambda *_args, **_kwargs: [{"ready": False, "growth_4h_pct": None, "quality_reason": "warming_up"}],
    )

    state = telegram_bot._db_quote_turnover_state("TESTUSDT")

    assert state["source"] == "BINANCE"
    assert state["ready"] is False
    assert telegram_bot._db_quote_turnover_gate(state) == (False, "warming_up")
    assert telegram_bot._stage3_volume_gate({"klines": {"vol_pct_4h": 120.0}}) == (True, "pass")


def test_stage3_alerts_adapt_limit_when_cycle_reserve_is_thin(monkeypatch):
    rows = [
        {
            "exchange": "BYBIT",
            "symbol": f"SYM{i}USDT",
            "stage3_transition_ts": f"2026-07-25T10:0{i}:00+00:00",
            "stage3_transition_age_minutes": 1,
        }
        for i in range(3)
    ]
    sent_symbols = []

    def fake_rows(query, *args, **kwargs):
        if "FROM core_state_v2 c" in query:
            return rows
        if "SELECT COUNT(*) AS cnt" in query:
            return [{"cnt": 0}]
        return []

    def fake_send(row, *args, **kwargs):
        sent_symbols.append(row["symbol"])
        return _delivery("text")

    monkeypatch.setenv("STAGE3_ALERTS_MAX_NEW_PER_CYCLE", "2")
    monkeypatch.setenv("STAGE3_ALERTS_LOW_RESERVE_MAX_NEW_PER_CYCLE", "1")
    monkeypatch.setenv("STAGE3_ALERTS_THIN_RESERVE_PCT", "20")
    monkeypatch.setattr(telegram_bot, "_safe_rows", fake_rows)
    monkeypatch.setattr(telegram_bot, "_read_stage3_alerted_keys", lambda: set())
    _mock_stage3_volume_queue(monkeypatch, 150.0)
    monkeypatch.setattr(telegram_bot, "_live_market_metrics", lambda *_args: {"klines": {"vol_pct_4h": 100.0}, "volume_source": "BINANCE"})
    monkeypatch.setattr(telegram_bot, "_build_stage3_alert_text", lambda row: "text")
    monkeypatch.setattr(telegram_bot, "send_stage3_alert_result", fake_send)
    monkeypatch.setattr(telegram_bot, "_append_stage3_alert_history", lambda *a, **k: None)
    monkeypatch.setattr(telegram_bot, "_append_stage3_delivery_history", lambda *a, **k: None)
    monkeypatch.setattr(telegram_bot, "_asset_universe_decide", lambda exchange, symbol: UniverseDecision(True, "test_fixture", "enforce", exchange, symbol))
    monkeypatch.setattr(
        telegram_bot,
        "_runtime_snapshot",
        lambda: ({}, {"cycle_latency_class": "thin_reserve", "cycle_reserve_pct": 5}),
    )

    result = telegram_bot.check_stage3_alerts()

    assert result["sent_count"] == 1
    assert sent_symbols == ["SYM0USDT"]
    assert result["stage3_alerts_max_new_per_cycle"] == 1
    assert result["stage3_alerts_adaptive_limit_applied"] is True


def test_coin_message_replaces_oi_slopes_with_tiger_market_snapshot(monkeypatch):
    monkeypatch.setattr(
        telegram_bot,
        "_live_market_metrics",
        lambda symbol, exchange: {
            "rank": "#1235",
            "klines": {
                "vol_1h_usd": 43_480_000,
                "vol_4h_usd": 202_050_000,
                "vol_pct_1h": -39.63,
                "vol_pct_4h": -21.18,
                "price_pct_1h": 20.20,
                "price_pct_4h": 53.08,
            },
            "ticker24": {"price_pct_24h": -20.79},
            "oi": {"oi_now_usd": 17_900_000, "oi_pct_5m": 0.59, "oi_pct_4h": 34.14},
            "accounts": {"long_pct": 41.33, "short_pct": 58.67},
            "funding": {"funding_pct": 0.11},
            "market_source": "BINANCE",
            "funding_source": "BYBIT",
        },
    )
    message = telegram_bot._build_coin_message(
        {"symbol": "ABCUSDT", "exchange": "BYBIT", "current_stage": 3, "stage_age_minutes": 0, "latest_cycle_ts": "2026-07-12T10:00:00+00:00"},
        [],
        [],
        {},
        title="🥇 NEW STAGE 3",
    )

    assert "<b>🏷 Капа-рейтинг:</b> #1235" in message
    assert "1ч: $43.48M | ⬇️ -39.63%" in message
    assert "4ч: ⬆️ +53.08%❗️❗️" in message
    assert "сейчас: $17.90M" in message
    assert "⬇️ лонг: +41.33% | ⬆️ шорт: +58.67%" in message
    assert "<b>💵 Объём (Binance):</b>" in message
    assert "<b>📈 Рост цены (Binance):</b>" in message
    assert "<b>📊 Открытый интерес (Binance):</b>" in message
    assert "<b>👥 Аккаунты (Binance):</b>" in message
    assert "<b>🩸 Фандинг (Bybit):</b>" in message
    assert "<b>Наклонка OI</b>" not in message


def test_coin_message_marks_mapped_bybit_analogue(monkeypatch):
    monkeypatch.setattr(telegram_bot, "_live_market_metrics", lambda *_args: {})
    monkeypatch.setattr(telegram_bot, "_resolve_bybit_symbol", lambda _symbol: ("GEUSDT", "mapped"))
    message = telegram_bot._build_coin_message(
        {"symbol": "GUSDT", "exchange": "BINANCE", "current_stage": 3},
        [], [], {}, title="🥇 NEW STAGE 3",
    )
    assert "⚠️ Bybit аналог: <b>GEUSDT</b>" in message


def test_coin_message_marks_coin_missing_on_bybit(monkeypatch):
    monkeypatch.setattr(telegram_bot, "_live_market_metrics", lambda *_args: {})
    monkeypatch.setattr(telegram_bot, "_resolve_bybit_symbol", lambda _symbol: ("", ""))
    message = telegram_bot._build_coin_message(
        {"symbol": "ONLYBINANCEUSDT", "exchange": "BINANCE", "current_stage": 3},
        [], [], {}, title="🥇 NEW STAGE 3",
    )
    assert "⚠️ На Bybit этой монеты нет" in message


def test_coin_message_has_no_bybit_warning_for_exact_symbol(monkeypatch):
    monkeypatch.setattr(telegram_bot, "_live_market_metrics", lambda *_args: {})
    monkeypatch.setattr(telegram_bot, "_resolve_bybit_symbol", lambda _symbol: ("ABCUSDT", "exact"))
    message = telegram_bot._build_coin_message(
        {"symbol": "ABCUSDT", "exchange": "BINANCE", "current_stage": 3},
        [], [], {}, title="🥇 NEW STAGE 3",
    )
    assert "⚠️" not in message


def test_stage3_volume_gate_requires_known_growth_of_at_least_100_percent():
    assert telegram_bot._stage3_volume_gate({"klines": {"vol_pct_4h": 100.0}}) == (True, "pass")
    assert telegram_bot._stage3_volume_gate({"klines": {"vol_pct_4h": 99.99}}) == (False, "below_100pct")
    assert telegram_bot._stage3_volume_gate({"klines": {"vol_pct_4h": None}}) == (False, "missing_4h_volume")


def test_live_metrics_fall_back_to_bybit_volume_only_when_binance_contract_is_absent(monkeypatch):
    monkeypatch.setattr(telegram_bot, "_resolve_binance_symbol", lambda _symbol: "")
    monkeypatch.setattr(telegram_bot, "_resolve_bybit_symbol", lambda _symbol: ("XDCUSDT", "exact"))
    monkeypatch.setattr(
        telegram_bot,
        "_fetch_bybit_kline_metrics",
        lambda _symbol: {"vol_4h_usd": 500_000, "vol_pct_4h": 123.0},
    )
    monkeypatch.setattr(telegram_bot, "_coingecko_rank", lambda _symbol: "#103")
    monkeypatch.setattr(telegram_bot, "_fetch_binance_24h_metrics", lambda _symbol: {})
    monkeypatch.setattr(telegram_bot, "_fetch_binance_oi_metrics", lambda _symbol: {})
    monkeypatch.setattr(telegram_bot, "_fetch_binance_account_ratio", lambda _symbol: {})
    monkeypatch.setattr(telegram_bot, "_fetch_bybit_funding", lambda *_args: {})
    telegram_bot._api_cache.clear()

    metrics = telegram_bot._live_market_metrics("XDCUSDT", "BYBIT")

    assert metrics["volume_source"] == "BYBIT"
    assert metrics["klines"]["vol_pct_4h"] == 123.0


def test_closed_minute_rows_exclude_the_in_progress_candle():
    rows = [[0] * 8, [60_000] * 8, [120_000] * 8]

    assert telegram_bot._closed_minute_kline_rows(rows, now_ms=150_000) == rows[:2]


def test_stage3_queue_keeps_old_candidate_until_db_window_unlocks(monkeypatch):
    row = {
        "exchange": "BINANCE",
        "symbol": "WAITUSDT",
        "stage3_transition_ts": "2026-09-22T10:00:00+00:00",
        "stage3_transition_age_minutes": 1440,
    }
    sent = []
    monkeypatch.setattr(telegram_bot, "_safe_rows", lambda query, *_a, **_k: [row] if "FROM core_state_v2 c" in query else ([{"cnt": 0}] if "SELECT COUNT(*) AS cnt" in query else []))
    monkeypatch.setattr(telegram_bot, "_read_stage3_alerted_keys", lambda: set())
    _mock_stage3_volume_queue(monkeypatch, 70.0)
    monkeypatch.setattr(telegram_bot, "_asset_universe_decide", lambda ex, sym: UniverseDecision(True, "test", "enforce", ex, sym))
    monkeypatch.setattr(telegram_bot, "send_stage3_alert_result", lambda *a, **k: sent.append(a) or _delivery())
    monkeypatch.setattr(telegram_bot, "_stage3_cycle_budget_state", lambda: {"is_thin_reserve": False, "cycle_latency_class": "normal", "cycle_reserve_pct": 50})

    result = telegram_bot.check_stage3_alerts()

    assert sent == []
    assert result["signals_waiting_volume"] == 1
    assert result["stage3_volume_queue"]["waiting"] == 1


def test_stage3_alerts_do_not_send_when_the_4h_volume_gate_fails(monkeypatch):
    rows = [{
        "exchange": "BINANCE",
        "symbol": "LOWVOLUSDT",
        "stage3_transition_ts": "2026-09-22T10:00:00+00:00",
        "stage3_transition_age_minutes": 1,
    }]
    sent = []

    def fake_rows(query, *args, **kwargs):
        if "FROM core_state_v2 c" in query:
            return rows
        if "SELECT COUNT(*) AS cnt" in query:
            return [{"cnt": 0}]
        return []

    monkeypatch.setattr(telegram_bot, "_safe_rows", fake_rows)
    monkeypatch.setattr(telegram_bot, "_read_stage3_alerted_keys", lambda: set())
    _mock_stage3_volume_queue(monkeypatch, 99.0)
    monkeypatch.setattr(telegram_bot, "_asset_universe_decide", lambda exchange, symbol: UniverseDecision(True, "test", "enforce", exchange, symbol))
    monkeypatch.setattr(telegram_bot, "_live_market_metrics", lambda *_args: {"klines": {"vol_pct_4h": 99.0}})
    monkeypatch.setattr(telegram_bot, "_enrich_stage3_decision_snapshot", lambda row: row)
    monkeypatch.setattr(telegram_bot, "_build_stage3_alert_text", lambda row: "should not be built")
    monkeypatch.setattr(telegram_bot, "send_stage3_alert_result", lambda *args, **kwargs: sent.append(args) or _delivery())
    monkeypatch.setattr(telegram_bot, "_stage3_cycle_budget_state", lambda: {"is_thin_reserve": False, "cycle_latency_class": "normal", "cycle_reserve_pct": 50})

    result = telegram_bot.check_stage3_alerts()

    assert sent == []
    assert result["signals_filtered_by_volume"] == 1

def test_stage3_universe_block_is_terminal_not_an_unlocked_queue_item(monkeypatch):
    row = {
        "exchange": "BYBIT",
        "symbol": "STOCKUSDT",
        "stage3_transition_ts": "2026-09-23T08:00:00+00:00",
        "latest_cycle_ts": "2026-09-23T08:05:00+00:00",
    }
    blocked = []
    monkeypatch.setattr(telegram_bot, "_safe_rows", lambda query, *_a, **_k: [row] if "FROM core_state_v2 c" in query else ([{"cnt": 0}] if "SELECT COUNT(*) AS cnt" in query else []))
    monkeypatch.setattr(telegram_bot, "_read_stage3_alerted_keys", lambda: set())
    _mock_stage3_volume_queue(monkeypatch, 150.0)
    monkeypatch.setattr(telegram_bot, "_asset_universe_decide", lambda ex, sym: UniverseDecision(False, "blocked:asset_class_stock", "enforce", ex, sym))
    monkeypatch.setattr(telegram_bot, "mark_stage3_volume_queue_blocked", lambda *args: blocked.append(args))
    monkeypatch.setattr(telegram_bot, "send_stage3_alert_result", lambda *a, **k: (_ for _ in ()).throw(AssertionError("blocked stock must not send")))
    monkeypatch.setattr(telegram_bot, "_stage3_cycle_budget_state", lambda: {"is_thin_reserve": False, "cycle_latency_class": "normal", "cycle_reserve_pct": 50})

    result = telegram_bot.check_stage3_alerts()

    assert len(blocked) == 1
    assert blocked[0][0:2] == ("BYBIT", "STOCKUSDT")
    assert blocked[0][3] == "blocked:asset_class_stock"
    assert result["signals_filtered_by_universe"] == 1
    assert result["stage3_volume_queue"]["unlocked"] == 0

def test_db_quote_turnover_state_builds_distribution_from_persisted_raw_window(monkeypatch):
    points = [49000.0] + [1000.0] * 47
    start = datetime(2026, 9, 23, 4, 0, tzinfo=timezone.utc)
    opens = [start + telegram_bot.timedelta(minutes=5 * i) for i in range(48)]
    captured = {}

    def fake_rows(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [{
            "ready": True,
            "quality_reason": "ready",
            "growth_4h_pct": 100.0,
            "current_4h_quote": 96000.0,
            "source_cycle_ts": datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
            "latest_ts_close": datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
            "current_4h_quote_opens": opens,
            "current_4h_quote_points": points,
        }]

    monkeypatch.setattr(telegram_bot, "_resolve_binance_symbol", lambda _symbol: "TESTUSDT")
    monkeypatch.setattr(telegram_bot, "_safe_rows", fake_rows)

    state = telegram_bot._db_quote_turnover_state("TESTUSDT")

    assert "FROM volume_raw" in captured["sql"]
    assert "LEFT JOIN stage3_volume_queue first_queue" in captured["sql"]
    assert "ts_close <= q.latest_ts_close" in captured["sql"]
    assert "ORDER BY v.ts_open DESC" in captured["sql"]
    assert "LIMIT 48" in captured["sql"]
    assert captured["params"] == (None, "TESTUSDT", None, "BINANCE", "TESTUSDT")
    assert state["current_4h_distribution"]["hourly_quote_totals"] == [60000.0, 12000.0, 12000.0, 12000.0]
    assert state["current_4h_distribution"]["largest_5m_share_pct"] == pytest.approx(49000.0 * 100.0 / 96000.0)
    assert sum(state["current_4h_distribution"]["hourly_quote_totals"]) == state["current_4h_quote"]
    assert state["current_4h_distribution_status"] == "ok"


def test_stage3_observation_includes_distribution_without_changing_gate_inputs():
    state = {
        "source": "BINANCE",
        "symbol": "TESTUSDT",
        "source_cycle_ts": datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
        "ready": True,
        "quality_reason": "ready",
        "growth_4h_pct": 100.0,
        "current_4h_distribution": {
            "hourly_quote_totals": [60000.0, 12000.0, 12000.0, 12000.0],
            "largest_5m_share_pct": 50.0,
            "top3_5m_share_pct": 53.0,
        },
    }

    observation = telegram_bot._stage3_volume_observation_snapshot(state)

    assert observation["current_4h_distribution"] == state["current_4h_distribution"]
    assert observation["volume_ready"] is True
    assert observation["volume_quality_reason"] == "ready"
    assert observation["growth_4h_pct"] == 100.0
    assert telegram_bot._db_quote_turnover_gate({
        "ready": True, "growth_4h_pct": 99.99,
        "current_4h_distribution": state["current_4h_distribution"],
    }) == (False, "below_100pct")
    assert telegram_bot._db_quote_turnover_gate({
        "ready": True, "growth_4h_pct": 100.0,
        "current_4h_distribution": None,
        "current_4h_distribution_status": "window_total_mismatch",
    }) == (True, "pass")
def test_stage3_observation_timestamp_uses_fresh_volume_source_cycle_not_stale_phase_row(monkeypatch):
    source_cycle_ts = datetime(2026, 9, 23, 8, 10, tzinfo=timezone.utc)
    stale_latest_cycle_ts = datetime(2026, 9, 23, 8, 5, tzinfo=timezone.utc)
    row = {
        "exchange": "BINANCE",
        "symbol": "TESTUSDT",
        "stage3_transition_ts": datetime(2026, 9, 23, 7, 0, tzinfo=timezone.utc),
        "latest_cycle_ts": stale_latest_cycle_ts,
    }
    state = {
        "source": "BINANCE",
        "symbol": "TESTUSDT",
        "source_cycle_ts": source_cycle_ts,
        "ready": True,
        "quality_reason": "ready",
        "growth_4h_pct": 70.0,
        "current_4h_distribution": {"hourly_quote_totals": [1, 2, 3, 4]},
        "current_4h_distribution_status": "ok",
    }
    captured = {}
    monkeypatch.setattr(telegram_bot, "_safe_rows", lambda query, *_a, **_k: [row] if "FROM core_state_v2 c" in query else [])
    monkeypatch.setattr(telegram_bot, "_read_stage3_alerted_keys", lambda: set())
    monkeypatch.setattr(telegram_bot, "_stage3_cycle_budget_state", lambda: {"is_thin_reserve": False, "cycle_latency_class": "normal", "cycle_reserve_pct": 50})
    monkeypatch.setattr(telegram_bot, "_asset_universe_decide", lambda ex, sym: UniverseDecision(True, "test", "enforce", ex, sym))
    monkeypatch.setattr(telegram_bot, "_db_quote_turnover_state", lambda _symbol, *_args: dict(state))
    monkeypatch.setattr(telegram_bot, "sync_stage3_volume_queue", lambda candidates: captured.setdefault("candidate", candidates[0]) and {
        "candidates": {(candidates[0]["exchange"], candidates[0]["symbol"]): {**candidates[0], "status": "waiting_volume", "volume_snapshot": None}},
        "waiting": 1,
        "unlocked": 0,
    })

    telegram_bot.check_stage3_alerts()

    assert captured["candidate"]["observed_at"] == source_cycle_ts
