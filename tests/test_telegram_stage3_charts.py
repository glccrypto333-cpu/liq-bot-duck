from __future__ import annotations

from datetime import datetime, timezone

import telegram_bot


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
