from __future__ import annotations

import queue
from datetime import datetime, timezone

import telegram_bot


class _Response:
    ok = True
    status_code = 200
    text = ""

    def json(self):
        return {"ok": True, "result": {"message_id": 7, "chat": {"id": -10077}}}


def _delivery() -> telegram_bot.TelegramDeliveryResult:
    return telegram_bot.TelegramDeliveryResult(
        ok=True,
        message_id=1,
        chat_id="primary",
        delivered_at=datetime.now(timezone.utc),
        attempts=1,
    )


def _reset_channel_runtime(monkeypatch, tmp_path, *, queue_size: int = 2):
    target = tmp_path / "signal_channel_chat_id.txt"
    monkeypatch.setattr(telegram_bot, "SIGNAL_CHANNEL_TARGET_FILE", target)
    monkeypatch.setattr(telegram_bot, "_signal_channel_delivery_queue", queue.Queue(maxsize=queue_size))
    monkeypatch.setattr(telegram_bot, "TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(telegram_bot, "BASE", "https://api.telegram.org/bottest-token")
    return target


def test_first_channel_post_is_captured_once(monkeypatch, tmp_path):
    target = _reset_channel_runtime(monkeypatch, tmp_path)

    assert telegram_bot._capture_signal_channel_id({"chat": {"type": "channel", "id": -100111}}) is True
    assert target.read_text(encoding="utf-8") == "-100111"
    assert telegram_bot._capture_signal_channel_id({"chat": {"type": "channel", "id": -100222}}) is False
    assert target.read_text(encoding="utf-8") == "-100111"


def test_channel_copy_enqueue_is_bounded_nonblocking(monkeypatch, tmp_path):
    target = _reset_channel_runtime(monkeypatch, tmp_path, queue_size=1)
    target.write_text("-100111", encoding="utf-8")

    assert telegram_bot._enqueue_signal_channel_copy("signal", ["/tmp/chart.png"], "HTML") is True
    assert telegram_bot._signal_channel_delivery_queue.get_nowait() == {
        "chat_id": "-100111", "text": "signal", "paths": ["/tmp/chart.png"], "parse_mode": "HTML"
    }
    telegram_bot._signal_channel_delivery_queue.put_nowait({"existing": True})
    assert telegram_bot._enqueue_signal_channel_copy("dropped", [], "HTML") is False


def test_channel_worker_sends_text_to_captured_channel(monkeypatch, tmp_path):
    target = _reset_channel_runtime(monkeypatch, tmp_path)
    target.write_text("-100111", encoding="utf-8")
    calls = []
    monkeypatch.setattr(telegram_bot.requests, "post", lambda *a, **k: calls.append((a, k)) or _Response())

    telegram_bot._deliver_signal_channel_job({"chat_id": "-100111", "text": "<b>signal</b>", "paths": [], "parse_mode": "HTML"})

    assert calls[0][0][0].endswith("/sendMessage")
    assert calls[0][1]["json"]["chat_id"] == "-100111"


def test_channel_worker_sends_album_and_cleans_media(monkeypatch, tmp_path):
    target = _reset_channel_runtime(monkeypatch, tmp_path)
    target.write_text("-100111", encoding="utf-8")
    paths = []
    for name in ("5m.png", "4h.png"):
        path = tmp_path / name
        path.write_bytes(b"fake-png")
        paths.append(str(path))
    calls = []
    monkeypatch.setattr(telegram_bot.requests, "post", lambda *a, **k: calls.append((a, k)) or _Response())

    telegram_bot._process_signal_channel_job({"chat_id": "-100111", "text": "signal", "paths": paths, "parse_mode": "HTML"})

    assert calls[0][0][0].endswith("/sendMediaGroup")
    assert all(not __import__("os").path.exists(path) for path in paths)


def test_channel_failure_cannot_break_primary_stage3_delivery(monkeypatch, tmp_path):
    _reset_channel_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr(telegram_bot, "_try_chart_screenshot", lambda row: {"requested": False, "paths": []})
    monkeypatch.setattr(telegram_bot, "send_message_result", lambda *a, **k: _delivery())
    monkeypatch.setattr(telegram_bot, "_enqueue_signal_channel_copy", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("channel down")))

    result = telegram_bot.send_stage3_alert_result({"symbol": "ABCUSDT", "exchange": "BYBIT"}, "signal", to_group=True)

    assert result.ok is True
