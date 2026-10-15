from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main
import run_autonomous_oi_once


def test_autonomous_watchdog_timeout_logs_byte_output_without_crashing(monkeypatch) -> None:
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=["runner"],
            timeout=120,
            output=b"child stdout\n",
            stderr=b"child stderr\n",
        )

    logs: list[str] = []
    monkeypatch.setattr(main.subprocess, "run", fake_run)
    monkeypatch.setattr(main, "log", logs.append)
    monkeypatch.setattr(main._timed_watchdog_step, "_timeout_streaks", {}, raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_inflight", set(), raising=False)

    result = main._timed_watchdog_step(
        [],
        "autonomous_oi_service",
        lambda: 0,
        "TEST_WATCHDOG_TIMEOUT",
        120,
    )

    assert result == -2
    assert any("WATCHDOG_TIMEOUT_STDOUT" in line and "child stdout" in line for line in logs)
    assert any("WATCHDOG_TIMEOUT_STDERR" in line and "child stderr" in line for line in logs)


def test_autonomous_watchdog_timeout_halves_replay_batch_for_retry(monkeypatch) -> None:
    captured = {}

    def fake_run(*args, **kwargs):
        captured.update(kwargs)
        raise subprocess.TimeoutExpired(cmd=["runner"], timeout=120)

    logs: list[str] = []
    monkeypatch.setenv("AUTONOMOUS_OI_MAX_SOURCE_CYCLES_PER_RUN", "10")
    monkeypatch.setattr(main.subprocess, "run", fake_run)
    monkeypatch.setattr(main, "log", logs.append)
    monkeypatch.setattr(main._timed_watchdog_step, "_timeout_streaks", {}, raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_inflight", set(), raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_oi_replay_batch_limit", 10, raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_oi_replay_successes", 0, raising=False)

    result = main._timed_watchdog_step(
        [],
        "autonomous_oi_service",
        lambda: 0,
        "TEST_WATCHDOG_TIMEOUT",
        120,
    )

    assert result == -2
    assert captured["env"]["AUTONOMOUS_OI_MAX_SOURCE_CYCLES_PER_RUN"] == "10"
    assert main._timed_watchdog_step._oi_replay_batch_limit == 5
    assert any("OI_REPLAY_BATCH_REDUCED" in line and "next=5" in line for line in logs)


def test_autonomous_watchdog_recovers_batch_after_three_successes(monkeypatch) -> None:
    captured = {}
    monkeypatch.setenv("AUTONOMOUS_OI_MAX_SOURCE_CYCLES_PER_RUN", "10")
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda *args, **kwargs: captured.update(kwargs) or type(
            "Completed", (), {"returncode": 0, "stdout": "1\n", "stderr": ""}
        )(),
    )
    logs: list[str] = []
    monkeypatch.setattr(main, "log", logs.append)
    monkeypatch.setattr(main._timed_watchdog_step, "_timeout_streaks", {}, raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_inflight", set(), raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_oi_replay_batch_limit", 5, raising=False)
    monkeypatch.setattr(main._timed_watchdog_step, "_oi_replay_successes", 2, raising=False)

    result = main._timed_watchdog_step(
        [],
        "autonomous_oi_service",
        lambda: 0,
        "TEST_WATCHDOG_TIMEOUT",
        120,
    )

    assert result == 1
    assert captured["env"]["AUTONOMOUS_OI_MAX_SOURCE_CYCLES_PER_RUN"] == "5"
    assert main._timed_watchdog_step._oi_replay_batch_limit == 6
    assert any("OI_REPLAY_BATCH_RECOVERED next=6" in line for line in logs)


def test_subprocess_runner_disables_duplicate_post_stage_analytics(monkeypatch, capsys) -> None:
    calls: list[dict] = []

    def fake_run_autonomous_oi_service(**kwargs) -> int:
        calls.append(kwargs)
        return 7

    monkeypatch.setattr(
        run_autonomous_oi_once,
        "run_autonomous_oi_service",
        fake_run_autonomous_oi_service,
    )

    run_autonomous_oi_once.main()

    assert calls == [{"run_post_stage_analytics": False}]
    assert capsys.readouterr().out == "7\n"
