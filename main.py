from __future__ import annotations
import time
import os
import json
import sys
import resource
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import threading
import traceback
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

from config import (
    APP_VERSION,
    СТАРТОВОЕ_СООБЩЕНИЕ,
    ДНЕЙ_ХРАНЕНИЯ,
    КОМАНДЫ,
    ИНТЕРВАЛ_ЦИКЛА_СЕК,
    ЛИМИТ_СИМВОЛОВ_BYBIT,
    ЛИМИТ_СИМВОЛОВ_BINANCE,
    BYBIT_COLLECT_WORKERS,
    BINANCE_COLLECT_WORKERS,
    ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК,
    AGGREGATES_EVERY_CYCLES,
    MAX_COLLECT_SECONDS_FOR_AGGREGATES,
)
from logger import log
from db import init_db, upsert_oi, upsert_price, upsert_volume, cleanup_old, migrate_canonical_ts_close, replace_active_universe, replace_request_failures, load_quarantine_symbols
from exchange_clients import (
    fetch_bybit_symbols,
    fetch_binance_symbols,
    fetch_bybit_oi_5m,
    fetch_binance_oi_5m,
    fetch_bybit_kline_5m,
    fetch_binance_kline_5m,
)
from aggregation_engine import rebuild_bot_aggregates
from audit_engine import rebuild_all
from research_engine import rebuild_market_research
from market_silence_engine import rebuild_market_silence
from market_price_engine import rebuild_price_state
from market_volume_engine import rebuild_volume_state
from market_oi_slope_engine import rebuild_oi_slope
from market_phase_engine import rebuild_market_phase
from market_phase_source import rebuild_market_phase_source
from export_engine import rebuild_exports
from telegram_bot import start_polling, send_message
from runtime_mode import runtime_mode_text


def _write_runtime_timing_report(timings: list[tuple[str, float]]) -> None:
    runtime_dir = Path("runtime")
    runtime_dir.mkdir(exist_ok=True)

    total = sum(seconds for _, seconds in timings)
    lines = [
        f"generated_at={datetime.now(timezone.utc).isoformat()}",
        f"total_seconds={round(total, 2)}",
        "",
        "step,seconds",
    ]

    for name, seconds in timings:
        lines.append(f"{name},{round(seconds, 2)}")

    (runtime_dir / "runtime_timing_report.txt").write_text("\n".join(lines) + "\n")



def _runtime_memory_mb() -> float:
    try:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / 1024 / 1024
        return rss / 1024
    except Exception:
        return 0.0


def _timed_step(timings: list[tuple[str, float]], name: str, fn):
    started = time.time()
    result = fn()
    elapsed = time.time() - started
    timings.append((name, elapsed))
    log(
        f"step resource: {name}={elapsed:.2f}s "
        f"memory_max_rss_mb={_runtime_memory_mb():.2f}"
    )
    return result



def _collect_binance_symbol(symbol: str):
    oi_rows = []
    price_rows = []
    volume_rows = []
    failures = []

    try:
        oi_rows.extend(fetch_binance_oi_5m(symbol, 24))
    except Exception as exc:
        failures.append(("BINANCE", symbol, "OI", exc))

    try:
        p, v = fetch_binance_kline_5m(symbol, 24)
        price_rows.extend(p)
        volume_rows.extend(v)
    except Exception as exc:
        failures.append(("BINANCE", symbol, "PRICE_VOLUME", exc))

    return oi_rows, price_rows, volume_rows, failures


def collect(symbols_bybit, symbols_binance):
    collect_started = time.time()
    oi_rows, price_rows, volume_rows = [], [], []
    failures = []
    now = datetime.now(timezone.utc)

    def record_failure(exchange: str, symbol: str, data_type: str, exc: Exception) -> None:
        failures.append((now, exchange, symbol, data_type, type(exc).__name__, str(exc)[:500]))

    bybit_collect_symbols = symbols_bybit

    def collect_bybit_symbol(s: str):
        local_oi, local_price, local_volume = [], [], []
        try:
            local_oi.extend(fetch_bybit_oi_5m(s, 24))
        except Exception as exc:
            record_failure("BYBIT", s, "OI", exc)

        try:
            p, v = fetch_bybit_kline_5m(s, 24)
            local_price.extend(p)
            local_volume.extend(v)
        except Exception as exc:
            record_failure("BYBIT", s, "PRICE_VOLUME", exc)

        return local_oi, local_price, local_volume

    bybit_workers = max(1, BYBIT_COLLECT_WORKERS)
    bybit_started = time.time()

    with ThreadPoolExecutor(max_workers=bybit_workers) as executor:
        futures = [
            executor.submit(collect_bybit_symbol, symbol)
            for symbol in bybit_collect_symbols
        ]

        for future in as_completed(futures):
            local_oi, local_price, local_volume = future.result()
            oi_rows.extend(local_oi)
            price_rows.extend(local_price)
            volume_rows.extend(local_volume)

    bybit_seconds = time.time() - bybit_started

    binance_collect_symbols = (
        symbols_binance
        if ЛИМИТ_СИМВОЛОВ_BINANCE <= 0
        else symbols_binance[:ЛИМИТ_СИМВОЛОВ_BINANCE]
    )

    workers = max(1, BINANCE_COLLECT_WORKERS)
    binance_started = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_collect_binance_symbol, symbol)
            for symbol in binance_collect_symbols
        ]

        for future in as_completed(futures):
            b_oi_rows, b_price_rows, b_volume_rows, symbol_failures = future.result()

            oi_rows.extend(b_oi_rows)
            price_rows.extend(b_price_rows)
            volume_rows.extend(b_volume_rows)

            for exchange, symbol, data_type, exc in symbol_failures:
                record_failure(exchange, symbol, data_type, exc)

    binance_seconds = time.time() - binance_started
    collect_seconds = time.time() - collect_started
    slow_side = "bybit" if bybit_seconds > binance_seconds else "binance"
    collect_health = "ok"
    if collect_seconds > 120:
        collect_health = "critical"
    elif collect_seconds > 90:
        collect_health = "slow"

    upsert_oi(oi_rows)
    upsert_price(price_rows)
    upsert_volume(volume_rows)
    replace_request_failures(failures)

    failure_types = {}

    for _, exchange, symbol, data_type, error_type, _ in failures:
        key = f"{exchange}:{data_type}:{error_type}"
        failure_types[key] = failure_types.get(key, 0) + 1

    failure_health = "ok"
    if len(failures) >= 50:
        failure_health = "critical"
    elif len(failures) >= 10:
        failure_health = "warning"

    if failures:
        top_failures = sorted(
            failure_types.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        log(
            f"REQUEST_FAILURES "
            f"count={len(failures)} "
            f"failure_health={failure_health} "
            f"top={top_failures}"
        )

    if failure_health != "ok":
        log(f"REQUEST_FAILURE_{failure_health.upper()} count={len(failures)}")

    if collect_health != "ok":
        log(f"COLLECT_{collect_health.upper()} elapsed={collect_seconds:.2f}s slow_side={slow_side}")

    log(
        f"collect ok: oi={len(oi_rows)} price={len(price_rows)} volume={len(volume_rows)} "
        f"request_failures={len(failures)} "
        f"failure_health={failure_health} "
        f"bybit_symbols={len(bybit_collect_symbols)} "
        f"bybit_workers={bybit_workers} "
        f"bybit_seconds={bybit_seconds:.2f} "
        f"binance_symbols={len(binance_collect_symbols)} "
        f"binance_workers={workers} "
        f"binance_seconds={binance_seconds:.2f} "
        f"collect_seconds={collect_seconds:.2f} "
        f"slow_side={slow_side} "
        f"collect_health={collect_health}"
    )



def _log_db_universe_check() -> None:
    try:
        from db import fetch

        tables = [
            "oi_5m_сырые",
            "price_5m_сырые",
            "volume_5m_сырые",
            "active_symbol_universe",
        ]

        for table in tables:
            rows = fetch(f"""
                SELECT
                    exchange,
                    COUNT(DISTINCT symbol) AS symbols,
                    COUNT(*) AS rows
                FROM {table}
                GROUP BY exchange
                ORDER BY exchange
            """)

            summary = " ".join(
                f'{r["exchange"]}:symbols={r["symbols"]}:rows={r["rows"]}'
                for r in rows
            )

            log(f"db universe check: {table} {summary}")

    except Exception as exc:
        log(f"db universe check error: {type(exc).__name__}: {exc}")


def _timed_watchdog_step(timings, name: str, func, timeout_env: str, default_timeout: int):
    timeout_seconds = float(os.getenv(timeout_env, str(default_timeout)))
    started = time.time()

    if not hasattr(_timed_watchdog_step, "_timeout_streaks"):
        _timed_watchdog_step._timeout_streaks = {}

    if not hasattr(_timed_watchdog_step, "_inflight"):
        _timed_watchdog_step._inflight = set()

    if name in _timed_watchdog_step._inflight:
        elapsed = time.time() - started
        timings.append((name, elapsed))
        log(
            f"WATCHDOG_INFLIGHT_SKIP "
            f"step={name} elapsed={elapsed:.2f}s "
            f"degraded=1"
        )
        return -3

    _timed_watchdog_step._inflight.add(name)

    def _run_and_release():
        try:
            return func()
        finally:
            _timed_watchdog_step._inflight.discard(name)

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_run_and_release)

    try:
        result = future.result(timeout=timeout_seconds)
        elapsed = time.time() - started
        timings.append((name, elapsed))
        _timed_watchdog_step._timeout_streaks[name] = 0
        log(
            f"step resource: {name}={elapsed:.2f}s "
            f"watchdog=ok timeout={timeout_seconds}s "
            f"watchdog_streak=0 "
            f"memory_max_rss_mb={_runtime_memory_mb():.2f}"
        )
        executor.shutdown(wait=True, cancel_futures=False)
        return result

    except TimeoutError:
        elapsed = time.time() - started
        timings.append((name, elapsed))
        future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)

        streak = _timed_watchdog_step._timeout_streaks.get(name, 0) + 1
        _timed_watchdog_step._timeout_streaks[name] = streak

        log(
            f"WATCHDOG_TIMEOUT "
            f"step={name} elapsed={elapsed:.2f}s "
            f"timeout={timeout_seconds}s degraded=1 "
            f"watchdog_streak={streak}"
        )

        if streak >= int(os.getenv("WATCHDOG_CRITICAL_STREAK", "3")):
            log(
                f"WATCHDOG_CRITICAL "
                f"step={name} streak={streak} "
                f"timeout={timeout_seconds}s"
            )

        return -2

    except Exception as exc:
        elapsed = time.time() - started
        timings.append((name, elapsed))
        executor.shutdown(wait=False, cancel_futures=True)
        log(f"WATCHDOG_ERROR step={name} error={type(exc).__name__}: {exc}")
        raise


def background(bybit_symbols, binance_symbols):
    last_export = 0.0
    cycle_no = 0

    while True:
        cycle_no += 1
        cycle_started = time.time()
        try:
            timings = []

            _timed_step(timings, "collect", lambda: collect(bybit_symbols, binance_symbols))
            collect_seconds = next((seconds for name, seconds in timings if name == "collect"), 0.0)

            if os.getenv("SKIP_HEAVY_AGGREGATES") == "1":
                agg_count = -1
                log("aggregates skipped: SKIP_HEAVY_AGGREGATES=1")
            elif collect_seconds > MAX_COLLECT_SECONDS_FOR_AGGREGATES:
                agg_count = -1
                log(f"aggregates skipped: collect too slow {collect_seconds:.2f}s > {MAX_COLLECT_SECONDS_FOR_AGGREGATES}s")
            elif cycle_no % max(1, AGGREGATES_EVERY_CYCLES) != 0:
                agg_count = -1
                log(f"aggregates skipped: scheduled every {AGGREGATES_EVERY_CYCLES} cycles")
            else:
                agg_count = _timed_watchdog_step(
                    timings,
                    "aggregates",
                    rebuild_bot_aggregates,
                    "WATCHDOG_AGGREGATES_SECONDS",
                    90,
                )
            if os.getenv("ENABLE_RUNTIME_VALIDATION_AUDIT") == "1":
                audit_count = _timed_step(timings, "validation_audit", rebuild_all)
            else:
                audit_count = -1
                log("validation_audit skipped: ENABLE_RUNTIME_VALIDATION_AUDIT!=1")

            auto_skip_stage2 = (
                os.getenv("SKIP_STAGE2_REBUILDS") == "1"
                or (
                    os.getenv("SKIP_HEAVY_AGGREGATES") == "1"
                    and os.getenv("FORCE_STAGE2_WITH_STALE_AGGREGATES") != "1"
                )
            )

            if auto_skip_stage2:
                research_count = silence_count = price_count = volume_count = oi_slope_count = phase_source_count = phase_count = -1
                log("stage2 rebuilds skipped: safe runtime mode")
            else:
                research_count = _timed_watchdog_step(
                    timings,
                    "market_research",
                    rebuild_market_research,
                    "WATCHDOG_MARKET_RESEARCH_SECONDS",
                    45,
                )
                silence_count = _timed_step(timings, "market_silence", rebuild_market_silence)
                price_count = _timed_step(timings, "price_state", rebuild_price_state)
                volume_count = _timed_step(timings, "volume_state", rebuild_volume_state)
                oi_slope_count = _timed_step(timings, "oi_slope", rebuild_oi_slope)
                phase_source_count = _timed_step(timings, "market_phase_source", rebuild_market_phase_source)
                phase_count = _timed_watchdog_step(
                    timings,
                    "market_phase",
                    rebuild_market_phase,
                    "WATCHDOG_MARKET_PHASE_SECONDS",
                    20,
                )

            _timed_step(timings, "cleanup_old", lambda: cleanup_old(ДНЕЙ_ХРАНЕНИЯ))

            now = time.time()

            # quick export отключён из автоцикла.
            # Экспорт собирается только по запросу через Telegram.
            if now - last_export >= ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК:
                last_export = now

            timing_text = " ".join([f"{name}={round(seconds, 2)}s" for name, seconds in timings])
            log(f"cycle timing: {timing_text}")
            rss_mb = _runtime_memory_mb()
            rss_health = "ok"
            if rss_mb >= float(os.getenv("RSS_CRITICAL_MB", "512")):
                rss_health = "critical"
            elif rss_mb >= float(os.getenv("RSS_WARNING_MB", "256")):
                rss_health = "warning"

            if rss_health != "ok":
                log(f"RSS_{rss_health.upper()} memory_max_rss_mb={rss_mb:.2f}")

            log(
                f"cycle resource: pid={os.getpid()} "
                f"memory_max_rss_mb={rss_mb:.2f} "
                f"rss_health={rss_health} "
                f"bybit_symbols={len(bybit_symbols)} bybit_workers={BYBIT_COLLECT_WORKERS} "
                f"binance_symbols={len(binance_symbols)} "
                f"binance_workers={BINANCE_COLLECT_WORKERS}"
            )

            Path("runtime_reports").mkdir(exist_ok=True)
            _write_runtime_timing_report(timings)
            watchdog_streaks = dict(getattr(_timed_watchdog_step, "_timeout_streaks", {}))
            watchdog_health = "critical" if any(
                streak >= int(os.getenv("WATCHDOG_CRITICAL_STREAK", "3"))
                for streak in watchdog_streaks.values()
            ) else ("degraded" if any(streak > 0 for streak in watchdog_streaks.values()) else "ok")

            Path("runtime_reports/watchdog_status.txt").write_text(
                "\n".join([
                    f"watchdog_health={watchdog_health}",
                    f"watchdog_streaks={watchdog_streaks}",
                    f"updated_at_utc={datetime.now(timezone.utc).isoformat()}",
                ]) + "\n"
            )

            runtime_health = {
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "app_version": APP_VERSION,
                "pid": os.getpid(),
                "rss_mb": round(rss_mb, 2),
                "rss_health": rss_health,
                "watchdog_health": watchdog_health,
                "watchdog_streaks": watchdog_streaks,
                "cycle_timing": timing_text,
                "cycle_health": "pending",
                "bybit_symbols": len(bybit_symbols),
                "binance_symbols": len(binance_symbols),
                "bybit_workers": BYBIT_COLLECT_WORKERS,
                "binance_workers": BINANCE_COLLECT_WORKERS,
                "skip_heavy": os.getenv("SKIP_HEAVY_AGGREGATES"),
                "skip_stage2": os.getenv("SKIP_STAGE2_REBUILDS"),
                "force_stage2": os.getenv("FORCE_STAGE2_WITH_STALE_AGGREGATES"),
                "derived_window_hours": os.getenv("DERIVED_WINDOW_HOURS"),
                "derived_batch_size": os.getenv("DERIVED_BATCH_SIZE"),
                "derived_retention_hours": os.getenv("DERIVED_RETENTION_HOURS"),
            }

            Path("runtime_reports/runtime_health.txt").write_text(
                "\n".join([f"{k}={v}" for k, v in runtime_health.items()]) + "\n"
            )
            runtime_health_json_path = Path("runtime_reports/runtime_health.json")

            snapshot_health = "ok"
            runtime_health["snapshot_health"] = snapshot_health
            runtime_health["snapshot_size"] = 0

            payload = json.dumps(runtime_health, ensure_ascii=False, indent=2) + "\n"
            runtime_health_json_path.write_text(payload)

            snapshot_size = runtime_health_json_path.stat().st_size
            if snapshot_size <= 32:
                snapshot_health = "critical"
                log(f"RUNTIME_SNAPSHOT_CORRUPTED size={snapshot_size}")

            runtime_health["snapshot_size"] = snapshot_size
            runtime_health["snapshot_health"] = snapshot_health

            runtime_health_json_path.write_text(
                json.dumps(runtime_health, ensure_ascii=False, indent=2) + "\n"
            )

            Path("runtime_reports/snapshot_status.txt").write_text(
                "\n".join([
                    f"snapshot_health={snapshot_health}",
                    f"snapshot_size={snapshot_size}",
                    f"updated_at_utc={runtime_health['updated_at_utc']}",
                ]) + "\n"
            )

            if snapshot_health != "ok":
                log(
                    f"RUNTIME_SNAPSHOT_{snapshot_health.upper()} "
                    f"size={snapshot_size}"
                )

            log(f"canonical validation cycle ok: aggregates={agg_count} audit={audit_count} research={research_count} silence={silence_count} price={price_count} volume={volume_count} oi_slope={oi_slope_count} phase_source={phase_source_count} phase={phase_count}")
            _log_db_universe_check()

        except Exception as exc:
            log(f"canonical validation cycle error: {type(exc).__name__}: {exc}")
            log(traceback.format_exc())

        elapsed = time.time() - cycle_started

        if not hasattr(background, "_overrun_streak"):
            background._overrun_streak = 0

        if elapsed > ИНТЕРВАЛ_ЦИКЛА_СЕК:
            background._overrun_streak += 1
            log(
                f"CYCLE_OVERRUN "
                f"elapsed={elapsed:.2f}s "
                f"target={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
                f"overrun={(elapsed - ИНТЕРВАЛ_ЦИКЛА_СЕК):.2f}s "
                f"streak={background._overrun_streak}"
            )

            if background._overrun_streak >= 3:
                log(f"CYCLE_OVERRUN_CRITICAL streak={background._overrun_streak}")
        else:
            background._overrun_streak = 0

        sleep_seconds = max(0, ИНТЕРВАЛ_ЦИКЛА_СЕК - elapsed)

        cycle_health = "ok"
        if elapsed > ИНТЕРВАЛ_ЦИКЛА_СЕК:
            cycle_health = "overrun"
        elif sleep_seconds < float(os.getenv("CYCLE_SLEEP_WARNING_SECONDS", "30")):
            cycle_health = "tight"

        log(
            f"cycle schedule: target={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
            f"elapsed={elapsed:.2f}s sleep={sleep_seconds:.2f}s "
            f"cycle_health={cycle_health}"
        )

        Path("runtime_reports").mkdir(exist_ok=True)
        cycle_status = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "cycle_target_seconds": ИНТЕРВАЛ_ЦИКЛА_СЕК,
            "cycle_elapsed_seconds": round(elapsed, 2),
            "cycle_sleep_seconds": round(sleep_seconds, 2),
            "cycle_health": cycle_health,
            "overrun_streak": getattr(background, "_overrun_streak", 0),
        }

        Path("runtime_reports/cycle_status.txt").write_text(
            "\n".join([f"{k}={v}" for k, v in cycle_status.items()]) + "\n"
        )
        Path("runtime_reports/cycle_status.json").write_text(
            json.dumps(cycle_status, ensure_ascii=False, indent=2) + "\n"
        )

        time.sleep(sleep_seconds)


def main():
    log(f"Новая чистая база {APP_VERSION} запущена")
    log(f"runtime mode: {runtime_mode_text()}")
    log(
        "runtime env: "
        f"cycle_interval={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
        f"skip_heavy={os.getenv('SKIP_HEAVY_AGGREGATES')} "
        f"skip_stage2={os.getenv('SKIP_STAGE2_REBUILDS')} "
        f"force_stage2={os.getenv('FORCE_STAGE2_WITH_STALE_AGGREGATES')} "
        f"derived_window_hours={os.getenv('DERIVED_WINDOW_HOURS')} "
        f"derived_batch_size={os.getenv('DERIVED_BATCH_SIZE')} "
        f"derived_retention_hours={os.getenv('DERIVED_RETENTION_HOURS')}"
    )

    log("init_db start")
    init_db()
    log("init_db ok")
    migrate_canonical_ts_close()

    start_polling()
    log("Telegram polling стартовал")

    send_message(
        СТАРТОВОЕ_СООБЩЕНИЕ.format(
            version=APP_VERSION,
            retention=ДНЕЙ_ХРАНЕНИЯ,
            commands=КОМАНДЫ,
        )
    )
    log("Telegram OK")

    bybit_symbols_all = fetch_bybit_symbols()
    binance_symbols_all = fetch_binance_symbols()

    bad_symbols = load_quarantine_symbols(95.0)

    bybit_symbols = [
        s for s in bybit_symbols_all
        if ("BYBIT", s) not in bad_symbols
    ]
    if ЛИМИТ_СИМВОЛОВ_BYBIT > 0:
        bybit_symbols = bybit_symbols[:ЛИМИТ_СИМВОЛОВ_BYBIT]

    binance_symbols_filtered = [
        s for s in binance_symbols_all
        if ("BINANCE", s) not in bad_symbols
    ]

    binance_symbols = (
        binance_symbols_filtered
        if ЛИМИТ_СИМВОЛОВ_BINANCE <= 0
        else binance_symbols_filtered[:ЛИМИТ_СИМВОЛОВ_BINANCE]
    )

    active_universe = (
        [("BYBIT", s, "runtime_limit_quarantine_filtered") for s in bybit_symbols] +
        [("BINANCE", s, "runtime_limit_quarantine_filtered") for s in binance_symbols]
    )
    replace_active_universe(active_universe)

    log(f"Bybit symbols: {len(bybit_symbols_all)}")
    log(f"Binance symbols: {len(binance_symbols_all)}")
    log(f"Limits: bybit={ЛИМИТ_СИМВОЛОВ_BYBIT}, binance={ЛИМИТ_СИМВОЛОВ_BINANCE}")
    log(f"Active universe: bybit={len(bybit_symbols)} binance={len(binance_symbols)} total={len(active_universe)}")
    log(f"Quarantine symbols excluded: {len(bad_symbols)}")

    threading.Thread(target=background, args=(bybit_symbols, binance_symbols), daemon=True).start()

    log("background workers started")

    while True:
        log("heartbeat ok")
        time.sleep(60)


if __name__ == "__main__":
    main()
