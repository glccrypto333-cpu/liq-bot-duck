from __future__ import annotations
import time
import os
import sys
import resource
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    BINANCE_COLLECT_WORKERS,
    ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК,
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
        oi_rows.extend(fetch_binance_oi_5m(symbol, 200))
    except Exception as exc:
        failures.append(("BINANCE", symbol, "OI", exc))

    try:
        p, v = fetch_binance_kline_5m(symbol, 200)
        price_rows.extend(p)
        volume_rows.extend(v)
    except Exception as exc:
        failures.append(("BINANCE", symbol, "PRICE_VOLUME", exc))

    return oi_rows, price_rows, volume_rows, failures


def collect(symbols_bybit, symbols_binance):
    oi_rows, price_rows, volume_rows = [], [], []
    failures = []
    now = datetime.now(timezone.utc)

    def record_failure(exchange: str, symbol: str, data_type: str, exc: Exception) -> None:
        failures.append((now, exchange, symbol, data_type, type(exc).__name__, str(exc)[:500]))

    for s in symbols_bybit[:ЛИМИТ_СИМВОЛОВ_BYBIT]:
        try:
            oi_rows.extend(fetch_bybit_oi_5m(s, 200))
        except Exception as exc:
            record_failure("BYBIT", s, "OI", exc)

        try:
            p, v = fetch_bybit_kline_5m(s, 200)
            price_rows.extend(p)
            volume_rows.extend(v)
        except Exception as exc:
            record_failure("BYBIT", s, "PRICE_VOLUME", exc)

    binance_collect_symbols = (
        symbols_binance
        if ЛИМИТ_СИМВОЛОВ_BINANCE <= 0
        else symbols_binance[:ЛИМИТ_СИМВОЛОВ_BINANCE]
    )

    workers = max(1, BINANCE_COLLECT_WORKERS)

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

    upsert_oi(oi_rows)
    upsert_price(price_rows)
    upsert_volume(volume_rows)
    replace_request_failures(failures)

    log(
        f"collect ok: oi={len(oi_rows)} price={len(price_rows)} volume={len(volume_rows)} "
        f"request_failures={len(failures)} "
        f"binance_symbols={len(binance_collect_symbols)} "
        f"binance_workers={workers}"
    )



def _log_db_universe_check() -> None:
    try:
        from db import fetch

        tables = [
            "oi_5m_сырые",
            "price_5m_сырые",
            "volume_5m_сырые",
            "bot_aggregates",
            "market_research",
            "market_oi_slope",
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


def background(bybit_symbols, binance_symbols):
    last_export = 0.0
    cycle_no = 0

    while True:
        cycle_no += 1
        cycle_started = time.time()
        try:
            timings = []

            _timed_step(timings, "collect", lambda: collect(bybit_symbols, binance_symbols))

            if os.getenv("SKIP_HEAVY_AGGREGATES") == "1":
                agg_count = -1
                log("aggregates skipped: SKIP_HEAVY_AGGREGATES=1")
            else:
                agg_count = _timed_step(timings, "aggregates", rebuild_bot_aggregates)
            if cycle_no % 6 == 0:
                audit_count = _timed_step(timings, "validation_audit", rebuild_all)
            else:
                audit_count = -1
                log("validation_audit skipped: scheduled every 6 cycles")

            auto_skip_stage2 = (
                os.getenv("SKIP_STAGE2_REBUILDS") == "1"
                or (
                    os.getenv("SKIP_HEAVY_AGGREGATES") == "1"
                    and os.getenv("FORCE_STAGE2_WITH_STALE_AGGREGATES") != "1"
                )
            )

            if auto_skip_stage2:
                research_count = silence_count = price_count = volume_count = oi_slope_count = phase_count = -1
                log("stage2 rebuilds skipped: safe runtime mode")
            else:
                research_count = _timed_step(timings, "market_research", rebuild_market_research)
                silence_count = _timed_step(timings, "market_silence", rebuild_market_silence)
                price_count = _timed_step(timings, "price_state", rebuild_price_state)
                volume_count = _timed_step(timings, "volume_state", rebuild_volume_state)
                oi_slope_count = _timed_step(timings, "oi_slope", rebuild_oi_slope)
                phase_count = _timed_step(timings, "market_phase", rebuild_market_phase)

            _timed_step(timings, "cleanup_old", lambda: cleanup_old(ДНЕЙ_ХРАНЕНИЯ))

            now = time.time()

            # quick export отключён из автоцикла.
            # Экспорт собирается только по запросу через Telegram.
            if now - last_export >= ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК:
                last_export = now

            timing_text = " ".join([f"{name}={round(seconds, 2)}s" for name, seconds in timings])
            log(f"cycle timing: {timing_text}")
            log(
                f"cycle resource: pid={os.getpid()} "
                f"memory_max_rss_mb={_runtime_memory_mb():.2f} "
                f"bybit_symbols={len(bybit_symbols)} "
                f"binance_symbols={len(binance_symbols)} "
                f"binance_workers={BINANCE_COLLECT_WORKERS}"
            )

            _write_runtime_timing_report(timings)

            log(f"canonical validation cycle ok: aggregates={agg_count} audit={audit_count} research={research_count} silence={silence_count} price={price_count} volume={volume_count} oi_slope={oi_slope_count} phase={phase_count}")
            _log_db_universe_check()

        except Exception as exc:
            log(f"canonical validation cycle error: {type(exc).__name__}: {exc}")
            log(traceback.format_exc())

        elapsed = time.time() - cycle_started
        sleep_seconds = max(0, ИНТЕРВАЛ_ЦИКЛА_СЕК - elapsed)
        log(
            f"cycle schedule: target={ИНТЕРВАЛ_ЦИКЛА_СЕК}s "
            f"elapsed={elapsed:.2f}s sleep={sleep_seconds:.2f}s"
        )
        time.sleep(sleep_seconds)


def main():
    log(f"Новая чистая база {APP_VERSION} запущена")
    log(f"runtime mode: {runtime_mode_text()}")

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
