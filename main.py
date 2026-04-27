from __future__ import annotations
import time
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
from market_regime_engine import rebuild_market_regime
from export_engine import rebuild_exports
from telegram_bot import start_polling, send_message


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


def _timed_step(timings: list[tuple[str, float]], name: str, fn):
    started = time.time()
    result = fn()
    timings.append((name, time.time() - started))
    return result


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

    for s in symbols_binance[:ЛИМИТ_СИМВОЛОВ_BINANCE]:
        try:
            oi_rows.extend(fetch_binance_oi_5m(s, 200))
        except Exception as exc:
            record_failure("BINANCE", s, "OI", exc)

        try:
            p, v = fetch_binance_kline_5m(s, 200)
            price_rows.extend(p)
            volume_rows.extend(v)
        except Exception as exc:
            record_failure("BINANCE", s, "PRICE_VOLUME", exc)

    upsert_oi(oi_rows)
    upsert_price(price_rows)
    upsert_volume(volume_rows)
    replace_request_failures(failures)

    log(
        f"collect ok: oi={len(oi_rows)} price={len(price_rows)} volume={len(volume_rows)} "
        f"request_failures={len(failures)}"
    )


def background(bybit_symbols, binance_symbols):
    last_export = 0.0

    while True:
        try:
            timings = []

            _timed_step(timings, "collect", lambda: collect(bybit_symbols, binance_symbols))

            agg_count = _timed_step(timings, "aggregates", rebuild_bot_aggregates)
            audit_count = _timed_step(timings, "validation_audit", rebuild_all)
            research_count = _timed_step(timings, "market_research", rebuild_market_research)
            silence_count = _timed_step(timings, "market_silence", rebuild_market_silence)
            price_count = _timed_step(timings, "price_state", rebuild_price_state)
            volume_count = _timed_step(timings, "volume_state", rebuild_volume_state)
            oi_slope_count = _timed_step(timings, "oi_slope", rebuild_oi_slope)
            regime_count = _timed_step(timings, "market_regime", rebuild_market_regime)

            _timed_step(timings, "cleanup_old", lambda: cleanup_old(ДНЕЙ_ХРАНЕНИЯ))

            now = time.time()

            if now - last_export >= ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК:
                bundle = _timed_step(timings, "quick_export", lambda: rebuild_exports("quick"))
                last_export = now
                log(f"quick export rebuilt: {bundle}")

            timing_text = " ".join([f"{name}={round(seconds, 2)}s" for name, seconds in timings])
            log(f"cycle timing: {timing_text}")

            _write_runtime_timing_report(timings)

            log(f"canonical validation cycle ok: aggregates={agg_count} audit={audit_count} research={research_count} silence={silence_count} price={price_count} volume={volume_count} oi_slope={oi_slope_count} regime={regime_count}")

        except Exception as exc:
            log(f"canonical validation cycle error: {type(exc).__name__}: {exc}")
            log(traceback.format_exc())

        time.sleep(ИНТЕРВАЛ_ЦИКЛА_СЕК)


def main():
    log(f"Новая чистая база {APP_VERSION} запущена")

    init_db()
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
    ][:ЛИМИТ_СИМВОЛОВ_BYBIT]

    binance_symbols = [
        s for s in binance_symbols_all
        if ("BINANCE", s) not in bad_symbols
    ][:ЛИМИТ_СИМВОЛОВ_BINANCE]

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
