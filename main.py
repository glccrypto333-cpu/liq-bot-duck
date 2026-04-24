from __future__ import annotations
import time
import threading
from dotenv import load_dotenv
load_dotenv()

from config import (
    APP_VERSION, СТАРТОВОЕ_СООБЩЕНИЕ, ДНЕЙ_ХРАНЕНИЯ, ДНЕЙ_ЭКСПОРТА, КОМАНДЫ,
    ИНТЕРВАЛ_ЦИКЛА_СЕК, ЛИМИТ_СИМВОЛОВ_BYBIT, ЛИМИТ_СИМВОЛОВ_BINANCE,
    ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК
)
from logger import log
from db import init_db, bulk_insert, cleanup_old_data
from exchange_clients import (
    fetch_bybit_symbols, fetch_binance_symbols,
    fetch_bybit_oi_5m, fetch_binance_oi_5m,
    fetch_bybit_kline_5m, fetch_binance_kline_5m
)
from aggregation_engine import rebuild_oi_aggregates, rebuild_price_aggregates, rebuild_volume_aggregates
from consistency_engine import rebuild_consistency
from audit_engine import rebuild_all_audits
from export_engine import rebuild_exports
from telegram_bot import start_polling, send_message

def collect_bybit(symbols: list[str]) -> tuple[list[tuple], list[tuple], list[tuple]]:
    oi_rows, price_rows, volume_rows = [], [], []
    for s in symbols[:ЛИМИТ_СИМВОЛОВ_BYBIT]:
        try:
            oi_rows.extend(fetch_bybit_oi_5m(s, limit=48))
        except Exception:
            pass
        try:
            p_rows, v_rows = fetch_bybit_kline_5m(s, limit=48)
            price_rows.extend(p_rows)
            volume_rows.extend(v_rows)
        except Exception:
            pass
    return oi_rows, price_rows, volume_rows

def collect_binance(symbols: list[str]) -> tuple[list[tuple], list[tuple], list[tuple]]:
    oi_rows, price_rows, volume_rows = [], [], []
    for s in symbols[:ЛИМИТ_СИМВОЛОВ_BINANCE]:
        try:
            oi_rows.extend(fetch_binance_oi_5m(s, limit=48))
        except Exception:
            pass
        try:
            p_rows, v_rows = fetch_binance_kline_5m(s, limit=48)
            price_rows.extend(p_rows)
            volume_rows.extend(v_rows)
        except Exception:
            pass
    return oi_rows, price_rows, volume_rows

def background(symbols_bybit: list[str], symbols_binance: list[str]) -> None:
    last_export_ts = 0.0
    while True:
        try:
            by_oi, by_price, by_vol = collect_bybit(symbols_bybit)
            bi_oi, bi_price, bi_vol = collect_binance(symbols_binance)

            if by_oi or bi_oi:
                bulk_insert("oi_5m_сырые", by_oi + bi_oi)
            if by_price or bi_price:
                bulk_insert("price_5m_сырые", by_price + bi_price)
            if by_vol or bi_vol:
                bulk_insert("volume_5m_сырые", by_vol + bi_vol)

            rebuild_oi_aggregates()
            rebuild_price_aggregates()
            rebuild_volume_aggregates()
            rebuild_consistency()
            rebuild_all_audits()
            cleanup_old_data(ДНЕЙ_ХРАНЕНИЯ)

            now = time.time()
            if now - last_export_ts >= ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК:
                rebuild_exports()
                last_export_ts = now
                log("exports rebuilt")

            log("background validation cycle ok")
        except Exception as exc:
            log(f"background validation cycle error: {exc}")
        time.sleep(ИНТЕРВАЛ_ЦИКЛА_СЕК)

def main() -> None:
    log(f"Новая чистая база {APP_VERSION} запущена")
    init_db()

    log("telegram init started")
    start_polling()
    log("Telegram polling стартовал")

    try:
        send_message(СТАРТОВОЕ_СООБЩЕНИЕ.format(version=APP_VERSION, retention=ДНЕЙ_ХРАНЕНИЯ, export_days=ДНЕЙ_ЭКСПОРТА, commands=КОМАНДЫ))
        log("Telegram OK")
    except Exception as exc:
        log(f"startup message error: {exc}")

    bybit_symbols = fetch_bybit_symbols()
    binance_symbols = fetch_binance_symbols()
    log(f"Bybit symbols: {len(bybit_symbols)}")
    log(f"Binance symbols: {len(binance_symbols)}")
    log(f"Limits: bybit={ЛИМИТ_СИМВОЛОВ_BYBIT}, binance={ЛИМИТ_СИМВОЛОВ_BINANCE}")

    threading.Thread(target=background, args=(bybit_symbols, binance_symbols), daemon=True).start()
    log("background workers started")

    while True:
        log("heartbeat ok")
        time.sleep(60)

if __name__ == "__main__":
    main()
