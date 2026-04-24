from __future__ import annotations
import time
import threading
from dotenv import load_dotenv
load_dotenv()

from config import APP_VERSION, СТАРТОВОЕ_СООБЩЕНИЕ, ДНЕЙ_ХРАНЕНИЯ, КОМАНДЫ, ИНТЕРВАЛ_ЦИКЛА_СЕК, ЛИМИТ_СИМВОЛОВ_BYBIT, ЛИМИТ_СИМВОЛОВ_BINANCE, ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК
from logger import log
from db import init_db, upsert_oi, upsert_price, upsert_volume, cleanup_old
from exchange_clients import fetch_bybit_symbols, fetch_binance_symbols, fetch_bybit_oi_5m, fetch_binance_oi_5m, fetch_bybit_kline_5m, fetch_binance_kline_5m
from aggregation_engine import rebuild_bot_aggregates
from audit_engine import rebuild_all
from export_engine import rebuild_exports
from telegram_bot import start_polling, send_message

def collect(symbols_bybit, symbols_binance):
    oi_rows, price_rows, volume_rows = [], [], []
    for s in symbols_bybit[:ЛИМИТ_СИМВОЛОВ_BYBIT]:
        try: oi_rows.extend(fetch_bybit_oi_5m(s, 200))
        except Exception: pass
        try:
            p, v = fetch_bybit_kline_5m(s, 200)
            price_rows.extend(p); volume_rows.extend(v)
        except Exception: pass
    for s in symbols_binance[:ЛИМИТ_СИМВОЛОВ_BINANCE]:
        try: oi_rows.extend(fetch_binance_oi_5m(s, 200))
        except Exception: pass
        try:
            p, v = fetch_binance_kline_5m(s, 200)
            price_rows.extend(p); volume_rows.extend(v)
        except Exception: pass
    upsert_oi(oi_rows)
    upsert_price(price_rows)
    upsert_volume(volume_rows)

def background(bybit_symbols, binance_symbols):
    last_export = 0.0
    while True:
        try:
            collect(bybit_symbols, binance_symbols)
            rebuild_bot_aggregates()
            rebuild_all()
            cleanup_old(ДНЕЙ_ХРАНЕНИЯ)
            now = time.time()
            if now - last_export >= ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК:
                rebuild_exports("quick")
                last_export = now
                log("quick export rebuilt")
            log("canonical validation cycle ok")
        except Exception as exc:
            log(f"canonical validation cycle error: {exc}")
        time.sleep(ИНТЕРВАЛ_ЦИКЛА_СЕК)

def main():
    log(f"Новая чистая база {APP_VERSION} запущена")
    init_db()
    start_polling()
    log("Telegram polling стартовал")
    send_message(СТАРТОВОЕ_СООБЩЕНИЕ.format(version=APP_VERSION, retention=ДНЕЙ_ХРАНЕНИЯ, commands=КОМАНДЫ))
    log("Telegram OK")

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
