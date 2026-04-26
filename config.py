import os
from pathlib import Path

APP_VERSION = "v3.5.4"

ИНТЕРВАЛ_ЦИКЛА_СЕК = int(os.getenv("COLLECT_INTERVAL_SECONDS", "180"))
ДНЕЙ_ХРАНЕНИЯ = int(os.getenv("RETENTION_DAYS", "30"))

ЛИМИТ_СИМВОЛОВ_BYBIT = int(os.getenv("LIMIT_SYMBOLS_BYBIT", "20"))
ЛИМИТ_СИМВОЛОВ_BINANCE = int(os.getenv("LIMIT_SYMBOLS_BINANCE", "20"))

QUICK_EXPORT_CANDLES = int(os.getenv("QUICK_EXPORT_CANDLES", "96"))
RESEARCH_EXPORT_DAYS = int(os.getenv("RESEARCH_EXPORT_DAYS", "7"))
RESEARCH_30D_EXPORT_DAYS = int(os.getenv("RESEARCH_30D_EXPORT_DAYS", "30"))

ИНТЕРВАЛ_ПЕРЕСБОРКИ_ЭКСПОРТА_СЕК = int(os.getenv("EXPORT_REBUILD_INTERVAL_SECONDS", "900"))

ПАПКА_ДАННЫХ = Path("runtime")
ПАПКА_ДАННЫХ.mkdir(exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BYBIT_BASE = "https://api.bybit.com"
BINANCE_BASE = "https://fapi.binance.com"

КОМАНДЫ = (
    "/ping\n"
    "/status\n"
    "/manifest\n"
    "/bundle\n"
    "/audit_report\n"
    "/research_report\n"
    "/timing\n"
    "/health\n"
    "/failures\n"
    "/gaps\n"
    "/active_universe\n"
    "/export_quick\n"
    "/export_research_7d\n"
    "/export_research_30d"
)

СТАРТОВОЕ_СООБЩЕНИЕ = (
    "🥇 Mighty Duck / {version}\n\n"
    "🚀 v3.5.4 final data quality hardening patch\n"
    "🟢 Canonical ts_close migration: active\n"
    "🟢 Export compression: 3 files\n"
    "🟢 Research source: active universe + invalid reason audit\n"
    "🧠 Состояния рынка: диапазон / продолжение / выдох / сжатие / нейтрально\n\n"
    "🗄 Хранение истории: {retention} дней\n\n"
    "Команды:\n{commands}"
)
