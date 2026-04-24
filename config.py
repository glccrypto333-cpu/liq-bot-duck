import os
from pathlib import Path

APP_VERSION = "v3.4.4"

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
    "/raw_market_5m\n"
    "/bot_aggregates\n"
    "/validation_audit\n"
    "/audit_report\n"
    "/export_quick\n"
    "/export_research_7d\n"
    "/export_research_30d"
)

СТАРТОВОЕ_СООБЩЕНИЕ = (
    "🥇 Mighty Duck / {version}\n\n"
    "🚀 aggregation repair + audit activation\n"
    "🟢 Canonical raw: active\n"
    "🟢 Aggregates: rebuild table mode\n"
    "🟢 Audit: exact ts_close matching\n"
    "🟢 Exports: 4 files + zip bundles\n\n"
    "🗄 Хранение истории: {retention} дней\n\n"
    "Команды:\n{commands}"
)
