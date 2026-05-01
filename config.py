import os
from pathlib import Path

APP_VERSION = "v3.5.4"

ИНТЕРВАЛ_ЦИКЛА_СЕК = int(os.getenv("COLLECT_INTERVAL_SECONDS", "180"))
ДНЕЙ_ХРАНЕНИЯ = int(os.getenv("RETENTION_DAYS", "30"))

ЛИМИТ_СИМВОЛОВ_BYBIT = int(os.getenv("LIMIT_SYMBOLS_BYBIT", "0"))
ЛИМИТ_СИМВОЛОВ_BINANCE = int(os.getenv("LIMIT_SYMBOLS_BINANCE", "0"))
BINANCE_UNIVERSE_SKIP_TOP = int(os.getenv("BINANCE_UNIVERSE_SKIP_TOP", "50"))
BYBIT_UNIVERSE_SKIP_TOP = int(os.getenv("BYBIT_UNIVERSE_SKIP_TOP", "50"))
BINANCE_COLLECT_WORKERS = int(os.getenv("BINANCE_COLLECT_WORKERS", "6"))
BYBIT_COLLECT_WORKERS = int(os.getenv("BYBIT_COLLECT_WORKERS", "8"))

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
    {"command": "panel", "description": "Главная панель"},
    {"command": "status", "description": "Короткий статус"},
    {"command": "runtime", "description": "Runtime / cycle / watchdog"},
    {"command": "exports", "description": "Состояние export файлов"},
    {"command": "reports", "description": "Runtime reports zip"},
    {"command": "bundle", "description": "Quick bundle"},
    {"command": "manifest", "description": "Storage manifest"},
    {"command": "health", "description": "Runtime health report"},
    {"command": "failures", "description": "Request failures"},
    {"command": "gaps", "description": "Gap report"},
    {"command": "active_universe", "description": "Active universe"},
    {"command": "backup", "description": "Backup policy/status"},
    {"command": "export_quick", "description": "Rebuild quick export"},
    {"command": "export_research_7d", "description": "Research 7d bundle"},
    {"command": "export_research_30d", "description": "Research 30d bundle"},
    {"command": "ping", "description": "Ping"},
)

СТАРТОВОЕ_СООБЩЕНИЕ = (
    "🥇 Mighty Duck / {version}\n\n"
    "Сборка: production\n"
    "Дата запуска: {started_at}\n"
    "Хранение истории: {retention} дней"
)

RAW_RETENTION_DAYS = int(os.getenv("RAW_RETENTION_DAYS", "7"))

AGGREGATES_EVERY_CYCLES = int(os.getenv("AGGREGATES_EVERY_CYCLES", "3"))

MAX_COLLECT_SECONDS_FOR_AGGREGATES = int(os.getenv("MAX_COLLECT_SECONDS_FOR_AGGREGATES", "120"))
