import os
from pathlib import Path

APP_VERSION = "v3.4.2"

ИНТЕРВАЛ_ЦИКЛА_СЕК = int(os.getenv("COLLECT_INTERVAL_SECONDS", "180"))
ДНЕЙ_ХРАНЕНИЯ = int(os.getenv("RETENTION_DAYS", "30"))
ДНЕЙ_ЭКСПОРТА = int(os.getenv("EXPORT_RAW_DAYS", "3"))

ЛИМИТ_СИМВОЛОВ_BYBIT = int(os.getenv("LIMIT_SYMBOLS_BYBIT", "20"))
ЛИМИТ_СИМВОЛОВ_BINANCE = int(os.getenv("LIMIT_SYMBOLS_BINANCE", "20"))

ЛИМИТ_СЫРЫХ_5М = int(os.getenv("EXPORT_LIMIT_RAW_5M", "3000"))
ЛИМИТ_АГРЕГАТОВ = int(os.getenv("EXPORT_LIMIT_AGG", "3000"))
ЛИМИТ_СВЕРКИ = int(os.getenv("EXPORT_LIMIT_CONSISTENCY", "1000"))
ЛИМИТ_АУДИТА = int(os.getenv("EXPORT_LIMIT_AUDIT", "3000"))
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
    "/oi_5m_raw\n"
    "/price_5m_raw\n"
    "/volume_5m_raw\n"
    "/oi_aggregates\n"
    "/oi_consistency\n"
    "/oi_quality\n"
    "/audit_oi\n"
    "/audit_price\n"
    "/audit_volume\n"
    "/audit_report\n"
    "/audit_debug"
)

СТАРТОВОЕ_СООБЩЕНИЕ = (
    "🥇 Mighty Duck / {version}\n\n"
    "🚀 v3.4.2 independent validation layer\n"
    "🟢 База: native 5m OI / price / volume\n"
    "🟢 Окна: 15м / 30м / 1ч / 4ч\n"
    "🟢 Telegram команды: активны\n"
    "🟢 Аудит OI / цены / объёма: активен\n\n"
    "🗄 Хранение истории: {retention} дней\n"
    "📤 Экспорт: {export_days} дней\n\n"
    "Команды:\n{commands}"
)
