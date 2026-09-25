"""CoinGlass chart screenshot config for Duck.

Все чувствительные настройки берутся только из env/storage_state.
В коде не должно быть дефолтных логинов, паролей или токенов.
"""

import os

PLAYWRIGHT_BROWSERS_PATH = os.getenv(
    "PLAYWRIGHT_BROWSERS_PATH",
    "/home/alexey/openclaw/runtime/playwright-browsers",
)
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", PLAYWRIGHT_BROWSERS_PATH)

ENABLE_CHART_SCREENSHOT = os.getenv("ENABLE_CHART_SCREENSHOT", "true").lower() == "true"
CHART_PROVIDER = os.getenv("CHART_PROVIDER", "coinglass")
CHART_SCREENSHOT_DIR = os.getenv("CHART_SCREENSHOT_DIR", "/tmp/duck_chart_screenshots")
CHART_LOAD_TIMEOUT_MS = int(os.getenv("CHART_LOAD_TIMEOUT_MS", "15000"))
CHART_SCREENSHOT_RETRIES = int(os.getenv("CHART_SCREENSHOT_RETRIES", "2"))
CHART_VIEWPORT_WIDTH = int(os.getenv("CHART_VIEWPORT_WIDTH", "1200"))
CHART_VIEWPORT_HEIGHT = int(os.getenv("CHART_VIEWPORT_HEIGHT", "800"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

# Таймфреймы для мульти-скриншота (5m + 4H альбомом). 4H берётся через дропдаун CoinGlass.
CHART_TIMEFRAMES = [
    t.strip() for t in os.getenv("CHART_TIMEFRAMES", "5m,4H").split(",") if t.strip()
]

COINGLASS_EMAIL = os.getenv("COINGLASS_EMAIL", "")
COINGLASS_PASSWORD = os.getenv("COINGLASS_PASSWORD", "")
COINGLASS_STORAGE_STATE_PATH = os.getenv(
    "COINGLASS_STORAGE_STATE_PATH",
    os.path.join(os.path.dirname(__file__), "..", "storage_state.json"),
)
COINGLASS_LOGIN_URL = os.getenv("COINGLASS_LOGIN_URL", "https://www.coinglass.com/login")

# Telegram caption limit (characters)
TG_CAPTION_LIMIT = 1024
