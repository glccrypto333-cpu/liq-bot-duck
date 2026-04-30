from __future__ import annotations
import time
from datetime import datetime, timezone, timedelta
import requests
from config import BYBIT_BASE, BINANCE_BASE, BINANCE_UNIVERSE_SKIP_TOP, BYBIT_UNIVERSE_SKIP_TOP

UA = {"User-Agent": "Mozilla/5.0 MightyDuck/1.0"}

def _get(url: str, params: dict | None = None, retries: int = 3):
    last_exc = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            time.sleep(1 + i)
    raise last_exc

def _dt_from_ms(ms) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)

def _norm_5m_close(ts_open: datetime) -> datetime:
    return ts_open + timedelta(minutes=5)

def _closed(ts_close: datetime) -> bool:
    return ts_close <= datetime.now(timezone.utc) - timedelta(seconds=30)

def fetch_bybit_symbols() -> list[str]:
    data = _get(f"{BYBIT_BASE}/v5/market/instruments-info", {"category": "linear", "limit": 1000})
    symbols = [
        x["symbol"]
        for x in data.get("result", {}).get("list", [])
        if x.get("status") == "Trading" and x.get("quoteCoin") == "USDT"
    ]
    return sorted(symbols)[BYBIT_UNIVERSE_SKIP_TOP:]

def fetch_binance_symbols() -> list[str]:
    """
    Active universe rule:
    - Binance USDT perpetuals only
    - sort by 24h quoteVolume
    - skip TOP-50
    - runtime limit is applied in main.py
    """
    exchange_info = _get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    allowed = {
        x["symbol"]
        for x in exchange_info.get("symbols", [])
        if x.get("status") == "TRADING"
        and x.get("quoteAsset") == "USDT"
        and x.get("contractType") == "PERPETUAL"
    }

    tickers = _get(f"{BINANCE_BASE}/fapi/v1/ticker/24hr")
    rows = []
    for item in tickers if isinstance(tickers, list) else []:
        symbol = item.get("symbol")
        if symbol not in allowed:
            continue
        try:
            quote_volume = float(item.get("quoteVolume", 0) or 0)
        except (TypeError, ValueError):
            quote_volume = 0.0
        rows.append((quote_volume, symbol))

    rows.sort(reverse=True)
    return [symbol for _, symbol in rows[BINANCE_UNIVERSE_SKIP_TOP:]]

def fetch_bybit_oi_5m(symbol: str, limit: int = 200) -> list[tuple]:
    data = _get(
        f"{BYBIT_BASE}/v5/market/open-interest",
        {"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": limit},
    )
    out = []
    for item in reversed(data.get("result", {}).get("list", [])):
        ts_close = _dt_from_ms(item["timestamp"])
        ts_open = ts_close - timedelta(minutes=5)
        ts_close_norm = _norm_5m_close(ts_open)
        if not _closed(ts_close_norm):
            continue
        oi = float(item["openInterest"])
        out.append((ts_open, ts_close_norm, "BYBIT", symbol, oi, oi, oi, oi))
    return out

def fetch_binance_oi_5m(symbol: str, limit: int = 200) -> list[tuple]:
    data = _get(
        f"{BINANCE_BASE}/futures/data/openInterestHist",
        {"symbol": symbol, "period": "5m", "limit": limit},
    )
    out = []
    for item in data if isinstance(data, list) else []:
        ts_close_raw = _dt_from_ms(item["timestamp"])
        ts_open = ts_close_raw - timedelta(minutes=5)
        ts_close_norm = _norm_5m_close(ts_open)
        if not _closed(ts_close_norm):
            continue
        oi = float(item["sumOpenInterest"])
        out.append((ts_open, ts_close_norm, "BINANCE", symbol, oi, oi, oi, oi))
    return out

def fetch_bybit_kline_5m(symbol: str, limit: int = 200) -> tuple[list[tuple], list[tuple]]:
    data = _get(
        f"{BYBIT_BASE}/v5/market/kline",
        {"category": "linear", "symbol": symbol, "interval": "5", "limit": limit},
    )
    price_rows, volume_rows = [], []
    for item in reversed(data.get("result", {}).get("list", [])):
        ts_open = _dt_from_ms(item[0])
        ts_close_norm = _norm_5m_close(ts_open)
        if not _closed(ts_close_norm):
            continue
        price_rows.append((ts_open, ts_close_norm, "BYBIT", symbol, float(item[1]), float(item[2]), float(item[3]), float(item[4])))
        volume_rows.append((ts_open, ts_close_norm, "BYBIT", symbol, float(item[5])))
    return price_rows, volume_rows

def fetch_binance_kline_5m(symbol: str, limit: int = 200) -> tuple[list[tuple], list[tuple]]:
    data = _get(
        f"{BINANCE_BASE}/fapi/v1/klines",
        {"symbol": symbol, "interval": "5m", "limit": limit},
    )
    price_rows, volume_rows = [], []
    for item in data if isinstance(data, list) else []:
        ts_open = _dt_from_ms(item[0])
        # Binance отдаёт close_time как xx:04:59.999.
        # Для всех сравнений приводим к canonical close: ts_open + 5m.
        ts_close_norm = _norm_5m_close(ts_open)
        if not _closed(ts_close_norm):
            continue
        price_rows.append((ts_open, ts_close_norm, "BINANCE", symbol, float(item[1]), float(item[2]), float(item[3]), float(item[4])))
        volume_rows.append((ts_open, ts_close_norm, "BINANCE", symbol, float(item[5])))
    return price_rows, volume_rows
