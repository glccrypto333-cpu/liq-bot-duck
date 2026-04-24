from __future__ import annotations
import time
from datetime import datetime, timezone, timedelta
import requests
from config import BYBIT_BASE, BINANCE_BASE

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

def fetch_bybit_symbols() -> list[str]:
    data = _get(f"{BYBIT_BASE}/v5/market/instruments-info", {"category": "linear", "limit": 1000})
    items = data.get("result", {}).get("list", [])
    return [x["symbol"] for x in items if x.get("status") == "Trading" and x.get("quoteCoin") == "USDT"]

def fetch_binance_symbols() -> list[str]:
    data = _get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    return [x["symbol"] for x in data.get("symbols", []) if x.get("status") == "TRADING" and x.get("quoteAsset") == "USDT"]

def _dt_from_ms(ms) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)

def fetch_bybit_oi_5m(symbol: str, limit: int = 48) -> list[tuple]:
    data = _get(f"{BYBIT_BASE}/v5/market/open-interest", {"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": limit})
    items = list(reversed(data.get("result", {}).get("list", [])))
    out = []
    for item in items:
        close_dt = _dt_from_ms(item["timestamp"])
        open_dt = close_dt - timedelta(minutes=5)
        oi = float(item["openInterest"])
        out.append((open_dt, close_dt, "BYBIT", symbol, oi, oi, oi, oi))
    return out

def fetch_binance_oi_5m(symbol: str, limit: int = 48) -> list[tuple]:
    data = _get(f"{BINANCE_BASE}/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": limit})
    out = []
    for item in data if isinstance(data, list) else []:
        close_dt = _dt_from_ms(item["timestamp"])
        open_dt = close_dt - timedelta(minutes=5)
        oi = float(item["sumOpenInterest"])
        out.append((open_dt, close_dt, "BINANCE", symbol, oi, oi, oi, oi))
    return out

def fetch_bybit_kline_5m(symbol: str, limit: int = 48) -> tuple[list[tuple], list[tuple]]:
    data = _get(f"{BYBIT_BASE}/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": "5", "limit": limit})
    items = list(reversed(data.get("result", {}).get("list", [])))
    price_rows, volume_rows = [], []
    for item in items:
        ts_open = _dt_from_ms(item[0])
        ts_close = ts_open + timedelta(minutes=5)
        open_p = float(item[1])
        high_p = float(item[2])
        low_p = float(item[3])
        close_p = float(item[4])
        volume = float(item[5])
        price_rows.append((ts_open, ts_close, "BYBIT", symbol, open_p, high_p, low_p, close_p))
        volume_rows.append((ts_open, ts_close, "BYBIT", symbol, volume))
    return price_rows, volume_rows

def fetch_binance_kline_5m(symbol: str, limit: int = 48) -> tuple[list[tuple], list[tuple]]:
    data = _get(f"{BINANCE_BASE}/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": limit})
    price_rows, volume_rows = [], []
    for item in data if isinstance(data, list) else []:
        ts_open = _dt_from_ms(item[0])
        ts_close = _dt_from_ms(item[6])
        open_p = float(item[1])
        high_p = float(item[2])
        low_p = float(item[3])
        close_p = float(item[4])
        volume = float(item[5])
        price_rows.append((ts_open, ts_close, "BINANCE", symbol, open_p, high_p, low_p, close_p))
        volume_rows.append((ts_open, ts_close, "BINANCE", symbol, volume))
    return price_rows, volume_rows
