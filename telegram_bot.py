from __future__ import annotations

import time
import atexit
import threading
import fcntl
import zipfile
import json
import os
import subprocess
import re
import asyncio
import socket as _socket
import sys
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from queue import Full, Queue
import csv
from pathlib import Path
import requests

from card_renderers import build_phase_history_lines
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ПАПКА_ДАННЫХ, APP_VERSION
from logger import log
from db import fetch, execute, sync_stage3_volume_queue, mark_stage3_volume_queue_sent, mark_stage3_volume_queue_blocked
from phase_common import value_slope_ratio
from reset_stage3 import reset_stage3
from time_utils import iso_мск
from quote_turnover_snapshot import (
    POINTS_PER_4H,
    build_current_4h_distribution,
    evaluate_stage3_volume_candidate,
    build_stage3_price_snapshot,
    should_validate_stage3_price_gate,
    stage3_price_veto_reason,
)

_UNIVERSE_RUNTIME = Path("/home/alexey/openclaw/runtime")
if str(_UNIVERSE_RUNTIME) not in sys.path:
    sys.path.insert(0, str(_UNIVERSE_RUNTIME))
from asset_universe_guard import decide as _asset_universe_decide

BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
_polling_started = False
_offset = 0
_export_lock = threading.Lock()
_csv_lock = threading.Lock()
RUNTIME_REPORTS_DIR = Path(__file__).resolve().parent / "runtime_reports"
RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"
SIGNAL_CHANNEL_TARGET_FILE = RUNTIME_DIR / "signal_channel_chat_id.txt"
SIGNAL_CHANNEL_QUEUE_MAXSIZE = 128
_signal_channel_delivery_queue: Queue[dict] = Queue(maxsize=SIGNAL_CHANNEL_QUEUE_MAXSIZE)
_signal_channel_worker_started = False
POLLING_LOCK_PATH = ПАПКА_ДАННЫХ / "telegram_polling.lock"
_polling_lock_file = None
BYBIT_SYMBOL_ALIASES = {
    "CHIPPUSDT": "CHIPUSDT",
}
BINANCE_FAPI_BASE = "https://fapi.binance.com"
BYBIT_API_BASE = "https://api.bybit.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
МОСКВА = timezone(timedelta(hours=3))
_api_cache_lock = threading.Lock()
_api_cache: dict[str, dict] = {}
_rank_cache: dict[str, object] = {"ts": 0.0, "data": {}}

# Фикс IPv6-blackhole для Telegram, уже доказавший себя в Moose.
# Сохраняем текущий transport Duck, но форсим IPv4 только для api.telegram.org,
# чтобы multipart-отправка скриншотов не висла на IPv6 timeout.
_orig_getaddrinfo = _socket.getaddrinfo


def _getaddrinfo_ipv4_telegram(host, *args, **kwargs):
    res = _orig_getaddrinfo(host, *args, **kwargs)
    if isinstance(host, str) and host.endswith("api.telegram.org"):
        v4 = [r for r in res if r[0] == _socket.AF_INET]
        if v4:
            return v4
    return res


_socket.getaddrinfo = _getaddrinfo_ipv4_telegram


def _cache_get(key: str, ttl: int = 20):
    with _api_cache_lock:
        row = _api_cache.get(key)
        if not row:
            return None
        if time.time() - float(row.get("ts") or 0.0) >= ttl:
            return None
        return row.get("data")


def _cache_set(key: str, data) -> None:
    with _api_cache_lock:
        _api_cache[key] = {"ts": time.time(), "data": data}


def _http_json(url: str, params: dict | None = None, *, ttl: int = 20, cache_key: str | None = None):
    key = cache_key or f"{url}?{json.dumps(params or {}, sort_keys=True, ensure_ascii=False)}"
    cached = _cache_get(key, ttl=ttl)
    if cached is not None:
        return cached
    try:
        response = requests.get(url, params=params or {}, timeout=12)
        response.raise_for_status()
        data = response.json()
        _cache_set(key, data)
        return data
    except Exception as exc:
        log(f"telegram live metrics api error: url={url} exc={exc}")
        return None


def _inline_keyboard(rows: list[list[tuple[str, str]]]) -> dict:
    return {
        "inline_keyboard": [
            [{"text": text, "callback_data": data} for text, data in row]
            for row in rows
        ]
    }


def _coin_actions_keyboard(symbol: str, stage: int | None = None) -> dict | None:
    del symbol, stage
    return None


def _feedback_prompt_keyboard(symbol: str) -> dict | None:
    del symbol
    return None


def _stage3_reset_actions_keyboard(symbol: str | None = None, allow_all: bool = False) -> dict:
    rows: list[list[tuple[str, str]]] = []
    if symbol:
        rows.append([("✅ Подтвердить reset", f"rstconfirm:{symbol}"), ("🚫 Отмена", "rstcancel")])
    if allow_all:
        rows.append([("🧨 Сбросить все Stage 3", "rstall:create")])
    return _inline_keyboard(rows or [[("🚫 Отмена", "rstcancel")]])


def _fmt_minutes(value) -> str:
    try:
        total = int(round(float(value or 0.0)))
    except Exception:
        return "n/a"
    hours, minutes = divmod(max(total, 0), 60)
    if hours <= 0:
        return f"{minutes}м"
    return f"{hours}ч {minutes}м"


def _fmt_ratio(value) -> str:
    try:
        return f"{float(value):.6f}"
    except Exception:
        return "n/a"


def _fmt_pct_signed(value) -> str:
    try:
        num = float(value)
    except Exception:
        return "n/a"
    arrow = "⬆️" if num > 0 else ("⬇️" if num < 0 else "➡️")
    return f"{arrow} {num:+.2f}%"


def _fmt_pct_plain(value) -> str:
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "n/a"


def _fmt_usd(value) -> str:
    try:
        num = float(value or 0.0)
    except Exception:
        return "н/д"
    if abs(num) >= 1_000_000_000:
        return f"${num / 1_000_000_000:.2f}B"
    if abs(num) >= 1_000_000:
        return f"${num / 1_000_000:.2f}M"
    if abs(num) >= 1_000:
        return f"${num / 1_000:.2f}K"
    return f"${num:.2f}"


def _fmt_usd_or_na(value) -> str:
    try:
        num = float(value)
    except Exception:
        return "н/д"
    if num <= 0:
        return "н/д"
    return _fmt_usd(num)


def _fmt_pct_or_na(value) -> str:
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "н/д"


def _human_oi_slope(value: str | None) -> str:
    return {
        "flat": "плоско",
        "weak_up": "слабый рост",
        "good_up": "хороший рост",
        "strong_up": "сильный рост",
        "weak_down": "слабое снижение",
        "strong_down": "сильное снижение",
    }.get(str(value or "").strip(), str(value or "н/д"))


def _visual_oi_slope(value: str | None) -> str:
    return {
        "strong_down": "⬜️⬜️⬜️⬜️⬜️",
        "weak_down": "1️⃣⬜️⬜️⬜️⬜️",
        "flat": "1️⃣2️⃣⬜️⬜️⬜️",
        "weak_up": "1️⃣2️⃣3️⃣⬜️⬜️",
        "good_up": "1️⃣2️⃣3️⃣4️⃣⬜️",
        "strong_up": "1️⃣2️⃣3️⃣4️⃣5️⃣",
    }.get(str(value or "").strip(), "⬜️⬜️⬜️⬜️⬜️")


def _visual_strength_token(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "⬜️⬜️⬜️⬜️⬜️"
    if text in {"сильное", "сильный", "очень_гладко", "очень гладко", "подтвержденное", "подтвержденный", "сильная", "хорошая"}:
        return "1️⃣2️⃣3️⃣4️⃣5️⃣"
    if text in {"хороший", "почти_нет", "почти нет", "гладко", "легкий", "легкая"}:
        return "1️⃣2️⃣3️⃣4️⃣⬜️"
    if text in {"замедляется, но держится", "ровно / без явного ускорения"}:
        return "1️⃣2️⃣3️⃣4️⃣⬜️"
    if text in {"средняя", "рабочая", "рабочий", "средне", "заметный"}:
        return "1️⃣2️⃣3️⃣⬜️⬜️"
    if text in {"нет", "срыв", "рвано", "ускоряется вниз", "плоско", "слабая", "слабый", "сильный", "доминирующий"}:
        return "1️⃣2️⃣⬜️⬜️⬜️"
    if text in {"плохая", "плохой", "пила", "мертвый", "всплеск_с_боковиком", "всплеск с боковиком"}:
        return "1️⃣⬜️⬜️⬜️⬜️"
    return "1️⃣2️⃣3️⃣⬜️⬜️"


def _price_arrow(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in {"сильно вниз", "вниз", "падение", "strong_down", "weak_down"} or "вниз" in text or "down" in text:
        return "⬇️"
    if text in {"сильный рост", "рост", "вверх", "strong_up", "good_up", "weak_up"} or "рост" in text or "up" in text or "вверх" in text:
        return "⬆️"
    return "➡️"


def _volume_icon(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in {"подтверждающий", "рабочий"}:
        return "🟢"
    if text in {"слабый"}:
        return "🟡"
    return "🔴"


def _bang_marks(metric_kind: str, value) -> str:
    try:
        num = float(value)
    except Exception:
        return ""
    key = str(metric_kind or "").lower()
    if key == "funding":
        if num <= -1.0:
            return "❗️❗️❗️"
        if num <= -0.5:
            return "❗️❗️"
        if num <= -0.1:
            return "❗️"
        return ""
    if num <= 0:
        return ""
    thresholds = {
        "volume": (100.0, 1000.0, 10000.0),
        "price": (10.0, 25.0, 100.0),
        "oi": (10.0, 25.0, 100.0),
    }.get(key)
    if not thresholds:
        return ""
    if num > thresholds[2]:
        return "❗️❗️❗️"
    if num > thresholds[1]:
        return "❗️❗️"
    if num > thresholds[0]:
        return "❗️"
    return ""


def _format_ts_compact(value) -> str:
    text = str(value or "").strip()
    if not text:
        return "н/д"
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(МОСКВА).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return text


def _format_ts_moscow_short(value) -> str:
    text = str(value or "").strip()
    if not text:
        return "н/д"
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(МОСКВА).strftime("%d.%m %H:%M МСК")
    except Exception:
        return text


def _phase_history_age(history_rows: list[dict], stage: int, fallback) -> str:
    age = _find_transition_age(history_rows, stage)
    if age == "n/a":
        return _fmt_minutes(fallback)
    return age


def _find_transition_ts(history_rows: list[dict], to_stage: int):
    for row in history_rows:
        if int(row.get("to_stage") or -1) == to_stage:
            return row.get("cycle_ts")
    return None


def _phase_history_line(history_rows: list[dict], *, phase_number: int, age_stage: int, entry_stage: int, fallback) -> str:
    age_text = _phase_history_age(history_rows, age_stage, fallback)
    entry_ts = _find_transition_ts(history_rows, entry_stage)
    if entry_ts:
        return f"Фаза {phase_number} - {age_text} | вход: {_format_ts_moscow_short(entry_ts)}"
    return f"Фаза {phase_number} - {age_text}"


def _phase_zero_history_line(history_rows: list[dict]) -> str | None:
    for row in history_rows:
        try:
            raw_from = row.get("from_stage")
            raw_to = row.get("to_stage")
            from_stage = int(-1 if raw_from is None else raw_from)
            to_stage = int(-1 if raw_to is None else raw_to)
        except Exception:
            continue
        if from_stage == 0 and to_stage >= 1:
            age = _fmt_minutes(row.get("stage_age_before_transition"))
            ts = _format_ts_moscow_short(row.get("cycle_ts"))
            reason = _human_phase_reason(row.get("reason"))
            if reason and reason != "n/a":
                return f"Фаза 0 - {age} | запрет снят: {ts} | до этого: {reason}"
            return f"Фаза 0 - {age} | запрет снят: {ts}"
    for row in history_rows:
        try:
            raw_to = row.get("to_stage")
            to_stage = int(-1 if raw_to is None else raw_to)
        except Exception:
            continue
        if to_stage == 0:
            reason = _human_phase_reason(row.get("reason"))
            ts = _format_ts_moscow_short(row.get("cycle_ts"))
            return f"Фаза 0 - был запрет | вход: {ts} | {reason}"
    return None


def _oi_ratio_note(row: dict) -> str:
    for key in ("oi_slope_ratio", "slope_ratio", "value_slope_ratio"):
        value = row.get(key)
        if value is None:
            continue
        try:
            return f" (K={float(value):.3f})"
        except Exception:
            continue
    return ""


def _stage_entry_ts(history_rows: list[dict], stage: int):
    ts = _find_transition_ts(history_rows, stage)
    return _format_ts_moscow_short(ts) if ts else "н/д"


def _current_stage_line(history_rows: list[dict], current_stage: int, current_age_minutes) -> str:
    age = _fmt_minutes(current_age_minutes)
    ts = _stage_entry_ts(history_rows, current_stage)
    return f"Фаза {current_stage} - {age} | вход: {ts} | текущая стадия"


def _pullback_ratio_from_window(row: dict) -> float:
    try:
        open_value = float(row.get("open_value") or 0.0)
        high_value = float(row.get("high_value") or 0.0)
        close_value = float(row.get("close_value") or 0.0)
    except Exception:
        return 0.0
    if high_value <= open_value:
        return 0.0
    return max(0.0, (high_value - close_value) / (high_value - open_value))


def _oi_window_explanation(row: dict) -> str:
    if not row:
        return "нет окна для пояснения"
    slope_class = str(row.get("oi_slope_class") or "").strip()
    ratio = _pullback_ratio_from_window(row)
    if slope_class in {"strong_up", "good_up", "weak_up"}:
        if ratio >= 0.60:
            return "рост уже с откатом внутри окна"
        if ratio >= 0.25:
            return "рост есть, но конец окна уже остыл"
        return "рост держится до конца окна"
    if slope_class == "flat":
        return "окно без выраженного набора"
    if slope_class in {"weak_down", "strong_down"}:
        return "внутри окна набор ослаб"
    return "нет пояснения"


def _volume_window_base_note(metric_windows: dict[tuple[str, str], dict], window_code: str) -> str:
    row = metric_windows.get(("VOLUME", window_code)) or {}
    if not row:
        return "нет сравнения с тихой базой Ф1"
    ratio = value_slope_ratio(row)
    if ratio >= 1.05:
        return f"выше тихой базы Ф1 в {ratio:.1f}x"
    if ratio <= 0.95:
        return f"ниже тихой базы Ф1, только {ratio:.1f}x"
    return f"около тихой базы Ф1, {ratio:.1f}x"


def _phase_reason_lines(value: str | None) -> list[str]:
    human = _human_phase_reason(value)
    if human == "n/a":
        return ["n/a"]
    parts = [chunk.strip() for chunk in human.split(";") if chunk.strip()]
    return parts or [human]


def _prefer_metric_exchange(symbol: str, current_exchange: str) -> str:
    symbol = str(symbol or "").upper().strip()
    current_exchange = str(current_exchange or "").upper().strip() or "BINANCE"
    if _symbol_exists_on_exchange("BINANCE", symbol):
        return "BINANCE"
    return current_exchange


def _metric_row(metric_windows: dict[tuple[str, str], dict], metric: str, window_code: str) -> dict:
    return metric_windows.get((metric, window_code)) or {}


def _strip_symbol_quote(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    for suffix in ("USDT", "PERP", "USD"):
        if sym.endswith(suffix):
            sym = sym[: -len(suffix)]
            break
    if sym.startswith("1000"):
        sym = sym[4:]
    return sym


def _coingecko_rank(symbol: str) -> str:
    now = time.time()
    with _api_cache_lock:
        data = _rank_cache.get("data") or {}
        ts = float(_rank_cache.get("ts") or 0.0)
        if data and now - ts < 60:
            rank = data.get(_strip_symbol_quote(symbol))
            return f"#{rank}" if rank else ">250 / н/д"
    rows = _http_json(
        f"{COINGECKO_BASE}/coins/markets",
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false",
        },
        ttl=60,
        cache_key="coingecko_top250",
    ) or []
    mapping: dict[str, int] = {}
    for row in rows if isinstance(rows, list) else []:
        base = str(row.get("symbol") or "").upper().strip()
        rank = row.get("market_cap_rank")
        try:
            if base and rank:
                mapping[base] = int(rank)
        except Exception:
            continue
    with _api_cache_lock:
        _rank_cache["ts"] = now
        _rank_cache["data"] = mapping
    rank = mapping.get(_strip_symbol_quote(symbol))
    return f"#{rank}" if rank else ">250 / н/д"


def _resolve_binance_symbol(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    if _symbol_exists_on_exchange("BINANCE", sym):
        return sym
    if sym.startswith("1000"):
        base = sym[4:]
        if _symbol_exists_on_exchange("BINANCE", base):
            return base
    return ""


def _pct_change(current, previous):
    try:
        cur = float(current)
        prev = float(previous)
        if prev == 0:
            return None
        return ((cur - prev) / prev) * 100.0
    except Exception:
        return None


def _sum_quote_volume(rows: list, start: int, end: int | None = None, *, quote_index: int = 7):
    try:
        sliced = rows[start:end]
        if not sliced:
            return None
        return sum(float(row[quote_index]) for row in sliced)
    except Exception:
        return None


def _closed_minute_kline_rows(rows: list, *, now_ms: int | None = None) -> list:
    """Remove only the still-forming one-minute candle from an ordered kline list."""
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    closed: list = []
    for row in rows or ():
        try:
            if int(row[0]) + 60_000 <= int(now_ms):
                closed.append(row)
        except (TypeError, ValueError, IndexError):
            continue
    return closed


def _empty_kline_metrics() -> dict:
    return {
        "price_pct_1h": None,
        "price_pct_4h": None,
        "vol_1h_usd": None,
        "vol_4h_usd": None,
        "vol_pct_1h": None,
        "vol_pct_4h": None,
    }


def _fetch_binance_kline_metrics(symbol: str) -> dict:
    sym = _resolve_binance_symbol(symbol)
    if not sym:
        return _empty_kline_metrics()
    data = _http_json(
        f"{BINANCE_FAPI_BASE}/fapi/v1/klines",
        {"symbol": sym, "interval": "1m", "limit": 481},
        ttl=20,
        cache_key=f"binance_klines_1m:{sym}",
    )
    rows = _closed_minute_kline_rows(data if isinstance(data, list) else [])
    out = _empty_kline_metrics()
    if len(rows) < 60:
        return out
    try:
        close_now = float(rows[-1][4])
        open_1h = float(rows[-60][1])
        out["price_pct_1h"] = _pct_change(close_now, open_1h)
        out["vol_1h_usd"] = _sum_quote_volume(rows, -60, None)
    except Exception:
        pass
    if len(rows) >= 240:
        try:
            open_4h = float(rows[-240][1])
            out["price_pct_4h"] = _pct_change(close_now, open_4h)
            out["vol_4h_usd"] = _sum_quote_volume(rows, -240, None)
        except Exception:
            pass
    if len(rows) >= 120:
        prev_1h = _sum_quote_volume(rows, -120, -60)
        out["vol_pct_1h"] = _pct_change(out["vol_1h_usd"], prev_1h)
    if len(rows) >= 480:
        prev_4h = _sum_quote_volume(rows, -480, -240)
        out["vol_pct_4h"] = _pct_change(out["vol_4h_usd"], prev_4h)
    return out


def _fetch_bybit_kline_metrics(symbol: str) -> dict:
    """Fallback quote-turnover metrics for a contract absent from Binance."""
    sym, _ = _resolve_bybit_symbol(symbol)
    if not sym:
        return _empty_kline_metrics()
    data = _http_json(
        f"{BYBIT_API_BASE}/v5/market/kline",
        {"category": "linear", "symbol": sym, "interval": "1", "limit": 481},
        ttl=20,
        cache_key=f"bybit_klines_1m:{sym}",
    ) or {}
    rows = ((data.get("result") or {}).get("list")) if isinstance(data, dict) else []
    rows = sorted(rows if isinstance(rows, list) else [], key=lambda row: int(row[0]))
    rows = _closed_minute_kline_rows(rows)
    out = _empty_kline_metrics()
    if len(rows) < 60:
        return out
    try:
        close_now = float(rows[-1][4])
        out["price_pct_1h"] = _pct_change(close_now, float(rows[-60][1]))
        out["vol_1h_usd"] = _sum_quote_volume(rows, -60, None, quote_index=6)
    except (TypeError, ValueError, IndexError):
        pass
    if len(rows) >= 240:
        try:
            out["price_pct_4h"] = _pct_change(close_now, float(rows[-240][1]))
            out["vol_4h_usd"] = _sum_quote_volume(rows, -240, None, quote_index=6)
        except (TypeError, ValueError, IndexError):
            pass
    if len(rows) >= 120:
        out["vol_pct_1h"] = _pct_change(out["vol_1h_usd"], _sum_quote_volume(rows, -120, -60, quote_index=6))
    if len(rows) >= 480:
        out["vol_pct_4h"] = _pct_change(out["vol_4h_usd"], _sum_quote_volume(rows, -480, -240, quote_index=6))
    return out


def _fetch_binance_24h_metrics(symbol: str) -> dict:
    sym = _resolve_binance_symbol(symbol)
    if not sym:
        return {"price_pct_24h": None, "vol_usd_24h": None}
    data = _http_json(
        f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr",
        {"symbol": sym},
        ttl=20,
        cache_key=f"binance_24h:{sym}",
    ) or {}
    return {
        "price_pct_24h": _safe_float(data.get("priceChangePercent")),
        "vol_usd_24h": _safe_float(data.get("quoteVolume")),
    }


def _bybit_linear_ticker(symbol: str) -> dict:
    sym, _ = _resolve_bybit_symbol(symbol)
    if not sym:
        return {}
    data = _http_json(
        f"{BYBIT_API_BASE}/v5/market/tickers",
        {"category": "linear", "symbol": sym},
        ttl=20,
        cache_key=f"bybit_ticker:{sym}",
    ) or {}
    rows = ((data.get("result") or {}).get("list")) if isinstance(data, dict) else []
    return rows[0] if isinstance(rows, list) and rows else {}


def _fetch_bybit_24h_metrics(symbol: str) -> dict:
    row = _bybit_linear_ticker(symbol)
    return {
        "price_pct_24h": _safe_float(row.get("price24hPcnt"), scale=100.0),
        "vol_usd_24h": _safe_float(row.get("turnover24h")),
    }


def _fetch_binance_oi_metrics(symbol: str) -> dict:
    sym = _resolve_binance_symbol(symbol)
    if not sym:
        return {
            "oi_now_usd": None,
            "oi_pct_5m": None,
            "oi_pct_4h": None,
        }
    hist_5m = _http_json(
        f"{BINANCE_FAPI_BASE}/futures/data/openInterestHist",
        {"symbol": sym, "period": "5m", "limit": 2},
        ttl=20,
        cache_key=f"binance_oi_hist_5m:{sym}",
    )
    hist_4h = _http_json(
        f"{BINANCE_FAPI_BASE}/futures/data/openInterestHist",
        {"symbol": sym, "period": "4h", "limit": 2},
        ttl=20,
        cache_key=f"binance_oi_hist_4h:{sym}",
    )
    out = {
        "oi_now_usd": None,
        "oi_pct_5m": None,
        "oi_pct_4h": None,
    }
    rows_5m = hist_5m if isinstance(hist_5m, list) else []
    rows_4h = hist_4h if isinstance(hist_4h, list) else []
    if rows_5m:
        out["oi_now_usd"] = _safe_float(rows_5m[-1].get("sumOpenInterestValue"))
    if len(rows_5m) >= 2:
        out["oi_pct_5m"] = _pct_change(rows_5m[-1].get("sumOpenInterest"), rows_5m[-2].get("sumOpenInterest"))
    if len(rows_4h) >= 2:
        out["oi_pct_4h"] = _pct_change(rows_4h[-1].get("sumOpenInterest"), rows_4h[-2].get("sumOpenInterest"))
    return out


def _bybit_open_interest_rows(symbol: str, interval: str) -> list[dict]:
    sym, _ = _resolve_bybit_symbol(symbol)
    if not sym:
        return []
    data = _http_json(
        f"{BYBIT_API_BASE}/v5/market/open-interest",
        {"category": "linear", "symbol": sym, "intervalTime": interval, "limit": 2},
        ttl=20,
        cache_key=f"bybit_oi_hist:{sym}:{interval}",
    ) or {}
    rows = ((data.get("result") or {}).get("list")) if isinstance(data, dict) else []
    if not isinstance(rows, list):
        return []
    return sorted(rows, key=lambda row: int(row.get("timestamp") or 0))


def _fetch_bybit_oi_metrics(symbol: str) -> dict:
    ticker = _bybit_linear_ticker(symbol)
    rows_5m = _bybit_open_interest_rows(symbol, "5min")
    rows_4h = _bybit_open_interest_rows(symbol, "4h")
    out = {
        "oi_now_usd": _safe_float(ticker.get("openInterestValue")),
        "oi_pct_5m": None,
        "oi_pct_4h": None,
    }
    if len(rows_5m) >= 2:
        out["oi_pct_5m"] = _pct_change(rows_5m[-1].get("openInterest"), rows_5m[-2].get("openInterest"))
    if len(rows_4h) >= 2:
        out["oi_pct_4h"] = _pct_change(rows_4h[-1].get("openInterest"), rows_4h[-2].get("openInterest"))
    return out


def _fetch_binance_account_ratio(symbol: str) -> dict:
    sym = _resolve_binance_symbol(symbol)
    if not sym:
        return {"long_pct": None, "short_pct": None}
    data = _http_json(
        f"{BINANCE_FAPI_BASE}/futures/data/globalLongShortAccountRatio",
        {"symbol": sym, "period": "5m", "limit": 1},
        ttl=20,
        cache_key=f"binance_long_short:{sym}",
    )
    rows = data if isinstance(data, list) else []
    if not rows:
        return {"long_pct": None, "short_pct": None}
    row = rows[-1]
    return {
        "long_pct": _safe_float(row.get("longAccount"), scale=100.0),
        "short_pct": _safe_float(row.get("shortAccount"), scale=100.0),
    }


def _fetch_bybit_account_ratio(symbol: str) -> dict:
    sym, _ = _resolve_bybit_symbol(symbol)
    if not sym:
        return {"long_pct": None, "short_pct": None}
    data = _http_json(
        f"{BYBIT_API_BASE}/v5/market/account-ratio",
        {"category": "linear", "symbol": sym, "period": "5min", "limit": 1},
        ttl=20,
        cache_key=f"bybit_account_ratio:{sym}",
    ) or {}
    rows = ((data.get("result") or {}).get("list")) if isinstance(data, dict) else []
    row = rows[0] if isinstance(rows, list) and rows else {}
    return {
        "long_pct": _safe_float(row.get("buyRatio"), scale=100.0),
        "short_pct": _safe_float(row.get("sellRatio"), scale=100.0),
    }


def _fetch_bybit_funding(symbol: str, exchange: str) -> dict:
    ex = str(exchange or "").upper().strip()
    bybit_symbol = str(symbol or "").upper().strip() if ex == "BYBIT" else _resolve_bybit_symbol(symbol)[0]
    if not bybit_symbol:
        return {"funding_pct": None}
    data = _http_json(
        f"{BYBIT_API_BASE}/v5/market/tickers",
        {"category": "linear", "symbol": bybit_symbol},
        ttl=20,
        cache_key=f"bybit_funding:{bybit_symbol}",
    ) or {}
    rows = (((data.get("result") or {}).get("list")) or [])
    first = rows[0] if rows else {}
    return {"funding_pct": _safe_float(first.get("fundingRate"), scale=100.0)}


def _safe_float(value, scale: float = 1.0):
    try:
        return float(value) * scale
    except Exception:
        return None


def _pct_with_marks(value, thresholds: tuple[float, float, float], *, negative_only: bool = False) -> str:
    if value is None:
        return "н/д"
    try:
        num = float(value)
    except Exception:
        return "н/д"
    arrow = "⬆️" if num > 0 else ("⬇️" if num < 0 else "")
    base = f"{num:+.2f}%"
    av = abs(num)
    marks = ""
    if negative_only:
        if num < thresholds[2]:
            marks = "❗️❗️❗️"
        elif num < thresholds[1]:
            marks = "❗️❗️"
        elif num < thresholds[0]:
            marks = "❗️"
    else:
        if av > thresholds[2]:
            marks = "❗️❗️❗️"
        elif av > thresholds[1]:
            marks = "❗️❗️"
        elif av > thresholds[0]:
            marks = "❗️"
    if arrow and marks:
        return f"{arrow} {base}{marks}"
    if arrow:
        return f"{arrow} {base}"
    return base


def _account_sentiment(value, side: str) -> str:
    try:
        pct = float(value)
    except Exception:
        return "➡️"
    if side == "long":
        return "⬆️" if pct >= 50.0 else "⬇️"
    return "⬆️" if pct >= 50.0 else "⬇️"


def _build_market_metrics_block(metrics: dict) -> list[str]:
    """Render the Tiger-compatible market snapshot; metrics never affect stages."""
    klines = metrics.get("klines") or {}
    ticker24 = metrics.get("ticker24") or {}
    oi = metrics.get("oi") or {}
    accounts = metrics.get("accounts") or {}
    funding = metrics.get("funding") or {}
    market_source = str(metrics.get("market_source") or "").upper()
    funding_source = str(metrics.get("funding_source") or "BYBIT").upper()
    source_label = {"BINANCE": "Binance", "BYBIT": "Bybit"}.get(market_source, "н/д")
    funding_label = {"BINANCE": "Binance", "BYBIT": "Bybit"}.get(funding_source, "н/д")

    rank = metrics.get("rank") or ">250 / н/д"
    long_pct = accounts.get("long_pct")
    short_pct = accounts.get("short_pct")
    return [
        f"<b>🏷 Капа-рейтинг:</b> {rank}",
        "",
        f"<b>💵 Объём ({source_label}):</b>",
        f"1ч: {_fmt_usd_or_na(klines.get('vol_1h_usd'))} | "
        f"{_pct_with_marks(klines.get('vol_pct_1h'), (100.0, 1000.0, 10000.0))}",
        f"4ч: {_fmt_usd_or_na(klines.get('vol_4h_usd'))} | "
        f"{_pct_with_marks(klines.get('vol_pct_4h'), (100.0, 1000.0, 10000.0))}",
        "",
        f"<b>📈 Рост цены ({source_label}):</b>",
        f"1ч: {_pct_with_marks(klines.get('price_pct_1h'), (10.0, 25.0, 100.0))}",
        f"4ч: {_pct_with_marks(klines.get('price_pct_4h'), (10.0, 25.0, 100.0))}",
        f"24ч: {_pct_with_marks(ticker24.get('price_pct_24h'), (10.0, 25.0, 100.0))}",
        "",
        f"<b>📊 Открытый интерес ({source_label}):</b> сейчас: {_fmt_usd_or_na(oi.get('oi_now_usd'))}",
        f"5м: {_pct_with_marks(oi.get('oi_pct_5m'), (10.0, 25.0, 100.0))}",
        f"4ч: {_pct_with_marks(oi.get('oi_pct_4h'), (10.0, 25.0, 100.0))}",
        "",
        f"<b>👥 Аккаунты ({source_label}):</b>",
        f"{_account_sentiment(long_pct, 'long')} лонг: {_fmt_pct_or_na(long_pct)} | "
        f"{_account_sentiment(short_pct, 'short')} шорт: {_fmt_pct_or_na(short_pct)}",
        "",
        f"<b>🩸 Фандинг ({funding_label}):</b>",
        _pct_with_marks(funding.get('funding_pct'), (0.5, 1.0, 2.0)),
    ]


def _live_market_metrics(symbol: str, exchange: str) -> dict:
    cache_key = f"duck_live_metrics:{exchange}:{symbol}"
    cached = _cache_get(cache_key, ttl=20)
    if cached is not None:
        return cached
    binance_symbol = _resolve_binance_symbol(symbol)
    market_source = "BINANCE" if binance_symbol else "BYBIT"
    if market_source == "BINANCE":
        kline_fetcher = _fetch_binance_kline_metrics
        ticker_fetcher = _fetch_binance_24h_metrics
        oi_fetcher = _fetch_binance_oi_metrics
        accounts_fetcher = _fetch_binance_account_ratio
    else:
        kline_fetcher = _fetch_bybit_kline_metrics
        ticker_fetcher = _fetch_bybit_24h_metrics
        oi_fetcher = _fetch_bybit_oi_metrics
        accounts_fetcher = _fetch_bybit_account_ratio
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            "rank": executor.submit(_coingecko_rank, symbol),
            "klines": executor.submit(kline_fetcher, symbol),
            "ticker24": executor.submit(ticker_fetcher, symbol),
            "oi": executor.submit(oi_fetcher, symbol),
            "accounts": executor.submit(accounts_fetcher, symbol),
            "funding": executor.submit(_fetch_bybit_funding, symbol, exchange),
        }
        out = {}
        for key, future in futures.items():
            try:
                out[key] = future.result(timeout=15)
            except Exception as exc:
                log(f"telegram live metrics future error: key={key} exc={exc}")
                out[key] = {} if key != "rank" else ">250 / н/д"
    out["market_source"] = market_source
    out["volume_source"] = market_source
    _cache_set(cache_key, out)
    return out


def _stage3_volume_gate(metrics: dict) -> tuple[bool, str]:
    """Hard signal gate: only a known 4h quote-turnover rise of at least 100% passes."""
    try:
        growth = (metrics.get("klines") or {}).get("vol_pct_4h")
        if growth is None:
            return False, "missing_4h_volume"
        if float(growth) < 100.0:
            return False, "below_100pct"
        return True, "pass"
    except (AttributeError, TypeError, ValueError):
        return False, "missing_4h_volume"


def _db_quote_turnover_state(symbol: str, stage3_transition_ts=None, queue_exchange=None) -> dict:
    """Read the canonical persisted source-native volume evidence used by the Stage-3 gate."""
    binance_symbol = _resolve_binance_symbol(symbol)
    if binance_symbol:
        source, source_symbol = "BINANCE", binance_symbol
    else:
        source_symbol, _ = _resolve_bybit_symbol(symbol)
        source = "BYBIT"
    if not source_symbol:
        return {"source": source, "symbol": "", "ready": False, "quality_reason": "missing_contract", "growth_4h_pct": None}
    rows = _safe_rows(f"""
        SELECT q.ready, q.quality_reason, q.growth_4h_pct,
               q.previous_1h_quote, q.current_1h_quote, q.growth_1h_pct,
               q.previous_4h_quote, q.current_4h_quote,
               q.previous_4h_points, q.current_4h_points, q.freshness_seconds,
               q.source_cycle_ts, q.latest_ts_close,
               first_queue.status AS existing_queue_status,
               first_queue.volume_unlocked_at AS first_volume_unlocked_at,
               NULLIF(first_queue.volume_snapshot->>'quote_window_latest_close','')::timestamptz
                   AS first_volume_unlock_cycle_ts,
               current_window.ts_opens AS current_4h_quote_opens,
               current_window.quote_points AS current_4h_quote_points
        FROM quote_turnover_state q
        LEFT JOIN LATERAL (
            SELECT
                ARRAY_AGG(recent.ts_open ORDER BY recent.ts_open) AS ts_opens,
                ARRAY_AGG(recent.quote_turnover ORDER BY recent.ts_open) AS quote_points
            FROM (
                SELECT v.ts_open, v.quote_turnover
                FROM volume_raw v
                WHERE v.exchange = q.exchange AND v.symbol = q.symbol
                  AND v.ts_close <= q.latest_ts_close
                ORDER BY v.ts_open DESC
                LIMIT {POINTS_PER_4H}
            ) recent
        ) current_window ON TRUE
        LEFT JOIN stage3_volume_queue first_queue
          ON first_queue.exchange=%s
         AND first_queue.symbol=%s
         AND first_queue.stage3_transition_ts=%s
        WHERE q.exchange = %s AND q.symbol = %s
    """, (queue_exchange, symbol, stage3_transition_ts, source, source_symbol))
    state = dict(rows[0]) if rows else {}
    state["source"] = source
    state["symbol"] = source_symbol
    state.setdefault("ready", False)
    state.setdefault("quality_reason", "missing_state")
    state.setdefault("growth_4h_pct", None)
    distribution = build_current_4h_distribution(
        state.pop("current_4h_quote_points", None) or [],
        ts_opens=state.pop("current_4h_quote_opens", None) or [],
        expected_latest_close=state.get("latest_ts_close"),
    )
    state["current_4h_distribution_status"] = "incomplete_or_non_contiguous_raw_window"
    if distribution is not None:
        current_total = state.get("current_4h_quote")
        try:
            from math import isclose
            matches_gate_window = current_total is not None and isclose(
                sum(distribution["hourly_quote_totals"]),
                float(current_total),
                rel_tol=1e-9,
                abs_tol=1e-6,
            )
        except (TypeError, ValueError):
            matches_gate_window = False
        if matches_gate_window:
            state["current_4h_distribution"] = distribution
            state["current_4h_distribution_status"] = "ok"
        else:
            state["current_4h_distribution"] = None
            state["current_4h_distribution_status"] = "window_total_mismatch"
    else:
        state["current_4h_distribution"] = None
    return state


def _stage3_volume_observation_snapshot(state: dict) -> dict:
    source_cycle_ts = state.get("source_cycle_ts")
    return {
        "source": state.get("source"),
        "source_symbol": state.get("symbol"),
        "source_cycle_ts": source_cycle_ts.isoformat() if hasattr(source_cycle_ts, "isoformat") else source_cycle_ts,
        "volume_ready": bool(state.get("ready")),
        "volume_quality_reason": state.get("quality_reason"),
        "quote_window_latest_close": (
            state.get("latest_ts_close").isoformat()
            if hasattr(state.get("latest_ts_close"), "isoformat")
            else state.get("latest_ts_close")
        ),
        "current_4h_distribution_status": state.get("current_4h_distribution_status") or (
            "ok" if state.get("current_4h_distribution") is not None else "unavailable"
        ),
        "previous_1h_quote": state.get("previous_1h_quote"),
        "current_1h_quote": state.get("current_1h_quote"),
        "growth_1h_pct": state.get("growth_1h_pct"),
        "previous_4h_quote": state.get("previous_4h_quote"),
        "current_4h_quote": state.get("current_4h_quote"),
        "growth_4h_pct": state.get("growth_4h_pct"),
        "previous_4h_points": state.get("previous_4h_points"),
        "current_4h_points": state.get("current_4h_points"),
        "freshness_seconds": state.get("freshness_seconds"),
        "current_4h_distribution": state.get("current_4h_distribution"),
    }


def _apply_db_volume_snapshot(metrics: dict, snapshot: dict) -> dict:
    """Render 1h/4h turnover from the exact DB snapshot used by the signal gate."""
    out = dict(metrics or {})
    klines = dict(out.get("klines") or {})
    klines.update({
        "vol_1h_usd": snapshot.get("current_1h_quote"),
        "vol_pct_1h": snapshot.get("growth_1h_pct"),
        "vol_4h_usd": snapshot.get("current_4h_quote"),
        "vol_pct_4h": snapshot.get("growth_4h_pct"),
    })
    out["klines"] = klines
    source = str(snapshot.get("source") or "н/д").upper()
    out["market_source"] = source
    out["volume_source"] = source
    return out


def _db_quote_turnover_gate(state: dict) -> tuple[bool, str]:
    if not state.get("ready"):
        return False, str(state.get("quality_reason") or "missing_state")
    try:
        if float(state.get("growth_4h_pct")) < 100.0:
            return False, "below_100pct"
    except (TypeError, ValueError):
        return False, "missing_4h_volume"
    return True, "pass"


def _human_price_window(regime: str | None, direction: str | None) -> str:
    direction = str(direction or "").strip()
    regime = str(regime or "").strip()
    if direction and direction != "ignored":
        return direction
    if regime and regime != "ignored":
        return regime
    return "n/a"


def _human_acceleration(window_map: dict[str, dict]) -> str:
    rank = {
        "strong_down": -2,
        "weak_down": -1,
        "flat": 0,
        "weak_up": 1,
        "good_up": 2,
        "strong_up": 3,
    }
    r15 = rank.get((window_map.get("15м") or {}).get("oi_slope_class"), 0)
    r30 = rank.get((window_map.get("30м") or {}).get("oi_slope_class"), 0)
    r1h = rank.get((window_map.get("1ч") or {}).get("oi_slope_class"), 0)
    if r15 > r30 >= r1h and r15 > 0:
        return "ускоряется вверх"
    if r15 < r30 <= r1h and r1h > 0:
        return "замедляется, но держится"
    if r15 == r30 == r1h == 0:
        return "плоско"
    if r15 < 0 and r30 < 0:
        return "ускоряется вниз"
    return "ровно / без явного ускорения"


def _human_text_token(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "н/д"
    return text.replace("_", " ")


def _human_price_state(value: str | None) -> str:
    return {
        "поддерживает_набор": "поддерживает набор",
        "ломает_набор": "ломает набор",
        "нейтрально": "нейтрально",
    }.get(str(value or "").strip(), _human_text_token(value))


def _human_transition_permission(value: str | None) -> str:
    return {
        "manual_only_stage_3": "третью фазу можно снять только вручную",
        "сброс_3_0_по_oi_4ч": "третья стадия сбрасывается из-за слабого OI 4ч",
        "блок_цены_4ч": "переход заблокирован ценой 4ч",
        "блок_oi_4ч": "переход заблокирован слабым OI 4ч",
        "снижение_2_1": "монета уходит из второй стадии в первую",
        "сброс_1_0": "монета сбрасывается из первой стадии в ноль",
        "удержание_2_по_15м": "вторая стадия удерживается, но 15м уже слабеет",
        "удержание_2": "вторая стадия удерживается",
        "ждем_60_минут_в_1": "первая стадия еще не прожила обязательный час",
        "удержание_1": "первая стадия удерживается",
        "фаза_0": "нулевая стадия",
        "разрешен_вход_в_1": "разрешен вход в первую стадию",
        "разрешен_вход_в_2": "разрешен вход во вторую стадию",
        "разрешен_вход_в_3": "разрешен вход в третью стадию",
        "неизвестно": "условие перехода не определено",
    }.get(str(value or "").strip(), _human_text_token(value))


def _human_transition_guard(value: str | None) -> str:
    return {
        "manual_hold_stage_3": "третья фаза удерживается вручную",
        "удержание_3:только_ручной_или_по_oi_4ч": "третья стадия удерживается до ручного сброса или слабого OI 4ч",
        "сброс_3_0:oi_4ч=weak_down": "третья стадия сброшена: OI 4ч ушел в слабое падение",
        "сброс_3_0:oi_4ч=strong_down": "третья стадия сброшена: OI 4ч ушел в сильное падение",
        "разрешен_вход_в_1": "вход в первую стадию",
        "разрешен_вход_в_2": "вход во вторую стадию",
        "разрешен_вход_в_3": "вход в третью стадию",
        "снижение_2_1": "откат из второй стадии в первую",
        "сброс_1_0": "сброс в нулевую стадию",
    }.get(str(value or "").strip(), _human_text_token(value))


def _human_phase_reason(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "n/a"

    parts: list[str] = []
    for chunk in raw.split(";"):
        token = chunk.strip()
        if not token:
            continue
        if token.startswith("oi_30m="):
            parts.append(f"OI 30м: {_human_oi_slope(token.split('=', 1)[1])}")
            continue
        if token.startswith("oi_1h="):
            parts.append(f"OI 1ч: {_human_oi_slope(token.split('=', 1)[1])}")
            continue
        if token.startswith("oi_4h="):
            parts.append(f"OI 4ч: {_human_oi_slope(token.split('=', 1)[1])}")
            continue
        if token.startswith("guard="):
            parts.append(_human_transition_guard(token.split("=", 1)[1]))
            continue
        if token.startswith("price_hard_ban:"):
            parts.append(f"жесткий блок цены: {_human_price_state(token.split(':', 1)[1])}")
            continue
        if token == "outside":
            continue
        if token == "working":
            parts.append("рабочая структура")
            continue
        if token == "no_hard_ban":
            parts.append("жесткого запрета нет")
            continue
        if token == "weak":
            parts.append("слабая структура")
            continue
        parts.append(_human_text_token(token))

    return "; ".join(parts) if parts else _human_text_token(raw)


def _normalize_cycle_ts_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text).isoformat()
    except Exception:
        pass
    if " " in text and "T" not in text:
        left, right = text.split(" ", 1)
        alt = f"{left}T{right}"
        try:
            return datetime.fromisoformat(alt).isoformat()
        except Exception:
            return alt
    return text


def _parse_iso_dt(value: str | None):
    text = _normalize_cycle_ts_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _is_canonical_stage3_alert_key(alert_key: str | None) -> bool:
    parts = str(alert_key or "").split("|")
    if len(parts) != 3:
        return False
    return _parse_iso_dt(parts[2]) is not None


def _stage3_alert_key_age_minutes(alert_key: str | None) -> float | None:
    parts = str(alert_key or "").split("|")
    if len(parts) != 3:
        return None
    alert_dt = _parse_iso_dt(parts[2])
    if alert_dt is None:
        return None
    now_dt = datetime.now(alert_dt.tzinfo or timezone.utc)
    return max(0.0, (now_dt - alert_dt).total_seconds() / 60.0)

_PHASE_PAGE_SIZE = 18


def _phase_rows(phase: int, offset: int = 0, limit: int = _PHASE_PAGE_SIZE) -> list[dict]:
    return _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE current_stage = %s
        ORDER BY stage_age_minutes DESC, latest_cycle_ts DESC, exchange, symbol
        LIMIT %s
        OFFSET %s
    """, (phase, limit, max(offset, 0)))


def _phase_total_count(phase: int) -> int:
    rows = _safe_rows(
        """
        SELECT COUNT(*) AS cnt
        FROM core_state_v2
        WHERE current_stage = %s
        """,
        (phase,),
    )
    if not rows:
        return 0
    try:
        return int(rows[0].get("cnt") or 0)
    except Exception:
        return 0


def _phase_list_keyboard(rows: list[dict], phase: int, offset: int = 0, total: int | None = None) -> dict | None:
    if not rows:
        return None
    buttons: list[list[tuple[str, str]]] = []
    current_row: list[tuple[str, str]] = []
    for row in rows[:_PHASE_PAGE_SIZE]:
        symbol = str(row.get("symbol") or "").upper()
        exchange = str(row.get("exchange") or "").upper()
        label = f"{symbol} [{_exchange_code(exchange)}]"
        current_row.append((label, f"coinx:{exchange}:{symbol}"))
        if len(current_row) == 2:
            buttons.append(current_row)
            current_row = []
    if current_row:
        buttons.append(current_row)
    shown = offset + len(rows)
    if total is not None and total > shown:
        buttons.append([("Ещё", f"phmore:{phase}:{shown}")])
    buttons.append([("⚙️ Фазы", "phases")])
    return _inline_keyboard(buttons)


def _phase_page_text(phase: int, offset: int, total: int) -> str:
    base_text = _build_stage3_text() if phase == 3 else _build_phases_text(phase)
    if total <= 0:
        return "\n".join([base_text, "", "Монет в этой фазе сейчас нет."])
    page = offset // _PHASE_PAGE_SIZE + 1
    shown_to = min(total, offset + _PHASE_PAGE_SIZE)
    return "\n".join([
        base_text,
        "",
        f"Страница: {page}",
        f"Показано: {offset + 1}-{shown_to} из {total}",
    ])


def _latest_metric_windows(symbol: str, exchange: str, as_of_ts=None) -> dict[tuple[str, str], dict]:
    params: list = [symbol, exchange]
    as_of_sql = ""
    if as_of_ts:
        as_of_sql = "AND ts_close <= %s"
        params.append(as_of_ts)
    rows = _safe_rows(f"""
        SELECT DISTINCT ON (metric, window_code)
            metric, window_code, ts_open, ts_close, open_value, high_value, low_value, close_value, delta_pct
        FROM aggregate_windows
        WHERE symbol = %s
          AND exchange = %s
          {as_of_sql}
          AND metric IN ('OI', 'PRICE', 'VOLUME')
          AND window_code IN ('15м', '30м', '1ч', '4ч', '24ч')
        ORDER BY metric, window_code, ts_close DESC
    """, tuple(params))
    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        out[(str(row.get("metric")), str(row.get("window_code")))] = row
    return out


def _price_slope_text(metric_windows: dict[tuple[str, str], dict], window_code: str) -> str:
    row = metric_windows.get(("PRICE", window_code)) or {}
    if not row:
        return "n/a"
    slope_ratio = value_slope_ratio(row)
    if slope_ratio < 0.98:
        return "сильно вниз"
    if slope_ratio < 0.995:
        return "вниз"
    if slope_ratio <= 1.005:
        return "боковик"
    if slope_ratio <= 1.02:
        return "рост"
    return "сильный рост"


def _collect_transition_history(symbol: str, exchange: str, as_of_ts=None) -> list[dict]:
    params: list = [symbol, exchange]
    as_of_sql = ""
    if as_of_ts:
        as_of_sql = "AND cycle_ts <= %s"
        params.append(as_of_ts)
    return _safe_rows(f"""
        SELECT *
        FROM transition_history_v2
        WHERE symbol = %s
          AND exchange = %s
          {as_of_sql}
        ORDER BY cycle_ts DESC
        LIMIT 20
    """, tuple(params))


def _load_window_rows(symbol: str, exchange: str, as_of_ts=None) -> list[dict]:
    params: list = [symbol, exchange]
    as_of_sql = ""
    if as_of_ts:
        as_of_sql = "AND cycle_ts <= %s"
        params.append(as_of_ts)
    return _safe_rows(f"""
        SELECT DISTINCT ON (window_code) *
        FROM window_state_v2
        WHERE symbol = %s
          AND exchange = %s
          {as_of_sql}
        ORDER BY window_code, cycle_ts DESC
    """, tuple(params))


def _find_transition_age(history_rows: list[dict], to_stage: int) -> str:
    for row in history_rows:
        if int(row.get("to_stage") or -1) == to_stage:
            return _fmt_minutes(row.get("stage_age_before_transition"))
    return "n/a"


def _build_symbol_header(symbol: str, exchange: str, ts_value) -> list[str]:
    return [
        f"{symbol} [{exchange}]",
        _format_ts_compact(ts_value),
    ]


def _load_symbol_exchange_rows(symbol: str) -> list[dict]:
    return _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 6
    """, (symbol,))


def _exchange_semantic_line(row: dict) -> str:
    exchange = str(row.get("exchange") or "").upper()
    stage = int(row.get("current_stage") or 0)
    age = _fmt_minutes(row.get("stage_age_minutes"))
    reason = _human_phase_reason(row.get("phase_reason"))
    price = row.get("price_summary") or {}
    price_state = _human_price_state(price.get("price_state"))
    return f"{exchange} — Фаза {stage} | возраст {age} | цена: {price_state} | {reason}"


def _build_exchange_semantic_block(symbol: str, current_exchange: str) -> list[str]:
    rows = _load_symbol_exchange_rows(symbol)
    if not rows:
        return []

    current_exchange = str(current_exchange or "").upper()
    current_row = None
    peer_rows: list[dict] = []
    for row in rows:
        ex = str(row.get("exchange") or "").upper()
        if ex == current_exchange and current_row is None:
            current_row = row
        else:
            peer_rows.append(row)

    if not current_row and rows:
        current_row = rows[0]
        peer_rows = rows[1:]

    if not peer_rows:
        return []

    lines = ["", "🌐 Контекст по биржам", _exchange_semantic_line(current_row)]
    for row in peer_rows:
        lines.append(_exchange_semantic_line(row))
    return lines


def _build_coin_message(core_row: dict, window_rows: list[dict], history_rows: list[dict], metric_windows: dict[tuple[str, str], dict], *, title: str, transition_ts=None, transition_reason: str | None = None) -> str:
    symbol = str(core_row.get("symbol") or "").upper()
    exchange = str(core_row.get("exchange") or "").upper()
    latest_ts = transition_ts or core_row.get("latest_cycle_ts")

    header_symbol, header_ts = _build_symbol_header(symbol, exchange, latest_ts)
    lines = [f"<b>{title}</b>", "", f"<b>{header_symbol}</b>", f"<b>{header_ts}</b>"]
    current_stage = int(core_row.get("current_stage") or 0)
    lines.extend([""])
    lines.extend(
        build_phase_history_lines(
            history_rows,
            current_stage=current_stage,
            current_age_minutes=core_row.get("stage_age_minutes"),
            humanize_reason=_human_phase_reason,
            volume_unlocked_at=core_row.get("volume_unlocked_at"),
        )
    )

    bybit_note = _bybit_availability_note(symbol, exchange)
    if bybit_note:
        lines.extend(["", bybit_note])

    del window_rows, metric_windows, transition_reason
    lines.extend([""])
    market_metrics = _live_market_metrics(symbol, exchange)
    volume_snapshot = core_row.get("volume_snapshot")
    if isinstance(volume_snapshot, dict):
        market_metrics = _apply_db_volume_snapshot(market_metrics, volume_snapshot)
    lines.extend(_build_market_metrics_block(market_metrics))

    lines.extend(["", _symbol_links(symbol, exchange)])
    return "\n".join(lines)


def _pending_feedback_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_pending_feedback.json"


def _save_pending_feedback(chat_id: str | int | None, symbol: str, seed: str | None = None) -> None:
    payload = {
        "created_at_utc": iso_мск(),
        "chat_id": str(chat_id) if chat_id is not None else "",
        "symbol": symbol.upper().strip(),
        "seed": str(seed or "").strip(),
    }
    _pending_feedback_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def _load_pending_feedback() -> dict:
    path = _pending_feedback_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}


def _clear_pending_feedback() -> None:
    path = _pending_feedback_path()
    if path.exists():
        path.unlink()


def _matches_pending_feedback(chat_id: str | int | None) -> bool:
    pending = _load_pending_feedback()
    if not pending:
        return False
    stored_chat_id = str(pending.get("chat_id") or "")
    if not stored_chat_id:
        return True
    return str(chat_id) == stored_chat_id


def _answer_callback_query(callback_id: str, text: str | None = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not callback_id:
        return
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = _safe_tg_text(text, 180)
    try:
        requests.post(f"{BASE}/answerCallbackQuery", json=payload, timeout=15)
    except Exception as exc:
        log(f"telegram callback answer error: {exc}")



def _release_polling_lock() -> None:
    global _polling_lock_file
    lock_file = _polling_lock_file
    _polling_lock_file = None
    if lock_file is None:
        return
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        lock_file.close()
    except Exception:
        pass


def _acquire_polling_lock() -> bool:
    global _polling_lock_file
    if _polling_lock_file is not None:
        return True

    POLLING_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_file = POLLING_LOCK_PATH.open("a+", encoding="utf-8")

    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        try:
            lock_file.seek(0)
            holder = lock_file.read().strip()
        except Exception:
            holder = ""
        log(f"telegram polling skipped: lock busy {holder}".strip())
        lock_file.close()
        return False

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(
        f"pid={os.getpid()} started_at={iso_мск()}\n"
    )
    lock_file.flush()
    _polling_lock_file = lock_file
    return True


atexit.register(_release_polling_lock)


def _main_keyboard() -> dict:
    return {
        "keyboard": [
            ["⚙️ Фазы", "🩺 Система"],
            ["🧨 Сброс всех Ф3", "❓ Помощь"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _phases_keyboard() -> dict:
    return {
        "keyboard": [
            ["🥉 Фаза 1", "🥈 Фаза 2"],
            ["🥇 Фаза 3", "🧯 Сброс фазы 3"],
            ["⬅️ Назад"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _stage3_reset_keyboard() -> dict:
    return {
        "keyboard": [
            ["Сбросить по тикеру"],
            ["⬅️ Назад"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def _safe_tg_text(text: str, limit: int = 3900) -> str:
    text = str(text or "")
    if len(text) <= limit:
        return text
    return text[:limit - 80] + "\n\n... truncated. Use download/report for full output."


TG_CAPTION_LIMIT = int(os.getenv("TG_CAPTION_LIMIT", "1024"))
CHART_CAPTURE_WARN_SECONDS = float(os.getenv("CHART_CAPTURE_WARN_SECONDS", "20"))
CHART_SEND_WARN_SECONDS = float(os.getenv("CHART_SEND_WARN_SECONDS", "10"))
CHART_TOTAL_WARN_SECONDS = float(os.getenv("CHART_TOTAL_WARN_SECONDS", "50"))


@dataclass(frozen=True)
class TelegramDeliveryResult:
    ok: bool
    message_id: int | None = None
    chat_id: str | None = None
    delivered_at: datetime | None = None
    attempts: int = 0
    delivery_mode: str = "text"
    chart_requested: bool = False
    chart_captured: bool = False
    chart_capture_failure_reason: str | None = None
    chart_delivery_failure_reason: str | None = None
    chart_requested_timeframes: tuple[str, ...] = ()
    chart_captured_timeframes: tuple[str, ...] = ()
    chart_timeframe_seconds: dict[str, float] | None = None
    chart_timeframe_verification_failures: tuple[str, ...] = ()
    chart_capture_seconds: float = 0.0
    chart_send_seconds: float = 0.0
    total_delivery_seconds: float = 0.0
    media_alerts: tuple[str, ...] = ()


def _telegram_result_from_response(response: requests.Response, *, attempts: int = 1) -> TelegramDeliveryResult:
    if not response.ok:
        raise RuntimeError(
            "telegram api error: "
            f"status={response.status_code} body={(response.text or '')[:500]}"
        )
    parsed = response.json()
    if not parsed.get("ok"):
        raise RuntimeError(f"telegram api error: {parsed}")
    result = parsed.get("result") or {}
    if isinstance(result, list):
        first = result[0] if result else {}
    else:
        first = result
    chat = first.get("chat") or {}
    return TelegramDeliveryResult(
        ok=True,
        message_id=first.get("message_id"),
        chat_id=str(chat.get("id")) if chat.get("id") is not None else str(TELEGRAM_CHAT_ID),
        delivered_at=datetime.now(timezone.utc),
        attempts=attempts,
    )


def send_message_result(
    text: str,
    reply_markup: dict | None = None,
    parse_mode: str | None = None,
    *,
    timeout: int = 30,
) -> TelegramDeliveryResult:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return TelegramDeliveryResult(ok=False, attempts=0)

    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": _safe_tg_text(text)}
    auto_parse_mode = parse_mode
    if auto_parse_mode is None and ("<a href=" in str(text) or "<code>" in str(text)):
        auto_parse_mode = "HTML"
    if auto_parse_mode:
        payload["parse_mode"] = auto_parse_mode
        payload["disable_web_page_preview"] = True
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        response = requests.post(
            f"{BASE}/sendMessage",
            json=payload,
            timeout=timeout,
        )
        result = _telegram_result_from_response(response)
        return TelegramDeliveryResult(
            ok=result.ok,
            message_id=result.message_id,
            chat_id=result.chat_id,
            delivered_at=result.delivered_at,
            attempts=result.attempts,
            delivery_mode="text",
        )
    except Exception as exc:
        log(f"telegram send error: {exc}")
        return TelegramDeliveryResult(ok=False, attempts=1, delivery_mode="text")


def send_message(text: str, reply_markup: dict | None = None, parse_mode: str | None = None) -> bool:
    return send_message_result(text, reply_markup, parse_mode).ok


def _send_photo_result(
    photo_path: str,
    *,
    caption: str | None = None,
    reply_markup: dict | None = None,
    parse_mode: str | None = "HTML",
) -> TelegramDeliveryResult:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return TelegramDeliveryResult(ok=False, attempts=0, delivery_mode="photo")
    data = {"chat_id": TELEGRAM_CHAT_ID}
    if caption:
        data["caption"] = caption
        if parse_mode:
            data["parse_mode"] = parse_mode
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    with open(photo_path, "rb") as fh:
        response = requests.post(
            f"{BASE}/sendPhoto",
            data=data,
            files={"photo": (Path(photo_path).name or "chart.png", fh, "image/png")},
            timeout=45,
        )
    result = _telegram_result_from_response(response)
    return TelegramDeliveryResult(
        ok=result.ok,
        message_id=result.message_id,
        chat_id=result.chat_id,
        delivered_at=result.delivered_at,
        attempts=result.attempts,
        delivery_mode="photo",
    )


def _send_media_group_result(
    photo_paths: list[str],
    *,
    caption: str | None = None,
    parse_mode: str | None = "HTML",
) -> TelegramDeliveryResult:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return TelegramDeliveryResult(ok=False, attempts=0, delivery_mode="album")
    media = []
    files = {}
    handles = []
    try:
        for idx, photo_path in enumerate(photo_paths):
            field_name = f"photo{idx}"
            item = {"type": "photo", "media": f"attach://{field_name}"}
            if idx == 0 and caption:
                item["caption"] = caption
                if parse_mode:
                    item["parse_mode"] = parse_mode
            media.append(item)
            fh = open(photo_path, "rb")
            handles.append(fh)
            files[field_name] = (Path(photo_path).name or f"chart{idx}.png", fh, "image/png")
        response = requests.post(
            f"{BASE}/sendMediaGroup",
            data={"chat_id": TELEGRAM_CHAT_ID, "media": json.dumps(media, ensure_ascii=False)},
            files=files,
            timeout=60,
        )
    finally:
        for fh in handles:
            try:
                fh.close()
            except Exception:
                pass
    result = _telegram_result_from_response(response)
    return TelegramDeliveryResult(
        ok=result.ok,
        message_id=result.message_id,
        chat_id=result.chat_id,
        delivered_at=result.delivered_at,
        attempts=result.attempts,
        delivery_mode="album",
    )


def _signal_channel_id() -> str | None:
    """Return the runtime-captured channel target; this is never sourced from .env."""
    try:
        value = SIGNAL_CHANNEL_TARGET_FILE.read_text(encoding="utf-8").strip()
        return value or None
    except OSError:
        return None


def _capture_signal_channel_id(channel_post: dict | None) -> bool:
    """Persist the first real channel_post target without exposing its ID in logs."""
    chat = (channel_post or {}).get("chat") or {}
    channel_id = str(chat.get("id") or "").strip()
    if chat.get("type") != "channel" or not channel_id or _signal_channel_id():
        return False
    try:
        RUNTIME_DIR.mkdir(exist_ok=True)
        temporary = SIGNAL_CHANNEL_TARGET_FILE.with_suffix(".tmp")
        temporary.write_text(channel_id, encoding="utf-8")
        os.replace(temporary, SIGNAL_CHANNEL_TARGET_FILE)
        log("signal channel captured; channel copies enabled")
        return True
    except Exception as exc:
        log(f"signal channel capture failed: {type(exc).__name__}")
        return False


def _enqueue_signal_channel_copy(
    text: str,
    paths: list[str],
    parse_mode: str | None,
) -> bool:
    """Best-effort bounded hand-off. It never performs channel I/O in the signal path."""
    channel_id = _signal_channel_id()
    if not channel_id:
        return False
    try:
        _signal_channel_delivery_queue.put_nowait(
            {
                "chat_id": channel_id,
                "text": text,
                "paths": list(paths or ()),
                "parse_mode": parse_mode,
            }
        )
        return True
    except Full:
        log("signal channel queue full; copy dropped")
        return False
    except Exception as exc:
        log(f"signal channel queue hand-off failed: {type(exc).__name__}")
        return False


def _channel_send_text(channel_id: str, text: str, parse_mode: str | None) -> None:
    payload = {"chat_id": channel_id, "text": _safe_tg_text(text), "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    response = requests.post(f"{BASE}/sendMessage", json=payload, timeout=30)
    _telegram_result_from_response(response)


def _channel_send_photo(channel_id: str, path: str, text: str, parse_mode: str | None) -> None:
    if len(text) > TG_CAPTION_LIMIT:
        with open(path, "rb") as fh:
            response = requests.post(
                f"{BASE}/sendPhoto",
                data={"chat_id": channel_id},
                files={"photo": (Path(path).name or "chart.png", fh, "image/png")},
                timeout=45,
            )
        _telegram_result_from_response(response)
        _channel_send_text(channel_id, text, parse_mode)
        return
    data = {"chat_id": channel_id, "caption": text}
    if parse_mode:
        data["parse_mode"] = parse_mode
    with open(path, "rb") as fh:
        response = requests.post(
            f"{BASE}/sendPhoto",
            data=data,
            files={"photo": (Path(path).name or "chart.png", fh, "image/png")},
            timeout=45,
        )
    _telegram_result_from_response(response)


def _channel_send_album(channel_id: str, paths: list[str], text: str, parse_mode: str | None) -> None:
    caption = text if len(text) <= TG_CAPTION_LIMIT else None
    handles = []
    try:
        media = []
        files = {}
        for index, path in enumerate(paths):
            field = f"photo{index}"
            item = {"type": "photo", "media": f"attach://{field}"}
            if index == 0 and caption:
                item["caption"] = caption
                if parse_mode:
                    item["parse_mode"] = parse_mode
            media.append(item)
            handle = open(path, "rb")
            handles.append(handle)
            files[field] = (Path(path).name or f"chart{index}.png", handle, "image/png")
        response = requests.post(
            f"{BASE}/sendMediaGroup",
            data={"chat_id": channel_id, "media": json.dumps(media, ensure_ascii=False)},
            files=files,
            timeout=60,
        )
        _telegram_result_from_response(response)
    finally:
        for handle in handles:
            try:
                handle.close()
            except Exception:
                pass
    if caption is None:
        _channel_send_text(channel_id, text, parse_mode)


def _deliver_signal_channel_job(job: dict) -> None:
    paths = [str(path) for path in (job.get("paths") or ()) if path and os.path.exists(path)]
    channel_id = str(job.get("chat_id") or "")
    if not channel_id:
        raise ValueError("missing channel target")
    if not paths:
        _channel_send_text(channel_id, str(job.get("text") or ""), job.get("parse_mode"))
    elif len(paths) == 1:
        _channel_send_photo(channel_id, paths[0], str(job.get("text") or ""), job.get("parse_mode"))
    else:
        _channel_send_album(channel_id, paths, str(job.get("text") or ""), job.get("parse_mode"))


def _process_signal_channel_job(job: dict) -> None:
    """Deliver once and always release worker-owned chart files."""
    try:
        _deliver_signal_channel_job(job)
    except Exception as exc:
        log(f"signal channel delivery failed: {type(exc).__name__}")
    finally:
        for path in job.get("paths") or ():
            try:
                os.remove(path)
            except OSError:
                pass


def _signal_channel_delivery_worker() -> None:
    log("signal channel worker started")
    while True:
        job = _signal_channel_delivery_queue.get()
        try:
            _process_signal_channel_job(job)
        finally:
            _signal_channel_delivery_queue.task_done()


def _start_signal_channel_worker() -> None:
    global _signal_channel_worker_started
    if _signal_channel_worker_started or not TELEGRAM_BOT_TOKEN:
        return
    _signal_channel_worker_started = True
    threading.Thread(target=_signal_channel_delivery_worker, daemon=True).start()


def _finish_stage3_primary_delivery(
    result: TelegramDeliveryResult,
    *,
    to_group: bool,
    text: str,
    parse_mode: str | None,
    chart_paths: list[str],
) -> TelegramDeliveryResult:
    """Queue only confirmed primary signal deliveries; never let channel failure escape."""
    if not to_group or not result.ok:
        return result
    try:
        if _enqueue_signal_channel_copy(text, chart_paths, parse_mode):
            chart_paths.clear()  # worker now owns cleanup of the generated files
    except Exception as exc:
        log(f"signal channel copy isolated after primary delivery: {type(exc).__name__}")
    return result


def _empty_chart_result(*, requested: bool = False, failure_reason: str | None = None) -> dict:
    return {
        "requested": requested,
        "paths": [],
        "requested_timeframes": [],
        "captured_timeframes": [],
        "capture_seconds": 0.0,
        "timeframe_seconds": {},
        "timeframe_verification_failures": [],
        "failure_reason": failure_reason,
    }


def _try_chart_screenshot(row: dict) -> dict:
    """Снимает CoinGlass 5m/4H best-effort.

    Любая ошибка Playwright/CoinGlass/config возвращает пустой результат.
    Сигнал Stage 3 из-за графика теряться не должен.
    """
    try:
        from chart_screenshot.config import ENABLE_CHART_SCREENSHOT, CHART_TIMEFRAMES

        if not ENABLE_CHART_SCREENSHOT:
            return _empty_chart_result()
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            return _empty_chart_result()
        exchange = str(row.get("exchange") or "").strip() or None
        from chart_screenshot.coinglass import capture_coinglass_screenshots

        result = asyncio.run(
            capture_coinglass_screenshots(symbol, exchange, timeframes=CHART_TIMEFRAMES)
        )
        return {
            "requested": True,
            "paths": list(result.photo_paths),
            "requested_timeframes": list(result.requested_timeframes),
            "captured_timeframes": list(result.captured_timeframes),
            "capture_seconds": float(result.capture_seconds_total or 0.0),
            "timeframe_seconds": dict(result.timeframe_seconds or {}),
            "timeframe_verification_failures": list(result.timeframe_verification_failures or ()),
            "failure_reason": result.failure_reason,
        }
    except Exception as exc:
        log(f"stage3 chart screenshot skipped: {type(exc).__name__}: {exc}")
        return _empty_chart_result(requested=True, failure_reason=type(exc).__name__)


def _finalize_media_alerts(
    alerts: list[str],
    *,
    chart_send_seconds: float,
    total_delivery_seconds: float,
) -> tuple[str, ...]:
    result = list(alerts)
    if chart_send_seconds >= CHART_SEND_WARN_SECONDS:
        result.append(f"долгая_отправка:{chart_send_seconds:.1f}с")
    if total_delivery_seconds >= CHART_TOTAL_WARN_SECONDS:
        result.append(f"долгая_доставка:{total_delivery_seconds:.1f}с")
    return tuple(result)


def _with_media_metrics(
    result: TelegramDeliveryResult,
    *,
    delivery_mode: str,
    chart_requested: bool,
    chart_captured: bool,
    chart_capture_failure_reason: str | None,
    chart_delivery_failure_reason: str | None,
    chart_requested_timeframes: tuple[str, ...],
    chart_captured_timeframes: tuple[str, ...],
    chart_timeframe_seconds: dict[str, float],
    chart_timeframe_verification_failures: tuple[str, ...],
    chart_capture_seconds: float,
    chart_send_seconds: float,
    total_delivery_seconds: float,
    media_alerts: tuple[str, ...],
) -> TelegramDeliveryResult:
    return TelegramDeliveryResult(
        ok=result.ok,
        message_id=result.message_id,
        chat_id=result.chat_id,
        delivered_at=result.delivered_at,
        attempts=result.attempts,
        delivery_mode=delivery_mode,
        chart_requested=chart_requested,
        chart_captured=chart_captured,
        chart_capture_failure_reason=chart_capture_failure_reason,
        chart_delivery_failure_reason=chart_delivery_failure_reason,
        chart_requested_timeframes=chart_requested_timeframes,
        chart_captured_timeframes=chart_captured_timeframes,
        chart_timeframe_seconds=dict(chart_timeframe_seconds),
        chart_timeframe_verification_failures=chart_timeframe_verification_failures,
        chart_capture_seconds=round(chart_capture_seconds, 3),
        chart_send_seconds=round(chart_send_seconds, 3),
        total_delivery_seconds=round(total_delivery_seconds, 3),
        media_alerts=media_alerts,
    )


def send_stage3_alert_result(
    row: dict,
    text: str,
    reply_markup: dict | None = None,
    parse_mode: str | None = "HTML",
    *,
    to_group: bool = False,
) -> TelegramDeliveryResult:
    started_at = time.monotonic()
    chart_result = _try_chart_screenshot(row)
    chart_paths = list(chart_result.get("paths") or [])
    chart_requested = bool(chart_result.get("requested"))
    chart_captured = bool(chart_paths)
    chart_capture_failure_reason = chart_result.get("failure_reason")
    chart_delivery_failure_reason: str | None = None
    chart_requested_timeframes = tuple(chart_result.get("requested_timeframes") or ())
    chart_captured_timeframes = tuple(chart_result.get("captured_timeframes") or ())
    chart_timeframe_seconds = dict(chart_result.get("timeframe_seconds") or {})
    chart_timeframe_verification_failures = tuple(
        chart_result.get("timeframe_verification_failures") or ()
    )
    chart_capture_seconds = float(chart_result.get("capture_seconds") or 0.0)
    chart_send_seconds = 0.0
    media_alerts: list[str] = []

    if chart_capture_failure_reason:
        media_alerts.append(f"график_не_снят:{chart_capture_failure_reason}")
    if chart_timeframe_verification_failures:
        media_alerts.append(
            f"таймфрейм_не_подтвержден:{','.join(chart_timeframe_verification_failures)}"
        )
    if chart_capture_seconds >= CHART_CAPTURE_WARN_SECONDS:
        media_alerts.append(f"долгая_съемка:{chart_capture_seconds:.1f}с")

    def finish(result: TelegramDeliveryResult) -> TelegramDeliveryResult:
        return _finish_stage3_primary_delivery(
            result,
            to_group=to_group,
            text=text,
            parse_mode=parse_mode,
            chart_paths=chart_paths,
        )

    try:
        if chart_paths:
            try:
                send_started_at = time.monotonic()
                if len(chart_paths) == 1:
                    if len(text) <= TG_CAPTION_LIMIT:
                        result = _send_photo_result(
                            chart_paths[0],
                            caption=text,
                            reply_markup=reply_markup,
                            parse_mode=parse_mode,
                        )
                        chart_send_seconds = time.monotonic() - send_started_at
                        return finish(_with_media_metrics(
                            result,
                            delivery_mode="photo",
                            chart_requested=chart_requested,
                            chart_captured=chart_captured,
                            chart_capture_failure_reason=chart_capture_failure_reason,
                            chart_delivery_failure_reason=chart_delivery_failure_reason,
                            chart_requested_timeframes=chart_requested_timeframes,
                            chart_captured_timeframes=chart_captured_timeframes,
                            chart_timeframe_seconds=chart_timeframe_seconds,
                            chart_timeframe_verification_failures=chart_timeframe_verification_failures,
                            chart_capture_seconds=chart_capture_seconds,
                            chart_send_seconds=chart_send_seconds,
                            total_delivery_seconds=time.monotonic() - started_at,
                            media_alerts=_finalize_media_alerts(
                                media_alerts,
                                chart_send_seconds=chart_send_seconds,
                                total_delivery_seconds=time.monotonic() - started_at,
                            ),
                        ))
                    result = _send_photo_result(chart_paths[0], caption=None, parse_mode=parse_mode)
                    chart_send_seconds = time.monotonic() - send_started_at
                    send_message_result(text, reply_markup=reply_markup, parse_mode=parse_mode)
                    return finish(_with_media_metrics(
                        result,
                        delivery_mode="photo+text",
                        chart_requested=chart_requested,
                        chart_captured=chart_captured,
                        chart_capture_failure_reason=chart_capture_failure_reason,
                        chart_delivery_failure_reason=chart_delivery_failure_reason,
                        chart_requested_timeframes=chart_requested_timeframes,
                        chart_captured_timeframes=chart_captured_timeframes,
                        chart_timeframe_seconds=chart_timeframe_seconds,
                        chart_timeframe_verification_failures=chart_timeframe_verification_failures,
                        chart_capture_seconds=chart_capture_seconds,
                        chart_send_seconds=chart_send_seconds,
                        total_delivery_seconds=time.monotonic() - started_at,
                        media_alerts=_finalize_media_alerts(
                            media_alerts,
                            chart_send_seconds=chart_send_seconds,
                            total_delivery_seconds=time.monotonic() - started_at,
                        ),
                    ))

                if len(text) <= TG_CAPTION_LIMIT:
                    result = _send_media_group_result(chart_paths, caption=text, parse_mode=parse_mode)
                    chart_send_seconds = time.monotonic() - send_started_at
                    return finish(_with_media_metrics(
                        result,
                        delivery_mode="album",
                        chart_requested=chart_requested,
                        chart_captured=chart_captured,
                        chart_capture_failure_reason=chart_capture_failure_reason,
                        chart_delivery_failure_reason=chart_delivery_failure_reason,
                        chart_requested_timeframes=chart_requested_timeframes,
                        chart_captured_timeframes=chart_captured_timeframes,
                        chart_timeframe_seconds=chart_timeframe_seconds,
                        chart_timeframe_verification_failures=chart_timeframe_verification_failures,
                        chart_capture_seconds=chart_capture_seconds,
                        chart_send_seconds=chart_send_seconds,
                        total_delivery_seconds=time.monotonic() - started_at,
                        media_alerts=_finalize_media_alerts(
                            media_alerts,
                            chart_send_seconds=chart_send_seconds,
                            total_delivery_seconds=time.monotonic() - started_at,
                        ),
                    ))
                result = _send_media_group_result(chart_paths, caption=None, parse_mode=parse_mode)
                chart_send_seconds = time.monotonic() - send_started_at
                send_message_result(text, reply_markup=reply_markup, parse_mode=parse_mode)
                return finish(_with_media_metrics(
                    result,
                    delivery_mode="album+text",
                    chart_requested=chart_requested,
                    chart_captured=chart_captured,
                    chart_capture_failure_reason=chart_capture_failure_reason,
                    chart_delivery_failure_reason=chart_delivery_failure_reason,
                    chart_requested_timeframes=chart_requested_timeframes,
                    chart_captured_timeframes=chart_captured_timeframes,
                    chart_timeframe_seconds=chart_timeframe_seconds,
                    chart_timeframe_verification_failures=chart_timeframe_verification_failures,
                    chart_capture_seconds=chart_capture_seconds,
                    chart_send_seconds=chart_send_seconds,
                    total_delivery_seconds=time.monotonic() - started_at,
                    media_alerts=_finalize_media_alerts(
                        media_alerts,
                        chart_send_seconds=chart_send_seconds,
                        total_delivery_seconds=time.monotonic() - started_at,
                    ),
                ))
            except Exception as exc:
                chart_delivery_failure_reason = type(exc).__name__
                media_alerts.append(f"график_не_доставлен:{chart_delivery_failure_reason}")
                log(f"stage3 chart delivery failed, fallback to text: {exc}")

        text_result = send_message_result(text, reply_markup=reply_markup, parse_mode=parse_mode)
        return finish(_with_media_metrics(
            text_result,
            delivery_mode="text",
            chart_requested=chart_requested,
            chart_captured=chart_captured,
            chart_capture_failure_reason=chart_capture_failure_reason,
            chart_delivery_failure_reason=chart_delivery_failure_reason,
            chart_requested_timeframes=chart_requested_timeframes,
            chart_captured_timeframes=chart_captured_timeframes,
            chart_timeframe_seconds=chart_timeframe_seconds,
            chart_timeframe_verification_failures=chart_timeframe_verification_failures,
            chart_capture_seconds=chart_capture_seconds,
            chart_send_seconds=chart_send_seconds,
            total_delivery_seconds=time.monotonic() - started_at,
            media_alerts=_finalize_media_alerts(
                media_alerts,
                chart_send_seconds=chart_send_seconds,
                total_delivery_seconds=time.monotonic() - started_at,
            ),
        ))
    finally:
        for chart_path in chart_paths:
            try:
                os.remove(chart_path)
            except Exception:
                pass


def send_panel_message(text: str) -> None:
    send_message(text, _main_keyboard())


def send_document(path: Path, caption: str | None = None) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    if not path.exists():
        send_message(f"Файл не найден: {path.name}")
        return

    try:
        with path.open("rb") as f:
            requests.post(
                f"{BASE}/sendDocument",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption or path.name},
                files={"document": (path.name, f)},
                timeout=180,
            )
    except Exception as exc:
        log(f"telegram send document error: {exc}")


def _build_runtime_reports_zip() -> Path:
    report_path = ПАПКА_ДАННЫХ / "runtime_reports.zip"

    data_files = [
        ПАПКА_ДАННЫХ / "runtime_timing_report.txt",
        ПАПКА_ДАННЫХ / "runtime_health_report.txt",
        ПАПКА_ДАННЫХ / "request_failure_report.csv",
        ПАПКА_ДАННЫХ / "gap_report.csv",
        ПАПКА_ДАННЫХ / "active_universe_report.csv",
        ПАПКА_ДАННЫХ / "storage_manifest.txt",
    ]
    runtime_files = [
        RUNTIME_REPORTS_DIR / "runtime_health.json",
        RUNTIME_REPORTS_DIR / "cycle_status.json",
        RUNTIME_REPORTS_DIR / "watchdog_status.txt",
        RUNTIME_REPORTS_DIR / "snapshot_status.txt",
        RUNTIME_REPORTS_DIR / "runtime_health.txt",
        RUNTIME_REPORTS_DIR / "cycle_status.txt",
    ]

    added = 0
    with zipfile.ZipFile(report_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path in data_files:
            if path.exists():
                z.write(path, arcname=path.name)
                added += 1
        for path in runtime_files:
            if path.exists():
                z.write(path, arcname=f"runtime_reports/{path.name}")
                added += 1

    if added == 0:
        raise FileNotFoundError("runtime reports are missing")

    return report_path


def _read_kv_file(path: Path) -> dict:
    data = {}

    if not path.exists():
        return data

    for line in path.read_text(errors="ignore").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
        elif ":" in line:
            k, v = line.split(":", 1)
            data[k.strip()] = v.strip()

    return data


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0

    lines = path.read_text(errors="ignore").splitlines()

    if not lines:
        return 0

    return max(len(lines) - 1, 0)


def _quick_export_is_fresh(max_age_seconds: int = 60) -> bool:
    bundle_path = ПАПКА_ДАННЫХ / "market_research_bundle.zip"

    if not bundle_path.exists():
        return False

    age = time.time() - bundle_path.stat().st_mtime
    return age <= max_age_seconds


def _build_status_text() -> str:
    timing = _read_kv_file(ПАПКА_ДАННЫХ / "runtime_timing_report.txt")
    health = _read_kv_file(ПАПКА_ДАННЫХ / "runtime_health_report.txt")

    files_count = len([path for path in ПАПКА_ДАННЫХ.glob("*") if path.is_file()])
    failures_count = _count_csv_rows(ПАПКА_ДАННЫХ / "request_failure_report.csv")
    gaps_count = _count_csv_rows(ПАПКА_ДАННЫХ / "gap_report.csv")
    active_count = _count_csv_rows(ПАПКА_ДАННЫХ / "active_universe_report.csv")

    total_seconds = timing.get("total_seconds", "n/a")
    generated_at = timing.get("generated_at", "n/a")
    memory_mb = health.get("memory_max_rss_mb", "n/a")
    export_mode = health.get("export_mode", "n/a")

    return (
        f"🥇 Mighty Duck / {APP_VERSION}\n\n"
        f"Cycle: OK\n"
        f"Last timing: {generated_at}\n"
        f"Duration: {total_seconds}s\n"
        f"Memory max RSS: {memory_mb} MB\n"
        f"Export mode: {export_mode}\n\n"
        f"Runtime reports:\n"
        f"Failures: {failures_count}\n"
        f"Gaps: {gaps_count}\n"
        f"Active universe rows: {active_count}\n"
        f"Runtime files: {files_count}\n\n"
        f"Downloads:\n"
        f"/bundle — research bundle\n"
        f"/reports — runtime reports bundle"
    )


def _read_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}


def _fmt_file(path: Path) -> str:
    if not path.exists():
        return f"{path.name}: missing"
    age = int(time.time() - path.stat().st_mtime)
    size_mb = path.stat().st_size / 1024 / 1024
    return f"{path.name}: {size_mb:.2f} MB, age={age}s"


def _runtime_snapshot() -> tuple[dict, dict]:
    runtime = _read_json_file(RUNTIME_REPORTS_DIR / "runtime_health.json")
    cycle = _read_json_file(RUNTIME_REPORTS_DIR / "cycle_status.json")
    canonical = _read_json_file(Path(__file__).resolve().parent / "runtime" / "health.json")
    if canonical:
        universe = canonical.get("universe") or {}
        metrics = canonical.get("metrics") or {}
        runtime.update(
            {
                "status": canonical.get("status"),
                "global_block_reason": canonical.get("global_block_reason"),
                "runtime_alerts": canonical.get("alerts") or runtime.get("runtime_alerts") or [],
                "symbols_total": universe.get("total_symbols"),
                "symbols_by_exchange": {
                    "BINANCE": (universe.get("sources", {}).get("binance") or {}).get("monitored"),
                    "BYBIT": (universe.get("sources", {}).get("bybit") or {}).get("monitored"),
                },
                "duck_universe_health": universe.get("universe_health"),
                "duck_listing_health": universe.get("listing_health"),
                "data_quality_state": universe.get("data_quality"),
                "symbols_incomplete_windows": universe.get("incomplete_windows"),
                "symbols_stale_windows": universe.get("stale_windows"),
                "symbols_absent_in_duck": universe.get("absent_in_duck"),
                "data_quality_quarantine_total": universe.get("data_quality_quarantine_total"),
                "data_quality_quarantine": universe.get("data_quality_quarantine") or universe.get("quarantine") or [],
            }
        )
        cycle.update(
            {
                "cycle_health": metrics.get("cycle_health"),
                "cycle_latency_class": metrics.get("cycle_latency_class"),
                "cycle_elapsed_seconds": metrics.get("cycle_elapsed_seconds"),
                "cycle_reserve_seconds": metrics.get("cycle_reserve_seconds"),
                "cycle_reserve_pct": metrics.get("cycle_reserve_pct"),
                "overrun_streak": metrics.get("overrun_streak"),
            }
        )
    return runtime, cycle


def _stage3_cycle_budget_state() -> dict:
    _, cycle = _runtime_snapshot()
    latency_class = str(cycle.get("cycle_latency_class") or "").strip().lower()
    try:
        reserve_pct = float(cycle.get("cycle_reserve_pct") or 0.0)
    except Exception:
        reserve_pct = 0.0
    try:
        thin_reserve_pct = float(os.getenv("STAGE3_ALERTS_THIN_RESERVE_PCT", "25") or "25")
    except Exception:
        thin_reserve_pct = 25.0
    is_thin = latency_class in {"thin_reserve", "tight"} or reserve_pct < thin_reserve_pct
    return {
        "cycle_latency_class": latency_class,
        "cycle_reserve_pct": reserve_pct,
        "thin_reserve_pct": thin_reserve_pct,
        "is_thin_reserve": is_thin,
    }


def _build_control_panel_text() -> str:
    runtime, cycle = _runtime_snapshot()

    return (
        f"🥇 Mighty Duck Control Panel / {APP_VERSION}\n\n"
        f"Runtime:\n"
        f"rss_health={runtime.get('rss_health', 'n/a')}\n"
        f"watchdog_health={runtime.get('watchdog_health', 'n/a')}\n"
        f"collect_reserve_health={runtime.get('collect_reserve_health', 'n/a')}\n"
        f"runtime_alert_count={runtime.get('runtime_alert_count', 'n/a')}\n\n"
        f"Cycle:\n"
        f"cycle_health={cycle.get('cycle_health', 'n/a')}\n"
        f"elapsed={cycle.get('cycle_elapsed_seconds', 'n/a')}s\n"
        f"sleep={cycle.get('cycle_sleep_seconds', 'n/a')}s\n"
        f"reserve_pct={cycle.get('cycle_reserve_pct', 'n/a')}\n"
        f"overrun_streak={cycle.get('overrun_streak', 'n/a')}\n\n"
        f"Управление: кнопки снизу"
    )


def _build_runtime_text() -> str:
    runtime, cycle = _runtime_snapshot()
    alerts = runtime.get("runtime_alerts", [])

    return (
        f"⚙️ Runtime\n\n"
        f"rss={runtime.get('rss_mb', 'n/a')} MB / {runtime.get('rss_health', 'n/a')}\n"
        f"watchdog={runtime.get('watchdog_health', 'n/a')}\n"
        f"collect={runtime.get('collect_seconds', 'n/a')}s\n"
        f"collect_reserve={runtime.get('collect_reserve_seconds', 'n/a')}s "
        f"({runtime.get('collect_reserve_health', 'n/a')})\n"
        f"cycle={cycle.get('cycle_elapsed_seconds', 'n/a')}s / {cycle.get('cycle_health', 'n/a')}\n"
        f"sleep={cycle.get('cycle_sleep_seconds', 'n/a')}s\n"
        f"alerts={alerts}"
    )


def _build_exports_text() -> str:
    files = [
        ПАПКА_ДАННЫХ / "market_research_bundle.zip",
        ПАПКА_ДАННЫХ / "market_research_bundle_quick.zip",
        ПАПКА_ДАННЫХ / "audit_report.txt",
        ПАПКА_ДАННЫХ / "research_report.txt",
        ПАПКА_ДАННЫХ / "storage_manifest.txt",
        ПАПКА_ДАННЫХ / "runtime_health_report.txt",
        ПАПКА_ДАННЫХ / "request_failure_report.csv",
    ]

    lines = ["📦 Exports", ""]
    lines.extend(_fmt_file(path) for path in files)
    return "\n".join(lines)


def _build_backup_text() -> str:
    return (
        "🧱 Backup / DB\n\n"
        "Telegram отдаёт лёгкие runtime/export файлы.\n"
        "Тяжёлый backup БД делаем отдельно через Postgres/Railway backup или pg_dump.\n\n"
        "Current files:\n"
        f"{_fmt_file(ПАПКА_ДАННЫХ / 'market_research_bundle.zip')}\n"
        f"{_fmt_file(ПАПКА_ДАННЫХ / 'storage_manifest.txt')}\n\n"
        "Next stage: отдельный безопасный backup/export контур без нагрузки на runtime loop."
    )


def _build_help_text() -> str:
    return "\n".join([
        "❓ Помощь",
        "",
        "Простой операторский режим:",
        "⚙️ Фазы — вход в меню фаз и ручного сброса фазы 3",
        "🩺 Система — быстрый статус здоровья контура",
        "🧨 Сброс всех Ф3 — мгновенно снимает все текущие фазы 3",
        "",
        "Основные команды:",
        "/phases",
        "/phase1 /phase2 /phase3",
        "/coin SYMBOL",
        "/reset_stage3 SYMBOL reason",
        "/health",
        "/system_health",
        "",
        "Карточка монеты отражает канонический OI-only decision surface.",
        "Reviewer-слой отключен и не участвует в рабочем Telegram-потоке.",
        "Лишний UI убран: TOP OI / Скачать / Карантин отключены и не являются рабочей поверхностью.",
    ])


def _is_admin_chat(chat_id: str | int | None = None) -> bool:
    if not TELEGRAM_CHAT_ID:
        return False
    if chat_id is None:
        return True
    return str(chat_id) == str(TELEGRAM_CHAT_ID)


def _is_admin() -> bool:
    return _is_admin_chat()


def _safe_rows(sql: str, params: tuple = ()) -> list[dict]:
    try:
        return fetch(sql, params) or []
    except Exception as exc:
        log(f"telegram db fetch error: {exc}")
        # fetch() owns its connection. Retry once without reaching into db.py
        # internals, so Telegram cannot close a connection used by main.py.
        try:
            return fetch(sql, params) or []
        except Exception as retry_exc:
            log(f"telegram db fetch retry failed: {retry_exc}")
        return []


def _admin_only(chat_id=None) -> bool:
    if not _is_admin_chat(chat_id):
        send_message("⛔ Admin-only команда.", _main_keyboard())
        return False
    return True



def _tf_norm(value) -> str | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    aliases = {
        "15m": "15м", "15м": "15м",
        "30m": "30м", "30м": "30м",
        "1h": "1ч", "1ч": "1ч",
        "4h": "4ч", "4ч": "4ч",
        "24h": "24ч", "24ч": "24ч",
    }
    return aliases.get(v, v)


def _tf_sql(value) -> str | None:
    v = _tf_norm(value)
    aliases = {
        "15м": "15м",
        "30м": "30м",
        "1ч": "1ч",
        "4ч": "4ч",
        "24ч": "24ч",
    }
    return aliases.get(v, v)


def _window_rank_sql(column: str = "window_code") -> str:
    return (
        f"CASE {column} "
        "WHEN '15м' THEN 1 "
        "WHEN '30м' THEN 2 "
        "WHEN '1ч' THEN 3 "
        "WHEN '4ч' THEN 4 "
        "WHEN '12ч' THEN 5 "
        "WHEN '24ч' THEN 6 "
        "ELSE 9 END"
    )


def _stage_label(stage: int | None) -> str:
    return {
        0: "вне сценария",
        1: "тихое накопление",
        2: "развивающийся набор",
        3: "подтвержденный набор",
    }.get(int(stage or 0), "неизвестно")


def _exchange_code(exchange) -> str:
    return "BY" if str(exchange).upper() == "BYBIT" else "BN"


def _esc_html(value: str) -> str:
    return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _coinglass_link(exchange: str, symbol: str) -> str:
    ex = "Binance" if str(exchange or "").upper() == "BINANCE" else "Bybit"
    return f"https://www.coinglass.com/tv/{ex}_{symbol}"


def _bybit_link(symbol: str) -> str:
    return f"https://www.bybit.com/trade/usdt/{symbol}"


def _binance_link(symbol: str) -> str:
    return f"https://www.binance.com/en/futures/{symbol}"


def _symbol_exists_on_exchange(exchange: str, symbol: str) -> bool:
    rows = _safe_rows("""
        SELECT 1
        FROM core_state_v2
        WHERE exchange = %s
          AND symbol = %s
        LIMIT 1
    """, (exchange, symbol))
    if rows:
        return True
    rows = _safe_rows("""
        SELECT 1
        FROM aggregate_windows
        WHERE exchange = %s
          AND symbol = %s
        LIMIT 1
    """, (exchange, symbol))
    return bool(rows)


def _resolve_bybit_symbol(symbol: str) -> tuple[str, str]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return "", ""
    alias = BYBIT_SYMBOL_ALIASES.get(sym)
    if alias and _symbol_exists_on_exchange("BYBIT", alias):
        return alias, "alias"
    if _symbol_exists_on_exchange("BYBIT", sym):
        return sym, "exact"
    for suffix in ("USDT", "PERP", "USD"):
        if sym.endswith(suffix):
            continue
        candidate = sym + suffix
        if _symbol_exists_on_exchange("BYBIT", candidate):
            return candidate, "mapped"
    if sym.startswith("1000"):
        base = sym[4:]
        if _symbol_exists_on_exchange("BYBIT", base):
            return base, "mapped"
    return "", ""


def _bybit_availability_note(symbol: str, exchange: str) -> str:
    if str(exchange or "").upper().strip() == "BYBIT":
        return ""
    bybit_symbol, mode = _resolve_bybit_symbol(symbol)
    if bybit_symbol and mode in {"alias", "mapped"}:
        return f"⚠️ Bybit аналог: <b>{_esc_html(bybit_symbol)}</b>"
    if not bybit_symbol:
        return "⚠️ На Bybit этой монеты нет"
    return ""


def _compact_links(exchange: str, symbol: str, elapsed_text: str = "", cycle_num=None) -> str:
    sym = str(symbol or "").upper().strip()
    ex = str(exchange or "").upper().strip() or "BINANCE"
    cg = _coinglass_link(ex, sym)
    copy_part = f"<code>{_esc_html(sym)}</code>"
    by_sym, _ = _resolve_bybit_symbol(sym)

    if ex == "BYBIT" and _symbol_exists_on_exchange("BYBIT", sym):
        left = f'🔗 <a href="{cg}">CG</a> | <a href="{_bybit_link(sym)}">BY</a> | {copy_part}'
    elif ex == "BYBIT" and by_sym:
        left = f'🔗 <a href="{cg}">CG</a> | <a href="{_bybit_link(by_sym)}">BY</a> | {copy_part}'
    elif ex == "BYBIT":
        left = f'🔗 <a href="{cg}">CG</a> | BY n/a | {copy_part}'
    elif by_sym:
        left = f'🔗 <a href="{cg}">CG</a> | <a href="{_bybit_link(by_sym)}">BY</a> | {copy_part}'
    else:
        left = f'🔗 <a href="{cg}">CG</a> | <a href="{_binance_link(sym)}">BN</a> | {copy_part}'

    parts = [left]
    if elapsed_text:
        parts.append("     " + _esc_html(elapsed_text))
    if cycle_num is not None:
        try:
            parts.append("     " + f"{int(cycle_num)}🔄")
        except Exception:
            pass
    return "".join(parts)


def _symbol_links(symbol: str, exchange=None) -> str:
    return _compact_links(str(exchange or ""), str(symbol or ""))


def _short_ts(value) -> str:
    return str(value or "n/a").replace("+00:00", " UTC")


def _table_health(table: str, ts_col: str, stale_minutes: int = 15) -> dict:
    allowed = {
        ("oi_raw", "ts_close"),
        ("aggregate_windows", "ts_close"),
        ("oi_core_state", "latest_cycle_ts"),
        ("oi_window_state", "cycle_ts"),
        ("oi_stage_history", "cycle_ts"),
        ("core_state_v2", "latest_cycle_ts"),
        ("window_state_v2", "cycle_ts"),
        ("transition_history_v2", "cycle_ts"),
    }
    if (table, ts_col) not in allowed:
        return {"table": table, "status": "ERROR", "rows": 0, "latest": None, "age_minutes": None}

    rows = _safe_rows(f"""
        SELECT
            COUNT(*) AS rows,
            MAX({ts_col}) AS latest,
            EXTRACT(EPOCH FROM (NOW() - MAX({ts_col}))) / 60.0 AS age_minutes
        FROM {table}
    """)

    if not rows:
        return {"table": table, "status": "ERROR", "rows": 0, "latest": None, "age_minutes": None}

    r = rows[0]
    count = int(r.get("rows") or 0)
    latest = r.get("latest")
    age = r.get("age_minutes")

    if count <= 0:
        status = "EMPTY"
    elif latest is None:
        status = "EMPTY"
    elif float(age or 999999) > stale_minutes:
        status = "STALE"
    else:
        status = "OK"

    return {
        "table": table,
        "status": status,
        "rows": count,
        "latest": latest,
        "age_minutes": round(float(age or 0), 1) if age is not None else None,
    }


def _sync_health_pair(
    legacy_table: str,
    legacy_ts_col: str,
    v2_table: str,
    v2_ts_col: str,
    label: str,
) -> dict:
    legacy = _table_health(legacy_table, legacy_ts_col, 15)
    v2 = _table_health(v2_table, v2_ts_col, 15)

    if legacy["status"] != "OK" or v2["status"] != "OK":
        status = "DEGRADED"
    elif int(legacy["rows"] or 0) != int(v2["rows"] or 0):
        status = "DRIFT"
    elif str(legacy["latest"]) != str(v2["latest"]):
        status = "DRIFT"
    else:
        status = "OK"

    return {
        "label": label,
        "status": status,
        "legacy_rows": legacy["rows"],
        "v2_rows": v2["rows"],
        "legacy_latest": legacy["latest"],
        "v2_latest": v2["latest"],
    }


def _build_health_text() -> str:
    checks = [
        ("oi_raw", "ts_close", 15),
        ("aggregate_windows", "ts_close", 15),
        ("oi_core_state", "latest_cycle_ts", 15),
        ("core_state_v2", "latest_cycle_ts", 15),
        ("oi_window_state", "cycle_ts", 15),
        ("window_state_v2", "cycle_ts", 15),
        ("oi_stage_history", "cycle_ts", 60),
        ("transition_history_v2", "cycle_ts", 60),
    ]

    lines = ["🩺 Health — OI runtime", ""]

    for table, ts_col, stale_min in checks:
        h = _table_health(table, ts_col, stale_min)
        lines.append(
            f"{h['status']} | {h['table']} | rows={h['rows']} | "
            f"latest={_short_ts(h['latest'])} | age_min={h['age_minutes']}"
        )

    parity = [
        _sync_health_pair("oi_core_state", "latest_cycle_ts", "core_state_v2", "latest_cycle_ts", "core"),
        _sync_health_pair("oi_window_state", "cycle_ts", "window_state_v2", "cycle_ts", "window"),
    ]
    lines.append("")
    lines.append("legacy-v2 parity:")
    for item in parity:
        lines.append(
            f"{item['status']} | {item['label']} | "
            f"legacy_rows={item['legacy_rows']} | v2_rows={item['v2_rows']} | "
            f"legacy_latest={_short_ts(item['legacy_latest'])} | v2_latest={_short_ts(item['v2_latest'])}"
        )

    runtime, cycle = _runtime_snapshot()
    lines.extend([
        "",
        f"runtime_rss={runtime.get('rss_mb', 'n/a')} MB / {runtime.get('rss_health', 'n/a')}",
        f"watchdog={runtime.get('watchdog_health', 'n/a')}",
        f"cycle={cycle.get('cycle_elapsed_seconds', 'n/a')}s / {cycle.get('cycle_health', 'n/a')}",
        f"stop_reason={cycle.get('stop_reason', 'n/a')}",
        "",
        "OK <= stale window | STALE > stale window | EMPTY rows=0 | ERROR query failed",
    ])
    return "\n".join(lines)


def _build_system_health_text() -> str:
    runtime, cycle = _runtime_snapshot()
    watchdog = _read_kv_file(RUNTIME_REPORTS_DIR / "watchdog_status.txt")
    snapshot = _read_kv_file(RUNTIME_REPORTS_DIR / "snapshot_status.txt")

    def process_alive() -> bool:
        try:
            pid = int(runtime.get("pid") or 0)
        except Exception:
            return False
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    core = _table_health("core_state_v2", "latest_cycle_ts", 15)
    windows = _table_health("window_state_v2", "cycle_ts", 15)
    transitions = _table_health("transition_history_v2", "cycle_ts", 60)
    parity_core = _sync_health_pair("oi_core_state", "latest_cycle_ts", "core_state_v2", "latest_cycle_ts", "core")
    parity_window = _sync_health_pair("oi_window_state", "cycle_ts", "window_state_v2", "cycle_ts", "window")

    def health_mark(value: str | None) -> str:
        norm = str(value or "").strip().upper()
        if norm in {"OK", "READY"}:
            return "✅"
        if norm in {"DEGRADED", "WARNING", "WARN"}:
            return "⚠️"
        if norm in {"CRITICAL", "ERROR", "STALE", "EMPTY", "DRIFT"}:
            return "❌"
        return "•"

    def health_status_ru(value: str | None) -> str:
        return {
            "OK": "норма",
            "READY": "готово",
            "DEGRADED": "просадка",
            "WARNING": "предупреждение",
            "WARN": "предупреждение",
            "CRITICAL": "критично",
            "ERROR": "ошибка",
            "STALE": "устарело",
            "EMPTY": "пусто",
            "DRIFT": "рассинхрон",
            "OVERRUN": "цикл вышел за лимит",
        }.get(str(value or "").strip().upper(), str(value or "n/a"))

    def stop_reason_ru(value: str | None) -> str:
        return {
            "ok": "шаг завершен штатно",
            "runtimeerror": "последний проход оборвался на runtime-проверке",
            "collect_too_slow_for_aggregates": "сбор слишком долгий для агрегатов",
            "aggregates_timeout": "агрегаты не уложились в лимит",
            "watchdog_stop": "цикл остановлен сторожем",
        }.get(str(value or "").strip().lower(), "нет оценки")

    def health_line(label: str, status: str | None, extra: str = "") -> str:
        tail = f" | {extra}" if extra else ""
        return f"{health_mark(status)} {label}: {health_status_ru(status)}{tail}"

    process_is_alive = process_alive()
    cycle_status = cycle.get("cycle_health")
    cycle_extra = f"цикл {cycle.get('cycle_elapsed_seconds', 'n/a')}с"
    if str(cycle_status or "").strip().lower() == "stopped" and process_is_alive:
        cycle_status = "WARNING"
        cycle_extra = f"последний проход оборвался, процесс жив | цикл {cycle.get('cycle_elapsed_seconds', 'n/a')}с"

    lines = [
        "🩺 Состояние системы",
        "",
        "Основной цикл",
        health_line("здоровье цикла", cycle_status, cycle_extra),
        f"{'✅' if process_is_alive else '❌'} Процесс бота: {'жив' if process_is_alive else 'не найден'} | pid {runtime.get('pid', 'n/a')}",
        f"⏱ Сон между циклами: {cycle.get('cycle_sleep_seconds', 'n/a')}с",
        f"🪫 Резерв цикла: {cycle.get('cycle_reserve_pct', 'n/a')}%",
        f"🛑 Причина остановки шага: {stop_reason_ru(cycle.get('stop_reason'))}",
        "",
        "Runtime",
        health_line("память процесса", runtime.get("rss_health"), f"{runtime.get('rss_mb', 'n/a')} MB"),
        health_line("сторож", runtime.get("watchdog_health", watchdog.get("watchdog_health", "n/a"))),
        health_line("резерв сбора", runtime.get("collect_reserve_health")),
        health_line("снимок runtime", runtime.get("snapshot_health", snapshot.get("snapshot_health", "n/a"))),
        f"🚨 Активных runtime-alerts: {runtime.get('runtime_alert_count', len(runtime.get('runtime_alerts', []) or []))}",
        "",
        "Таблицы",
        health_line("core_state_v2", core["status"], f"возраст {core['age_minutes']}м | строк {core['rows']}"),
        health_line("window_state_v2", windows["status"], f"возраст {windows['age_minutes']}м | строк {windows['rows']}"),
        health_line("transition_history_v2", transitions["status"], f"возраст {transitions['age_minutes']}м | строк {transitions['rows']}"),
        "",
        "Синхронность слоев",
        health_line("legacy ↔ v2 core", parity_core["status"]),
        health_line("legacy ↔ v2 window", parity_window["status"]),
        "",
        "Норма: цикл должен быть в норме или с мягкой просадкой, а core/window/transition таблицы должны быть свежими.",
    ]
    return "\n".join(lines)


def _health_banner_for_table(table: str, ts_col: str, stale_minutes: int = 10) -> str:
    h = _table_health(table, ts_col, stale_minutes)
    return (
        f"health={h['status']} | rows={h['rows']} | "
        f"latest={_short_ts(h['latest'])} | age_min={h['age_minutes']}"
    )


def _fmt_pct(value) -> str:
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "n/a"


def _tf_rank_sql() -> str:
    return "CASE timeframe WHEN '4h' THEN 1 WHEN '1h' THEN 2 WHEN '30m' THEN 3 WHEN '15m' THEN 4 ELSE 9 END"


def _build_phases_text(phase: int | None = None) -> str:
    if phase is None:
        rows = _safe_rows("""
            SELECT current_stage, COUNT(*) AS cnt, MAX(latest_cycle_ts) AS latest
            FROM core_state_v2
            WHERE current_stage > 0
            GROUP BY current_stage
            ORDER BY current_stage DESC
        """)
        if not rows:
            return "⚙️ Фазы\n\nАктивных фаз нет."

        lines = ["⚙️ Фазы", ""]
        for r in rows:
            lines.append(
                f"Фаза {r.get('current_stage')} — {_stage_label(r.get('current_stage'))} | "
                f"монет={r.get('cnt')} | latest={_short_ts(r.get('latest'))}"
            )
        lines.extend(["", "Выбери фазу кнопками ниже."])
        return "\n".join(lines)

    rows = _phase_rows(phase)
    title = f"Фаза {phase}"
    if not rows:
        return f"{title}\n\nСейчас монет в фазе нет."

    total_count = _phase_total_count(phase)
    lines = [
        f"⚙️ {title}",
        f"Монет сейчас: {total_count}",
        f"Показано в списке: {min(len(rows), 12)} из {len(rows)} загруженных",
        "",
        "Нажми на монету ниже, чтобы открыть карточку.",
        "",
    ]
    for r in rows[:12]:
        oi = r.get("oi_summary") or {}
        price = r.get("price_summary") or {}
        lines.append(
            f"{r.get('symbol')} [{r.get('exchange')}] — "
            f"{oi.get('oi_pattern_label') or oi.get('oi_pattern_code') or 'n/a'} | "
            f"цена={price.get('price_state') or 'n/a'} | "
            f"возраст={_fmt_minutes(r.get('stage_age_minutes'))}"
        )
    return "\n".join(lines)


def _build_stage3_text() -> str:
    return _build_phases_text(3)


def _build_coin_card(symbol: str, exchange: str | None = None, as_of_ts=None) -> str:
    symbol = symbol.upper().strip()
    exchange = str(exchange or "").upper().strip() or None

    params: list = [symbol]
    exchange_sql = ""
    if exchange:
        exchange_sql = "AND exchange = %s"
        params.append(exchange)

    core_rows = _safe_rows(f"""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
          {exchange_sql}
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 5
    """, tuple(params))

    if not core_rows:
        return f"🪙 {symbol}\n\nНет данных. Формат: /coin BTCUSDT"

    core_row = core_rows[0]
    exchange = str(core_row.get("exchange") or exchange or "").upper()
    window_rows = _load_window_rows(symbol, exchange, as_of_ts=as_of_ts)
    history_rows = _collect_transition_history(symbol, exchange, as_of_ts=as_of_ts)
    metric_windows = _latest_metric_windows(symbol, exchange, as_of_ts=as_of_ts)
    title = f"🪙 CARD — Фаза {core_row.get('current_stage')}"
    return _build_coin_message(core_row, window_rows, history_rows, metric_windows, title=title, transition_ts=as_of_ts)


def _coin_stage(core_rows: list[dict]) -> int:
    if not core_rows:
        return 0
    try:
        return int(core_rows[0].get("current_stage") or 0)
    except Exception:
        return 0

def _save_debug_cases(symbol: str, comment: str, core_rows: list[dict], window_rows: list[dict], history_rows: list[dict]) -> int:
    written = 0
    for r in core_rows:
        execute(
            """
            INSERT INTO debug_cases_v2(
                exchange, symbol, cycle_ts, current_stage,
                oi_summary, price_summary, volume_summary,
                user_comment, status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, NOW(), NOW())
            """,
            (
                r.get("exchange"),
                symbol,
                r.get("latest_cycle_ts"),
                r.get("current_stage"),
                json.dumps(r.get("oi_summary") or {}, ensure_ascii=False, default=str),
                json.dumps(r.get("price_summary") or {}, ensure_ascii=False, default=str),
                json.dumps(r.get("volume_summary") or {}, ensure_ascii=False, default=str),
                comment,
                "new",
            ),
        )
        written += 1
    return written

def _build_debug_cases_text(symbol: str | None = None) -> str:
    symbol = str(symbol or "").upper().strip() or None
    params = []
    where = ""
    title = "🧪 Debug cases"
    if symbol:
        where = "WHERE symbol = %s"
        params.append(symbol)
        title = f"🧪 Debug cases — {symbol}"

    rows = _safe_rows(
        f"""
        SELECT exchange, symbol, cycle_ts, current_stage, oi_summary, user_comment, status, created_at
        FROM debug_cases_v2
        {where}
        ORDER BY created_at DESC
        LIMIT 12
        """,
        tuple(params),
    )

    if not rows:
        return f"{title}\n\nНет строк в debug_cases_v2."

    lines = [title, "_latest v2 debug snapshots_"]
    for idx, row in enumerate(rows, 1):
        oi = row.get("oi_summary") or {}
        lines.append(
            f"{idx}. {row.get('symbol')} [{row.get('exchange')}] | "
            f"stage={row.get('current_stage')} | pattern={oi.get('oi_pattern_label') or oi.get('oi_pattern_code')} | "
            f"status={row.get('status') or 'n/a'} | cycle={_short_ts(row.get('cycle_ts'))}"
        )
        comment = str(row.get("user_comment") or "").strip()
        if comment:
            lines.append(f"   comment={comment[:180]}")
    return "\n".join(lines)

def _fmt_post_stage_delta(trigger_price, future_price) -> str:
    if trigger_price in (None, 0) or future_price is None:
        return "n/a"
    try:
        delta_pct = ((float(future_price) - float(trigger_price)) / float(trigger_price)) * 100.0
    except Exception:
        return "n/a"
    return f"{delta_pct:+.2f}%"


def _post_stage_quality_ru(value: str | None) -> str:
    mapping = {
        "positive": "positive",
        "negative": "negative",
        "flat": "flat",
        "partial": "partial",
        "pending_24h": "pending_24h",
        "pending": "pending",
    }
    return mapping.get(str(value or "").strip(), str(value or "n/a"))


def _build_post_stage_text(symbol: str | None = None) -> str:
    symbol = str(symbol or "").upper().strip() or None
    params = []
    where = ""
    title = "📊 Post-stage analytics"
    if symbol:
        where = "WHERE symbol = %s"
        params.append(symbol)
        title = f"📊 Post-stage analytics — {symbol}"

    rows = _safe_rows(
        f"""
        SELECT exchange, symbol, stage_triggered, triggered_at, trigger_price,
               price_after_1h, price_after_4h, price_after_12h, price_after_24h,
               quality_label, notes
        FROM post_stage_analytics_v2
        {where}
        ORDER BY triggered_at DESC
        LIMIT 12
        """,
        tuple(params),
    )

    if not rows:
        return f"{title}\n\nНет строк в post_stage_analytics_v2."

    quality_counts = {}
    matured_24h = 0
    for row in rows:
        quality = str(row.get("quality_label") or "n/a")
        quality_counts[quality] = quality_counts.get(quality, 0) + 1
        if row.get("price_after_24h") is not None:
            matured_24h += 1

    summary = ", ".join(f"{k}={v}" for k, v in sorted(quality_counts.items()))
    lines = [
        title,
        f"rows={len(rows)} | matured_24h={matured_24h} | {summary}",
        "_latest v2 post-stage outcomes_",
    ]
    for idx, row in enumerate(rows, 1):
        lines.append(
            f"{idx}. {row.get('symbol')} [{row.get('exchange')}] | stage={row.get('stage_triggered')} | "
            f"trigger={_short_ts(row.get('triggered_at'))} | quality={_post_stage_quality_ru(row.get('quality_label'))}"
        )
        lines.append(
            f"   1h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_1h'))} | "
            f"4h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_4h'))} | "
            f"12h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_12h'))} | "
            f"24h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_24h'))}"
        )
        notes = str(row.get("notes") or "").strip()
        if notes:
            lines.append(f"   notes={notes[:180]}")
    return "\n".join(lines)


def _stage3_policy_hint(current_stage, stage_age_minutes, pattern_code, manual_reset_required) -> str:
    try:
        stage = int(current_stage or 0)
    except Exception:
        stage = 0
    try:
        age = float(stage_age_minutes or 0.0)
    except Exception:
        age = 0.0
    pattern = str(pattern_code or "").strip()
    manual = bool(manual_reset_required)

    stale_patterns = {"мертвая_форма", "ложный_всплеск", "поломка_набора"}
    if stage != 3:
        return "not_stage3"
    if pattern in stale_patterns and age >= 120:
        return "stale_stage3_reset_candidate"
    if manual and age >= 240:
        return "manual_hold_review_needed"
    if age < 60:
        return "fresh_stage3"
    return "observe_stage3"


def _build_review_case_text(symbol: str) -> str:
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        return "Формат: /review BTCUSDT"

    core_rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 6
    """, (symbol,))
    feedback_rows = _safe_rows("""
        SELECT exchange, symbol, cycle_ts, current_stage, user_comment, status, created_at
        FROM debug_cases_v2
        WHERE symbol = %s
        ORDER BY created_at DESC
        LIMIT 8
    """, (symbol,))
    history_rows = _safe_rows("""
        SELECT exchange, symbol, from_stage, to_stage, cycle_ts, transition_allowed, stage_age_before_transition, reason
        FROM transition_history_v2
        WHERE symbol = %s
        ORDER BY cycle_ts DESC
        LIMIT 8
    """, (symbol,))
    post_rows = _safe_rows("""
        SELECT exchange, symbol, stage_triggered, triggered_at, quality_label, notes,
               trigger_price, price_after_1h, price_after_4h, price_after_12h, price_after_24h
        FROM post_stage_analytics_v2
        WHERE symbol = %s
        ORDER BY triggered_at DESC
        LIMIT 6
    """, (symbol,))

    if not core_rows and not feedback_rows and not history_rows and not post_rows:
        return f"🧭 Review case — {symbol}\n\nНет данных."

    lines = [f"🧭 Review case — {symbol}", ""]

    if core_rows:
        lines.append("CURRENT STATE:")
        for row in core_rows[:3]:
            oi = row.get("oi_summary") or {}
            ex = row.get("exchange")
            policy = _stage3_policy_hint(
                row.get("current_stage"),
                row.get("stage_age_minutes"),
                oi.get("oi_pattern_code"),
                row.get("manual_reset_required"),
            )
            lines.append(
                f"{ex} | stage={row.get('current_stage')} {_stage_label(row.get('current_stage'))} | "
                f"age={row.get('stage_age_minutes')}m | policy={policy}"
            )
            lines.append(
                f"pattern={oi.get('oi_pattern_label') or oi.get('oi_pattern_code')} | "
                f"reason={row.get('phase_reason')}"
            )
        lines.append("")

        exchange_block = _build_exchange_semantic_block(symbol, str(core_rows[0].get("exchange") or ""))
        if exchange_block:
            lines.append("EXCHANGE SENSITIVITY:")
            lines.extend(exchange_block[1:])
            lines.append("")

    if history_rows:
        lines.append("LATEST TRANSITIONS:")
        for row in history_rows[:5]:
            lines.append(
                f"{row.get('exchange')} | {row.get('from_stage')}->{row.get('to_stage')} | "
                f"{_short_ts(row.get('cycle_ts'))} | age_before={row.get('stage_age_before_transition')}m"
            )
            lines.append(f"reason={row.get('reason')}")
        lines.append("")

    if feedback_rows:
        lines.append("OPERATOR FEEDBACK:")
        for row in feedback_rows[:5]:
            comment = str(row.get("user_comment") or "").strip()
            lines.append(
                f"{row.get('exchange')} | stage={row.get('current_stage')} | "
                f"created={_short_ts(row.get('created_at'))} | status={row.get('status') or 'n/a'}"
            )
            if comment:
                lines.append(f"comment={comment[:220]}")
        lines.append("")
    else:
        lines.append("OPERATOR FEEDBACK:\nнет комментариев\n")

    if post_rows:
        lines.append("POST-STAGE OUTCOMES:")
        for row in post_rows[:4]:
            lines.append(
                f"{row.get('exchange')} | stage={row.get('stage_triggered')} | "
                f"trigger={_short_ts(row.get('triggered_at'))} | quality={_post_stage_quality_ru(row.get('quality_label'))}"
            )
            lines.append(
                f"1h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_1h'))} | "
                f"4h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_4h'))} | "
                f"12h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_12h'))} | "
                f"24h={_fmt_post_stage_delta(row.get('trigger_price'), row.get('price_after_24h'))}"
            )
            notes = str(row.get("notes") or "").strip()
            if notes:
                lines.append(f"notes={notes[:180]}")
        lines.append("")
    else:
        lines.append("POST-STAGE OUTCOMES:\nнет строк\n")

    lines.append(f"Actions: /coin {symbol} | /feedback {symbol} | /debug_cases {symbol} | /post_stage {symbol}")
    return "\n".join(lines)
def _feedback_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_feedback.csv"



def _save_feedback_snapshot(symbol: str, comment: str, source: str = "text_command") -> str:
    symbol = symbol.upper().strip()
    comment = str(comment or "").strip()
    if not symbol or not comment:
        return "Нужны SYMBOL и текст комментария."

    core_rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 20
    """, (symbol,))

    if not core_rows:
        return f"Нет core_state_v2 snapshot для {symbol}. Комментарий не сохранён."

    window_rows = _safe_rows(f"""
        SELECT *
        FROM window_state_v2
        WHERE symbol = %s
        ORDER BY exchange, {_window_rank_sql()}, cycle_ts DESC
    """, (symbol,))

    history_rows = _safe_rows("""
        SELECT *
        FROM transition_history_v2
        WHERE symbol = %s
        ORDER BY cycle_ts DESC
        LIMIT 20
    """, (symbol,))

    stored_comment = f"[source={source}] {comment}"
    debug_written = _save_debug_cases(symbol, stored_comment, core_rows, window_rows, history_rows)

    path = _feedback_path()
    new_file = not path.exists()

    header = [
        "created_at_utc",
        "symbol",
        "exchange",
        "current_stage",
        "oi_pattern_code",
        "oi_pattern_label",
        "price_state_summary",
        "volume_state_summary",
        "oi_stage_age_minutes",
        "oi_transition_permission",
        "blocked_stage_max",
        "latest_cycle_ts",
        "decision_reason",
        "core_state_json",
        "window_state_json",
        "stage_history_json",
        "user_comment",
    ]

    now = iso_мск()
    written = 0

    with _csv_lock:
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(header)

            for r in core_rows:
                ex = r.get("exchange")
                ex_windows = [row for row in window_rows if row.get("exchange") == ex]
                ex_history = [row for row in history_rows if row.get("exchange") == ex]
                oi = r.get("oi_summary") or {}
                price = r.get("price_summary") or {}
                volume = r.get("volume_summary") or {}

                w.writerow([
                    now,
                    symbol,
                    ex,
                    r.get("current_stage"),
                    oi.get("oi_pattern_code"),
                    oi.get("oi_pattern_label"),
                    price.get("price_state"),
                    volume.get("volume_state"),
                    r.get("stage_age_minutes"),
                    r.get("transition_permission"),
                    price.get("blocked_stage_max"),
                    r.get("latest_cycle_ts"),
                    r.get("phase_reason"),
                    json.dumps(r, ensure_ascii=False, default=str),
                    json.dumps(ex_windows, ensure_ascii=False, default=str),
                    json.dumps(ex_history, ensure_ascii=False, default=str),
                    stored_comment,
                ])
                written += 1

    return f"✅ Feedback snapshot v2 сохранён: {symbol}, rows={written}, debug_cases={debug_written}"


def _save_feedback(text: str) -> str:
    parts = text.split(maxsplit=2)
    if len(parts) < 2:
        return "Формат: /feedback SYMBOL текст"
    if len(parts) == 2:
        _, symbol = parts
        return _begin_feedback_flow(None, symbol, source="slash_command")

    _, symbol, comment = parts
    return _save_feedback_snapshot(symbol, comment, source="slash_command")


def _begin_feedback_flow(chat_id: str | int | None, symbol: str, source: str = "inline_button", seed: str | None = None) -> str:
    symbol = symbol.upper().strip()
    core_rows = _safe_rows("""
        SELECT *
        FROM core_state_v2
        WHERE symbol = %s
        ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
        LIMIT 2
    """, (symbol,))
    if not core_rows:
        return f"Нет snapshot для {symbol}. Режим обратной связи не открыт."

    stage = _coin_stage(core_rows)
    _save_pending_feedback(chat_id, symbol, seed=seed)
    seed_line = f"\nШаблон: {seed}" if seed else ""
    return "\n".join([
        "🗣 Режим обратной связи открыт",
        "",
        f"Монета: {symbol}",
        f"Текущая стадия: {stage} {_stage_label(stage)}",
        f"Причина: {_human_phase_reason(core_rows[0].get('phase_reason'))}",
        seed_line.strip(),
        "",
        "Следующее обычное сообщение в этот чат сохраню как комментарий к этой монете.",
        "Отмена: /cancel_feedback",
    ]).replace("\n\n\n", "\n\n")


def _handle_pending_feedback_message(text: str, chat_id: str | int | None) -> str | None:
    if not _matches_pending_feedback(chat_id):
        return None
    pending = _load_pending_feedback()
    if not pending:
        return None
    if not text or text.startswith("/") or text in {
        "⬅️ Назад",
        "⚙️ Фазы",
        "❓ Помощь",
        "🥉 Фаза 1",
        "🥈 Фаза 2",
        "🥇 Фаза 3",
        "🧯 Сброс фазы 3",
        "📈 Топ ОИ",
        "📈 ТОП OI",
        "⬇️ Скачать",
        "🧱 Карантин",
        "🧱 Quarantine",
    }:
        return None

    comment = text
    seed = str(pending.get("seed") or "").strip()
    if seed:
        comment = f"{seed} | {comment}"
    result = _save_feedback_snapshot(pending.get("symbol") or "", comment, source="pending_button_flow")
    _clear_pending_feedback()
    return result


def _handle_cancel_feedback(chat_id=None) -> None:
    if not _admin_only(chat_id):
        return
    pending = _load_pending_feedback()
    _clear_pending_feedback()
    if pending:
        send_message(f"✅ Режим обратной связи отменён: {pending.get('symbol')}", _main_keyboard())
    else:
        send_message("Режим обратной связи не был открыт.", _main_keyboard())

def _pending_reset_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_pending_reset_stage3.json"


def _save_pending_reset(symbol: str, reason: str) -> None:
    payload = {
        "created_at_utc": iso_мск(),
        "symbol": symbol.upper(),
        "reason": reason,
    }
    _pending_reset_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def _load_pending_reset() -> dict:
    path = _pending_reset_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(errors="ignore"))
    except Exception:
        return {}


def _clear_pending_reset() -> None:
    path = _pending_reset_path()
    if path.exists():
        path.unlink()


def _handle_stage3_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        send_message("Формат: /reset_stage3 SYMBOL reason", _main_keyboard())
        return

    _, symbol, reason = parts
    symbol = symbol.upper().strip()
    reason = reason.strip()

    _save_pending_reset(symbol, reason)

    send_message(
        "\n".join([
            "⚠️ Подготовлен ручной сброс фазы 3",
            "",
            f"Монета: {symbol}",
            f"Причина: {reason}",
            "",
            f"Подтвердить: /confirm_reset {symbol}",
            "Отменить: /cancel_reset",
        ]),
        _stage3_reset_actions_keyboard(symbol=symbol),
    )


def _handle_confirm_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    pending = _load_pending_reset()
    if not pending:
        send_message("Нет ожидающего ручного сброса.", _main_keyboard())
        return

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        send_message("Формат: /confirm_reset SYMBOL", _main_keyboard())
        return

    _, symbol = parts
    symbol = symbol.upper().strip()

    if symbol != pending.get("symbol"):
        send_message(
            f"Ожидающий сброс не совпадает. Сейчас выбран: {pending.get('symbol')}",
            _main_keyboard(),
        )
        return

    total = 0
    reason = pending.get("reason") or "confirmed_reset"

    if symbol == "ALL":
        rows = _safe_rows("""
            SELECT DISTINCT exchange, symbol
            FROM core_state_v2
            WHERE current_stage = 3
            ORDER BY exchange, symbol
        """)
        for row in rows:
            try:
                total += max(
                    reset_stage3(row.get("exchange"), row.get("symbol"), "n/a", reason, dry_run=False),
                    0,
                )
            except Exception as exc:
                log(f"telegram confirm_reset all error: {exc}")
    else:
        for exchange in ("BYBIT", "BINANCE"):
            try:
                total += max(reset_stage3(exchange, symbol, "n/a", reason, dry_run=False), 0)
            except Exception as exc:
                log(f"telegram confirm_reset error: {exc}")

    _clear_pending_reset()

    send_message(
        f"✅ Ручной сброс фазы 3 подтвержден: {symbol} | затронуто строк: {total}",
        _main_keyboard(),
    )


def _reset_stage3_all_now(reason: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return
    total = 0
    rows = _safe_rows("""
        SELECT DISTINCT exchange, symbol
        FROM core_state_v2
        WHERE current_stage = 3
        ORDER BY exchange, symbol
    """)
    for row in rows:
        try:
            total += max(
                reset_stage3(row.get("exchange"), row.get("symbol"), "n/a", reason, dry_run=False),
                0,
            )
        except Exception as exc:
            log(f"telegram reset all immediate error: {exc}")
    send_message(
        f"✅ Все текущие Фазы 3 сняты сразу | затронуто строк: {total}",
        _main_keyboard(),
    )


def _handle_cancel_reset(text: str, chat_id=None) -> None:
    if not _admin_only(chat_id):
        return

    pending = _load_pending_reset()
    _clear_pending_reset()

    if pending:
        send_message(
            f"✅ Ожидающий ручной сброс отменён: {pending.get('symbol')}",
            _main_keyboard(),
        )
    else:
        send_message("Ожидающий ручной сброс не найден.", _main_keyboard())
def _stage3_alert_history_path() -> Path:
    return ПАПКА_ДАННЫХ / "telegram_stage3_alert_history.csv"


def _read_stage3_alerted_keys(include_legacy: bool = False) -> set[str]:
    keys: set[str] = set()
    try:
        rows = _safe_rows("SELECT alert_key FROM telegram_stage3_alert_history WHERE alert_key IS NOT NULL")
        for row in rows:
            key = row.get("alert_key")
            if not key:
                continue
            if not include_legacy and not _is_canonical_stage3_alert_key(str(key)):
                continue
            keys.add(str(key))
    except Exception as exc:
        logger.warning("Не удалось прочитать историю stage3 alerts из БД: %s", exc)

    path = _stage3_alert_history_path()
    if not path.exists():
        return keys

    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = row.get("alert_key")
            if not key:
                continue
            if not include_legacy and not _is_canonical_stage3_alert_key(key):
                continue
            keys.add(key)
    return keys


def _json_object(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _enrich_stage3_decision_snapshot(row: dict) -> dict:
    """Flatten canonical core_state_v2 summaries for explainable Stage3 history."""
    enriched = dict(row)
    oi = _json_object(row.get("oi_summary"))
    price = _json_object(row.get("price_summary"))
    volume = _json_object(row.get("volume_summary"))
    enriched["oi_pattern_code"] = row.get("oi_pattern_code") or oi.get("oi_pattern_code")
    enriched["oi_pattern_label"] = row.get("oi_pattern_label") or oi.get("oi_pattern_label")
    enriched["price_state_summary"] = row.get("price_state_summary") or price.get("price_state")
    enriched["volume_state_summary"] = row.get("volume_state_summary") or volume.get("volume_state")
    enriched["oi_stage_age_minutes"] = row.get("oi_stage_age_minutes") or row.get("stage_age_minutes")
    enriched["decision_reason"] = row.get("decision_reason") or row.get("phase_reason")
    return enriched


STAGE3_HISTORY_RETENTION_DAYS = max(1, int(os.getenv("STAGE3_HISTORY_RETENTION_DAYS", "7") or "7"))
STAGE3_HISTORY_MAX_ROWS = max(100, int(os.getenv("STAGE3_HISTORY_MAX_ROWS", "5000") or "5000"))


def _prune_stage3_alert_history() -> None:
    """Keep explainability history bounded in both Postgres and CSV."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=STAGE3_HISTORY_RETENTION_DAYS)
    path = _stage3_alert_history_path()
    try:
        if path.exists():
            with path.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
            kept = []
            for item in rows:
                try:
                    created = datetime.fromisoformat(str(item.get("created_at_utc") or "").replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    kept.append(item)
                    continue
                if created >= cutoff:
                    kept.append(item)
            kept = kept[-STAGE3_HISTORY_MAX_ROWS:]
            if len(kept) != len(rows):
                with path.open("w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=[
                        "created_at_utc", "alert_key", "exchange", "symbol", "current_stage",
                        "oi_pattern_code", "oi_pattern_label", "price_state_summary",
                        "volume_state_summary", "oi_stage_age_minutes", "latest_cycle_ts",
                        "decision_reason",
                    ])
                    writer.writeheader()
                    writer.writerows(kept)
    except Exception as exc:
        logger.warning("Не удалось ограничить CSV историю stage3: %s", exc)
    try:
        execute(
            "DELETE FROM telegram_stage3_alert_history WHERE created_at < NOW() - (%s * INTERVAL '1 day')",
            (STAGE3_HISTORY_RETENTION_DAYS,),
        )
        execute(
            "DELETE FROM telegram_stage3_alert_history WHERE id NOT IN (SELECT id FROM telegram_stage3_alert_history ORDER BY created_at DESC, id DESC LIMIT %s)",
            (STAGE3_HISTORY_MAX_ROWS,),
        )
    except Exception as exc:
        logger.warning("Не удалось ограничить DB историю stage3: %s", exc)


def _append_stage3_alert_history(row: dict, alert_key: str) -> None:
    path = _stage3_alert_history_path()
    new_file = not path.exists()

    header = [
        "created_at_utc",
        "alert_key",
        "exchange",
        "symbol",
        "current_stage",
        "oi_pattern_code",
        "oi_pattern_label",
        "price_state_summary",
        "volume_state_summary",
        "oi_stage_age_minutes",
        "latest_cycle_ts",
        "decision_reason",
    ]

    with _csv_lock:
        with path.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(header)

            created_at_text = iso_мск()
            w.writerow([
                created_at_text,
                alert_key,
                row.get("exchange"),
                row.get("symbol"),
                row.get("current_stage"),
                row.get("oi_pattern_code"),
                row.get("oi_pattern_label"),
                row.get("price_state_summary"),
                row.get("volume_state_summary"),
                row.get("oi_stage_age_minutes"),
                row.get("latest_cycle_ts"),
                row.get("decision_reason"),
            ])

    try:
        execute(
            """
            INSERT INTO telegram_stage3_alert_history(
                created_at_text,
                alert_key,
                exchange,
                symbol,
                current_stage,
                oi_pattern_code,
                oi_pattern_label,
                price_state_summary,
                volume_state_summary,
                oi_stage_age_minutes,
                latest_cycle_ts,
                decision_reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (alert_key) DO NOTHING
            """,
            (
                created_at_text,
                alert_key,
                row.get("exchange"),
                row.get("symbol"),
                row.get("current_stage"),
                row.get("oi_pattern_code"),
                row.get("oi_pattern_label"),
                row.get("price_state_summary"),
                row.get("volume_state_summary"),
                row.get("oi_stage_age_minutes"),
                row.get("latest_cycle_ts"),
                row.get("decision_reason"),
            ),
        )
    except Exception as exc:
        logger.warning("Не удалось записать историю stage3 alerts в БД: %s", exc)


    _prune_stage3_alert_history()

def _append_stage3_delivery_history(
    row: dict,
    alert_key: str,
    delivery: TelegramDeliveryResult,
) -> None:
    """JSONL-история расширенной доставки без миграции production DB."""
    try:
        RUNTIME_DIR.mkdir(exist_ok=True)
        path = RUNTIME_DIR / "telegram_stage3_delivery_history.jsonl"
        payload = {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "alert_key": alert_key,
            "exchange": row.get("exchange"),
            "symbol": row.get("symbol"),
            "message_id": delivery.message_id,
            "chat_id": delivery.chat_id,
            "delivered_at": delivery.delivered_at.isoformat() if delivery.delivered_at else None,
            "attempts": delivery.attempts,
            "delivery_mode": delivery.delivery_mode,
            "chart_requested": delivery.chart_requested,
            "chart_captured": delivery.chart_captured,
            "chart_capture_failure_reason": delivery.chart_capture_failure_reason,
            "chart_delivery_failure_reason": delivery.chart_delivery_failure_reason,
            "chart_requested_timeframes": list(delivery.chart_requested_timeframes),
            "chart_captured_timeframes": list(delivery.chart_captured_timeframes),
            "chart_timeframe_seconds": delivery.chart_timeframe_seconds or {},
            "chart_timeframe_verification_failures": list(
                delivery.chart_timeframe_verification_failures
            ),
            "chart_capture_seconds": delivery.chart_capture_seconds,
            "chart_send_seconds": delivery.chart_send_seconds,
            "total_delivery_seconds": delivery.total_delivery_seconds,
            "media_alerts": list(delivery.media_alerts),
        }
        with _csv_lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        log(f"stage3 delivery history write failed: {exc}")


def _stage3_media_metric_template() -> dict:
    return {
        "media_albums_sent": 0,
        "media_photos_sent": 0,
        "media_text_fallbacks": 0,
        "media_capture_errors": 0,
        "media_delivery_errors": 0,
        "media_charts_requested": 0,
        "media_charts_captured": 0,
        "media_chart_capture_seconds_total": 0.0,
        "media_chart_send_seconds_total": 0.0,
        "media_total_delivery_seconds_total": 0.0,
        "media_alerts": [],
    }


def _accumulate_stage3_media_metrics(metrics: dict, delivery: TelegramDeliveryResult) -> None:
    if delivery.delivery_mode.startswith("album"):
        metrics["media_albums_sent"] += 1
    elif delivery.delivery_mode.startswith("photo"):
        metrics["media_photos_sent"] += 1
    if delivery.delivery_mode == "text":
        metrics["media_text_fallbacks"] += 1
    if delivery.chart_capture_failure_reason:
        metrics["media_capture_errors"] += 1
    if delivery.chart_delivery_failure_reason:
        metrics["media_delivery_errors"] += 1
    if delivery.chart_requested:
        metrics["media_charts_requested"] += len(delivery.chart_requested_timeframes)
    if delivery.chart_captured:
        metrics["media_charts_captured"] += len(delivery.chart_captured_timeframes)
    metrics["media_chart_capture_seconds_total"] += float(delivery.chart_capture_seconds or 0.0)
    metrics["media_chart_send_seconds_total"] += float(delivery.chart_send_seconds or 0.0)
    metrics["media_total_delivery_seconds_total"] += float(delivery.total_delivery_seconds or 0.0)
    for alert in delivery.media_alerts:
        if alert not in metrics["media_alerts"]:
            metrics["media_alerts"].append(alert)


def _build_stage3_alert_text(r: dict) -> str:
    symbol = r.get("symbol")
    ex = r.get("exchange")
    transition_ts = r.get("stage3_transition_ts") or r.get("latest_cycle_ts")
    transition_reason = r.get("stage3_transition_reason") or r.get("phase_reason")
    ex_windows = _load_window_rows(str(symbol), str(ex), as_of_ts=transition_ts)
    history_rows = _collect_transition_history(str(symbol), str(ex), as_of_ts=transition_ts)
    metric_windows = _latest_metric_windows(str(symbol), str(ex), as_of_ts=transition_ts)
    if not ex_windows:
        ex_windows = _load_window_rows(str(symbol), str(ex))
    if not metric_windows:
        metric_windows = _latest_metric_windows(str(symbol), str(ex))
    return _build_coin_message(
        r,
        ex_windows,
        history_rows,
        metric_windows,
        title="🥇 NEW STAGE 3",
        transition_ts=transition_ts,
        transition_reason=transition_reason,
    )
def _stage3_price_snapshot_at_unlock(exchange: str, symbol: str, transition_ts, unlock_cycle_ts) -> dict:
    """Read latest source-native closed PRICE windows available at the frozen volume anchor."""
    rows = _safe_rows("""
        SELECT window_code, ts_close, open_value, close_value
        FROM aggregate_windows_history
        WHERE metric='PRICE'
          AND exchange=%s AND symbol=%s
          AND window_code IN ('30м','1ч')
          AND ts_close <= %s
          AND ts_close >= %s - INTERVAL '5 minutes'
          AND ts_close > %s
        ORDER BY window_code, ts_close DESC
    """, (exchange, symbol, unlock_cycle_ts, unlock_cycle_ts, transition_ts))
    snapshot = build_stage3_price_snapshot(
        rows or [],
        transition_ts=transition_ts,
        volume_unlock_cycle_ts=unlock_cycle_ts,
    )
    return snapshot


def check_stage3_alerts() -> dict:
    alerted = _read_stage3_alerted_keys()
    try:
        max_new_per_cycle = max(0, int(os.getenv("STAGE3_ALERTS_MAX_NEW_PER_CYCLE", "2") or "2"))
    except Exception:
        max_new_per_cycle = 2
    try:
        low_reserve_max_new_per_cycle = max(
            0,
            int(os.getenv("STAGE3_ALERTS_LOW_RESERVE_MAX_NEW_PER_CYCLE", "1") or "1"),
        )
    except Exception:
        low_reserve_max_new_per_cycle = 1

    budget_state = _stage3_cycle_budget_state()
    adaptive_limit_applied = False
    if budget_state["is_thin_reserve"] and low_reserve_max_new_per_cycle:
        max_new_per_cycle = min(max_new_per_cycle, low_reserve_max_new_per_cycle) if max_new_per_cycle else low_reserve_max_new_per_cycle
        adaptive_limit_applied = True

    rows = _safe_rows("""
        SELECT
            c.*,
            th.cycle_ts AS stage3_transition_ts,
            th.reason AS stage3_transition_reason,
            phase_obs.oi_1h AS stage3_oi_1h_class,
            phase_obs.cycle_ts AS stage3_oi_1h_cycle_ts,
            price_obs.price_30m_class AS stage3_price_30m_class,
            price_obs.price_1h_class AS stage3_price_1h_class,
            price_obs.cycle_ts AS stage3_price_cycle_ts,
            EXTRACT(EPOCH FROM (NOW() - COALESCE(th.cycle_ts, c.latest_cycle_ts))) / 60.0 AS stage3_transition_age_minutes
        FROM core_state_v2 c
        LEFT JOIN LATERAL (
            SELECT cycle_ts, reason
            FROM transition_history_v2 th
            WHERE th.exchange = c.exchange
              AND th.symbol = c.symbol
              AND th.to_stage = 3
            ORDER BY th.cycle_ts DESC, th.created_at DESC
            LIMIT 1
        ) th ON TRUE
        LEFT JOIN LATERAL (
            SELECT p.cycle_ts, p.oi_1h
            FROM phase_decision_observations p
            WHERE p.exchange=c.exchange
              AND p.symbol=c.symbol
              AND p.cycle_ts=c.latest_cycle_ts
            ORDER BY p.id DESC
            LIMIT 1
        ) phase_obs ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                cycle_ts,
                MAX(price_direction) FILTER (WHERE window_code='30м') AS price_30m_class,
                MAX(price_direction) FILTER (WHERE window_code='1ч') AS price_1h_class
            FROM window_state_v2 w
            WHERE w.exchange=c.exchange
              AND w.symbol=c.symbol
              AND w.cycle_ts=c.latest_cycle_ts
              AND w.window_code IN ('30м','1ч')
            GROUP BY cycle_ts
        ) price_obs ON TRUE
        WHERE c.current_stage = 3
        ORDER BY COALESCE(th.cycle_ts, c.latest_cycle_ts) ASC
        LIMIT 2000
    """)

    # One current transition per pair stays queued until the canonical phase exits 3.
    queue_candidates = []
    universe_decisions = {}
    price_data_incidents = []
    for row in rows:
        transition_ts = row.get("stage3_transition_ts")
        exchange = str(row.get("exchange") or "").upper()
        symbol = str(row.get("symbol") or "").upper()
        if not transition_ts or not exchange or not symbol:
            continue
        universe_decision = _asset_universe_decide(exchange, symbol)
        universe_decisions[(exchange, symbol)] = universe_decision
        state = _db_quote_turnover_state(symbol, transition_ts, exchange)
        allowed_by_volume, _ = _db_quote_turnover_gate(state)
        decision = evaluate_stage3_volume_candidate(
            current_stage=3,
            ready=bool(state.get("ready")),
            growth_4h_pct=state.get("growth_4h_pct"),
            observed_at=state.get("source_cycle_ts") if allowed_by_volume else None,
            previous_status=state.get("existing_queue_status"),
            volume_unlocked_at=state.get("first_volume_unlocked_at"),
        )
        volume_unlock_cycle_ts = state.get("first_volume_unlock_cycle_ts")
        if (
            state.get("first_volume_unlocked_at") is None
            and decision["volume_unlocked_at"] is not None
        ):
            # First crossing this cycle: use the exact closed-window anchor. Later
            # checks must keep using the anchor persisted with the first snapshot.
            volume_unlock_cycle_ts = state.get("latest_ts_close")
        source_cycle_ts = state.get("source_cycle_ts")
        observation_snapshot = _stage3_volume_observation_snapshot(state)
        oi_1h_class = str(row.get("stage3_oi_1h_class") or "").lower() or None
        oi_cycle_ts = row.get("stage3_oi_1h_cycle_ts")
        if observation_snapshot is None:
            observation_snapshot = {}
        observation_snapshot["oi_1h_class"] = oi_1h_class
        observation_snapshot["oi_cycle_ts"] = oi_cycle_ts.isoformat() if oi_cycle_ts else None
        price_snapshot = {}
        validate_price_gate = should_validate_stage3_price_gate(
            universe_allowed=universe_decision.allowed,
            candidate_status=decision["status"],
            previous_queue_status=state.get("existing_queue_status"),
        )
        # Pairs excluded by the asset universe never become Telegram candidates;
        # do not run candidate-only price validation or raise data incidents for them.
        if validate_price_gate and decision["volume_unlocked_at"] is not None:
            if volume_unlock_cycle_ts is None:
                price_snapshot = {
                    "price_30m_cycle_ts": None,
                    "price_1h_cycle_ts": None,
                    "price_30m_class": None,
                    "price_1h_class": None,
                    "price_veto_anchor_ts": None,
                    "price_data_error": "missing_volume_unlock_anchor",
                }
            else:
                price_snapshot = _stage3_price_snapshot_at_unlock(
                    exchange, symbol, transition_ts, volume_unlock_cycle_ts
                )
        price_30m_class = str(price_snapshot.get("price_30m_class") or "").lower() or None
        price_1h_class = str(price_snapshot.get("price_1h_class") or "").lower() or None
        price_30m_cycle_ts = price_snapshot.get("price_30m_cycle_ts")
        price_1h_cycle_ts = price_snapshot.get("price_1h_cycle_ts")
        if not price_snapshot:
            price_30m_class = str(row.get("stage3_price_30m_class") or "").lower() or None
            price_1h_class = str(row.get("stage3_price_1h_class") or "").lower() or None
            price_30m_cycle_ts = row.get("stage3_price_cycle_ts")
            price_1h_cycle_ts = price_30m_cycle_ts
        snapshot_for_observation = dict(price_snapshot)
        for key in ("price_30m_cycle_ts", "price_1h_cycle_ts", "price_veto_anchor_ts"):
            value = snapshot_for_observation.get(key)
            snapshot_for_observation[key] = value.isoformat() if hasattr(value, "isoformat") else value
        observation_snapshot.update(snapshot_for_observation)
        observation_snapshot["price_30m_class"] = price_30m_class
        observation_snapshot["price_1h_class"] = price_1h_class
        observation_snapshot["price_30m_cycle_ts"] = (
            price_30m_cycle_ts.isoformat() if hasattr(price_30m_cycle_ts, "isoformat") else price_30m_cycle_ts
        )
        observation_snapshot["price_1h_cycle_ts"] = (
            price_1h_cycle_ts.isoformat() if hasattr(price_1h_cycle_ts, "isoformat") else price_1h_cycle_ts
        )
        observation_snapshot["price_cycle_ts"] = observation_snapshot["price_30m_cycle_ts"]
        # OI veto is a Telegram-queue gate only: a fresh phase observation must
        # post-date this Stage-3 transition and be no later than first volume unlock.
        oi_decline_before_unlock = (
            oi_1h_class in {"weak_down", "strong_down"}
            and oi_cycle_ts is not None
            and oi_cycle_ts > transition_ts
            and (
                decision["volume_unlocked_at"] is None
                or oi_cycle_ts <= decision["volume_unlocked_at"]
            )
        )
        price_veto_reason = (
            stage3_price_veto_reason(
                price_30m_class=price_30m_class,
                price_1h_class=price_1h_class,
                price_30m_cycle_ts=price_30m_cycle_ts,
                price_1h_cycle_ts=price_1h_cycle_ts,
                transition_ts=transition_ts,
                volume_unlocked_at=decision["volume_unlocked_at"],
                volume_unlock_cycle_ts=volume_unlock_cycle_ts,
            )
            if validate_price_gate
            else None
        )
        # Freeze the first qualifying evidence for delivery; keep each cycle separately below.
        volume_snapshot = observation_snapshot if decision["volume_unlocked_at"] is not None else None
        queue_status = decision["status"] if universe_decision.allowed else "blocked_universe"
        block_reason = None if universe_decision.allowed else str(universe_decision.reason)
        if price_snapshot.get("price_data_error"):
            incident = {
                "exchange": exchange,
                "symbol": symbol,
                "reason": price_snapshot["price_data_error"],
                "volume_unlock_cycle_ts": str(volume_unlock_cycle_ts),
            }
            price_data_incidents.append(incident)
            log(
                "CRITICAL stage3 price data missing at volume unlock: "
                f"{exchange}:{symbol} missing={incident['reason']} "
                f"anchor={incident['volume_unlock_cycle_ts']}"
            )
        if oi_decline_before_unlock:
            queue_status = "invalidated_oi1h"
            block_reason = "blocked:oi_1h_decline_before_volume"
        elif price_veto_reason:
            queue_status = "invalidated_price"
            block_reason = price_veto_reason
        queue_candidates.append({
            "exchange": exchange,
            "symbol": symbol,
            "transition_ts": transition_ts,
            "observed_at": source_cycle_ts or row.get("latest_cycle_ts") or transition_ts,
            "source": state.get("source"),
            "source_symbol": state.get("symbol"),
            "ready": bool(state.get("ready")),
            "gate_status": decision["status"],
            "delivery_block_reason": block_reason,
            "observation_snapshot": observation_snapshot,
            "status": queue_status,
            "volume_unlocked_at": decision["volume_unlocked_at"],
            "growth_4h_pct": state.get("growth_4h_pct"),
            "quality_reason": state.get("quality_reason"),
            "volume_snapshot": volume_snapshot,
            "oi_1h_class": oi_1h_class,
            "oi_cycle_ts": oi_cycle_ts,
        })
    queue_state = sync_stage3_volume_queue(queue_candidates)
    queued = queue_state.get("candidates") or {}

    sent = 0
    already_active = 0
    observations_total = 0
    delivery_failed = 0
    new_signals: list[dict] = []
    media_metrics = _stage3_media_metric_template()
    limit_reached = False
    universe_filtered = 0
    volume_waiting = 0
    oi_filtered = 0
    price_filtered = 0

    for row in rows:
        if max_new_per_cycle and sent >= max_new_per_cycle:
            limit_reached = True
            break
        transition_ts = row.get("stage3_transition_ts")
        if not transition_ts:
            log(
                "stage3 alert skipped: missing canonical 2->3 transition "
                f"{row.get('exchange')} {row.get('symbol')}"
            )
            continue
        observations_total += 1
        exchange = str(row.get("exchange") or "").upper()
        symbol = str(row.get("symbol") or "").upper()
        key = "|".join([exchange, symbol, str(transition_ts)])
        queue_record = queued.get((exchange, symbol)) or {}
        queue_status = str(queue_record.get("status") or "")
        if key in alerted or queue_status == "sent":
            already_active += 1
            if key in alerted and queue_status != "sent":
                mark_stage3_volume_queue_sent(exchange, symbol, transition_ts)
            continue

        universe_decision = universe_decisions.get((exchange, symbol))
        if universe_decision is None:
            universe_decision = _asset_universe_decide(exchange, symbol)
        if queue_status == "invalidated_oi1h":
            oi_filtered += 1
            log(
                "stage3 alert candidate invalidated by OI 1h before volume unlock: "
                f"{key} oi={queue_record.get('oi_1h_class')} "
                f"cycle={queue_record.get('oi_cycle_ts')}"
            )
            continue
        if queue_status == "invalidated_price":
            price_filtered += 1
            snapshot = queue_record.get("volume_snapshot") or {}
            log(
                "stage3 alert candidate invalidated by price at first volume unlock: "
                f"{key} price30m={snapshot.get('price_30m_class')} "
                f"price1h={snapshot.get('price_1h_class')} "
                f"cycle={snapshot.get('price_cycle_ts')}"
            )
            continue
        if not universe_decision.allowed:
            mark_stage3_volume_queue_blocked(exchange, symbol, transition_ts, str(universe_decision.reason))
            universe_filtered += 1
            log(f"stage3 alert filtered by asset universe: {key} reason={universe_decision.reason}")
            continue
        if queue_status != "unlocked" or not queue_record.get("volume_snapshot"):
            volume_waiting += 1
            log(
                "stage3 alert waiting for DB 4h volume: "
                f"{key} status={queue_status or 'missing'} "
                f"growth={queue_record.get('growth_4h_pct')} "
                f"quality={queue_record.get('quality_reason')}"
            )
            continue

        enriched = _enrich_stage3_decision_snapshot(row)
        enriched["volume_snapshot"] = queue_record.get("volume_snapshot")
        enriched["volume_unlocked_at"] = queue_record.get("volume_unlocked_at")
        delivery = send_stage3_alert_result(
            enriched,
            _build_stage3_alert_text(enriched),
            _main_keyboard(),
            parse_mode="HTML",
            to_group=True,
        )
        _accumulate_stage3_media_metrics(media_metrics, delivery)
        if not delivery.ok:
            log(f"stage3 alert delivery failed: {key}")
            delivery_failed += 1
            continue

        mark_stage3_volume_queue_sent(exchange, symbol, transition_ts)
        _append_stage3_alert_history(enriched, key)
        _append_stage3_delivery_history(enriched, key, delivery)
        sent += 1
        new_signals.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "transition_ts": str(transition_ts),
                "alert_key": key,
                "message_id": delivery.message_id,
                "delivered_at": delivery.delivered_at.isoformat() if delivery.delivered_at else None,
                "delivery_mode": delivery.delivery_mode,
                "chart_requested": delivery.chart_requested,
                "chart_captured": delivery.chart_captured,
                "chart_capture_failure_reason": delivery.chart_capture_failure_reason,
                "chart_delivery_failure_reason": delivery.chart_delivery_failure_reason,
                "chart_requested_timeframes": list(delivery.chart_requested_timeframes),
                "chart_captured_timeframes": list(delivery.chart_captured_timeframes),
                "chart_timeframe_verification_failures": list(delivery.chart_timeframe_verification_failures),
                "chart_capture_seconds": delivery.chart_capture_seconds,
                "chart_send_seconds": delivery.chart_send_seconds,
                "total_delivery_seconds": delivery.total_delivery_seconds,
                "media_alerts": list(delivery.media_alerts),
            }
        )

    waiting_rows = _safe_rows("""
        SELECT COUNT(*) AS cnt FROM core_state_v2
        WHERE current_stage IN (1, 2)
          AND strpos(COALESCE(transition_permission, ''), 'ждем_') = 1
    """)
    waiting_confirmation = int((waiting_rows[0] or {}).get("cnt", 0) or 0) if waiting_rows else 0
    return {
        "sent_count": sent,
        "signal_observations_total": observations_total,
        "signals_already_active": already_active,
        "signals_waiting_confirmation": waiting_confirmation,
        "signals_waiting_volume": volume_waiting,
        "signals_repeat_on_cooldown": 0,
        "delivery_failed": delivery_failed,
        "signals_filtered_by_universe": universe_filtered,
        "signals_filtered_by_volume": volume_waiting,
        "signals_filtered_by_oi_1h": oi_filtered,
        "signals_filtered_by_price_at_volume_unlock": price_filtered,
        "stage3_price_data_incident_count": len(price_data_incidents),
        "stage3_price_data_incidents": price_data_incidents[:20],
        "stage3_volume_queue": {
            "waiting": int(queue_state.get("waiting", 0) or 0),
            "unlocked": int(queue_state.get("unlocked", 0) or 0),
            "oi_invalidated_72h": int(queue_state.get("oi_invalidated_72h", 0) or 0),
            "oi_filtered_this_cycle": oi_filtered,
            "price_invalidated_72h": int(queue_state.get("price_invalidated_72h", 0) or 0),
            "price_filtered_this_cycle": price_filtered,
            "sent_this_cycle": sent,
            "observations_72h": int(queue_state.get("observations_72h", 0) or 0),
        },
        "new_signals": new_signals,
        "stage3_alerts_limit_reached": limit_reached,
        "stage3_alerts_max_new_per_cycle": max_new_per_cycle,
        "stage3_alerts_adaptive_limit_applied": adaptive_limit_applied,
        "stage3_alerts_cycle_latency_class": budget_state["cycle_latency_class"],
        "stage3_alerts_cycle_reserve_pct": budget_state["cycle_reserve_pct"],
        **media_metrics,
    }
def _archive_index_path() -> Path:
    return Path("archive") / "manifests" / "archive_index.json"


def _read_archive_index() -> list[dict]:
    p = _archive_index_path()
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _latest_archive_entry(kind: str | None = None) -> dict | None:
    rows = _read_archive_index()
    if kind:
        rows = [r for r in rows if r.get("type") == kind]
    return rows[-1] if rows else None


def _build_archive_text() -> str:
    rows = _read_archive_index()
    if not rows:
        return "🗂 Archive\n\nПока manifest пуст."

    last = rows[-1]
    backups = [r for r in rows if r.get("type") == "backup_db"]
    last_backup = backups[-1] if backups else None

    lines = [
        "🗂 Archive",
        "",
        f"entries={len(rows)}",
    ]

    if last_backup:
        lines += [
            "",
            "Last DB backup:",
            f"status={last_backup.get('status')}",
            f"file={last_backup.get('file')}",
            f"size_mb={last_backup.get('size_mb')}",
            f"duration_sec={last_backup.get('duration_sec')}",
            f"finished_at={last_backup.get('finished_at')}",
        ]

    lines += [
        "",
        "Commands:",
        "/backup_db",
    ]

    return "\n".join(lines)


def _run_backup_db() -> str:
    lock = Path("archive") / "locks" / "heavy_job.lock"

    if lock.exists():
        return f"⛔ Heavy job уже идёт\n\nlock={lock}"

    send_message("⏳ DB backup started\n\nЭто heavy job. Runtime не трогаем.")

    started = time.time()

    proc = subprocess.run(
        ["python3", "backup_db.py"],
        capture_output=True,
        text=True,
        timeout=1800,
    )

    duration = round(time.time() - started, 2)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "")[-3000:]
        return f"❌ DB backup failed\n\nduration_sec={duration}\n\n{err}"

    last = _latest_archive_entry("backup_db")
    if not last:
        return f"⚠️ Backup finished, но manifest не найден\nduration_sec={duration}"

    return "\n".join([
        "✅ DB backup complete",
        "",
        f"file={last.get('file')}",
        f"size_mb={last.get('size_mb')}",
        f"duration_sec={last.get('duration_sec')}",
        f"total_runtime_sec={duration}",
        f"finished_at={last.get('finished_at')}",
    ])


def _handle(text: str, chat_id=None) -> None:
    text = text.strip()

    if not _is_admin_chat(chat_id):
        log(f"telegram unauthorized chat ignored: chat_id={chat_id} text={text[:120]}")
        return

    if text in {"/start", "/help", "❓ Помощь"}:
        send_message(_build_help_text(), _main_keyboard())

    elif text in {"/panel", "/control"}:
        send_message(_build_control_panel_text(), _main_keyboard())

    elif text in {"/system_health", "🩺 Система"}:
        send_message(_build_system_health_text(), _main_keyboard())

    elif text in {"/phases", "⚙️ Фазы"}:
        send_message(_build_phases_text(), _phases_keyboard())

    elif text in {"/phase1", "🥉 Фаза 1"}:
        total = _phase_total_count(1)
        rows = _phase_rows(1, 0, _PHASE_PAGE_SIZE)
        send_message(_phase_page_text(1, 0, total), _phase_list_keyboard(rows, 1, 0, total) or _phases_keyboard())

    elif text in {"/phase2", "🥈 Фаза 2"}:
        total = _phase_total_count(2)
        rows = _phase_rows(2, 0, _PHASE_PAGE_SIZE)
        send_message(_phase_page_text(2, 0, total), _phase_list_keyboard(rows, 2, 0, total) or _phases_keyboard())

    elif text in {"/phase3", "🥇 Фаза 3"}:
        total = _phase_total_count(3)
        rows = _phase_rows(3, 0, _PHASE_PAGE_SIZE)
        send_message(_phase_page_text(3, 0, total), _phase_list_keyboard(rows, 3, 0, total) or _phases_keyboard())

    elif text in {"/top_oi", "📈 ТОП OI", "📈 Топ ОИ"}:
        send_message("⛔ TOP OI убран из рабочего Telegram UX.", _main_keyboard())


    elif text in {"⬅️ Назад", "/menu"}:
        send_message("Главное меню", _main_keyboard())

    elif text == "🧯 Сброс фазы 3":
        send_message("Сброс фазы 3", _stage3_reset_keyboard())

    elif text == "Сбросить по тикеру":
        send_message("Формат: /reset_stage3 SYMBOL reason", _stage3_reset_keyboard())

    elif text in {"Сбросить все", "🧨 Сброс всех Ф3"}:
        _reset_stage3_all_now("mass_reset_from_button", chat_id)

    elif text in {"15м", "30м", "4ч", "24ч"} or text.startswith("/top_oi ") or text in {
        "🏆 BINANCE /30м", "🏆 BINANCE /30m",
        "🏆 BYBIT /30м", "🏆 BYBIT /30m",
        "🏆 BINANCE /4ч", "🏆 BINANCE /4h",
        "🏆 BYBIT /4ч", "🏆 BYBIT /4h",
        "🏆 BINANCE /24ч", "🏆 BINANCE /24h",
        "🏆 BYBIT /24ч", "🏆 BYBIT /24h",
    }:
        send_message("⛔ TOP OI убран из рабочего Telegram UX.", _main_keyboard())

    elif text in {"🧱 Карантин", "🧱 Quarantine", "/quarantine"}:
        send_message("⛔ Карантин убран из рабочего Telegram UX.", _main_keyboard())

    elif text in {"/coin", "🪙 Coin"}:
        send_message("Формат: /coin BTCUSDT", _main_keyboard())

    elif text.startswith("/coin "):
        symbol = text.split(maxsplit=1)[1].upper().strip()
        core_rows = _safe_rows("""
            SELECT *
            FROM core_state_v2
            WHERE symbol = %s
            ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
            LIMIT 2
        """, (symbol,))
        best_exchange = str((core_rows[0] or {}).get("exchange") or "").upper() if core_rows else None
        send_message(_build_coin_card(symbol, best_exchange), _main_keyboard())

    elif text.startswith("/feedback ") or text == "/cancel_feedback":
        send_message("⛔ Обратная связь убрана из Telegram. Разбираем сигналы напрямую в рабочем чате.", _main_keyboard())

    elif text == "/debug_cases":
        send_message(_build_debug_cases_text(), _main_keyboard())

    elif text.startswith("/debug_cases "):
        send_message(_build_debug_cases_text(text.split(maxsplit=1)[1]), _main_keyboard())

    elif text == "/post_stage":
        send_message(_build_post_stage_text(), _main_keyboard())

    elif text.startswith("/post_stage "):
        send_message(_build_post_stage_text(text.split(maxsplit=1)[1]), _main_keyboard())

    elif text == "/review":
        send_message("Формат: /review BTCUSDT", _main_keyboard())

    elif text.startswith("/review "):
        symbol = text.split(maxsplit=1)[1].upper().strip()
        send_message(_build_review_case_text(symbol), _main_keyboard())

    elif text in {"/downloads", "⬇️ Скачать"} or text.startswith("/download "):
        send_message("⛔ Скачать убрано из рабочего Telegram UX.", _main_keyboard())

    elif text == "/backup_db":
        send_message(_run_backup_db(), _main_keyboard())

    elif text == "/archive":
        send_message(_build_archive_text(), _main_keyboard())

    elif text in {"/quarantine", "🧱 Quarantine"} or text.startswith("/quarantine "):
        send_message("⛔ Карантин убран из рабочего Telegram UX.", _main_keyboard())

    elif text.startswith("/reset_stage3 "):
        _handle_stage3_reset(text, chat_id)

    elif text.startswith("/confirm_reset "):
        _handle_confirm_reset(text, chat_id)

    elif text == "/cancel_reset":
        _handle_cancel_reset(text, chat_id)

    elif text == "/ping":
        send_message("pong", _main_keyboard())

    elif text == "/manifest":
        send_document(ПАПКА_ДАННЫХ / "storage_manifest.txt", "manifest")

    elif text == "/audit_report":
        send_document(ПАПКА_ДАННЫХ / "audit_report.txt", "audit report")

    elif text == "/research_report":
        send_document(ПАПКА_ДАННЫХ / "research_report.txt", "research report")

    elif text == "/timing":
        send_document(ПАПКА_ДАННЫХ / "runtime_timing_report.txt", "runtime timing report")

    elif text == "/health":
        send_message(_build_health_text(), _main_keyboard())

    elif text == "/failures":
        send_document(ПАПКА_ДАННЫХ / "request_failure_report.csv", "request failures")

    elif text == "/gaps":
        send_document(ПАПКА_ДАННЫХ / "gap_report.csv", "gap report")

    elif text == "/active_universe":
        send_document(ПАПКА_ДАННЫХ / "active_universe_report.csv", "active universe")

    elif text == "/export_quick":
        send_message("⛔ Rebuild через Telegram отключён.", _main_keyboard())

    elif text in {"/export_research_7d", "/export_research_30d"}:
        send_message("⛔ Heavy export убран из рабочего Telegram UX.", _main_keyboard())


def _reset() -> None:
    global _offset

    try:
        requests.get(
            f"{BASE}/deleteWebhook",
            params={"drop_pending_updates": "true"},
            timeout=20,
        )
    except Exception as exc:
        log(f"deleteWebhook error: {exc}")

    _offset = 0


def _loop() -> None:
    global _offset

    time.sleep(6)
    _reset()

    while True:
        try:
            response = requests.get(
                f"{BASE}/getUpdates",
                params={"timeout": 30, "offset": _offset + 1, "allowed_updates": ["message", "callback_query", "channel_post"]},
                timeout=40,
            )
            response.raise_for_status()

            for item in response.json().get("result", []):
                _offset = item["update_id"]

                if _capture_signal_channel_id(item.get("channel_post")):
                    continue

                message = item.get("message", {}) or {}
                text = message.get("text", "")
                chat_id = (message.get("chat", {}) or {}).get("id")
                callback = item.get("callback_query", {}) or {}
                callback_id = callback.get("id")
                callback_data = str(callback.get("data") or "").strip()
                callback_message = callback.get("message", {}) or {}
                callback_chat_id = (callback_message.get("chat", {}) or {}).get("id")

                if text:
                    _handle(text.strip(), chat_id)
                elif callback_data:
                    _handle_callback(callback_data, callback_id, callback_chat_id)
        except Exception as exc:
            log(f"telegram polling error: {exc}")
            time.sleep(10 if "409" in str(exc) else 5)


def _handle_callback(data: str, callback_id: str | None, chat_id=None) -> None:
    if not _is_admin_chat(chat_id):
        _answer_callback_query(str(callback_id or ""), "⛔ Admin-only")
        return

    action, _, payload = data.partition(":")
    payload = payload.strip()

    if action == "coin" and payload:
        _answer_callback_query(str(callback_id or ""), f"Карточка {payload}")
        core_rows = _safe_rows("""
            SELECT *
            FROM core_state_v2
            WHERE symbol = %s
            ORDER BY current_stage DESC, stage_age_minutes DESC, latest_cycle_ts DESC, exchange
            LIMIT 2
        """, (payload.upper(),))
        best_exchange = str((core_rows[0] or {}).get("exchange") or "").upper() if core_rows else None
        send_message(_build_coin_card(payload, best_exchange), _main_keyboard())
        return

    if action == "coinx" and payload:
        exchange, _, symbol = payload.partition(":")
        symbol = symbol.upper().strip()
        exchange = exchange.upper().strip()
        _answer_callback_query(str(callback_id or ""), f"Карточка {symbol}")
        send_message(_build_coin_card(symbol, exchange), _main_keyboard())
        return

    if action == "phases":
        _answer_callback_query(str(callback_id or ""), "Список фаз")
        send_message(_build_phases_text(), _phases_keyboard())
        return

    if action == "phmore" and payload:
        try:
            phase_text, offset_text = payload.split(":", 1)
            phase = int(phase_text)
            offset = max(0, int(offset_text))
        except Exception:
            _answer_callback_query(str(callback_id or ""), "Не удалось открыть следующую страницу")
            return
        total = _phase_total_count(phase)
        rows = _phase_rows(phase, offset, _PHASE_PAGE_SIZE)
        if not rows:
            _answer_callback_query(str(callback_id or ""), "Дальше монет нет")
            return
        _answer_callback_query(str(callback_id or ""), f"Фаза {phase}: еще")
        send_message(
            _phase_page_text(phase, offset, total),
            _phase_list_keyboard(rows, phase, offset, total) or _phases_keyboard(),
        )
        return

    if action == "fb" and payload:
        _answer_callback_query(str(callback_id or ""), "Обратная связь убрана")
        send_message("⛔ Обратная связь убрана из Telegram. Разбираем сигналы напрямую в рабочем чате.", _main_keyboard())
        return

    if action == "fbcancel":
        _answer_callback_query(str(callback_id or ""), "Обратная связь убрана")
        return

    if action == "post" and payload:
        _answer_callback_query(str(callback_id or ""), f"Post-stage {payload}")
        send_message(_build_post_stage_text(payload), _main_keyboard())
        return

    if action == "dbg" and payload:
        _answer_callback_query(str(callback_id or ""), f"Debug {payload}")
        send_message(_build_debug_cases_text(payload), _main_keyboard())
        return

    if action == "rv" and payload:
        _answer_callback_query(str(callback_id or ""), f"Review {payload}")
        send_message(_build_review_case_text(payload), _main_keyboard())
        return

    if action == "rst" and payload:
        _answer_callback_query(str(callback_id or ""), f"Ручной сброс {payload}")
        _save_pending_reset(payload.upper(), "button_reset_stage3")
        send_message(
            "\n".join([
                "⚠️ Подготовлен ручной сброс фазы 3",
                "",
                f"Монета: {payload.upper()}",
                "Причина: button_reset_stage3",
                "",
                f"Подтвердить: /confirm_reset {payload.upper()}",
                "Отменить: /cancel_reset",
            ]),
            _stage3_reset_actions_keyboard(symbol=payload.upper()),
        )
        return

    if action == "rstall":
        _answer_callback_query(str(callback_id or ""), "Снимаю все Фазы 3")
        _reset_stage3_all_now("mass_reset_from_inline_button", chat_id)
        return

    if action == "rstconfirm" and payload:
        _answer_callback_query(str(callback_id or ""), f"Подтвердить сброс {payload}")
        _handle_confirm_reset(f"/confirm_reset {payload}", chat_id)
        return

    if action == "rstcancel":
        _answer_callback_query(str(callback_id or ""), "Сброс отменён")
        _handle_cancel_reset("/cancel_reset", chat_id)
        return

    _answer_callback_query(str(callback_id or ""), "Неизвестное действие")


def start_polling() -> None:
    global _polling_started

    if _polling_started or not TELEGRAM_BOT_TOKEN:
        return

    if not _acquire_polling_lock():
        return

    _polling_started = True
    try:
        _start_signal_channel_worker()
        threading.Thread(target=_loop, daemon=True).start()
    except Exception:
        _polling_started = False
        _release_polling_lock()
        raise
