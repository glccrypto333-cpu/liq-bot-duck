from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone
from db import fetch_rows, replace_table
from metrics import абсолютное_расхождение, класс_надёжности

def rebuild_consistency() -> None:
    rows = fetch_rows("""
        SELECT окно, ts_open, exchange, symbol, oi_изменение_pct
        FROM oi_агрегаты
        WHERE ts_open >= NOW() - interval '2 days'
        ORDER BY exchange, symbol, окно, ts_open
    """)

    grouped = defaultdict(lambda: defaultdict(list))
    for r in rows:
        grouped[(r["symbol"], r["окно"])][r["exchange"]].append(r)

    out = []
    now = datetime.now(timezone.utc)
    symbols = sorted(set(k[0] for k in grouped.keys()))

    for symbol in symbols:
        by_window = {}
        есть_две_биржи = False
        окон_готово = 0

        for окно in ["15м", "30м", "1ч", "4ч"]:
            exmap = grouped.get((symbol, окно), {})
            row_bybit = exmap.get("BYBIT", [])
            row_binance = exmap.get("BINANCE", [])
            latest_bybit = row_bybit[-1] if row_bybit else None
            latest_binance = row_binance[-1] if row_binance else None
            if latest_bybit and latest_binance:
                есть_две_биржи = True
            canonical = latest_bybit or latest_binance
            if canonical:
                окон_готово += 1
            by_window[окно] = {
                "наклон": canonical["oi_изменение_pct"] if canonical else None,
                "расхождение": абсолютное_расхождение(
                    latest_bybit["oi_изменение_pct"] if latest_bybit else None,
                    latest_binance["oi_изменение_pct"] if latest_binance else None
                ),
                "bybit": latest_bybit is not None,
                "binance": latest_binance is not None,
                "exchange": canonical["exchange"] if canonical else None,
            }

        наклон_15м = by_window["15м"]["наклон"]
        наклон_30м = by_window["30м"]["наклон"]
        наклон_1ч = by_window["1ч"]["наклон"]
        наклон_4ч = by_window["4ч"]["наклон"]

        соглас_15м = абсолютное_расхождение(наклон_15м, наклон_4ч)
        соглас_30м = абсолютное_расхождение(наклон_30м, наклон_4ч)
        соглас_1ч = абсолютное_расхождение(наклон_1ч, наклон_4ч)

        расх_15м = by_window["15м"]["расхождение"]
        расх_30м = by_window["30м"]["расхождение"]
        расх_1ч = by_window["1ч"]["расхождение"]
        расх_4ч = by_window["4ч"]["расхождение"]

        valid_межбиржа = [x for x in [расх_15м, расх_30м, расх_1ч, расх_4ч] if x is not None]
        valid_межокна = [x for x in [соглас_15м, соглас_30м, соглас_1ч] if x is not None]

        среднее_межокна = sum(valid_межокна) / len(valid_межокна) if valid_межокна else 2.0
        штраф_межокна = min(100.0, среднее_межокна * 100.0)

        источник_основной = "BYBIT" if any(by_window[w]["bybit"] for w in by_window) else "BINANCE"
        источник_подтверждения = "BINANCE" if источник_основной == "BYBIT" and any(by_window[w]["binance"] for w in by_window) else ("BYBIT" if источник_основной == "BINANCE" and any(by_window[w]["bybit"] for w in by_window) else "нет")

        if есть_две_биржи and valid_межбиржа:
            среднее_межбиржа = sum(valid_межбиржа) / len(valid_межбиржа)
            шум_api = max(0.0, 100.0 - среднее_межбиржа * 100.0)
            тип_состояния = "полная_сверка"
            причина = "есть Bybit и Binance"
        else:
            шум_api = max(0.0, 85.0 - штраф_межокна * 0.25)
            тип_состояния = "одна_биржа"
            причина = "нет второй биржи или нет полной пары окон"

        if штраф_межокна > 35 and окон_готово >= 3:
            тип_состояния = "слабая_межоконная_согласованность"
            причина = "окна 15м / 30м / 1ч / 4ч конфликтуют"

        потери_точек = min(100.0, окон_готово / 4.0 * 100.0)
        оценка_качества = max(0.0, min(100.0, шум_api * 0.55 + потери_точек * 0.25 + max(0.0, 100.0 - штраф_межокна) * 0.20))
        класс = класс_надёжности(оценка_качества, тип_состояния)
        exchange = by_window["4ч"]["exchange"] or by_window["1ч"]["exchange"] or by_window["30м"]["exchange"] or by_window["15м"]["exchange"] or "UNKNOWN"

        out.append((
            now, exchange, symbol, источник_основной, источник_подтверждения, тип_состояния,
            наклон_15м, наклон_30м, наклон_1ч, наклон_4ч,
            соглас_15м, соглас_30м, соглас_1ч,
            расх_15м, расх_30м, расх_1ч, расх_4ч,
            шум_api, потери_точек, оценка_качества, класс, причина
        ))

    replace_table("oi_сверка", out)
