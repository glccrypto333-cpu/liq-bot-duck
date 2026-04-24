from __future__ import annotations
from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv
from config import ПАПКА_ДАННЫХ, APP_VERSION, ДНЕЙ_ХРАНЕНИЯ, ДНЕЙ_ЭКСПОРТА, ЛИМИТ_СЫРЫХ_5М, ЛИМИТ_АГРЕГАТОВ, ЛИМИТ_СВЕРКИ, ЛИМИТ_АУДИТА
from db import fetch_rows

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

def rebuild_exports() -> None:
    ts = _now()
    since = datetime.now(timezone.utc) - timedelta(days=ДНЕЙ_ЭКСПОРТА)

    oi_raw = fetch_rows(f"SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close FROM oi_5m_сырые WHERE ts_open >= %s ORDER BY ts_open DESC LIMIT {ЛИМИТ_СЫРЫХ_5М}", (since,))
    price_raw = fetch_rows(f"SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close FROM price_5m_сырые WHERE ts_open >= %s ORDER BY ts_open DESC LIMIT {ЛИМИТ_СЫРЫХ_5М}", (since,))
    volume_raw = fetch_rows(f"SELECT ts_open, ts_close, exchange, symbol, volume FROM volume_5m_сырые WHERE ts_open >= %s ORDER BY ts_open DESC LIMIT {ЛИМИТ_СЫРЫХ_5М}", (since,))
    oi_aggs = fetch_rows(f"SELECT окно, ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close, oi_изменение_pct FROM oi_агрегаты WHERE ts_open >= %s ORDER BY ts_open DESC LIMIT {ЛИМИТ_АГРЕГАТОВ}", (since,))
    consistency = fetch_rows(f"SELECT * FROM oi_сверка ORDER BY оценка_качества DESC NULLS LAST, symbol LIMIT {ЛИМИТ_СВЕРКИ}")
    raw_integrity = fetch_rows(f"SELECT * FROM raw_integrity_report ORDER BY integrity_score ASC, metric, symbol LIMIT {ЛИМИТ_АУДИТА}")
    audit_oi = fetch_rows(f"SELECT * FROM аудит_ои ORDER BY drift_oi_delta_pct DESC NULLS LAST, symbol LIMIT {ЛИМИТ_АУДИТА}")
    audit_price = fetch_rows(f"SELECT * FROM аудит_цены ORDER BY drift_price_delta_pct DESC NULLS LAST, symbol LIMIT {ЛИМИТ_АУДИТА}")
    audit_volume = fetch_rows(f"SELECT * FROM аудит_объёма ORDER BY drift_volume_pct DESC NULLS LAST, symbol LIMIT {ЛИМИТ_АУДИТА}")

    _write_text(
        ПАПКА_ДАННЫХ / "storage_manifest.txt",
        (
            f"Mighty Duck {APP_VERSION}\n\n"
            f"generated_at: {ts}\n"
            f"retention_days: {ДНЕЙ_ХРАНЕНИЯ}\n"
            f"export_days: {ДНЕЙ_ЭКСПОРТА}\n\n"
            "available files:\n"
            "- сырые_5м_ои.csv\n"
            "- сырые_5м_цены.csv\n"
            "- сырые_5м_объёмы.csv\n"
            "- агрегаты_ои.csv\n"
            "- сверка_ои.csv\n"
            "- raw_integrity_report.csv\n"
            "- аудит_ои.csv\n"
            "- аудит_цены.csv\n"
            "- аудит_объёма.csv\n"
            "- отчет_по_аудиту.txt\n"
            "- debug_audit.txt\n"
            "- calculation_debug.txt\n"
        )
    )

    _write_csv(ПАПКА_ДАННЫХ / "сырые_5м_ои.csv", ["ts_open","ts_close","биржа","монета","oi_open","oi_high","oi_low","oi_close"], [[r["ts_open"], r["ts_close"], r["exchange"], r["symbol"], r["oi_open"], r["oi_high"], r["oi_low"], r["oi_close"]] for r in oi_raw])
    _write_csv(ПАПКА_ДАННЫХ / "сырые_5м_цены.csv", ["ts_open","ts_close","биржа","монета","price_open","price_high","price_low","price_close"], [[r["ts_open"], r["ts_close"], r["exchange"], r["symbol"], r["price_open"], r["price_high"], r["price_low"], r["price_close"]] for r in price_raw])
    _write_csv(ПАПКА_ДАННЫХ / "сырые_5м_объёмы.csv", ["ts_open","ts_close","биржа","монета","volume"], [[r["ts_open"], r["ts_close"], r["exchange"], r["symbol"], r["volume"]] for r in volume_raw])
    _write_csv(ПАПКА_ДАННЫХ / "агрегаты_ои.csv", ["окно","ts_open","ts_close","биржа","монета","oi_open","oi_high","oi_low","oi_close","oi_изменение_pct"], [[r["окно"], r["ts_open"], r["ts_close"], r["exchange"], r["symbol"], r["oi_open"], r["oi_high"], r["oi_low"], r["oi_close"], r["oi_изменение_pct"]] for r in oi_aggs])

    _write_csv(
        ПАПКА_ДАННЫХ / "сверка_ои.csv",
        ["монета","биржа","источник_основной","источник_подтверждения","тип_состояния","наклон_15м","наклон_30м","наклон_1ч","наклон_4ч","согласованность_15м_к_4ч","согласованность_30м_к_4ч","согласованность_1ч_к_4ч","расхождение_bybit_binance_15м","расхождение_bybit_binance_30м","расхождение_bybit_binance_1ч","расхождение_bybit_binance_4ч","шум_api","потери_точек","оценка_качества","класс_надёжности","причина_оценки"],
        [[r["symbol"], r["exchange"], r["источник_основной"], r["источник_подтверждения"], r["тип_состояния"], r["наклон_15м"], r["наклон_30м"], r["наклон_1ч"], r["наклон_4ч"], r["согласованность_15м_к_4ч"], r["согласованность_30м_к_4ч"], r["согласованность_1ч_к_4ч"], r["расхождение_bybit_binance_15м"], r["расхождение_bybit_binance_30м"], r["расхождение_bybit_binance_1ч"], r["расхождение_bybit_binance_4ч"], r["шум_api"], r["потери_точек"], r["оценка_качества"], r["класс_надёжности"], r["причина_оценки"]] for r in consistency]
    )

    _write_csv(ПАПКА_ДАННЫХ / "raw_integrity_report.csv", ["metric","exchange","symbol","duplicates_found","missing_candles","invalid_timestamps","empty_rows","integrity_score"], [[r["metric"], r["exchange"], r["symbol"], r["duplicates_found"], r["missing_candles"], r["invalid_timestamps"], r["empty_rows"], r["integrity_score"]] for r in raw_integrity])
    _write_csv(ПАПКА_ДАННЫХ / "аудит_ои.csv", ["symbol","exchange","timeframe","bot_oi_open","audit_oi_open","bot_oi_close","audit_oi_close","bot_oi_delta_pct","audit_oi_delta_pct","drift_oi_delta_pct","unique_candles","validation_status"], [[r["symbol"], r["exchange"], r["timeframe"], r["bot_oi_open"], r["audit_oi_open"], r["bot_oi_close"], r["audit_oi_close"], r["bot_oi_delta_pct"], r["audit_oi_delta_pct"], r["drift_oi_delta_pct"], r["unique_candles"], r["validation_status"]] for r in audit_oi])
    _write_csv(ПАПКА_ДАННЫХ / "аудит_цены.csv", ["symbol","exchange","timeframe","bot_price_open","audit_price_open","bot_price_close","audit_price_close","bot_price_delta_pct","audit_price_delta_pct","drift_price_delta_pct","unique_candles","validation_status"], [[r["symbol"], r["exchange"], r["timeframe"], r["bot_price_open"], r["audit_price_open"], r["bot_price_close"], r["audit_price_close"], r["bot_price_delta_pct"], r["audit_price_delta_pct"], r["drift_price_delta_pct"], r["unique_candles"], r["validation_status"]] for r in audit_price])
    _write_csv(ПАПКА_ДАННЫХ / "аудит_объёма.csv", ["symbol","exchange","timeframe","bot_volume_sum","audit_volume_sum","bot_volume_avg","audit_volume_avg","drift_volume_pct","unique_candles","validation_status"], [[r["symbol"], r["exchange"], r["timeframe"], r["bot_volume_sum"], r["audit_volume_sum"], r["bot_volume_avg"], r["audit_volume_avg"], r["drift_volume_pct"], r["unique_candles"], r["validation_status"]] for r in audit_volume])

    report_lines = [f"Mighty Duck {APP_VERSION}", f"generated_at: {ts}", "", "OI:"]
    report_lines.extend([f'{r["symbol"]} [{r["exchange"]}] {r["timeframe"]} drift={r["drift_oi_delta_pct"]} status={r["validation_status"]}' for r in audit_oi[:50]])
    report_lines.append("")
    report_lines.append("PRICE:")
    report_lines.extend([f'{r["symbol"]} [{r["exchange"]}] {r["timeframe"]} drift={r["drift_price_delta_pct"]} status={r["validation_status"]}' for r in audit_price[:50]])
    report_lines.append("")
    report_lines.append("VOLUME:")
    report_lines.extend([f'{r["symbol"]} [{r["exchange"]}] {r["timeframe"]} drift={r["drift_volume_pct"]} status={r["validation_status"]}' for r in audit_volume[:50]])
    _write_text(ПАПКА_ДАННЫХ / "отчет_по_аудиту.txt", "\n".join(report_lines))

    def _stat(lines, name, rows, key):
        vals = [r[key] for r in rows if r[key] is not None]
        lines.append(f"{name}: count={len(rows)} metric_count={len(vals)} max={max(vals) if vals else 'NA'} avg={sum(vals)/len(vals) if vals else 'NA'}")

    debug = [f"Mighty Duck {APP_VERSION}", f"generated_at: {ts}", "", "debug_audit:"]
    _stat(debug, "OI", audit_oi, "drift_oi_delta_pct")
    _stat(debug, "PRICE", audit_price, "drift_price_delta_pct")
    _stat(debug, "VOLUME", audit_volume, "drift_volume_pct")
    debug.append(f"raw_integrity_rows={len(raw_integrity)}")
    _write_text(ПАПКА_ДАННЫХ / "debug_audit.txt", "\n".join(debug))

    calc_debug = [f"Mighty Duck {APP_VERSION}", f"generated_at: {ts}", "", "calculation debug:", "источник истины = native 5m OI / price / volume", "бот не является источником истины", "аудит сравнивает данные бота с независимым пересчётом"]
    _write_text(ПАПКА_ДАННЫХ / "calculation_debug.txt", "\n".join(calc_debug))
