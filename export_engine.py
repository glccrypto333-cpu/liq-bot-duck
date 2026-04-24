from __future__ import annotations
from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv
import zipfile

from config import ПАПКА_ДАННЫХ, APP_VERSION, QUICK_EXPORT_CANDLES, RESEARCH_EXPORT_DAYS, RESEARCH_30D_EXPORT_DAYS
from db import fetch

def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")

def _zip(zip_path: Path, files: list[Path]) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in files:
            if f.exists():
                z.write(f, arcname=f.name)

def rebuild_exports(mode: str = "quick") -> Path:
    now = datetime.now(timezone.utc)

    if mode == "research_30d":
        since = now - timedelta(days=RESEARCH_30D_EXPORT_DAYS)
        suffix = "research_30d"
    elif mode == "research_7d":
        since = now - timedelta(days=RESEARCH_EXPORT_DAYS)
        suffix = "research_7d"
    else:
        since = now - timedelta(minutes=QUICK_EXPORT_CANDLES * 5)
        suffix = "quick"

    oi = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close
        FROM oi_5m_сырые
        WHERE ts_open >= %s
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    pr = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close
        FROM price_5m_сырые
        WHERE ts_open >= %s
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    vo = fetch("""
        SELECT ts_open, ts_close, exchange, symbol, volume
        FROM volume_5m_сырые
        WHERE ts_open >= %s
        ORDER BY exchange, symbol, ts_open
    """, (since,))

    oi_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in oi}
    price_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in pr}
    volume_map = {(r["exchange"], r["symbol"], r["ts_open"]): r for r in vo}

    keys = sorted(set(oi_map.keys()) | set(price_map.keys()) | set(volume_map.keys()), key=lambda x: (x[0], x[1], x[2]))

    raw_rows = []
    for key in keys:
        exchange, symbol, ts_open = key
        o = oi_map.get(key)
        p = price_map.get(key)
        v = volume_map.get(key)

        # canonical normalized close для всех бирж одинаковый:
        candle_close_norm = ts_open + timedelta(minutes=5)

        raw_rows.append([
            ts_open,
            candle_close_norm,
            candle_close_norm,
            exchange,
            symbol,
            o["oi_open"] if o else None,
            o["oi_high"] if o else None,
            o["oi_low"] if o else None,
            o["oi_close"] if o else None,
            p["price_open"] if p else None,
            p["price_high"] if p else None,
            p["price_low"] if p else None,
            p["price_close"] if p else None,
            v["volume"] if v else None,
        ])

    ag = fetch("""
        SELECT *
        FROM bot_aggregates
        WHERE ts_open >= %s
        ORDER BY metric, exchange, symbol, timeframe, ts_close
    """, (since,))

    au = fetch("""
        SELECT *
        FROM validation_audit
        WHERE ts_close >= %s
        ORDER BY metric, exchange, symbol, timeframe, ts_close
    """, (since,))

    integ = fetch("SELECT * FROM raw_integrity_report ORDER BY metric, exchange, symbol")

    raw_path = ПАПКА_ДАННЫХ / "raw_market_5m.csv"
    ag_path = ПАПКА_ДАННЫХ / "bot_aggregates.csv"
    audit_path = ПАПКА_ДАННЫХ / "validation_audit.csv"
    report_path = ПАПКА_ДАННЫХ / "audit_report.txt"
    manifest_path = ПАПКА_ДАННЫХ / "storage_manifest.txt"

    _write_csv(
        raw_path,
        [
            "ts_open",
            "ts_close",
            "candle_close_norm",
            "exchange",
            "symbol",
            "oi_open",
            "oi_high",
            "oi_low",
            "oi_close",
            "price_open",
            "price_high",
            "price_low",
            "price_close",
            "volume",
        ],
        raw_rows,
    )

    _write_csv(
        ag_path,
        [
            "metric",
            "timeframe",
            "ts_open",
            "ts_close",
            "exchange",
            "symbol",
            "open_value",
            "high_value",
            "low_value",
            "close_value",
            "sum_value",
            "avg_value",
            "delta_pct",
            "unique_candles",
        ],
        [
            [
                r["metric"],
                r["timeframe"],
                r["ts_open"],
                r["ts_close"],
                r["exchange"],
                r["symbol"],
                r["open_value"],
                r["high_value"],
                r["low_value"],
                r["close_value"],
                r["sum_value"],
                r["avg_value"],
                r["delta_pct"],
                r["unique_candles"],
            ]
            for r in ag
        ],
    )

    _write_csv(
        audit_path,
        [
            "calculated_at",
            "metric",
            "timeframe",
            "ts_close",
            "exchange",
            "symbol",
            "bot_open",
            "audit_open",
            "bot_close",
            "audit_close",
            "bot_delta_pct",
            "audit_delta_pct",
            "bot_sum",
            "audit_sum",
            "bot_avg",
            "audit_avg",
            "drift",
            "unique_candles",
            "validation_status",
        ],
        [
            [
                r["calculated_at"],
                r["metric"],
                r["timeframe"],
                r["ts_close"],
                r["exchange"],
                r["symbol"],
                r["bot_open"],
                r["audit_open"],
                r["bot_close"],
                r["audit_close"],
                r["bot_delta_pct"],
                r["audit_delta_pct"],
                r["bot_sum"],
                r["audit_sum"],
                r["bot_avg"],
                r["audit_avg"],
                r["drift"],
                r["unique_candles"],
                r["validation_status"],
            ]
            for r in au
        ],
    )

    bad = [r for r in au if r["validation_status"] != "валидно"]

    lines = [
        f"Mighty Duck {APP_VERSION}",
        f"generated_at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"mode: {mode}",
        "",
        "timestamp_normalization: active",
        "canonical_close: ts_open + 5m",
        "",
        f"raw_rows: {len(raw_rows)}",
        f"bot_aggregates_rows: {len(ag)}",
        f"validation_audit_rows: {len(au)}",
        f"invalid_rows: {len(bad)}",
        f"integrity_rows: {len(integ)}",
        "",
        "Top invalid:",
    ]

    for r in bad[:100]:
        lines.append(f'{r["metric"]} {r["symbol"]} {r["exchange"]} {r["timeframe"]} drift={r["drift"]} status={r["validation_status"]}')

    _write_text(report_path, "\n".join(lines))

    _write_text(
        manifest_path,
        (
            f"Mighty Duck {APP_VERSION}\n"
            f"mode={mode}\n"
            "timestamp_normalization=active\n"
            "canonical_close=ts_open_plus_5m\n"
            "files=raw_market_5m.csv, bot_aggregates.csv, validation_audit.csv, audit_report.txt\n"
        ),
    )

    zip_path = ПАПКА_ДАННЫХ / f"market_research_bundle_{suffix}.zip"
    _zip(zip_path, [raw_path, ag_path, audit_path, report_path, manifest_path])
    return zip_path
