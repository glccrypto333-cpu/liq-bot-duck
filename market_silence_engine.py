
from __future__ import annotations

from datetime import datetime, timezone
from db import fetch, execute, replace_market_silence
from logger import log


def _f(v, d=0.0):
    try:
        return float(v) if v is not None else d
    except Exception:
        return d


def _stage(row):
    oi = _f(row.get("oi_delta_pct"))
    price = _f(row.get("price_delta_pct"))
    volume = _f(row.get("volume_delta_pct"))
    width = _f(row.get("range_width_pct"))
    state = row.get("market_state")
    bad = row.get("invalid_reason")

    if state == "invalid_data" or bad:
        return 0, "нет данных", 0, "данные плохие"

    quiet_oi = abs(oi) <= 0.35
    quiet_price = abs(price) <= 0.45 and width <= 1.20
    quiet_volume = abs(volume) <= 25.0

    if quiet_oi and quiet_price and quiet_volume:
        return 0, "тишина", 85, "ОИ тихий, цена в боковике, объемы спокойные"

    if 0.35 < oi <= 1.00 and abs(price) <= 0.70 and width <= 1.80:
        return 1, "наблюдение", 60, "ОИ начинает расти, цена еще сдержана"

    if oi > 1.00 and (abs(price) > 0.70 or abs(volume) > 25.0):
        return 2, "возня", 70, "ОИ растет, цена или объемы начали двигаться"

    if oi >= 1.50 and abs(price) >= 1.00 and abs(volume) >= 40.0:
        return 3, "подтверждение", 80, "ОИ, цена и объемы двигаются вместе"

    return 0, "сухой рынок", 40, "явной структуры нет"


def rebuild_market_silence() -> int:
    execute("""
        CREATE TABLE IF NOT EXISTS market_silence(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            stage INTEGER NOT NULL,
            stage_name TEXT NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            reason TEXT NOT NULL,
            oi_delta_pct DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,
            market_state TEXT,
            invalid_reason TEXT
        )
    """)

    rows = fetch("""
        SELECT *
        FROM market_research
        WHERE ts_close >= (
            SELECT MAX(ts_close) - '24 hours'::interval
            FROM market_research
        )
        ORDER BY exchange, symbol, timeframe, ts_close
    """)

    now = datetime.now(timezone.utc)
    out = []

    for r in rows:
        stage, name, score, reason = _stage(r)
        out.append((
            now,
            r["ts_close"],
            r["exchange"],
            r["symbol"],
            r["timeframe"],
            stage,
            name,
            score,
            reason,
            r.get("oi_delta_pct"),
            r.get("price_delta_pct"),
            r.get("volume_delta_pct"),
            r.get("range_width_pct"),
            r.get("market_state"),
            r.get("invalid_reason"),
        ))

    replace_market_silence(out)

    counts = {}
    for r in out:
        counts[r[6]] = counts.get(r[6], 0) + 1

    log("market silence rebuilt: rows={} {}".format(len(out), counts))
    return len(out)
