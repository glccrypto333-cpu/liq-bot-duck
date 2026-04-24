from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from db import fetch, replace_validation, replace_integrity, replace_coverage, replace_gaps
from metrics import изменение_в_процентах, abs_diff, rel_diff_pct
from logger import log

WINDOWS = {"15м": 3, "30м": 6, "1ч": 12, "4ч": 48}
FIVE_MINUTES = timedelta(minutes=5)
FIVE_MINUTES_SECONDS = 300


def _status(metric: str, candles: int, needed: int, drift: float | None) -> str:
    if candles < needed:
        return "недостаточно_данных"
    if drift is None:
        return "ошибка_агрегации_бота"
    if metric == "OI" and drift >= 0.50:
        return "расхождение_выше_допуска"
    if metric == "PRICE" and drift >= 0.05:
        return "расхождение_выше_допуска"
    if metric == "VOLUME" and drift >= 3.0:
        return "расхождение_выше_допуска"
    return "валидно"


def _groups(rows):
    g = defaultdict(list)
    for r in rows:
        g[(r["exchange"], r["symbol"])].append(r)
    for k in g:
        g[k].sort(key=lambda x: x["ts_open"])
    return g


def _bot_map():
    return {(r["metric"], r["timeframe"], r["exchange"], r["symbol"], r["ts_close"]): r for r in fetch("SELECT * FROM bot_aggregates")}


def _is_contiguous_5m(chunk) -> bool:
    if not chunk:
        return False

    for prev, current in zip(chunk, chunk[1:]):
        if current["ts_open"] - prev["ts_open"] != FIVE_MINUTES:
            return False

    return True


def _quality_status(coverage_pct: float, invalid_timestamps: int) -> str:
    if invalid_timestamps > 0:
        return "invalid_timestamps"
    if coverage_pct >= 97.0:
        return "ok"
    if coverage_pct >= 90.0:
        return "warning"
    return "critical"


def rebuild_validation_audit() -> int:
    now = datetime.now(timezone.utc)
    bot = _bot_map()
    out = []
    skipped_non_contiguous = 0

    oi_rows = fetch("SELECT ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close FROM oi_5m_сырые WHERE ts_close <= NOW() - interval '30 seconds' ORDER BY exchange, symbol, ts_open")
    for (exchange, symbol), items in _groups(oi_rows).items():
        for tf, n in WINDOWS.items():
            if len(items) < n:
                continue
            for i in range(n - 1, len(items)):
                b = items[i-n+1:i+1]
                if not _is_contiguous_5m(b):
                    skipped_non_contiguous += 1
                    continue
                ts_close = b[-1]["ts_close"]
                audit_open = b[0]["oi_open"]
                audit_close = b[-1]["oi_close"]
                audit_delta = изменение_в_процентах(audit_open, audit_close)
                bot_r = bot.get(("OI", tf, exchange, symbol, ts_close))
                bot_delta = bot_r["delta_pct"] if bot_r else None
                drift = abs_diff(bot_delta, audit_delta)
                out.append((now, "OI", tf, ts_close, exchange, symbol, bot_r["open_value"] if bot_r else None, audit_open, bot_r["close_value"] if bot_r else None, audit_close, bot_delta, audit_delta, None, None, None, None, drift, len(b), _status("OI", len(b), n, drift)))

    price_rows = fetch("SELECT ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close FROM price_5m_сырые WHERE ts_close <= NOW() - interval '30 seconds' ORDER BY exchange, symbol, ts_open")
    for (exchange, symbol), items in _groups(price_rows).items():
        for tf, n in WINDOWS.items():
            if len(items) < n:
                continue
            for i in range(n - 1, len(items)):
                b = items[i-n+1:i+1]
                if not _is_contiguous_5m(b):
                    skipped_non_contiguous += 1
                    continue
                ts_close = b[-1]["ts_close"]
                audit_open = b[0]["price_open"]
                audit_close = b[-1]["price_close"]
                audit_delta = изменение_в_процентах(audit_open, audit_close)
                bot_r = bot.get(("PRICE", tf, exchange, symbol, ts_close))
                bot_delta = bot_r["delta_pct"] if bot_r else None
                drift = abs_diff(bot_delta, audit_delta)
                out.append((now, "PRICE", tf, ts_close, exchange, symbol, bot_r["open_value"] if bot_r else None, audit_open, bot_r["close_value"] if bot_r else None, audit_close, bot_delta, audit_delta, None, None, None, None, drift, len(b), _status("PRICE", len(b), n, drift)))

    vol_rows = fetch("SELECT ts_open, ts_close, exchange, symbol, volume FROM volume_5m_сырые WHERE ts_close <= NOW() - interval '30 seconds' ORDER BY exchange, symbol, ts_open")
    for (exchange, symbol), items in _groups(vol_rows).items():
        for tf, n in WINDOWS.items():
            if len(items) < n:
                continue
            for i in range(n - 1, len(items)):
                b = items[i-n+1:i+1]
                if not _is_contiguous_5m(b):
                    skipped_non_contiguous += 1
                    continue
                ts_close = b[-1]["ts_close"]
                vals = [x["volume"] for x in b]
                audit_sum = sum(vals)
                audit_avg = audit_sum / len(vals)
                bot_r = bot.get(("VOLUME", tf, exchange, symbol, ts_close))
                bot_sum = bot_r["sum_value"] if bot_r else None
                bot_avg = bot_r["avg_value"] if bot_r else None
                drift = rel_diff_pct(bot_sum, audit_sum)
                out.append((now, "VOLUME", tf, ts_close, exchange, symbol, None, None, None, None, None, None, bot_sum, audit_sum, bot_avg, audit_avg, drift, len(b), _status("VOLUME", len(b), n, drift)))

    replace_validation(out)
    log(
        f"validation audit rebuilt: bot_map={len(bot)} "
        f"audit_rows={len(out)} skipped_non_contiguous={skipped_non_contiguous}"
    )
    return len(out)


def rebuild_integrity() -> int:
    now = datetime.now(timezone.utc)
    out = []
    for metric, table in [("OI", "oi_5m_сырые"), ("PRICE", "price_5m_сырые"), ("VOLUME", "volume_5m_сырые")]:
        rows = fetch(f"SELECT ts_open, ts_close, exchange, symbol FROM {table} ORDER BY exchange, symbol, ts_open")
        for (exchange, symbol), items in _groups(rows).items():
            missing = 0
            invalid = 0
            prev = None
            for x in items:
                if x["ts_close"] != x["ts_open"] + FIVE_MINUTES:
                    invalid += 1
                if prev is not None:
                    diff = (x["ts_open"] - prev).total_seconds()
                    if diff <= 0:
                        invalid += 1
                    elif diff > FIVE_MINUTES_SECONDS:
                        missing += int(diff // FIVE_MINUTES_SECONDS) - 1
                prev = x["ts_open"]
            score = max(0.0, min(100.0, 100 - missing * 0.5 - invalid * 10))
            out.append((now, metric, exchange, symbol, len(items), missing, invalid, score))
    replace_integrity(out)
    log(f"integrity rebuilt: rows={len(out)}")
    return len(out)


def rebuild_coverage_and_gaps() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    coverage_rows = []
    gap_rows = []

    for metric, table in [("OI", "oi_5m_сырые"), ("PRICE", "price_5m_сырые"), ("VOLUME", "volume_5m_сырые")]:
        rows = fetch(f"SELECT ts_open, ts_close, exchange, symbol FROM {table} ORDER BY exchange, symbol, ts_open")

        for (exchange, symbol), items in _groups(rows).items():
            actual = len(items)
            invalid = 0
            missing = 0

            first_ts = items[0]["ts_open"] if items else None
            last_ts = items[-1]["ts_open"] if items else None

            prev = None
            for x in items:
                if x["ts_close"] != x["ts_open"] + FIVE_MINUTES:
                    invalid += 1

                if prev is not None:
                    diff = (x["ts_open"] - prev).total_seconds()

                    if diff <= 0:
                        invalid += 1
                    elif diff > FIVE_MINUTES_SECONDS:
                        miss = int(diff // FIVE_MINUTES_SECONDS) - 1
                        missing += miss
                        gap_rows.append((
                            now,
                            metric,
                            exchange,
                            symbol,
                            prev + FIVE_MINUTES,
                            x["ts_open"] - FIVE_MINUTES,
                            miss,
                            diff / 60.0,
                        ))

                prev = x["ts_open"]

            if first_ts and last_ts:
                expected = int((last_ts - first_ts).total_seconds() // FIVE_MINUTES_SECONDS) + 1
            else:
                expected = 0

            coverage_pct = (actual / expected * 100.0) if expected > 0 else 0.0
            missing_pct = max(0.0, 100.0 - coverage_pct)
            status = _quality_status(coverage_pct, invalid)

            coverage_rows.append((
                now,
                metric,
                exchange,
                symbol,
                first_ts,
                last_ts,
                expected,
                actual,
                missing,
                coverage_pct,
                missing_pct,
                invalid,
                status,
            ))

    replace_coverage(coverage_rows)
    replace_gaps(gap_rows)
    log(f"coverage rebuilt: rows={len(coverage_rows)} gaps={len(gap_rows)}")
    return len(coverage_rows), len(gap_rows)


def rebuild_all() -> int:
    rebuild_integrity()
    rebuild_coverage_and_gaps()
    return rebuild_validation_audit()
