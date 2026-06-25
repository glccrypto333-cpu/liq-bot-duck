from __future__ import annotations

import math
import os
from datetime import timedelta

from logger import log


def _validate_raw_rows(name: str, rows: list[tuple], expected_len: int) -> None:
    if not rows:
        raise RuntimeError(f"raw_validate failed: {name} rows empty")

    for i, row in enumerate(rows[:20]):
        if len(row) != expected_len:
            raise RuntimeError(
                f"raw_validate failed: {name} row_len={len(row)} expected={expected_len} index={i}"
            )

        ts_open, ts_close, exchange, symbol = row[0], row[1], row[2], row[3]

        if not ts_open or not ts_close or not exchange or not symbol:
            raise RuntimeError(f"raw_validate failed: {name} null key fields index={i}")

        if ts_close != ts_open + timedelta(minutes=5):
            raise RuntimeError(f"raw_validate failed: {name} bad ts_close index={i} symbol={symbol}")

        for value in row[4:]:
            if value is None:
                raise RuntimeError(f"raw_validate failed: {name} null value index={i} symbol={symbol}")


def _format_missing_details(missing_keys: set[tuple[str, str]]) -> str:
    if not missing_keys:
        return "missing_exchange_counts=none missing_sample=none"

    exchange_counts: dict[str, int] = {}
    for exchange, _symbol in missing_keys:
        exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1

    exchange_summary = ",".join(
        f"{exchange}:{exchange_counts[exchange]}"
        for exchange in sorted(exchange_counts)
    )
    sample = ",".join(
        f"{exchange}:{symbol}"
        for exchange, symbol in sorted(missing_keys)[:8]
    )
    return (
        f"missing_exchange_counts={exchange_summary} "
        f"missing_sample={sample}"
    )


def validate_collected_raw(batch: dict, symbols_bybit: list[str], symbols_binance: list[str]) -> dict:
    if not batch:
        raise RuntimeError("raw_validate failed: empty collect batch")

    oi_rows = batch.get("oi_rows") or []
    price_rows = batch.get("price_rows") or []
    volume_rows = batch.get("volume_rows") or []
    failures = batch.get("failures") or []

    if failures:
        raise RuntimeError(f"raw_validate failed: request failures count={len(failures)}")

    _validate_raw_rows("oi", oi_rows, 8)
    _validate_raw_rows("price", price_rows, 8)
    _validate_raw_rows("volume", volume_rows, 5)

    expected = {("BYBIT", s) for s in symbols_bybit} | {("BINANCE", s) for s in symbols_binance}

    oi_keys = {(r[2], r[3]) for r in oi_rows}
    price_keys = {(r[2], r[3]) for r in price_rows}
    volume_keys = {(r[2], r[3]) for r in volume_rows}

    missing_oi = expected - oi_keys
    missing_price = expected - price_keys
    missing_volume = expected - volume_keys

    tolerated_price_volume_missing = int(os.getenv("RAW_VALIDATE_TOLERATED_PRICE_VOLUME_MISSING", "0"))
    tolerated_price_volume_missing_pct = float(os.getenv("RAW_VALIDATE_TOLERATED_PRICE_VOLUME_MISSING_PCT", "0"))
    tolerated_price_volume_missing_dynamic = math.ceil(len(expected) * tolerated_price_volume_missing_pct)
    tolerated_price_volume_missing_limit = max(
        tolerated_price_volume_missing,
        tolerated_price_volume_missing_dynamic,
    )

    if missing_oi or missing_price or missing_volume:
        tolerated_partial = (
            not missing_oi
            and tolerated_price_volume_missing_limit > 0
            and missing_price == missing_volume
            and len(missing_price) <= tolerated_price_volume_missing_limit
        )
        if tolerated_partial:
            log(
                "raw_validate tolerated partial collect: "
                f"missing_price={len(missing_price)} "
                f"missing_volume={len(missing_volume)} "
                f"{_format_missing_details(missing_price)} "
                f"tolerance_abs={tolerated_price_volume_missing} "
                f"tolerance_pct={tolerated_price_volume_missing_pct:.4f} "
                f"tolerance_limit={tolerated_price_volume_missing_limit}"
            )
        else:
            raise RuntimeError(
                "raw_validate failed: partial collect "
                f"missing_oi={len(missing_oi)} "
                f"missing_price={len(missing_price)} "
                f"missing_volume={len(missing_volume)} "
                f"{_format_missing_details(missing_price if missing_price == missing_volume else missing_price | missing_volume)} "
                f"tolerance_limit={tolerated_price_volume_missing_limit}"
            )

    log(
        f"raw_validate ok: oi={len(oi_rows)} "
        f"price={len(price_rows)} "
        f"volume={len(volume_rows)} "
        f"symbols={len(expected)}"
    )

    return batch
