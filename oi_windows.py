from __future__ import annotations

from datetime import datetime, timedelta


FIVE_MINUTES = timedelta(minutes=5)


def binance_oi_bucket_close(timestamp: datetime) -> datetime:
    """Map an irregular Binance OI snapshot time to its enclosing 5m close."""
    bucket_minute = (timestamp.minute // 5) * 5
    bucket_start = timestamp.replace(
        minute=bucket_minute,
        second=0,
        microsecond=0,
    )
    if timestamp == bucket_start:
        return bucket_start
    return bucket_start + FIVE_MINUTES
