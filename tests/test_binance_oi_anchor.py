from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from aggregation_engine import _resolve_symbol_anchor_map_from_db


def test_latest_anchor_uses_canonical_binance_oi_close(monkeypatch) -> None:
    oi_snapshot_ts = datetime(2026, 10, 15, 15, 59, 59, tzinfo=timezone.utc)
    price_close = datetime(2026, 10, 15, 16, 0, tzinfo=timezone.utc)

    def fake_latest(table, *_args, **_kwargs):
        latest = oi_snapshot_ts if table == "oi_raw" else price_close
        return {("BINANCE", "TESTUSDT"): latest}

    monkeypatch.setattr("aggregation_engine._fetch_latest_ts_map", fake_latest)

    anchors = _resolve_symbol_anchor_map_from_db({"OI", "PRICE"}, True, price_close)

    assert anchors == {("BINANCE", "TESTUSDT"): price_close}
