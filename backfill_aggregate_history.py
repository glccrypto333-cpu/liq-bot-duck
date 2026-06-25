from __future__ import annotations

import json

from aggregation_engine import backfill_aggregate_history


if __name__ == "__main__":
    result = backfill_aggregate_history()
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
