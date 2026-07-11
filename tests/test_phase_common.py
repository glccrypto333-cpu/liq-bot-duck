from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from phase_common import classify_oi_slope


def test_oi_slope_classification_remains_canonical_after_form_removal() -> None:
    assert classify_oi_slope("1ч", 1.01) == "weak_up"
    assert classify_oi_slope("1ч", 1.04) == "good_up"
    assert classify_oi_slope("1ч", 1.06) == "strong_up"
