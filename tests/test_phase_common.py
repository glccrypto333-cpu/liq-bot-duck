from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from phase_common import pullback_ratio_from_points, retention_ratio_from_points, smoothness_ratio_from_points


def test_retention_ratio_is_clamped_to_unit_interval() -> None:
    points = [12778.96, 12778.96, 12772.86, 12762.58, 12737.44, 12771.58, 12755.98, 12742.34, 12726.16, 12717.82, 12679.02, 12681.46, 12615.88, 12447.16, 12481.74, 12487.86, 12478.44, 12474.8, 12470.58, 12449.36, 12494.3, 12795.14, 12776.02, 12752.82, 12113.08, 12119.09, 12099.72, 12072.8, 12050.21, 12021.34, 12000.94, 11994.72, 11983.98, 11941.54, 11920.47, 11925.52, 11923.22, 11901.1, 11876.18, 11816.0, 11793.82, 11831.16, 11771.6, 11690.37, 11665.78, 11886.6, 11833.38, 11886.17, 12123.74]
    ratio = retention_ratio_from_points(points)
    assert 0.0 <= ratio <= 1.0
    assert ratio == 0.0


def test_pullback_ratio_is_clamped_to_unit_interval() -> None:
    points = [390.34, 390.34, 418.72, 421.68, 421.16, 397.64, 395.12, 397.04, 395.64, 393.76, 348.04, 400.78, 404.27]
    ratio = pullback_ratio_from_points(points)
    assert 0.0 <= ratio <= 1.0
    assert ratio == 1.0


def test_smoothness_ratio_stays_in_unit_interval() -> None:
    points = [12290095.2, 12290095.2, 12365190.2, 12328232.4, 12392719.8, 12006878.0, 12123926.9, 12252996.2, 12923627.3, 13076291.6, 13247243.9, 13530668.9, 13398648.7]
    ratio = smoothness_ratio_from_points(points)
    assert 0.0 <= ratio <= 1.0


def test_smoothness_rewards_stepwise_growth_more_than_choppy_rebound() -> None:
    healthy_stair = [10.0, 11.0, 10.5, 11.5, 11.0, 12.0, 11.5, 12.5, 12.0, 13.0]
    choppy_rebound = [10.0, 13.0, 11.0, 14.0, 10.0, 12.0]

    healthy_ratio = smoothness_ratio_from_points(healthy_stair)
    choppy_ratio = smoothness_ratio_from_points(choppy_rebound)

    assert healthy_ratio > 0.55
    assert choppy_ratio < 0.40
    assert healthy_ratio > choppy_ratio
