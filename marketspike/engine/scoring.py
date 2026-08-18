import math
from typing import Optional

VOL_WEIGHT = 0.6
SPREAD_WEIGHT = 0.4
MAX_COMPONENT = 4.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def composite_score(v_ratio: Optional[float], spread_z: float) -> float:
    """Combine realised volatility and quoted spread into a 0-4 score (§7.4).

    Two signals rather than one because they diverge: volatility can rise on
    thin genuine movement without spread widening, and spread can widen on
    liquidity withdrawal before price moves. Either alone yields false
    negatives.
    """
    if v_ratio is None or v_ratio <= 0:
        vol_component = 0.0
    else:
        vol_component = _clamp(math.log(v_ratio, 2), 0.0, MAX_COMPONENT)
    spread_component = _clamp(spread_z / 2.0, 0.0, MAX_COMPONENT)
    return VOL_WEIGHT * vol_component + SPREAD_WEIGHT * spread_component
