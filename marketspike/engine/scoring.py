import math
from typing import Optional, Tuple

VOL_WEIGHT = 0.6
SPREAD_WEIGHT = 0.4
MAX_COMPONENT = 4.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def score_components(v_ratio: Optional[float], spread_z: float) -> Tuple[float, float]:
    """Return the weighted (vol_component, spread_component) contributions.

    This is the single implementation of the composite-score maths:
    `composite_score` sums these two, and `dominant_signal` compares them,
    so the two can never drift apart.
    """
    if v_ratio is None or v_ratio <= 0:
        vol_component = 0.0
    else:
        vol_component = _clamp(math.log(v_ratio, 2), 0.0, MAX_COMPONENT)
    spread_component = _clamp(spread_z / 2.0, 0.0, MAX_COMPONENT)
    return VOL_WEIGHT * vol_component, SPREAD_WEIGHT * spread_component


def composite_score(v_ratio: Optional[float], spread_z: float) -> float:
    """Combine realised volatility and quoted spread into a 0-4 score (§7.4).

    Two signals rather than one because they diverge: volatility can rise on
    thin genuine movement without spread widening, and spread can widen on
    liquidity withdrawal before price moves. Either alone yields false
    negatives.
    """
    vol_component, spread_component = score_components(v_ratio, spread_z)
    return vol_component + spread_component


def dominant_signal(v_ratio: Optional[float], spread_z: float) -> str:
    """Return which weighted contribution actually drove the score.

    "vol_ratio" when the volatility contribution is strictly larger,
    "spread" when the spread contribution is strictly larger, and "both"
    when they are equal (including the both-zero case). Used for honest
    causal attribution on regime transitions, rather than assuming
    direction alone tells you which signal fired.
    """
    vol_component, spread_component = score_components(v_ratio, spread_z)
    if vol_component > spread_component:
        return "vol_ratio"
    if spread_component > vol_component:
        return "spread"
    return "both"
