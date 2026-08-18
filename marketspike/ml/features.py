import math
from dataclasses import dataclass
from typing import Dict, List

from marketspike.risk.slippage import FEATURE_ORDER


class LeakageError(ValueError):
    """Raised when a target observation is not strictly after decision time.

    This is the one defect that would make the model look excellent and be
    worthless (predicting the past), so it is enforced structurally -- via
    the separate `history`/`target` arguments to `build_sample` plus this
    explicit check -- rather than by convention (spec Task 19).
    """


@dataclass(frozen=True)
class TickRow:
    ts_ns: int
    mid: float
    spread_bps: float
    book_imbalance: float
    quote_rate_hz: float
    v_ratio: float
    spread_z: float
    abs_return_5s: float
    latency_ms: float
    regime: str


@dataclass(frozen=True)
class Sample:
    t_ns: int
    symbol: str
    features: Dict[str, float]
    delta_ms: float
    direction: int
    cost_bps: float
    regime: str


def _features_at(decision: TickRow, event_clock, symbol: str) -> Dict[str, float]:
    """Build the feature vector from `decision` alone.

    Every value here is drawn from the decision-time row (or from the event
    clock evaluated *at* decision time) -- never from the target -- which is
    what keeps this function leakage-safe by construction.
    """
    signed_secs = event_clock.signed_seconds(decision.ts_ns, symbol)
    phase = event_clock.phase(decision.ts_ns, symbol)
    values = {
        "log_v_ratio": math.log(max(decision.v_ratio, 1e-9)),
        "spread_z": decision.spread_z,
        "log_spread_bps": math.log(max(decision.spread_bps, 1e-6)),
        "log_latency_ms": math.log(max(decision.latency_ms, 1e-3)),
        "quote_rate_hz": decision.quote_rate_hz,
        "book_imbalance": decision.book_imbalance,
        "signed_secs_to_event": signed_secs,
        "in_event_window": 1.0 if phase == "EVENT_WINDOW" else 0.0,
        "abs_return_5s": decision.abs_return_5s,
    }
    return {name: values[name] for name in FEATURE_ORDER}


def build_sample(
    history: List[TickRow],
    target: TickRow,
    delta_ms: float,
    direction: int,
    event_clock,
    symbol: str,
) -> Sample:
    """Implementation shortfall against arrival price.

    A trader decides at `t` looking at `mid_t`; the order reaches the venue
    at `t + delta`, and the cost paid is a function of what the market did
    over that interval. `history` (features) and `target` (the label) are
    separate arguments precisely so a caller cannot accidentally reach
    forward when constructing features -- the leakage guard is structural,
    not conventional.

    Decomposition: cost_bps = half_spread_bps + direction * drift_bps.
    - half_spread_bps is the cost of crossing the spread, taken at the
      target (that's the spread actually paid when the order arrives).
    - drift_bps = (target.mid - decision.mid) / decision.mid * 1e4 is the
      market's move over the interval, expressed relative to the decision
      mid (the reference price the trader was looking at).
    Both a buy (+1) and a sell (-1) are scored from the same observation:
    over a horizon of tens to low-hundreds of milliseconds there is no
    directional edge to assume, so summing the two and halving recovers
    exactly the half-spread term -- that is the invariant
    `test_direction_symmetry_recovers_the_half_spread` checks, and it is
    what makes the empirical p50 ~= half-spread result meaningful rather
    than coincidental.
    """
    decision = history[-1]
    if target.ts_ns <= decision.ts_ns:
        raise LeakageError(
            "target ts {0} must be strictly after decision ts {1}".format(
                target.ts_ns, decision.ts_ns
            )
        )

    half_spread_bps = target.spread_bps / 2.0
    drift_bps = (target.mid - decision.mid) / decision.mid * 1e4
    cost_bps = half_spread_bps + direction * drift_bps

    return Sample(
        t_ns=decision.ts_ns,
        symbol=symbol,
        features=_features_at(decision, event_clock, symbol),
        delta_ms=delta_ms,
        direction=direction,
        cost_bps=cost_bps,
        regime=decision.regime,
    )


def build_dataset(
    rows: List[TickRow],
    delta_ms: float,
    event_clock,
    symbol: str,
) -> List[Sample]:
    """Emit both directions (+1 buy, -1 sell) per decision point.

    Runs a single forward pointer over `rows` rather than rescanning for
    each decision point, so this is O(n) rather than O(n^2) -- required
    because it runs over hundreds of thousands of recorded ticks. `rows`
    must be sorted ascending by `ts_ns` (as recorded tick history is).
    """
    delta_ns = int(delta_ms * 1_000_000)
    samples: List[Sample] = []
    target_index = 0

    for index, decision in enumerate(rows):
        wanted_ts = decision.ts_ns + delta_ns
        if target_index < index:
            target_index = index
        while target_index < len(rows) and rows[target_index].ts_ns < wanted_ts:
            target_index += 1
        if target_index >= len(rows):
            # No row this far forward exists for this decision point (or any
            # later one, since rows are ascending) -- nothing more to emit.
            break
        target = rows[target_index]
        if target.ts_ns <= decision.ts_ns:
            continue
        for direction in (1, -1):
            samples.append(
                build_sample([decision], target, delta_ms, direction, event_clock, symbol)
            )
    return samples
