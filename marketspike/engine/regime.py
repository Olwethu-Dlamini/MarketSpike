from dataclasses import dataclass
from typing import Dict, List, Optional

NORMAL = "NORMAL"
ELEVATED = "ELEVATED"
SPIKE = "SPIKE"
MARKET_CLOSED = "MARKET_CLOSED"


@dataclass(frozen=True)
class Transition:
    to: str
    threshold: float
    direction: str  # "above" or "below"
    dwell_s: float


# Entry thresholds sit above exit thresholds (hysteresis) and exit dwell
# exceeds entry dwell. The asymmetry is deliberate: failing to warn a trader
# of a spike costs real money, while a regime that lingers ten seconds too
# long costs nothing (spec §7.5).
TRANSITIONS: Dict[str, List[Transition]] = {
    NORMAL: [Transition(ELEVATED, 1.5, "above", 3.0)],
    ELEVATED: [
        Transition(SPIKE, 2.8, "above", 2.0),
        Transition(NORMAL, 1.1, "below", 15.0),
    ],
    SPIKE: [Transition(ELEVATED, 2.0, "below", 10.0)],
}


class RegimeFSM:
    def __init__(self, initial: str = NORMAL, transitions: Optional[Dict] = None) -> None:
        self._transitions = transitions or TRANSITIONS
        self.state = initial
        self.entered_ns: Optional[int] = None
        self.last_trigger = ""
        self._since: Dict[str, int] = {}

    def force(self, state: str, ts_ns: int) -> None:
        """Used for MARKET_CLOSED, which is driven by tradeability not price."""
        self.state = state
        self.entered_ns = ts_ns
        self._since.clear()

    def update(self, ts_ns: int, score: float) -> Optional[str]:
        """Return the new state if a transition fired, else None."""
        for transition in self._transitions.get(self.state, []):
            if transition.direction == "above":
                met = score >= transition.threshold
            else:
                met = score < transition.threshold

            if not met:
                self._since.pop(transition.to, None)
                continue

            started = self._since.setdefault(transition.to, ts_ns)
            if (ts_ns - started) >= transition.dwell_s * 1_000_000_000:
                self.state = transition.to
                self.entered_ns = ts_ns
                self.last_trigger = (
                    "vol_ratio" if transition.direction == "above" else "decay"
                )
                self._since.clear()
                return transition.to
        return None
