from collections import deque
from typing import Deque, Optional, Tuple


class SkewEstimator:
    """Reports transit latency *in excess of* a rolling baseline.

    Absolute one-way transit cannot be measured against a venue clock: the
    observed difference is skew plus transit, and the two are inseparable from
    a single sample. Subtracting the window minimum cancels the skew term and
    leaves queueing above baseline (spec §6.2).

    Uses a monotonic deque so the minimum is O(1) amortised rather than O(n)
    per tick — this runs on the hot path of a latency product.
    """

    def __init__(self, window_s: float = 60.0) -> None:
        self._window_ns = int(window_s * 1_000_000_000)
        self._mono: Deque[Tuple[int, int]] = deque()

    @property
    def floor_ns(self) -> Optional[int]:
        return self._mono[0][1] if self._mono else None

    def update(self, venue_ts_ns: int, recv_ts_ns: int) -> int:
        raw = recv_ts_ns - venue_ts_ns

        cutoff = recv_ts_ns - self._window_ns
        while self._mono and self._mono[0][0] < cutoff:
            self._mono.popleft()

        while self._mono and self._mono[-1][1] >= raw:
            self._mono.pop()
        self._mono.append((recv_ts_ns, raw))

        excess_ns = raw - self._mono[0][1]
        return max(0, excess_ns // 1000)
