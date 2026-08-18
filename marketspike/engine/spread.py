from collections import deque
from typing import Deque, List, Optional, Tuple

MAD_TO_SIGMA = 1.4826


def median(sorted_values: List[float]) -> float:
    count = len(sorted_values)
    if count == 0:
        return 0.0
    middle = count // 2
    if count % 2 == 1:
        return float(sorted_values[middle])
    return (sorted_values[middle - 1] + sorted_values[middle]) / 2.0


class SpreadTracker:
    """Rolling robust z-score of quoted spread (spec §7.3).

    Spread distributions are fat-tailed, so mean and standard deviation are the
    wrong estimators — the outliers are the signal, and they would inflate the
    very scale used to detect them. Median and MAD are unaffected.

    The 1.4826 factor makes MAD a consistent estimator of sigma under
    normality, keeping z on the familiar scale.
    """

    def __init__(self, window_s: float = 3600.0, recompute_interval_s: float = 5.0) -> None:
        self._window_ns = int(window_s * 1_000_000_000)
        self._recompute_ns = int(recompute_interval_s * 1_000_000_000)
        self._samples: Deque[Tuple[int, float]] = deque()
        self._median: Optional[float] = None
        self._mad: Optional[float] = None
        self._last_recompute_ns: Optional[int] = None

    @property
    def median_bps(self) -> float:
        return self._median if self._median is not None else 0.0

    @property
    def mad_bps(self) -> float:
        return self._mad if self._mad is not None else 0.0

    def update(self, ts_ns: int, spread_bps: float) -> float:
        self._samples.append((ts_ns, spread_bps))
        cutoff = ts_ns - self._window_ns
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

        due = (
            self._last_recompute_ns is None
            or (ts_ns - self._last_recompute_ns) >= self._recompute_ns
        )
        if due:
            self._recompute()
            self._last_recompute_ns = ts_ns
        return self.z(spread_bps)

    def _recompute(self) -> None:
        values = sorted(value for _, value in self._samples)
        self._median = median(values)
        deviations = sorted(abs(value - self._median) for value in values)
        self._mad = median(deviations)

    def z(self, spread_bps: float) -> float:
        if self._median is None or not self._mad:
            return 0.0
        return (spread_bps - self._median) / (MAD_TO_SIGMA * self._mad)
