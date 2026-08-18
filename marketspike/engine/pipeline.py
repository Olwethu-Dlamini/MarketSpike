import math
from collections import deque
from typing import Deque, List, Tuple

from marketspike.clock.skew import SkewEstimator
from marketspike.feeds.base import Tick


def percentile(sorted_values: List[int], q: float) -> int:
    """Linear-interpolated percentile, matching numpy's default method."""
    if not sorted_values:
        return 0
    position = (len(sorted_values) - 1) * q
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return int(sorted_values[low])
    lower = sorted_values[low]
    upper = sorted_values[high]
    return int(lower + (upper - lower) * (position - low))


class LatencyAggregator:
    """Rolling-window percentiles.

    Percentiles, not means: a 20ms mean with a 400ms p99 is a materially
    different trading environment from a 20ms mean with a 25ms p99, and the
    mean cannot distinguish them (spec §6.4).

    Sorting happens on read, not on write, because frames are emitted at a few
    hertz while ticks arrive at tens of hertz.
    """

    def __init__(self, window_s: float = 300.0) -> None:
        self._window_ns = int(window_s * 1_000_000_000)
        self._samples: Deque[Tuple[int, int]] = deque()

    def add(self, ts_ns: int, value_us: int) -> None:
        self._samples.append((ts_ns, value_us))
        self._evict(ts_ns)

    def _evict(self, now_ns: int) -> None:
        cutoff = now_ns - self._window_ns
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def percentiles(self, ts_ns: int) -> Tuple[int, int, int]:
        self._evict(ts_ns)
        values = sorted(value for _, value in self._samples)
        return (
            percentile(values, 0.50),
            percentile(values, 0.95),
            percentile(values, 0.99),
        )


class PipelineTimer:
    """Stamps the three measurable hops for one symbol (spec §6.1)."""

    def __init__(self, skew_window_s: float = 60.0, agg_window_s: float = 300.0) -> None:
        self._skew = SkewEstimator(window_s=skew_window_s)
        self.transit = LatencyAggregator(window_s=agg_window_s)
        self.engine = LatencyAggregator(window_s=agg_window_s)
        self.total = LatencyAggregator(window_s=agg_window_s)
        self._last_excess_us = 0

    def on_receive(self, tick: Tick) -> int:
        self._last_excess_us = self._skew.update(tick.venue_ts_ns, tick.recv_ts_ns)
        return self._last_excess_us

    def on_processed(self, tick: Tick, done_ts_ns: int) -> Tuple[int, int]:
        # Same machine, same clock: exact, no correction needed.
        engine_us = max(0, (done_ts_ns - tick.recv_ts_ns) // 1000)
        excess_us = self._last_excess_us
        self.transit.add(tick.recv_ts_ns, excess_us)
        self.engine.add(tick.recv_ts_ns, engine_us)
        self.total.add(tick.recv_ts_ns, excess_us + engine_us)
        return excess_us, engine_us
