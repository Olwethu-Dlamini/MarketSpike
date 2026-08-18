import math
from typing import Optional

MIN_DT_S = 1e-3
MAX_ABS_RETURN = 0.05


class VolatilityCalc:
    """Time-weighted EWMA of variance **per second** (spec §7.1).

    Quote updates arrive irregularly and arrive faster during volatility, so a
    tick-count EWMA double-counts spikes: the quantity being measured alters
    the sampling rate of the measurement. Decaying on elapsed time and
    normalising r^2 by dt removes that dependence.

    Both horizons must use this same per-second normalisation. Normalising by
    tau/dt instead would express each horizon in variance-per-its-own-horizon,
    and the fast/slow ratio would silently carry a factor of tau_fast/tau_slow.
    """

    def __init__(self, tau_s: float) -> None:
        self._tau = tau_s
        self._variance: Optional[float] = None
        self._last_ts_ns: Optional[int] = None
        self._last_mid: Optional[float] = None
        self.rejected = 0

    def seed(self, var_per_second: float) -> None:
        self._variance = var_per_second

    @property
    def variance(self) -> Optional[float]:
        return self._variance

    @property
    def sigma(self) -> Optional[float]:
        if self._variance is None or self._variance < 0:
            return None
        return math.sqrt(self._variance)

    @property
    def ready(self) -> bool:
        return self._variance is not None

    def update(self, ts_ns: int, mid: float) -> Optional[float]:
        if mid <= 0:
            return self._variance
        if self._last_ts_ns is None or self._last_mid is None:
            self._last_ts_ns, self._last_mid = ts_ns, mid
            return self._variance

        dt = (ts_ns - self._last_ts_ns) / 1e9
        if dt < MIN_DT_S:
            return self._variance

        log_return = math.log(mid / self._last_mid)
        self._last_ts_ns, self._last_mid = ts_ns, mid

        if abs(log_return) > MAX_ABS_RETURN:
            self.rejected += 1
            return self._variance

        rate = (log_return * log_return) / dt
        decay = math.exp(-dt / self._tau)
        if self._variance is None:
            self._variance = rate
        else:
            self._variance = decay * self._variance + (1.0 - decay) * rate
        return self._variance


class VolatilityPair:
    """Fast and slow horizons sharing one update, yielding the ratio V."""

    def __init__(self, tau_fast_s: float, tau_slow_s: float) -> None:
        self.fast = VolatilityCalc(tau_fast_s)
        self.slow = VolatilityCalc(tau_slow_s)

    def seed_slow(self, var_per_second: float) -> None:
        self.slow.seed(var_per_second)

    @property
    def ready(self) -> bool:
        return self.fast.ready and self.slow.ready

    def update(self, ts_ns: int, mid: float) -> Optional[float]:
        self.fast.update(ts_ns, mid)
        self.slow.update(ts_ns, mid)
        fast_sigma = self.fast.sigma
        slow_sigma = self.slow.sigma
        if fast_sigma is None or slow_sigma is None:
            return None
        # Degenerate baseline: if slow sigma is zero or negative, the ratio is
        # undefined and we cannot compute it. This is a degenerate state of the
        # estimator, not an "unready" state, so it does not trigger the None
        # above which checks for actual uninitialization.
        if slow_sigma <= 0.0:
            return None
        return fast_sigma / slow_sigma
