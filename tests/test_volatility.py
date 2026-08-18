import math

from marketspike.engine.volatility import VolatilityCalc, VolatilityPair

SECOND = 1_000_000_000


def drive(calc, dt_s, log_return, steps, start_ts=0, start_mid=100.0):
    ts, mid = start_ts, start_mid
    for _ in range(steps):
        ts += int(dt_s * SECOND)
        mid *= math.exp(log_return)
        calc.update(ts, mid)
    return calc


def test_variance_rate_is_invariant_to_sampling_density():
    """A path sampled 10x more often, with returns scaled by sqrt(10), is the
    same underlying process and must produce the same variance rate."""
    fast_sampled = drive(VolatilityCalc(tau_s=30.0), 0.1, 0.001, steps=3000)
    slow_sampled = drive(
        VolatilityCalc(tau_s=30.0), 1.0, 0.001 * math.sqrt(10), steps=300
    )
    a = fast_sampled.variance
    b = slow_sampled.variance
    assert abs(a - b) / b < 0.01


def test_variance_rate_matches_the_analytic_value():
    calc = drive(VolatilityCalc(tau_s=30.0), 0.1, 0.001, steps=3000)
    expected = (0.001 ** 2) / 0.1  # r^2 / dt
    assert abs(calc.variance - expected) / expected < 0.01


def test_seed_makes_the_calculator_ready_immediately():
    calc = VolatilityCalc(tau_s=1800.0)
    assert calc.ready is False
    calc.seed(1e-5)
    assert calc.ready is True
    assert calc.sigma == math.sqrt(1e-5)


def test_sub_millisecond_duplicate_updates_are_ignored():
    calc = VolatilityCalc(tau_s=30.0)
    calc.seed(1e-5)
    calc.update(SECOND, 100.0)
    before = calc.variance
    calc.update(SECOND + 100_000, 500.0)  # 0.1ms later, absurd price
    assert calc.variance == before


def test_implausible_returns_are_rejected_as_bad_prints():
    calc = VolatilityCalc(tau_s=30.0)
    calc.seed(1e-5)
    calc.update(SECOND, 100.0)
    before = calc.variance
    calc.update(2 * SECOND, 100.0 * math.exp(0.5))  # 50% jump
    assert calc.variance == before


def test_ratio_is_one_when_both_horizons_agree():
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0)
    pair.seed_slow(1e-5)
    ratio = drive_pair(pair)
    assert ratio is not None and abs(ratio - 1.0) < 0.05


def drive_pair(pair):
    ts, mid, ratio = 0, 100.0, None
    for _ in range(3000):
        ts += int(0.1 * SECOND)
        mid *= math.exp(0.001)
        ratio = pair.update(ts, mid)
    return ratio


def test_ratio_is_one_not_60_when_fast_and_slow_see_identical_path():
    """Guards the per-horizon-normalisation error described in the module
    docstring: if variance were normalised by tau/dt instead of by dt alone,
    each horizon's variance would be expressed in units of per-that-horizon
    rather than per-second, and the fast/slow sigma ratio would be silently
    wrong by a factor of tau_fast/tau_slow (60x here) even though both
    horizons observe exactly the same price path. A future refactor that
    reintroduces that bug must fail this test, not just look "roughly right"
    on the more forgiving test above.
    """
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0)
    pair.seed_slow(1e-5)
    ratio = drive_pair(pair)
    assert ratio is not None
    assert abs(ratio - 1.0) < 0.05
    assert abs(ratio - 60.0) > 1.0
    assert abs(ratio - (1.0 / 60.0)) > 0.5
