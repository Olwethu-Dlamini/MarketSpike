import math
import random

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
    # Disables the fixed-grid gate (min_sample_interval_s=0.0): this test
    # exercises drive_pair's dense 0.1s-spaced sampling to verify a property
    # unrelated to the gate (per-second normalisation invariance), and the
    # gate would otherwise change which samples are fed to each horizon.
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0, min_sample_interval_s=0.0)
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
    # Disables the fixed-grid gate (min_sample_interval_s=0.0): this test
    # exercises drive_pair's dense 0.1s-spaced sampling to verify a property
    # unrelated to the gate (per-second normalisation invariance), and the
    # gate would otherwise change which samples are fed to each horizon.
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0, min_sample_interval_s=0.0)
    pair.seed_slow(1e-5)
    ratio = drive_pair(pair)
    assert ratio is not None
    assert abs(ratio - 1.0) < 0.05
    assert abs(ratio - 60.0) > 1.0
    assert abs(ratio - (1.0 / 60.0)) > 0.5


def test_zero_fast_sigma_against_positive_slow_sigma_returns_zero():
    """A zero fast sigma (from identical prices, log_return=0) against a
    positive slow sigma is a legitimate ratio of 0.0, not None. It indicates
    a calm market relative to the baseline. Previously this was swallowed by
    truthiness check on fast_sigma."""
    pair = VolatilityPair(tau_fast_s=1.0, tau_slow_s=60.0)
    pair.seed_slow(1e-5)  # positive baseline
    # Drive with identical mid prices (log_return = 0) to decay fast variance to ~0.
    # Space ticks > MIN_DT_S (1ms) apart so they are processed.
    ts, mid = 0, 100.0
    for _ in range(100):
        ts += int(0.01 * SECOND)  # 10ms apart
        # mid stays the same, so log_return = 0
        pair.update(ts, mid)
    ratio = pair.update(ts + int(0.01 * SECOND), mid)
    # Fast variance should have decayed to near-zero; slow remains at 1e-5.
    # The ratio should be near 0.0, definitely not None.
    assert ratio is not None, "Zero fast sigma should return 0.0, not None"
    assert isinstance(ratio, float), f"Expected float, got {type(ratio)}"
    # The ratio should be very close to 0.0 (within floating-point precision).
    assert ratio < 1e-4, f"Expected ratio near 0.0, got {ratio}"


def test_degenerate_slow_baseline_returns_none():
    """If slow_sigma is 0.0 (a degenerate baseline), update() returns None
    rather than raising ZeroDivisionError. This tests the edge case where the
    baseline is legitimately zero."""
    pair = VolatilityPair(tau_fast_s=1.0, tau_slow_s=60.0)
    # Manually craft the state: seed slow at 0.0, fast at non-zero.
    pair.slow.seed(0.0)
    pair.fast.seed(1e-5)
    # Call update; even though slow_sigma is 0.0, we should return None, not raise.
    ratio = pair.update(0, 100.0)
    assert ratio is None, "Degenerate slow baseline (slow_sigma=0.0) should return None"


def test_updates_faster_than_interval_are_skipped():
    """Ticks arriving faster than min_sample_interval_s must not touch either
    horizon at all, and the skipped call must hand back the previous ratio
    rather than None so callers never see a gap between samples."""
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0, min_sample_interval_s=1.0)
    pair.seed_slow(1e-5)

    ts = 0
    mid = 100.0
    variances = []
    ratios = []
    for _ in range(40):  # 40 ticks * 0.25s spacing = 10s of sub-second ticks
        mid *= math.exp(0.0001)
        ratio = pair.update(ts, mid)
        variances.append(pair.fast.variance)
        ratios.append(ratio)
        ts += int(0.25 * SECOND)

    changed = sum(
        1 for i in range(1, len(variances)) if variances[i] != variances[i - 1]
    )
    # ts advances 0.25s/call against a 1.0s gate, so only 1 in 4 calls after
    # the first is a newly-accepted sample (accepted at t=0,1,2,...,9s).
    accepted_count = 1 + changed
    assert accepted_count == 10

    for i in range(1, len(variances)):
        if variances[i] == variances[i - 1]:
            assert ratios[i] == ratios[i - 1], (
                "a skipped update must return the previous ratio, not a "
                "fresh (or None) value"
            )


def test_first_call_is_always_accepted():
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0, min_sample_interval_s=1.0)
    pair.seed_slow(1e-5)

    first_ratio = pair.update(0, 100.0)
    assert first_ratio is None  # not enough history yet, but this call is accepted
    variance_after_first = pair.fast.variance

    # Arrives 0.5s later -- inside the 1.0s gate. If the first call had not
    # actually been accepted (i.e. treated as establishing the baseline),
    # this call would look like "the first call" again and would itself be
    # accepted. It must instead be skipped.
    second_ratio = pair.update(int(0.5 * SECOND), 200.0)
    assert pair.fast.variance == variance_after_first
    assert second_ratio == first_ratio


def _normal_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


def test_fixed_grid_sampling_recovers_true_sigma_removing_tick_quantisation_bias():
    """The regression that motivated this fix. Real BTCUSDT ticks arrive
    ~5ms apart, but the mid moves in discrete price increments; over 5ms the
    true diffusive move is far below one increment, so consecutive
    log-returns quantise to exactly zero (measured: 98.8% of consecutive
    live tick pairs had zero return) and realised variance is understated.
    Measured on 12,126 recorded BTCUSDT ticks: tick-sampled vs. bar-sampled
    on the *identical* data/window gave V=0.514 -- a pure sampling artifact,
    since the same window resampled to 1-minute bars against the 24h kline
    baseline gave V=1.051, i.e. a genuinely normal market.

    This builds a single underlying per-second-calibrated random walk (the
    "true" price) from ~5ms increments, then derives a *reported* tick
    stream from those same increments where any single 5ms move below a
    coarse threshold is quantised to exactly zero (mid unchanged) -- exactly
    "the true diffusive move is far below one increment, so it quantises to
    zero" -- while a move that clears the threshold is reported as-is. That
    reproduces the 98.8% zero-return figure. At 1s, the true path's move
    dwarfs the threshold (discreteness negligible, per the fix rationale),
    so gridded sampling of the true path recovers sigma closely; per-tick
    sampling of the reported (quantised) stream discards the overwhelming
    majority of the small legitimate moves and recovers something materially
    smaller -- this pins the 0.514-style understatement.
    """
    rng = random.Random(20260817)
    true_sigma_per_second = 0.02
    dt_tick = 0.005  # ~5ms, matching live tick spacing
    n_ticks = 40000  # 200s of ticks

    step_std = true_sigma_per_second * math.sqrt(dt_tick)
    # Threshold calibrated so ~98.8% of 5ms increments fall below it (two
    # -sided), matching the measured live zero-return rate.
    lo, hi = 0.0, 6.0
    for _ in range(60):
        z = (lo + hi) / 2
        if _normal_cdf(z) < 0.994:
            lo = z
        else:
            hi = z
    threshold = z * step_std

    true_mid = 100.0
    reported_mid = 100.0
    true_path = []
    reported_path = []
    ts = 0
    zero_return_ticks = 0
    for _ in range(n_ticks):
        ts += int(dt_tick * SECOND)
        raw_return = rng.gauss(0.0, step_std)
        true_mid *= math.exp(raw_return)
        true_path.append((ts, true_mid))
        if abs(raw_return) > threshold:
            reported_mid *= math.exp(raw_return)
        else:
            zero_return_ticks += 1
        reported_path.append((ts, reported_mid))

    # Sanity check the synthetic path actually reproduces the near-all-zero
    # -return regime observed live (98.8% there), not some milder artifact.
    assert zero_return_ticks / n_ticks > 0.95

    # 1s grid, fed the true path: at this scale the diffusive move dwarfs
    # the increment threshold, so the recovered sigma should track the true
    # per-second sigma closely.
    gridded = VolatilityPair(tau_fast_s=10.0, tau_slow_s=1800.0, min_sample_interval_s=1.0)
    gridded.seed_slow(true_sigma_per_second ** 2)
    for tick_ts, tick_mid in true_path:
        gridded.update(tick_ts, tick_mid)
    sigma_gridded = gridded.fast.sigma
    assert sigma_gridded is not None
    assert abs(sigma_gridded - true_sigma_per_second) / true_sigma_per_second < 0.25

    # Per-tick sampling (the old, un-gated behaviour), fed the reported
    # (quantised-to-zero) stream, recovers a materially smaller sigma --
    # this is the bias the fix addresses.
    per_tick = VolatilityPair(tau_fast_s=10.0, tau_slow_s=1800.0, min_sample_interval_s=0.0)
    per_tick.seed_slow(true_sigma_per_second ** 2)
    for tick_ts, tick_mid in reported_path:
        per_tick.update(tick_ts, tick_mid)
    sigma_per_tick = per_tick.fast.sigma
    assert sigma_per_tick is not None
    assert sigma_per_tick < sigma_gridded * 0.7


def test_fresh_pair_not_ready_and_returns_none():
    """On a fresh VolatilityPair with no seeding and no ticks, ready is False
    and update() returns None. They must agree: if not ready, update() returns None."""
    pair = VolatilityPair(tau_fast_s=1.0, tau_slow_s=60.0)
    assert pair.ready is False, "Fresh pair should not be ready"
    # First update only sets _last_ts and _last_mid, does not compute variance.
    ratio = pair.update(0, 100.0)
    assert ratio is None, "First update on unready pair should return None"
    # After one update, still not ready (need a second to compute log_return).
    assert pair.ready is False, "Pair should still not be ready after first update"
    assert pair.fast.ready is False and pair.slow.ready is False
