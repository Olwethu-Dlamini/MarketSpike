import random

from marketspike.clock.skew import SkewEstimator

SECOND = 1_000_000_000


def test_first_sample_has_zero_excess():
    est = SkewEstimator(window_s=60.0)
    # Venue clock is 5s behind ours; that offset is unmeasurable and must
    # not appear as latency.
    assert est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND) == 0


def test_excess_is_measured_above_the_running_floor():
    est = SkewEstimator(window_s=60.0)
    est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND)            # raw 5.000s
    excess = est.update(venue_ts_ns=SECOND, recv_ts_ns=6 * SECOND + 3_000_000)
    assert excess == 3000  # 3ms above floor, in microseconds


def test_a_faster_sample_lowers_the_floor_and_never_goes_negative():
    est = SkewEstimator(window_s=60.0)
    est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND)
    excess = est.update(venue_ts_ns=SECOND, recv_ts_ns=SECOND + 2 * SECOND)
    assert excess == 0
    assert est.floor_ns == 2 * SECOND


def test_stale_samples_leave_the_window():
    est = SkewEstimator(window_s=1.0)
    est.update(venue_ts_ns=0, recv_ts_ns=1 * SECOND)              # fast, raw 1s
    # 10s later the fast sample has expired, so this slow one becomes the floor.
    excess = est.update(venue_ts_ns=6 * SECOND, recv_ts_ns=11 * SECOND)
    assert excess == 0
    assert est.floor_ns == 5 * SECOND


def test_clock_drift_backwards_is_clamped_not_negative():
    est = SkewEstimator(window_s=60.0)
    est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND)
    assert est.update(venue_ts_ns=10 * SECOND, recv_ts_ns=10 * SECOND) == 0


def test_monotonic_deque_matches_brute_force_window_minimum():
    # The monotonic deque is an optimisation over "scan the window for the
    # minimum". Prove the optimisation is exact: replay a long randomised
    # sequence and independently recompute the window minimum by brute
    # force, then assert the estimator's excess matches at every step.
    rng = random.Random(1234567)
    window_s = 5.0
    est = SkewEstimator(window_s=window_s)
    window_ns = int(window_s * SECOND)

    history = []  # list of (recv_ts_ns, raw_ns), append-only, in time order
    recv_ts_ns = 0
    for _ in range(5000):
        recv_ts_ns += rng.randint(1, 50_000_000)  # up to 50ms between ticks
        skew_and_transit = 179_000_000  # constant-ish baseline, like the brief's Binance example
        jitter = rng.randint(0, 20_000_000)  # queueing noise, occasionally large
        venue_ts_ns = recv_ts_ns - (skew_and_transit + jitter)

        raw = recv_ts_ns - venue_ts_ns
        history.append((recv_ts_ns, raw))
        cutoff = recv_ts_ns - window_ns
        while history and history[0][0] < cutoff:
            history.pop(0)

        expected_floor = min(r for _, r in history)
        expected_excess = max(0, (raw - expected_floor) // 1000)

        actual_excess = est.update(venue_ts_ns=venue_ts_ns, recv_ts_ns=recv_ts_ns)

        assert actual_excess == expected_excess
        assert est.floor_ns == expected_floor


def test_internal_deque_stays_bounded_across_many_windows():
    # The whole point of the monotonic deque is that it doesn't degrade into
    # keeping every sample. Feed far more ticks than fit in one window and
    # assert the internal deque never grows anywhere near that count.
    est = SkewEstimator(window_s=1.0)
    window_ns = SECOND
    n_samples = 20_000
    recv_ts_ns = 0
    rng = random.Random(42)
    max_len = 0
    for _ in range(n_samples):
        recv_ts_ns += 100_000  # 0.1ms per tick -> ~10 windows' worth of ticks
        raw = 179_000_000 + rng.randint(0, 3_000_000)
        venue_ts_ns = recv_ts_ns - raw
        est.update(venue_ts_ns=venue_ts_ns, recv_ts_ns=recv_ts_ns)
        max_len = max(max_len, len(est._mono))

    assert len(est._mono) < n_samples // 10
    assert max_len < n_samples // 10
