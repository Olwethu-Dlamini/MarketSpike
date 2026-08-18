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
