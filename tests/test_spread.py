from marketspike.engine.spread import SpreadTracker, median

SECOND = 1_000_000_000


def feed(tracker, values, start_ts=0, step_s=1.0):
    ts = start_ts
    result = 0.0
    for value in values:
        result = tracker.update(ts, value)
        ts += int(step_s * SECOND)
    return result


def test_median_of_even_length_series_averages_the_middle_pair():
    assert median([1.0, 2.0, 3.0, 4.0]) == 2.5


def test_median_of_empty_series_is_zero():
    assert median([]) == 0.0


def test_z_score_uses_scaled_mad():
    tracker = SpreadTracker(window_s=3600.0, recompute_interval_s=0.0)
    feed(tracker, [1.0, 1.1, 1.2, 1.3, 1.4])
    assert abs(tracker.median_bps - 1.2) < 1e-9
    assert abs(tracker.mad_bps - 0.1) < 1e-9
    assert abs(tracker.z(1.7) - (0.5 / (1.4826 * 0.1))) < 1e-6


def test_median_and_mad_resist_a_large_outlier():
    tracker = SpreadTracker(window_s=3600.0, recompute_interval_s=0.0)
    feed(tracker, [1.0, 1.1, 1.2, 1.3, 1.4])
    baseline_median = tracker.median_bps
    feed(tracker, [500.0], start_ts=10 * SECOND)
    assert abs(tracker.median_bps - baseline_median) < 0.15


def test_zero_dispersion_yields_zero_rather_than_dividing_by_zero():
    tracker = SpreadTracker(window_s=3600.0, recompute_interval_s=0.0)
    feed(tracker, [1.2, 1.2, 1.2, 1.2])
    assert tracker.mad_bps == 0.0
    assert tracker.z(9.9) == 0.0


def test_samples_outside_the_window_are_evicted():
    tracker = SpreadTracker(window_s=2.0, recompute_interval_s=0.0)
    feed(tracker, [50.0, 50.0, 50.0])
    feed(tracker, [1.0, 1.0, 1.0], start_ts=100 * SECOND)
    assert abs(tracker.median_bps - 1.0) < 1e-9
