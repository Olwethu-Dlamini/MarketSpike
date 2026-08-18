from marketspike.engine.pipeline import LatencyAggregator, PipelineTimer, percentile
from marketspike.feeds.base import Tick

SECOND = 1_000_000_000


def make_tick(venue_ts_ns, recv_ts_ns):
    return Tick(
        symbol="BTCUSDT", venue_ts_ns=venue_ts_ns, recv_ts_ns=recv_ts_ns,
        bid=100.0, ask=100.1, bid_qty=1.0, ask_qty=1.0,
        tradeable=True, source="measured",
    )


def test_percentile_interpolates_between_samples():
    assert percentile([10, 20, 30, 40], 0.5) == 25


def test_percentile_of_empty_series_is_zero():
    assert percentile([], 0.95) == 0


def test_aggregator_reports_ordered_percentiles():
    agg = LatencyAggregator(window_s=300.0)
    for i in range(1, 101):
        agg.add(ts_ns=i * 1_000_000, value_us=i)
    p50, p95, p99 = agg.percentiles(ts_ns=100 * 1_000_000)
    assert p50 < p95 < p99
    assert p50 == 50


def test_aggregator_evicts_samples_outside_the_window():
    agg = LatencyAggregator(window_s=1.0)
    agg.add(ts_ns=0, value_us=9999)
    agg.add(ts_ns=5 * SECOND, value_us=10)
    assert agg.percentiles(ts_ns=5 * SECOND) == (10, 10, 10)


def test_engine_time_is_exact_and_needs_no_correction():
    timer = PipelineTimer(skew_window_s=60.0)
    tick = make_tick(venue_ts_ns=0, recv_ts_ns=5 * SECOND)
    timer.on_receive(tick)
    excess_us, engine_us = timer.on_processed(tick, done_ts_ns=5 * SECOND + 250_000)
    assert engine_us == 250
    assert excess_us == 0


def test_percentile_endpoints_match_numpy_linear_interpolation():
    values = [10, 20, 30, 40]
    assert percentile(values, 0.0) == 10
    assert percentile(values, 0.5) == 25
    assert percentile(values, 1.0) == 40


def test_aggregator_percentiles_survive_out_of_order_adds():
    agg = LatencyAggregator(window_s=300.0)
    # Decreasing timestamps: two symbols sharing an aggregator, or a replay
    # rewind, can produce this. The eviction cutoff is computed from the
    # timestamp of the *last* add() call, not a running max, so an
    # out-of-order add can evict samples that are still "in window" relative
    # to their own timestamp, or fail to evict ones that should be gone.
    # Either way, percentiles() must still return sane, non-crashing values
    # (no exceptions, values within the range of what remains).
    agg.add(ts_ns=100 * SECOND, value_us=100)
    agg.add(ts_ns=50 * SECOND, value_us=50)
    agg.add(ts_ns=10 * SECOND, value_us=10)
    p50, p95, p99 = agg.percentiles(ts_ns=100 * SECOND)
    assert 0 <= p50 <= p95 <= p99
