import threading
import time

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
    excess_us = timer.on_receive(tick)
    excess_us, engine_us = timer.on_processed(tick, done_ts_ns=5 * SECOND + 250_000, excess_us=excess_us)
    assert engine_us == 250
    assert excess_us == 0


def test_percentile_endpoints_match_numpy_linear_interpolation():
    values = [10, 20, 30, 40]
    assert percentile(values, 0.0) == 10
    assert percentile(values, 0.5) == 25
    assert percentile(values, 1.0) == 40


def test_aggregator_evicts_out_of_order_sample_once_window_moves_on():
    # A sample appended after a newer one (e.g. a reordered network packet)
    # sits behind the live front of the deque. Once real time has moved far
    # enough forward, the stale, out-of-order value must be gone from the
    # reported percentiles -- it must not survive just because it once sat
    # behind a live front.
    agg = LatencyAggregator(window_s=1.0)
    agg.add(ts_ns=10 * SECOND, value_us=10)  # establishes the live front
    agg.add(ts_ns=0, value_us=9999)  # arrives late, older than the front
    agg.add(ts_ns=100 * SECOND, value_us=20)  # time moves far enough forward
    p50, p95, p99 = agg.percentiles(ts_ns=100 * SECOND)
    assert 9999 not in (p50, p95, p99)
    assert (p50, p95, p99) == (20, 20, 20)


def test_aggregator_keeps_stored_order_non_decreasing_despite_decreasing_arrivals():
    # _evict only ever inspects the front of the deque, so its correctness
    # depends entirely on stored timestamps being non-decreasing. Feed
    # samples in deliberately decreasing timestamp order (e.g. a replay
    # rewind, or two symbols sharing an aggregator) and check that
    # invariant directly: if storage order isn't non-decreasing, an old
    # value can lodge behind a live front and _evict can stop before ever
    # reaching it, no matter how much later it is queried.
    agg = LatencyAggregator(window_s=300.0)
    agg.add(ts_ns=100 * SECOND, value_us=100)
    agg.add(ts_ns=50 * SECOND, value_us=50)
    agg.add(ts_ns=10 * SECOND, value_us=10)
    stored_timestamps = [ts for ts, _ in agg._samples]
    assert stored_timestamps == sorted(stored_timestamps)


def test_add_and_percentiles_survive_genuine_cross_thread_access():
    """Regression for the confirmed production 500.

    FastAPI runs plain `def` routes (e.g. /latency/summary) in a worker
    threadpool, while the engine's tick path calls add() from the asyncio
    event-loop thread. Reading percentiles() while add() is concurrently
    mutating the same deque previously raised
    `RuntimeError: deque mutated during iteration`. This test drives both
    sides from real OS threads (not simulated/interleaved in one thread) so
    it only passes if the two really are safe to run concurrently.
    """
    iterations = 50_000
    rounds = 5  # each round independently has a high chance of tripping the
    # race; repeating amplifies detection probability to effectively certain
    # while a correctly-locked implementation passes every round every time.
    errors = []

    for _ in range(rounds):
        if errors:
            break
        agg = LatencyAggregator(window_s=300.0)
        stop = threading.Event()

        def writer():
            ts = 0
            try:
                for i in range(iterations):
                    ts += 1000
                    agg.add(ts_ns=ts, value_us=i % 1000)
            except Exception as exc:  # pragma: no cover - only on regression
                errors.append(exc)
            finally:
                stop.set()

        def reader():
            try:
                while not stop.is_set():
                    agg.percentiles(ts_ns=time.time_ns())
            except Exception as exc:  # pragma: no cover - only on regression
                errors.append(exc)

        writer_threads = [threading.Thread(target=writer) for _ in range(2)]
        reader_threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in reader_threads + writer_threads:
            t.start()
        for t in writer_threads:
            t.join(timeout=30)
        stop.set()
        for t in reader_threads:
            t.join(timeout=30)

    assert not errors, "cross-thread access raised: {0}".format(errors)
