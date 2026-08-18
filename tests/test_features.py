import pytest

from marketspike.calendar.clock import CalendarEvent, EventClock
from marketspike.ml.features import (
    LeakageError, TickRow, build_dataset, build_sample,
)
from marketspike.risk.slippage import FEATURE_ORDER

SECOND = 1_000_000_000
CLOCK = EventClock([
    CalendarEvent("US CPI", "high", "US", 500 * SECOND, ["EURUSD"])
])


def row(ts_ns, mid, spread_bps=2.0):
    return TickRow(
        ts_ns=ts_ns, mid=mid, spread_bps=spread_bps, book_imbalance=0.1,
        quote_rate_hz=5.0, v_ratio=1.5, spread_z=0.5, abs_return_5s=0.001,
        latency_ms=40.0, regime="NORMAL",
    )


def test_target_before_decision_time_is_rejected():
    history = [row(1000 * SECOND, 100.0)]
    with pytest.raises(LeakageError):
        build_sample(history, row(999 * SECOND, 100.0), 50.0, 1, CLOCK, "EURUSD")


def test_target_at_exactly_decision_time_is_rejected():
    history = [row(1000 * SECOND, 100.0)]
    with pytest.raises(LeakageError):
        build_sample(history, row(1000 * SECOND, 100.0), 50.0, 1, CLOCK, "EURUSD")


def test_features_use_only_the_decision_time_row():
    history = [row(1000 * SECOND, 100.0, spread_bps=2.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0, spread_bps=99.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    # log_spread_bps must reflect 2.0, never the target's 99.0.
    assert sample.features["log_spread_bps"] == pytest.approx(0.6931, abs=1e-3)


def test_every_declared_feature_is_populated():
    history = [row(1000 * SECOND, 100.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    assert set(sample.features) == set(FEATURE_ORDER)


def test_flat_market_cost_is_the_half_spread():
    history = [row(1000 * SECOND, 100.0, spread_bps=4.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0, spread_bps=4.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    assert sample.cost_bps == pytest.approx(2.0)


def test_adverse_drift_raises_cost_for_a_buy_and_lowers_it_for_a_sell():
    decision_mid = 100.0
    target_mid = 100.1  # +10 bps drift over the interval
    target_spread_bps = 4.0
    history = [row(1000 * SECOND, decision_mid, spread_bps=target_spread_bps)]
    target = row(1000 * SECOND + 50_000_000, target_mid, spread_bps=target_spread_bps)
    buy = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    sell = build_sample(history, target, 50.0, -1, CLOCK, "EURUSD")
    assert buy.cost_bps > sell.cost_bps

    # Derive the expectation independently from first principles: the
    # ask/bid are quoted around the target mid using target.spread_bps
    # (which is relative to target_mid), then each cost is measured
    # relative to decision_mid (the arrival price actually observed).
    spread_abs_target = target_spread_bps / 1e4 * target_mid
    ask_target = target_mid + spread_abs_target / 2.0
    bid_target = target_mid - spread_abs_target / 2.0
    cost_buy = (ask_target - decision_mid) / decision_mid * 1e4
    cost_sell = (decision_mid - bid_target) / decision_mid * 1e4
    expected_mean = (cost_buy + cost_sell) / 2.0

    assert (buy.cost_bps + sell.cost_bps) / 2 == pytest.approx(expected_mean, abs=1e-6)


def test_dataset_emits_both_directions_for_each_decision_point():
    rows = [row(i * 100 * 1_000_000, 100.0 + i * 0.01) for i in range(20)]
    samples = build_dataset(rows, delta_ms=200.0, event_clock=CLOCK, symbol="EURUSD")
    assert samples
    assert len(samples) % 2 == 0
    directions = {s.direction for s in samples}
    assert directions == {1, -1}


def test_dataset_never_emits_a_sample_whose_target_precedes_its_features():
    rows = [row(i * 100 * 1_000_000, 100.0 + i * 0.01) for i in range(20)]
    samples = build_dataset(rows, delta_ms=200.0, event_clock=CLOCK, symbol="EURUSD")
    for sample in samples:
        assert sample.t_ns + sample.delta_ms * 1_000_000 <= rows[-1].ts_ns


def test_feature_keys_are_exactly_feature_order():
    # Import FEATURE_ORDER rather than hardcoding it, so the feature dict
    # and the model's declared feature order cannot silently drift apart --
    # a mismatch here would break inference without raising anything.
    history = [row(1000 * SECOND, 100.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    assert set(sample.features.keys()) == set(FEATURE_ORDER)
    assert len(sample.features) == len(FEATURE_ORDER)


def test_direction_symmetry_recovers_the_half_spread():
    # This is the invariant behind the empirical p50 ~= half-spread result:
    # a buy and a sell scored off the same decision/target pair differ only
    # by the sign of the drift term, so their mean must equal the
    # direction-independent half-spread term exactly (to float tolerance),
    # for every decision point -- not just in a hand-picked example. A sign
    # error in the drift term would show up here even if it happened to
    # cancel out in a single hardcoded case.
    rows = [
        row(i * 137 * 1_000_000, 100.0 + i * 0.037, spread_bps=1.0 + 0.1 * i)
        for i in range(30)
    ]
    delta_ms = 60.0
    delta_ns = int(delta_ms * 1_000_000)

    for index, decision in enumerate(rows):
        wanted_ts = decision.ts_ns + delta_ns
        target = next((r for r in rows if r.ts_ns >= wanted_ts), None)
        if target is None:
            continue
        buy = build_sample([decision], target, delta_ms, 1, CLOCK, "EURUSD")
        sell = build_sample([decision], target, delta_ms, -1, CLOCK, "EURUSD")

        # Derive the expectation independently: the half-spread is quoted
        # relative to target.mid, then rescaled to decision.mid (the
        # arrival price) so it is comparable to the drift term.
        expected_half_spread = (target.spread_bps / 2.0) * (target.mid / decision.mid)
        assert (buy.cost_bps + sell.cost_bps) / 2 == pytest.approx(
            expected_half_spread, abs=1e-9
        )
