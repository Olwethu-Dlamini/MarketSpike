import math

import pytest

from marketspike.engine.regime import RegimeFSM
from marketspike.engine.scoring import composite_score

SECOND = 1_000_000_000


def test_score_is_zero_when_volatility_matches_baseline_and_spread_is_normal():
    assert composite_score(v_ratio=1.0, spread_z=0.0) == 0.0


def test_score_combines_both_signals_with_documented_weights():
    # log2(4) = 2 -> 0.6*2 = 1.2 ; z=4 -> clamp(4/2)=2 -> 0.4*2 = 0.8
    assert abs(composite_score(v_ratio=4.0, spread_z=4.0) - 2.0) < 1e-9


def test_score_clamps_extreme_inputs():
    assert composite_score(v_ratio=1e9, spread_z=1e9) == pytest.approx(4.0)


def test_score_handles_missing_ratio_during_warmup():
    assert composite_score(v_ratio=None, spread_z=2.0) == pytest.approx(0.4)


def test_transition_requires_the_dwell_period_to_elapse():
    fsm = RegimeFSM()
    assert fsm.update(0, 2.0) is None            # above 1.5 but dwell not met
    assert fsm.update(2 * SECOND, 2.0) is None   # still under 3s
    assert fsm.update(3 * SECOND, 2.0) == "ELEVATED"
    assert fsm.state == "ELEVATED"


def test_dwell_timer_resets_when_the_condition_lapses():
    fsm = RegimeFSM()
    fsm.update(0, 2.0)
    fsm.update(2 * SECOND, 1.0)                  # condition lost, timer resets
    assert fsm.update(4 * SECOND, 2.0) is None   # only 0s of dwell so far
    assert fsm.state == "NORMAL"


def test_score_oscillating_around_a_threshold_does_not_flap():
    """The defect this FSM exists to prevent: the draft reassigned regime
    every 500ms and produced three transitions per second."""
    fsm = RegimeFSM()
    transitions = []
    for step in range(400):
        ts = step * (SECOND // 2)               # every 500ms
        score = 1.5 + (0.2 if step % 2 == 0 else -0.2)
        changed = fsm.update(ts, score)
        if changed:
            transitions.append(changed)
    assert transitions == []
    assert fsm.state == "NORMAL"


def test_full_spike_cycle_with_asymmetric_exit_dwell():
    fsm = RegimeFSM()
    ts = 0
    for _ in range(10):
        ts += SECOND
        fsm.update(ts, 3.5)
    assert fsm.state == "SPIKE"

    # Exit requires 10s below 2.0 — entry took 2s, exit deliberately does not.
    ts += SECOND
    assert fsm.update(ts, 1.0) is None
    ts += 5 * SECOND
    assert fsm.update(ts, 1.0) is None
    ts += 6 * SECOND
    assert fsm.update(ts, 1.0) == "ELEVATED"


def test_entered_timestamp_is_recorded_on_transition():
    fsm = RegimeFSM()
    for step in range(5):
        fsm.update(step * SECOND, 2.0)
    assert fsm.entered_ns == 3 * SECOND
