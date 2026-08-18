import math

import pytest

from marketspike.engine.regime import RegimeFSM
from marketspike.engine.scoring import composite_score, dominant_signal, score_components

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


def test_score_components_sum_to_the_composite_score():
    v_ratio, spread_z = 4.0, 4.0
    vol_component, spread_component = score_components(v_ratio, spread_z)
    assert vol_component + spread_component == composite_score(v_ratio, spread_z)


def test_dominant_signal_is_vol_ratio_when_volatility_contribution_is_larger():
    # vol_component = 0.6*log2(4) = 1.2 ; spread_component = 0.4*(0.5/2) = 0.1
    assert dominant_signal(v_ratio=4.0, spread_z=0.5) == "vol_ratio"


def test_dominant_signal_is_spread_when_spread_contribution_is_larger():
    # v_ratio=1.0 -> vol_component == 0.0 ; spread_z=4.0 -> spread_component == 0.8
    assert dominant_signal(v_ratio=1.0, spread_z=4.0) == "spread"


def test_dominant_signal_is_both_when_contributions_are_equal():
    assert dominant_signal(v_ratio=1.0, spread_z=0.0) == "both"


def test_dominant_signal_is_both_when_contributions_are_equal_and_nonzero():
    # vol_component = 0.6*log2(2) = 0.6 ; spread_component = 0.4*(2.9999999999999996/2)
    # chosen so both weighted contributions are bit-for-bit 0.6 (0.6*1.5 vs 0.4*3
    # do not agree exactly in float64; these values were verified to).
    v_ratio, spread_z = 2.0, 2.9999999999999996
    vol_component, spread_component = score_components(v_ratio, spread_z)
    assert vol_component == spread_component  # sanity: genuinely tied, not approx
    assert dominant_signal(v_ratio, spread_z) == "both"


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


def test_transition_fires_exactly_at_the_dwell_boundary_not_early_or_late():
    fsm = RegimeFSM()
    assert fsm.update(0, 2.0) is None                      # starts the dwell timer
    assert fsm.update(3 * SECOND - 1, 2.0) is None         # one ns short of dwell
    assert fsm.update(3 * SECOND, 2.0) == "ELEVATED"       # elapsed == dwell exactly
    assert fsm.state == "ELEVATED"


def test_evaluating_the_other_elevated_candidate_does_not_disturb_the_running_dwell_timer():
    """NOTE on scope: from ELEVATED there are two outgoing candidates, SPIKE
    (score >= 2.8) and NORMAL (score < 1.1). These threshold ranges are
    mutually exclusive and leave a gap (1.1 <= score < 2.8) -- no single
    score can ever satisfy both at once. So genuine *simultaneous*
    contention, where both dwell timers are concurrently ticking toward
    their targets, is structurally impossible for this FSM configuration;
    this test proves the weaker (but still real) property instead.

    While SPIKE's dwell timer is accumulating (score sustained >= 2.8),
    NORMAL's condition is evaluated on every single one of those same calls
    and is necessarily found not-met (since 2.8 is not < 1.1) -- i.e. the
    "other candidate" lapses on every tick. Assert this repeated eval-and-
    lapse of NORMAL never disturbs SPIKE's independently-keyed timer: the
    transition to SPIKE still fires exactly at its 2.0s dwell boundary.
    """
    fsm = RegimeFSM(initial="ELEVATED")
    assert fsm.update(0, 2.9) is None                     # starts SPIKE's timer; NORMAL evaluated + lapses
    assert "NORMAL" not in fsm._since
    assert fsm.update(2 * SECOND - 1, 2.9) is None        # one ns short; NORMAL lapses again
    assert "NORMAL" not in fsm._since
    assert fsm.update(2 * SECOND, 2.9) == "SPIKE"         # SPIKE timer was undisturbed -> fires exactly on time
    assert fsm.state == "SPIKE"


def test_transition_fires_when_score_exactly_equals_the_entry_threshold():
    """Entry ("above") semantics are >=: a score exactly at the threshold
    must qualify, pinning the boundary rather than just the dwell timing."""
    fsm = RegimeFSM()
    assert fsm.update(0, 1.5) is None                     # exactly at threshold, dwell not yet met
    assert fsm.update(3 * SECOND, 1.5) == "ELEVATED"
    assert fsm.state == "ELEVATED"


def test_transition_does_not_fire_when_score_is_one_ulp_below_the_entry_threshold():
    fsm = RegimeFSM()
    just_under = math.nextafter(1.5, 0.0)
    for step in range(10):
        assert fsm.update(step * SECOND, just_under) is None
    assert fsm.state == "NORMAL"
    assert fsm._since == {}


def test_transition_does_not_fire_when_score_exactly_equals_the_exit_threshold():
    """Exit ("below") semantics are strict <: a score exactly at the exit
    threshold must NOT qualify."""
    fsm = RegimeFSM(initial="ELEVATED")
    for step in range(20):
        assert fsm.update(step * SECOND, 1.1) is None     # exactly at exit threshold -> not met
    assert fsm.state == "ELEVATED"
    assert fsm._since == {}


def test_transition_fires_when_score_is_one_ulp_below_the_exit_threshold():
    fsm = RegimeFSM(initial="ELEVATED")
    just_under = math.nextafter(1.1, 0.0)
    assert fsm.update(0, just_under) is None
    assert fsm.update(15 * SECOND, just_under) == "NORMAL"
    assert fsm.state == "NORMAL"


def test_last_trigger_uses_the_supplied_trigger_for_an_above_transition():
    fsm = RegimeFSM()
    fsm.update(0, 2.0)
    fsm.update(3 * SECOND, 2.0, trigger="spread")
    assert fsm.last_trigger == "spread"


def test_last_trigger_defaults_to_vol_ratio_for_an_above_transition_when_no_trigger_supplied():
    fsm = RegimeFSM()
    fsm.update(0, 2.0)
    assert fsm.update(3 * SECOND, 2.0) == "ELEVATED"
    assert fsm.last_trigger == "vol_ratio"


def test_last_trigger_is_always_decay_for_a_below_transition_regardless_of_supplied_trigger():
    fsm = RegimeFSM(initial="ELEVATED")
    fsm.update(0, 0.5, trigger="spread")
    assert fsm.update(15 * SECOND, 0.5, trigger="spread") == "NORMAL"
    assert fsm.last_trigger == "decay"


def test_transition_driven_purely_by_spread_reports_spread_as_the_trigger():
    """Load-bearing end-to-end proof for Finding 1: a transition with
    v_ratio=1.0 (zero volatility contribution) driven entirely by a large
    spread_z must report last_trigger == "spread", not the old direction-only
    "vol_ratio" default. This fails unless `dominant_signal` is computed from
    the score and threaded through as `trigger`.
    """
    fsm = RegimeFSM()
    v_ratio, spread_z = 1.0, 100.0
    score = composite_score(v_ratio, spread_z)
    trigger = dominant_signal(v_ratio, spread_z)
    assert fsm.update(0, score, trigger=trigger) is None
    assert fsm.update(3 * SECOND, score, trigger=trigger) == "ELEVATED"
    assert fsm.last_trigger == "spread"


def test_force_resets_last_trigger_to_a_neutral_value():
    fsm = RegimeFSM()
    fsm.update(0, 2.0)
    fsm.update(3 * SECOND, 2.0)
    assert fsm.last_trigger == "vol_ratio"
    fsm.force("MARKET_CLOSED", 4 * SECOND)
    assert fsm.last_trigger == "forced"
