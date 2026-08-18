import json
import os
import tempfile

import pytest

from marketspike.calendar.clock import CalendarEvent, EventClock, load_events

SECOND = 1_000_000_000
EVENT_TS = 1_000_000 * SECOND

CPI = CalendarEvent(
    name="US CPI (YoY)", importance="high", country="US",
    event_ts_ns=EVENT_TS, affects=["EURUSD", "BTCUSDT"],
)
CLOCK = EventClock([CPI])


def test_far_before_the_release_is_clear():
    assert CLOCK.phase(EVENT_TS - 7200 * SECOND, "EURUSD") == "CLEAR"


def test_thirty_minutes_before_is_pre_event():
    assert CLOCK.phase(EVENT_TS - 1799 * SECOND, "EURUSD") == "PRE_EVENT"


def test_thirty_seconds_before_is_the_event_window():
    assert CLOCK.phase(EVENT_TS - 30 * SECOND, "EURUSD") == "EVENT_WINDOW"


def test_five_minutes_after_is_still_the_event_window():
    assert CLOCK.phase(EVENT_TS + 300 * SECOND, "EURUSD") == "EVENT_WINDOW"


def test_an_hour_after_is_clear_again():
    assert CLOCK.phase(EVENT_TS + 3600 * SECOND, "EURUSD") == "CLEAR"


def test_symbols_the_event_does_not_affect_stay_clear():
    assert CLOCK.phase(EVENT_TS - 30 * SECOND, "XAUUSD") == "CLEAR"


def test_signed_seconds_are_negative_before_and_positive_after():
    assert CLOCK.signed_seconds(EVENT_TS - 600 * SECOND, "EURUSD") == pytest.approx(-600)
    assert CLOCK.signed_seconds(EVENT_TS + 600 * SECOND, "EURUSD") == pytest.approx(600)


def test_signed_seconds_are_clipped_to_the_feature_range():
    assert CLOCK.signed_seconds(EVENT_TS - 99999 * SECOND, "EURUSD") == -1800.0
    assert CLOCK.signed_seconds(EVENT_TS + 99999 * SECOND, "EURUSD") == 1800.0


def test_upcoming_filters_by_horizon_and_symbol():
    now = EVENT_TS - 3600 * SECOND
    assert CLOCK.upcoming(now, hours=2, symbol="EURUSD") == [CPI]
    assert CLOCK.upcoming(now, hours=2, symbol="XAUUSD") == []
    assert CLOCK.upcoming(now, hours=0.5, symbol="EURUSD") == []


def test_nearest_event_wins_when_two_are_close():
    later = CalendarEvent(
        name="FOMC", importance="high", country="US",
        event_ts_ns=EVENT_TS + 1200 * SECOND, affects=["EURUSD"],
    )
    clock = EventClock([CPI, later])
    assert clock.relevant(EVENT_TS + 1100 * SECOND, "EURUSD").name == "FOMC"


def test_approaching_event_wins_over_nearer_but_stale_one():
    """Regression for the masking bug: a stale event 1200s in the past is
    numerically nearer to `now` than an approaching event 1500s out, but the
    approaching one is the actionable one and must be selected."""
    now = EVENT_TS
    stale = CalendarEvent(
        name="Stale NFP", importance="high", country="US",
        event_ts_ns=now - 1200 * SECOND, affects=["EURUSD"],
    )
    approaching = CalendarEvent(
        name="Approaching CPI", importance="high", country="US",
        event_ts_ns=now + 1500 * SECOND, affects=["EURUSD"],
    )
    clock = EventClock([stale, approaching])
    assert clock.relevant(now, "EURUSD").name == "Approaching CPI"
    assert clock.phase(now, "EURUSD") == "PRE_EVENT"


def test_event_window_outranks_a_temporally_nearer_pre_event():
    now = EVENT_TS
    in_window = CalendarEvent(
        name="InWindow", importance="high", country="US",
        event_ts_ns=now - 800 * SECOND, affects=["EURUSD"],
    )
    pre_event = CalendarEvent(
        name="PreEvent", importance="high", country="US",
        event_ts_ns=now + 100 * SECOND, affects=["EURUSD"],
    )
    clock = EventClock([in_window, pre_event])
    # pre_event is numerically nearer (100s vs 800s) but only PRE_EVENT;
    # in_window is farther but actionable right now, so it must win.
    assert clock.relevant(now, "EURUSD").name == "InWindow"
    assert clock.phase(now, "EURUSD") == "EVENT_WINDOW"


def test_same_phase_still_tie_breaks_to_the_nearer_event():
    now = EVENT_TS
    near = CalendarEvent(
        name="Near", importance="high", country="US",
        event_ts_ns=now - 1000 * SECOND, affects=["EURUSD"],
    )
    far = CalendarEvent(
        name="Far", importance="high", country="US",
        event_ts_ns=now - 2000 * SECOND, affects=["EURUSD"],
    )
    # Both are CLEAR (> 900s past); the nearer one must still win.
    clock = EventClock([near, far])
    assert clock.relevant(now, "EURUSD").name == "Near"


def test_phase_boundaries_have_no_gaps_or_overlaps():
    """Sweep delta from -2000s to +1000s in 10s steps: the phase must
    transition exactly at -1800, -60, and +900, with no gap or overlap
    between CLEAR / PRE_EVENT / EVENT_WINDOW at any sampled point."""
    step = 10 * SECOND
    seen_transitions = []
    previous_phase = None
    delta_s = -2000
    while delta_s <= 1000:
        now_ns = EVENT_TS + delta_s * SECOND
        phase = CLOCK.phase(now_ns, "EURUSD")

        if delta_s < -1800:
            expected = "CLEAR"
        elif delta_s < -60:
            expected = "PRE_EVENT"
        elif delta_s <= 900:
            expected = "EVENT_WINDOW"
        else:
            expected = "CLEAR"
        assert phase == expected, "delta={0}s phase={1} expected={2}".format(
            delta_s, phase, expected
        )

        if previous_phase is not None and phase != previous_phase:
            seen_transitions.append((delta_s, previous_phase, phase))
        previous_phase = phase
        delta_s += 10

    # Transitions are reported at the first *sampled* point of the new
    # phase. CLEAR->PRE_EVENT and PRE_EVENT->EVENT_WINDOW start exactly at
    # the inclusive boundary (-1800 and -60 are themselves multiples of the
    # 10s step). EVENT_WINDOW->CLEAR is different: delta=900 is still the
    # *last* EVENT_WINDOW point (`delta <= EVENT_EXIT_S`), so CLEAR begins
    # one 10s step later, at 910 -- there is no gap, just an asymmetric
    # (closed) upper bound on EVENT_WINDOW.
    boundaries = [delta for delta, _, _ in seen_transitions]
    assert boundaries == [-1800, -60, 900 + 10]


def test_event_alert_fires_once_on_clear_to_pre_event_transition():
    from marketspike.engine.bus import Bus
    from marketspike.engine.symbol_state import SymbolEngine
    from marketspike.feeds.base import Tick

    class FakeRecorder:
        def submit_tick(self, *args, **kwargs):
            return True

        def submit_regime(self, **kwargs):
            return True

    def tick_at(ts_ns):
        return Tick(
            symbol="EURUSD", venue_ts_ns=ts_ns, recv_ts_ns=ts_ns,
            bid=1.0999, ask=1.1001, bid_qty=1.0, ask_qty=1.0,
            tradeable=True, source="measured",
        )

    engine = SymbolEngine(
        symbol="EURUSD", bus=Bus(), recorder=FakeRecorder(),
        tau_fast_s=30.0, tau_slow_s=1800.0, skew_window_s=60.0,
        ws_max_hz=1000.0, event_clock=CLOCK,
    )
    sub = engine.bus.subscribe(maxlen=1000)

    # Far before the event: CLEAR, no alert.
    engine.on_tick(tick_at(EVENT_TS - 7200 * SECOND))
    # Crosses into PRE_EVENT: exactly one alert expected.
    engine.on_tick(tick_at(EVENT_TS - 1700 * SECOND))
    # Still PRE_EVENT on the next few ticks: no repeated alert.
    engine.on_tick(tick_at(EVENT_TS - 1600 * SECOND))
    engine.on_tick(tick_at(EVENT_TS - 1500 * SECOND))

    alerts = [f for f in sub.drain() if f["type"] == "event_alert"]
    assert len(alerts) == 1
    assert alerts[0]["phase"] == "PRE_EVENT"
    assert alerts[0]["name"] == "US CPI (YoY)"


def test_confidence_defaults_to_estimated_when_absent():
    event = CalendarEvent(
        name="X", importance="high", country="US",
        event_ts_ns=EVENT_TS, affects=["EURUSD"],
    )
    assert event.confidence == "estimated"


def test_load_events_reads_confidence_and_defaults_when_absent():
    payload = {
        "events": [
            {
                "name": "Confirmed Release", "importance": "high", "country": "US",
                "event_ts": "2026-09-04T12:30:00Z", "affects": ["EURUSD"],
                "confidence": "confirmed",
            },
            {
                "name": "Guessed Release", "importance": "medium", "country": "US",
                "event_ts": "2026-09-10T12:30:00Z", "affects": ["EURUSD"],
                # no confidence field at all
            },
        ]
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(payload, handle)
        path = handle.name
    try:
        events = load_events(path)
    finally:
        os.unlink(path)

    by_name = {event.name: event for event in events}
    assert by_name["Confirmed Release"].confidence == "confirmed"
    assert by_name["Guessed Release"].confidence == "estimated"


def test_static_events_json_marks_the_unsourced_ppi_date_as_estimated():
    """The Oct 15 PPI date could not be sourced against BLS/Fed and is an
    acknowledged guess; every other static event was verified. The JSON
    must say so explicitly rather than presenting them with equal
    precision."""
    events = load_events()
    by_key = {(event.name, event.event_ts_ns): event for event in events}
    oct_ppi = [
        event for (name, _), event in by_key.items()
        if name == "US PPI (MoM)"
    ]
    estimated = [event for event in oct_ppi if event.confidence == "estimated"]
    confirmed_events = [
        event for event in events
        if event not in estimated
    ]
    assert len(estimated) == 1, "expected exactly one estimated PPI entry"
    assert all(event.confidence == "confirmed" for event in confirmed_events)
