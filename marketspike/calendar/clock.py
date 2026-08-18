import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

from marketspike.feeds.base import rfc3339_to_ns

PRE_EVENT_LEAD_S = 1800
EVENT_ENTER_S = 60
EVENT_EXIT_S = 900

CLEAR = "CLEAR"
PRE_EVENT = "PRE_EVENT"
EVENT_WINDOW = "EVENT_WINDOW"

_PATH = os.path.join(os.path.dirname(__file__), "static_events.json")

# Priority used to pick the *actionable* event when more than one affects a
# symbol: an event inside its window outranks one merely approaching, which
# outranks one that is neither (stale or too far out) -- see EventClock.relevant.
_PHASE_RANK = {"EVENT_WINDOW": 0, "PRE_EVENT": 1, "CLEAR": 2}


def _phase_for_delta(delta_s: float) -> str:
    """The single implementation of the phase boundary logic, for one event
    at one instant. `EventClock.phase` and the ranking inside `relevant`
    both call this so the boundaries can never drift apart between them."""
    if -EVENT_ENTER_S <= delta_s <= EVENT_EXIT_S:
        return EVENT_WINDOW
    if -PRE_EVENT_LEAD_S <= delta_s < -EVENT_ENTER_S:
        return PRE_EVENT
    return CLEAR


@dataclass(frozen=True)
class CalendarEvent:
    name: str
    importance: str
    country: str
    event_ts_ns: int
    affects: List[str] = field(default_factory=list)
    # "confirmed" (verified against a primary source, e.g. BLS/Fed) or
    # "estimated" (best guess, not sourced). Defaults to the weaker claim
    # so an unmarked entry is never presented as more certain than it is.
    confidence: str = "estimated"


def load_events(path: Optional[str] = None) -> List[CalendarEvent]:
    with open(path or _PATH, "r") as handle:
        raw = json.load(handle)
    return [
        CalendarEvent(
            name=entry["name"],
            importance=entry.get("importance", "medium"),
            country=entry.get("country", ""),
            event_ts_ns=rfc3339_to_ns(entry["event_ts"]),
            affects=list(entry.get("affects") or []),
            confidence=entry.get("confidence", "estimated"),
        )
        for entry in raw.get("events", [])
    ]


class EventClock:
    """Forward-looking context that price-derived regime cannot supply (§7.6).

    Regime detection is derived from price and is therefore backward-looking
    by construction -- it cannot tell a trader that a CPI print is thirty
    minutes away. Event context is carried orthogonally to the price-derived
    regime for exactly that reason.
    """

    def __init__(self, events: List[CalendarEvent]) -> None:
        self._events = sorted(events, key=lambda event: event.event_ts_ns)

    def _for_symbol(self, symbol: str) -> List[CalendarEvent]:
        return [event for event in self._events if symbol in event.affects]

    def relevant(self, now_ns: int, symbol: str) -> Optional[CalendarEvent]:
        candidates = self._for_symbol(symbol)
        if not candidates:
            return None

        def rank(event: CalendarEvent):
            delta_s = (now_ns - event.event_ts_ns) / 1e9
            phase = _phase_for_delta(delta_s)
            # Actionability first (EVENT_WINDOW < PRE_EVENT < CLEAR), then
            # nearest-in-absolute-time as the tie-break among equals -- this
            # is what stops a just-passed event from masking one that is
            # about to happen (see clock.py module docs / review finding).
            return (_PHASE_RANK[phase], abs(now_ns - event.event_ts_ns))

        return min(candidates, key=rank)

    def signed_seconds(self, now_ns: int, symbol: str) -> float:
        event = self.relevant(now_ns, symbol)
        if event is None:
            return float(PRE_EVENT_LEAD_S)
        delta = (now_ns - event.event_ts_ns) / 1e9
        return max(-float(PRE_EVENT_LEAD_S), min(float(PRE_EVENT_LEAD_S), delta))

    def phase(self, now_ns: int, symbol: str) -> str:
        event = self.relevant(now_ns, symbol)
        if event is None:
            return CLEAR
        delta = (now_ns - event.event_ts_ns) / 1e9
        return _phase_for_delta(delta)

    def upcoming(
        self, now_ns: int, hours: float, symbol: Optional[str] = None
    ) -> List[CalendarEvent]:
        horizon_ns = now_ns + int(hours * 3600 * 1e9)
        return [
            event
            for event in self._events
            if now_ns <= event.event_ts_ns <= horizon_ns
            and (symbol is None or symbol in event.affects)
        ]
