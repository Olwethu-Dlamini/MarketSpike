import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

try:
    from typing import Protocol
except ImportError:  # pragma: no cover - Python 3.7 fallback
    Protocol = object  # type: ignore


@dataclass(frozen=True)
class Tick:
    symbol: str
    venue_ts_ns: int
    recv_ts_ns: int
    bid: float
    ask: float
    bid_qty: float
    ask_qty: float
    tradeable: bool
    source: str

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def spread_bps(self) -> float:
        mid = self.mid
        return 0.0 if mid <= 0 else (self.spread / mid) * 10000.0

    @property
    def book_imbalance(self) -> float:
        total = self.bid_qty + self.ask_qty
        return 0.0 if total <= 0 else (self.bid_qty - self.ask_qty) / total


_OFFSET_RE = re.compile(r"([+-])(\d{2}):(\d{2})$")


def rfc3339_to_ns(value: str) -> int:
    """Parse an RFC3339 timestamp to integer nanoseconds.

    datetime cannot represent nanoseconds, so the fractional part is handled
    separately rather than through strptime. A trailing "Z" or a numeric
    UTC offset (e.g. "+00:00", "-05:30") is both accepted; the offset is
    applied to shift the result to UTC.
    """
    text = value.strip()
    offset_seconds = 0
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1]
    else:
        match = _OFFSET_RE.search(text)
        if match:
            sign, hours, minutes = match.groups()
            offset_seconds = int(hours) * 3600 + int(minutes) * 60
            if sign == "-":
                offset_seconds = -offset_seconds
            text = text[: match.start()]
    head, _, frac = text.partition(".")
    moment = datetime.strptime(head, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    total = (int(moment.timestamp()) - offset_seconds) * 1_000_000_000
    if frac:
        total += int(frac.ljust(9, "0")[:9])
    return total


class FeedAdapter(Protocol):
    symbol: str
    venue: str

    def stream(self) -> AsyncIterator[Tick]:
        """Yield normalised ticks until cancelled.

        Must not raise on transient network failure — reconnect internally
        with backoff.
        """
        ...

    async def seed_baseline(self) -> Optional[float]:
        """Return initial slow-horizon variance per second, or None."""
        ...
