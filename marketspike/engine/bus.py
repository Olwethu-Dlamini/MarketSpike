import asyncio
from collections import deque
from statistics import median as _median
from typing import Any, Deque, Dict, List


class Subscription:
    """A bounded per-client mailbox.

    One slow browser must not add latency for every other client, so each
    subscriber drops its own oldest frames rather than blocking the publisher
    (spec §4.2).
    """

    def __init__(self, maxlen: int = 200) -> None:
        self._queue: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._event = asyncio.Event()
        self.dropped = 0

    def push(self, frame: Dict[str, Any]) -> bool:
        overflowed = len(self._queue) == self._queue.maxlen
        if overflowed:
            self.dropped += 1
        self._queue.append(frame)
        self._event.set()
        return not overflowed

    async def get(self) -> Dict[str, Any]:
        while not self._queue:
            self._event.clear()
            await self._event.wait()
        return self._queue.popleft()

    def drain(self) -> List[Dict[str, Any]]:
        """Remove and return all currently-pending frames, in FIFO order.

        Returns an empty list when the mailbox has nothing pending. This is
        the public alternative to reaching into `_queue` directly.
        """
        frames = list(self._queue)
        self._queue.clear()
        return frames


class Bus:
    """In-process fan-out.

    Publishing is synchronous and non-blocking. Swapping this for Redis
    pub/sub is a one-file change (spec §4.1).
    """

    def __init__(self) -> None:
        self._subscribers: List[Subscription] = []
        self._seq = 0
        # Recent one-way client delivery samples (microseconds), bounded so
        # a long-lived connection doesn't grow this without limit and so a
        # stale sample from long ago doesn't outvote current conditions.
        self._delivery: Deque[int] = deque(maxlen=64)

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def subscribe(self, maxlen: int = 200) -> Subscription:
        sub = Subscription(maxlen=maxlen)
        self._subscribers.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subscribers:
            self._subscribers.remove(sub)

    def publish(self, frame: Dict[str, Any]) -> None:
        for sub in self._subscribers:
            sub.push(frame)

    @property
    def total_dropped(self) -> int:
        return sum(sub.dropped for sub in self._subscribers)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def record_delivery(self, delivery_us: int) -> None:
        """Record one client-reported one-way delivery sample (microseconds).

        Negative values (a clock-sync artefact, never a real duration) are
        dropped rather than recorded.
        """
        if delivery_us >= 0:
            self._delivery.append(delivery_us)

    @property
    def delivery_us(self) -> Any:
        """Median of recent client delivery samples, or None before any.

        This is engine/api's seam for delivery latency: the WebSocket
        handshake (api/ws.py) is the only place with the client's own
        timestamps, and it reports here via `record_delivery()` rather than
        engine/ importing api/.
        """
        if not self._delivery:
            return None
        return int(_median(self._delivery))
