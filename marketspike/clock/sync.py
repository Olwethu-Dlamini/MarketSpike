from collections import deque
from typing import Deque, Optional, Tuple


def compute_sync(
    client_send_ns: int,
    server_recv_ns: int,
    server_send_ns: int,
    client_recv_ns: int,
) -> Tuple[int, int]:
    """Standard NTP four-timestamp exchange (spec §6.3).

    Unlike the venue path, both endpoints cooperate here, so absolute offset
    and round-trip are separately recoverable.
    """
    round_trip = (client_recv_ns - client_send_ns) - (server_send_ns - server_recv_ns)
    offset = ((server_recv_ns - client_send_ns) + (server_send_ns - client_recv_ns)) // 2
    return round_trip, offset


class SyncFilter:
    """Keeps the least-delayed recent sample.

    The sample with the lowest round-trip carries the least path asymmetry and
    therefore the least offset error — NTP's own clock filter.
    """

    def __init__(self, keep: int = 8) -> None:
        self._samples: Deque[Tuple[int, int]] = deque(maxlen=keep)

    def add(self, round_trip_ns: int, offset_ns: int) -> None:
        self._samples.append((round_trip_ns, offset_ns))

    def _best(self) -> Optional[Tuple[int, int]]:
        return min(self._samples) if self._samples else None

    @property
    def best_offset_ns(self) -> Optional[int]:
        best = self._best()
        return best[1] if best else None

    @property
    def best_round_trip_ns(self) -> Optional[int]:
        best = self._best()
        return best[0] if best else None
