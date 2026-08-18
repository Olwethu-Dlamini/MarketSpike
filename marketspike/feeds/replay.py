import asyncio
import glob
import json
import os
import time
from typing import AsyncIterator, Dict, List, Optional

from marketspike.feeds.base import Tick

FIELDS = (
    "symbol", "venue_ts_ns", "recv_ts_ns", "bid", "ask",
    "bid_qty", "ask_qty", "tradeable", "source",
)


def write_scenario(path: str, ticks: List[Tick]) -> int:
    """Serialise `ticks` to newline-delimited JSON, one object per line.

    Always writes `source: "simulated"`, even if the original tick's source
    was "measured" — a captured tick, once it becomes replay material, is no
    longer a live measurement (spec's non-negotiable honesty rule).
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w") as handle:
        for tick in ticks:
            handle.write(
                json.dumps(
                    {
                        "symbol": tick.symbol,
                        "venue_ts_ns": tick.venue_ts_ns,
                        "recv_ts_ns": tick.recv_ts_ns,
                        "bid": tick.bid, "ask": tick.ask,
                        "bid_qty": tick.bid_qty, "ask_qty": tick.ask_qty,
                        "tradeable": tick.tradeable,
                        "source": "simulated",
                    }
                )
                + "\n"
            )
    return len(ticks)


def read_scenario(path: str) -> List[Dict]:
    rows: List[Dict] = []
    with open(path, "r") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def list_scenarios(directory: str = "scenarios") -> List[str]:
    """Names (without extension) of every `*.ndjson` scenario on disk."""
    if not os.path.isdir(directory):
        return []
    return sorted(
        os.path.splitext(os.path.basename(p))[0]
        for p in glob.glob(os.path.join(directory, "*.ndjson"))
    )


class ReplayAdapter:
    """Emits recorded ticks through the identical engine code path.

    Demo mode is not a branch in the engine, it is a different adapter --
    which is what makes the replay trustworthy: the same logic runs,
    differing only in the `source` field (spec §5.1). `feeds/` must not
    import `api/` or `store/`, so this module knows nothing about the bus,
    the recorder, or REST; the caller (a REST route) is responsible for
    feeding yielded ticks into a `SymbolEngine`.
    """

    venue = "replay"

    def __init__(self, symbol: str, path: str, speed: float = 1.0) -> None:
        self.symbol = symbol
        self.path = path
        self.speed = max(speed, 0.01)
        self.connected = False
        self.progress_pct = 0.0
        self.scenario = os.path.splitext(os.path.basename(path))[0]

    async def seed_baseline(self) -> Optional[float]:
        """Replay never seeds a slow-horizon baseline of its own.

        The engine being replayed into is expected to already be seeded
        (from live capture) or to warm up naturally from the scenario's own
        calm lead-in, matching the same code path a live feed would take.
        """
        return None

    async def stream(self) -> AsyncIterator[Tick]:
        """Yield ticks paced to wall-clock time, rebased onto "now".

        Rows are stored with their original `venue_ts_ns`/`recv_ts_ns`; on
        replay both are rebased onto the current wall clock so the transit
        gap (recv - venue) each tick originally carried is preserved
        exactly. That keeps the skew estimator (spec §6.2) seeing a
        plausible, bounded excess-transit series instead of huge or
        negative values computed against a stale historical clock.
        """
        rows = read_scenario(self.path)
        if not rows:
            return
        self.connected = True
        base_ns = rows[0]["recv_ts_ns"]
        started_ns = time.time_ns()

        try:
            for index, row in enumerate(rows):
                offset_ns = int((row["recv_ts_ns"] - base_ns) / self.speed)
                due_ns = started_ns + offset_ns
                delay_s = (due_ns - time.time_ns()) / 1e9
                if delay_s > 0:
                    await asyncio.sleep(delay_s)

                now_ns = time.time_ns()
                transit_ns = max(0, row["recv_ts_ns"] - row["venue_ts_ns"])
                self.progress_pct = (index + 1) / len(rows) * 100.0

                yield Tick(
                    symbol=self.symbol,
                    venue_ts_ns=now_ns - transit_ns,
                    recv_ts_ns=now_ns,
                    bid=row["bid"], ask=row["ask"],
                    bid_qty=row.get("bid_qty", 0.0), ask_qty=row.get("ask_qty", 0.0),
                    tradeable=bool(row.get("tradeable", True)),
                    source="simulated",
                )
        finally:
            self.connected = False
