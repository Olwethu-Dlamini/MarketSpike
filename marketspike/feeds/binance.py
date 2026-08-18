import asyncio
import json
import math
import random
import time
from typing import AsyncIterator, Dict, List, Optional

import httpx
import websockets

from marketspike.feeds.base import Tick

# NOTE ON THE DEPTH SIDE-CHANNEL:
# Binance's bookTicker stream (verified empirically against the live venue)
# does NOT emit an "E" (event time) field -- its payload keys are exactly
# ['A','B','a','b','s','u']. bookTicker is still the right stream to drive
# ticks/prices (it's the tightest top-of-book feed), but it gives us no way
# to recover venue-side timing on its own.
#
# depth@100ms frames, in contrast, DO carry "E" (keys include 'E','U','a',
# 'b','e','s','u'). We subscribe to depth@100ms on the SAME combined socket
# purely as a timing side-channel: we read its "E" field to measure the
# connection's raw transit latency (recv_ts_ns - venue_ts_ns) and then
# discard the frame entirely -- no order-book snapshot, no diff
# application, no state. Transit latency is a property of the connection,
# not of an individual quote, so sampling it at 10 Hz on the same socket
# and applying it to bookTicker ticks is legitimate.
#
# Do NOT delete this subscription as "unused" -- it is the only source of
# venue timing for every tick this adapter emits.
WS_URL = "wss://stream.binance.com:9443/stream?streams={0}@bookTicker/{0}@depth@100ms"
KLINES_URL = "https://api.binance.com/api/v3/klines"


def parse_book_ticker(raw: Dict, recv_ts_ns: int, venue_ts_ns: int) -> Optional[Tick]:
    data = raw.get("data", raw)
    required_keys = ("s", "b", "a", "B", "A")
    if any(key not in data for key in required_keys):
        return None
    return Tick(
        symbol=data["s"],
        venue_ts_ns=venue_ts_ns,
        recv_ts_ns=recv_ts_ns,
        bid=float(data["b"]),
        ask=float(data["a"]),
        bid_qty=float(data["B"]),
        ask_qty=float(data["A"]),
        tradeable=True,
        source="measured",
    )


def parse_depth_event_ts_ns(raw: Dict) -> Optional[int]:
    """Extract venue event time (ns) from a depth@100ms frame, else None.

    This is the timing side-channel described above: depth frames are never
    used for order-book state, only for their "E" field.
    """
    data = raw.get("data", raw)
    if data.get("e") != "depthUpdate" or "E" not in data:
        return None
    return int(data["E"]) * 1_000_000


def variance_per_second_from_closes(closes: List[float], interval_s: float) -> float:
    """Mean squared log return divided by the bar interval (§7.2)."""
    if len(closes) < 2 or interval_s <= 0:
        return 0.0
    returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    if not returns:
        return 0.0
    return (sum(r * r for r in returns) / len(returns)) / interval_s


class BinanceAdapter:
    venue = "binance"

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.connected = False
        self._last_raw_transit_ns = None  # type: Optional[int]

    async def seed_baseline(self) -> Optional[float]:
        params = {"symbol": self.symbol, "interval": "1m", "limit": 1440}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(KLINES_URL, params=params)
                response.raise_for_status()
                closes = [float(row[4]) for row in response.json()]
        except Exception:
            return None
        return variance_per_second_from_closes(closes, interval_s=60.0)

    async def stream(self) -> AsyncIterator[Tick]:
        url = WS_URL.format(self.symbol.lower())
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as socket:
                    self.connected = True
                    backoff = 1.0
                    async for message in socket:
                        recv_ts_ns = time.time_ns()
                        raw = json.loads(message)
                        depth_venue_ts_ns = parse_depth_event_ts_ns(raw)
                        if depth_venue_ts_ns is not None:
                            self._last_raw_transit_ns = recv_ts_ns - depth_venue_ts_ns
                            continue
                        if self._last_raw_transit_ns is None:
                            venue_ts_ns = recv_ts_ns
                        else:
                            venue_ts_ns = recv_ts_ns - self._last_raw_transit_ns
                        tick = parse_book_ticker(raw, recv_ts_ns, venue_ts_ns)
                        if tick is not None:
                            yield tick
            except asyncio.CancelledError:
                raise
            except Exception:
                self.connected = False
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30.0)
