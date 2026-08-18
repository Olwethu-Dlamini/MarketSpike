import asyncio
import json
import math
import random
import time
from typing import AsyncIterator, Dict, List, Optional

import httpx
import websockets

from marketspike.feeds.base import Tick

WS_URL = "wss://stream.binance.com:9443/stream?streams={0}@bookTicker"
KLINES_URL = "https://api.binance.com/api/v3/klines"


def parse_book_ticker(raw: Dict, recv_ts_ns: int) -> Optional[Tick]:
    data = raw.get("data", raw)
    required_keys = ("s", "E", "b", "a", "B", "A")
    if any(key not in data for key in required_keys):
        return None
    return Tick(
        symbol=data["s"],
        venue_ts_ns=int(data["E"]) * 1_000_000,
        recv_ts_ns=recv_ts_ns,
        bid=float(data["b"]),
        ask=float(data["a"]),
        bid_qty=float(data["B"]),
        ask_qty=float(data["A"]),
        tradeable=True,
        source="measured",
    )


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
                        tick = parse_book_ticker(json.loads(message), recv_ts_ns)
                        if tick is not None:
                            yield tick
            except asyncio.CancelledError:
                raise
            except Exception:
                self.connected = False
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30.0)
