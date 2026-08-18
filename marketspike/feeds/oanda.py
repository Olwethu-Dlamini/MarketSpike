import asyncio
import json
import random
import time
from typing import AsyncIterator, Dict, List, Optional

import httpx

from marketspike.feeds.base import Tick, rfc3339_to_ns
from marketspike.feeds.binance import variance_per_second_from_closes

STREAM_URL = "https://stream-fxpractice.oanda.com/v3/accounts/{0}/pricing/stream"
CANDLES_URL = "https://api-fxpractice.oanda.com/v3/instruments/{0}/candles"


def _to_instrument(symbol: str) -> str:
    return symbol[:3] + "_" + symbol[3:] if "_" not in symbol else symbol


def parse_price(raw: Dict, recv_ts_ns: int) -> Optional[Tick]:
    if raw.get("type") != "PRICE":
        return None
    required_keys = ("time", "instrument", "bids", "asks")
    if any(key not in raw for key in required_keys):
        return None
    bids: List[Dict] = raw["bids"] or []
    asks: List[Dict] = raw["asks"] or []
    if not bids or not asks:
        return None
    bid0 = bids[0]
    ask0 = asks[0]
    if "price" not in bid0 or "price" not in ask0:
        return None
    return Tick(
        symbol=raw["instrument"].replace("_", ""),
        venue_ts_ns=rfc3339_to_ns(raw["time"]),
        recv_ts_ns=recv_ts_ns,
        bid=float(bid0["price"]),
        ask=float(ask0["price"]),
        bid_qty=float(bid0.get("liquidity", 0.0)),
        ask_qty=float(ask0.get("liquidity", 0.0)),
        tradeable=bool(raw.get("tradeable", True)),
        source="measured",
    )


class OandaAdapter:
    venue = "oanda"

    def __init__(self, symbol: str, token: str, account_id: str) -> None:
        self.symbol = symbol
        self.instrument = _to_instrument(symbol)
        self._token = token
        self._account_id = account_id
        self.connected = False

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer {0}".format(self._token)}

    async def seed_baseline(self) -> Optional[float]:
        params = {"price": "M", "granularity": "M1", "count": 1440}
        url = CANDLES_URL.format(self.instrument)
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, params=params, headers=self._headers())
                response.raise_for_status()
                candles = response.json().get("candles", [])
                closes = [
                    float(c["mid"]["c"]) for c in candles if c.get("complete")
                ]
        except Exception:
            return None
        return variance_per_second_from_closes(closes, interval_s=60.0)

    async def stream(self) -> AsyncIterator[Tick]:
        url = STREAM_URL.format(self._account_id)
        params = {"instruments": self.instrument}
        backoff = 1.0
        while True:
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream(
                        "GET", url, params=params, headers=self._headers()
                    ) as response:
                        response.raise_for_status()
                        self.connected = True
                        backoff = 1.0
                        async for line in response.aiter_lines():
                            if not line.strip():
                                continue
                            recv_ts_ns = time.time_ns()
                            tick = parse_price(json.loads(line), recv_ts_ns)
                            if tick is not None:
                                yield tick
            except asyncio.CancelledError:
                raise
            except Exception:
                self.connected = False
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30.0)
