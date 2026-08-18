import json

import pytest

from marketspike.feeds import oanda
from marketspike.feeds.oanda import parse_price

PRICE = {
    "type": "PRICE",
    "time": "2026-08-17T14:23:01.123456789Z",
    "instrument": "EUR_USD",
    "bids": [{"price": "1.08512", "liquidity": 10000000}],
    "asks": [{"price": "1.08525", "liquidity": 10000000}],
    "status": "tradeable",
    "tradeable": True,
}


def test_parse_price_normalises_symbol_and_prices():
    tick = parse_price(PRICE, recv_ts_ns=1723891200_200_000_000)
    assert tick.symbol == "EURUSD"
    assert tick.bid == 1.08512
    assert tick.ask == 1.08525
    assert tick.bid_qty == 10000000.0
    assert tick.source == "measured"


def test_parse_price_preserves_nanosecond_venue_time():
    tick = parse_price(PRICE, recv_ts_ns=1)
    assert tick.venue_ts_ns % 1_000_000_000 == 123456789


def test_parse_price_skips_heartbeats():
    assert parse_price({"type": "HEARTBEAT", "time": "2026-08-17T14:23:01Z"}, 1) is None


def test_parse_price_marks_untradeable_when_market_closed():
    closed = dict(PRICE, tradeable=False, status="non-tradeable")
    assert parse_price(closed, recv_ts_ns=1).tradeable is False


def test_parse_price_returns_none_when_book_side_missing():
    assert parse_price(dict(PRICE, bids=[]), recv_ts_ns=1) is None


def test_parse_price_returns_none_when_time_key_missing():
    truncated = dict(PRICE)
    del truncated["time"]
    assert parse_price(truncated, recv_ts_ns=1) is None


def test_parse_price_returns_none_when_bid_price_key_missing():
    # The first bids entry is present but lacks its "price" field.
    malformed = dict(PRICE, bids=[{"liquidity": 10000000}])
    assert parse_price(malformed, recv_ts_ns=1) is None


def test_parse_price_tradeable_defaults_true_when_key_absent():
    frame = dict(PRICE)
    del frame["tradeable"]
    tick = parse_price(frame, recv_ts_ns=1)
    assert tick.tradeable is True


@pytest.mark.asyncio
async def test_stream_reconnects_after_transient_failure(monkeypatch):
    attempts = {"n": 0}
    heartbeat = {"type": "HEARTBEAT", "time": "2026-08-17T14:23:01Z"}
    lines = [json.dumps(PRICE), json.dumps(heartbeat)]

    class FakeResponse:
        def __init__(self, lines):
            self._lines = lines

        def raise_for_status(self):
            return None

        async def aiter_lines(self):
            for line in self._lines:
                yield line

    class FakeStreamCM:
        def __init__(self, lines):
            self._lines = lines

        async def __aenter__(self):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("simulated transient failure")
            return FakeResponse(self._lines)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeAsyncClient:
        def __init__(self, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def stream(self, method, url, params=None, headers=None):
            return FakeStreamCM(lines)

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(oanda.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(oanda.asyncio, "sleep", fake_sleep)

    adapter = oanda.OandaAdapter("EURUSD", token="tok", account_id="acct")
    ticks = []
    async for tick in adapter.stream():
        ticks.append(tick)
        break

    assert len(ticks) == 1
    assert ticks[0].bid == 1.08512
    assert ticks[0].ask == 1.08525
    assert attempts["n"] == 2
