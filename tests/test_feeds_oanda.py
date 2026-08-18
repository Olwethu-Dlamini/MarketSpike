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
