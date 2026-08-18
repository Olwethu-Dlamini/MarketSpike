import math

from marketspike.feeds.base import rfc3339_to_ns
from marketspike.feeds.binance import parse_book_ticker, variance_per_second_from_closes

FRAME = {
    "stream": "btcusdt@bookTicker",
    "data": {
        "u": 400900217, "s": "BTCUSDT", "E": 1723891200123,
        "b": "63120.50", "B": "1.234", "a": "63121.90", "A": "0.876",
    },
}


def test_parse_book_ticker_maps_all_fields():
    tick = parse_book_ticker(FRAME, recv_ts_ns=1723891200_200_000_000)
    assert tick.symbol == "BTCUSDT"
    assert tick.venue_ts_ns == 1723891200123 * 1_000_000
    assert tick.bid == 63120.50
    assert tick.ask == 63121.90
    assert tick.bid_qty == 1.234
    assert tick.source == "measured"
    assert tick.tradeable is True


def test_parse_book_ticker_computes_mid_and_spread():
    tick = parse_book_ticker(FRAME, recv_ts_ns=1)
    assert tick.mid == (63120.50 + 63121.90) / 2
    assert abs(tick.spread - 1.40) < 1e-9


def test_parse_book_ticker_ignores_non_tick_frames():
    assert parse_book_ticker({"result": None, "id": 1}, recv_ts_ns=1) is None


def test_rfc3339_to_ns_keeps_nanosecond_precision():
    assert rfc3339_to_ns("2026-08-17T14:23:01.123456789Z") % 1_000_000_000 == 123456789


def test_rfc3339_to_ns_handles_absent_fraction():
    assert rfc3339_to_ns("2026-08-17T14:23:01Z") % 1_000_000_000 == 0


def test_variance_per_second_normalises_by_interval():
    # Constant 1% move each minute -> per-minute variance is (ln 1.01)^2.
    closes = [100.0 * (1.01 ** i) for i in range(11)]
    var_s = variance_per_second_from_closes(closes, interval_s=60.0)
    assert abs(var_s - (math.log(1.01) ** 2) / 60.0) < 1e-12
