import json
import math

import pytest

from marketspike.feeds import binance
from marketspike.feeds.base import Tick, rfc3339_to_ns
from marketspike.feeds.binance import (
    parse_book_ticker,
    parse_depth_event_ts_ns,
    variance_per_second_from_closes,
)

# Real bookTicker payload (verified empirically against the live venue):
# Binance's bookTicker stream carries NO "E" (event time) field. Its keys
# are exactly ['A','B','a','b','s','u']. A fixture that fabricates an "E"
# key (as this file previously did) hides the exact regression that shipped.
FRAME = {
    "stream": "btcusdt@bookTicker",
    "data": {
        "u": 400900217, "s": "BTCUSDT",
        "b": "63120.50", "B": "1.234", "a": "63121.90", "A": "0.876",
    },
}

DEPTH_FRAME = {
    "stream": "btcusdt@depth@100ms",
    "data": {
        "e": "depthUpdate", "E": 1723891200123, "s": "BTCUSDT",
        "U": 100, "u": 101, "b": [], "a": [],
    },
}


def test_parse_book_ticker_maps_all_fields():
    tick = parse_book_ticker(
        FRAME, recv_ts_ns=1723891200_200_000_000, venue_ts_ns=1723891200123 * 1_000_000
    )
    assert tick.symbol == "BTCUSDT"
    assert tick.venue_ts_ns == 1723891200123 * 1_000_000
    assert tick.bid == 63120.50
    assert tick.ask == 63121.90
    assert tick.bid_qty == 1.234
    assert tick.source == "measured"
    assert tick.tradeable is True


def test_parse_book_ticker_computes_mid_and_spread():
    tick = parse_book_ticker(FRAME, recv_ts_ns=1, venue_ts_ns=1)
    assert tick.mid == (63120.50 + 63121.90) / 2
    assert abs(tick.spread - 1.40) < 1e-9


def test_parse_book_ticker_ignores_non_tick_frames():
    assert parse_book_ticker({"result": None, "id": 1}, recv_ts_ns=1, venue_ts_ns=1) is None


def test_parse_book_ticker_returns_none_for_partial_frame():
    # "a" (ask price) is missing even though s/b are present.
    partial = {
        "data": {
            "u": 400900217, "s": "BTCUSDT",
            "b": "63120.50", "B": "1.234", "A": "0.876",
        },
    }
    assert parse_book_ticker(partial, recv_ts_ns=1, venue_ts_ns=1) is None


def test_parse_book_ticker_parses_real_payload_with_no_event_time_field():
    # Regression test: this is the exact payload shape that shipped broken.
    # bookTicker never carries "E"; the parser must not require it.
    real_payload = {
        "u": 400900217, "s": "BTCUSDT",
        "b": "63120.50", "B": "1.234", "a": "63121.90", "A": "0.876",
    }
    assert "E" not in real_payload
    tick = parse_book_ticker(real_payload, recv_ts_ns=42, venue_ts_ns=7)
    assert tick is not None
    assert tick.symbol == "BTCUSDT"
    assert tick.bid == 63120.50
    assert tick.ask == 63121.90
    assert tick.venue_ts_ns == 7
    assert tick.recv_ts_ns == 42


def test_parse_depth_event_ts_ns_converts_ms_to_ns():
    assert parse_depth_event_ts_ns(DEPTH_FRAME) == 1723891200123 * 1_000_000


def test_parse_depth_event_ts_ns_returns_none_for_book_ticker_frame():
    assert parse_depth_event_ts_ns(FRAME) is None


def test_rfc3339_to_ns_keeps_nanosecond_precision():
    assert rfc3339_to_ns("2026-08-17T14:23:01.123456789Z") % 1_000_000_000 == 123456789


def test_rfc3339_to_ns_handles_absent_fraction():
    assert rfc3339_to_ns("2026-08-17T14:23:01Z") % 1_000_000_000 == 0


def test_rfc3339_to_ns_truncates_fraction_beyond_nine_digits():
    assert rfc3339_to_ns("2026-08-17T14:23:01.1234567891Z") % 1_000_000_000 == 123456789


def test_rfc3339_to_ns_handles_zero_numeric_offset_same_as_z():
    z_form = rfc3339_to_ns("2026-08-17T14:23:01.123456789Z")
    offset_form = rfc3339_to_ns("2026-08-17T14:23:01.123456789+00:00")
    assert offset_form == z_form


def test_rfc3339_to_ns_handles_negative_numeric_offset():
    assert rfc3339_to_ns("2026-08-17T16:23:01-02:00") == rfc3339_to_ns("2026-08-17T18:23:01Z")


def test_variance_per_second_normalises_by_interval():
    # Constant 1% move each minute -> per-minute variance is (ln 1.01)^2.
    closes = [100.0 * (1.01 ** i) for i in range(11)]
    var_s = variance_per_second_from_closes(closes, interval_s=60.0)
    assert abs(var_s - (math.log(1.01) ** 2) / 60.0) < 1e-12


def _tick(**overrides):
    fields = dict(
        symbol="BTCUSDT",
        venue_ts_ns=1,
        recv_ts_ns=1,
        bid=100.0,
        ask=101.0,
        bid_qty=2.0,
        ask_qty=1.0,
        tradeable=True,
        source="measured",
    )
    fields.update(overrides)
    return Tick(**fields)


def test_tick_spread_bps_computed_against_mid():
    tick = _tick(bid=100.0, ask=101.0)
    expected = (1.0 / 100.5) * 10000.0
    assert abs(tick.spread_bps - expected) < 1e-9


def test_tick_spread_bps_zero_mid_guard():
    tick = _tick(bid=0.0, ask=0.0)
    assert tick.spread_bps == 0.0


def test_tick_book_imbalance_normal_case():
    tick = _tick(bid_qty=3.0, ask_qty=1.0)
    assert abs(tick.book_imbalance - 0.5) < 1e-9


def test_tick_book_imbalance_zero_total_guard():
    tick = _tick(bid_qty=0.0, ask_qty=0.0)
    assert tick.book_imbalance == 0.0


@pytest.mark.asyncio
async def test_stream_reconnects_after_transient_failure(monkeypatch):
    attempts = {"n": 0}

    class FakeSocket:
        def __init__(self, messages):
            self._it = iter(messages)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self._it)
            except StopIteration:
                raise StopAsyncIteration

    class FakeConnectCM:
        async def __aenter__(self):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("simulated transient failure")
            # Combined-stream envelope: a depth@100ms frame (the timing
            # side-channel) followed by a bookTicker frame with no "E".
            return FakeSocket([json.dumps(DEPTH_FRAME), json.dumps(FRAME)])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(url, ping_interval=20):
        return FakeConnectCM()

    async def fake_sleep(_seconds):
        return None

    recv_times_ns = iter([2_000_000_000, 5_000_000_000])
    monkeypatch.setattr(binance.time, "time_ns", lambda: next(recv_times_ns))
    monkeypatch.setattr(binance.websockets, "connect", fake_connect)
    monkeypatch.setattr(binance.asyncio, "sleep", fake_sleep)

    adapter = binance.BinanceAdapter("BTCUSDT")
    ticks = []
    async for tick in adapter.stream():
        ticks.append(tick)
        break

    assert len(ticks) == 1
    assert ticks[0].symbol == "BTCUSDT"
    assert attempts["n"] == 2

    # The depth frame arrived at recv_ts_ns=2_000_000_000 with venue "E" of
    # 1723891200123ms -> raw transit = 2_000_000_000 - 1723891200123_000_000.
    depth_venue_ts_ns = 1723891200123 * 1_000_000
    expected_transit_ns = 2_000_000_000 - depth_venue_ts_ns
    # The bookTicker frame arrived at recv_ts_ns=5_000_000_000; its
    # synthesised venue_ts_ns must reflect that measured transit, not
    # simply equal recv_ts_ns.
    expected_venue_ts_ns = 5_000_000_000 - expected_transit_ns
    assert ticks[0].venue_ts_ns == expected_venue_ts_ns
    assert ticks[0].venue_ts_ns != ticks[0].recv_ts_ns
    assert ticks[0].recv_ts_ns == 5_000_000_000
