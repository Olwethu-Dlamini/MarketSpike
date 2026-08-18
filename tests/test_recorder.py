import pytest

from marketspike.feeds.base import Tick
from marketspike.store.db import apply_schema, open_db
from marketspike.store.recorder import Recorder


def make_tick(ts=1):
    return Tick(
        symbol="BTCUSDT", venue_ts_ns=ts, recv_ts_ns=ts + 1000,
        bid=100.0, ask=100.2, bid_qty=1.0, ask_qty=2.0,
        tradeable=True, source="measured",
    )


@pytest.fixture
def recorder(tmp_path):
    conn = open_db(str(tmp_path / "r.db"))
    apply_schema(conn)
    return Recorder(conn, max_queue=4, batch_size=2)


async def test_flush_persists_submitted_ticks(recorder):
    recorder.submit_tick(make_tick(1), excess_transit_us=10, engine_us=5)
    recorder.submit_tick(make_tick(2), excess_transit_us=20, engine_us=6)
    await recorder.flush_once()
    rows = list(recorder.conn.execute("SELECT venue_ts_ns, excess_transit_us FROM ticks"))
    assert [(r[0], r[1]) for r in rows] == [(1, 10), (2, 20)]


async def test_queue_overflow_drops_and_counts(recorder):
    for i in range(10):
        recorder.submit_tick(make_tick(i), excess_transit_us=0, engine_us=0)
    assert recorder.counters["recorder_dropped_total"] == 6


async def test_submit_returns_false_when_dropped(recorder):
    results = [
        recorder.submit_tick(make_tick(i), excess_transit_us=0, engine_us=0)
        for i in range(6)
    ]
    assert results[:4] == [True, True, True, True]
    assert results[4:] == [False, False]


async def test_regime_events_persist(recorder):
    recorder.submit_regime(
        ts_ns=5, symbol="EURUSD", from_state="NORMAL", to_state="ELEVATED",
        score=1.6, v_ratio=2.1, spread_z=1.2, trigger="vol_ratio",
        event_context="CLEAR",
    )
    await recorder.flush_once()
    row = recorder.conn.execute(
        "SELECT to_state, trigger FROM regime_events"
    ).fetchone()
    assert row[0] == "ELEVATED"
    assert row[1] == "vol_ratio"
