import asyncio
import sqlite3

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


async def test_write_failure_is_counted_and_does_not_raise(recorder):
    recorder.submit_tick(make_tick(1), excess_transit_us=0, engine_us=0)
    recorder.submit_tick(make_tick(2), excess_transit_us=0, engine_us=0)

    real_write = recorder._write
    calls = {"n": 0}

    def failing_write(batch):
        calls["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    recorder._write = failing_write
    written = await recorder.flush_once()
    assert written == 0
    assert calls["n"] == 1
    assert recorder.counters["recorder_write_failed_total"] == 2
    assert recorder.counters["recorder_written_total"] == 0

    # A subsequent successful flush still works.
    recorder._write = real_write
    recorder.submit_tick(make_tick(3), excess_transit_us=0, engine_us=0)
    written = await recorder.flush_once()
    assert written == 1
    assert recorder.counters["recorder_written_total"] == 1
    rows = list(recorder.conn.execute("SELECT venue_ts_ns FROM ticks"))
    assert [r[0] for r in rows] == [3]


async def test_run_survives_failing_flush(recorder):
    recorder.submit_tick(make_tick(1), excess_transit_us=0, engine_us=0)

    def failing_write(batch):
        raise sqlite3.OperationalError("database is locked")

    recorder._write = failing_write
    recorder._flush_interval_s = 0.01

    task = asyncio.ensure_future(recorder.run())
    await asyncio.sleep(0.05)
    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert recorder.counters["recorder_write_failed_total"] >= 1


async def test_concurrent_flush_once_calls_are_serialised(tmp_path):
    conn = open_db(str(tmp_path / "concurrent.db"))
    apply_schema(conn)
    recorder = Recorder(conn, max_queue=1000, batch_size=25)

    total_rows = 100
    for i in range(total_rows):
        assert recorder.submit_tick(make_tick(i), excess_transit_us=0, engine_us=0)

    await asyncio.gather(
        recorder.flush_once(),
        recorder.flush_once(),
        recorder.flush_once(),
        recorder.flush_once(),
        recorder.flush_once(),
    )

    rows = list(recorder.conn.execute("SELECT venue_ts_ns FROM ticks"))
    assert len(rows) == total_rows
    assert len(set(r[0] for r in rows)) == total_rows
    assert recorder.counters["recorder_written_total"] == total_rows


async def test_partial_batch_flush_updates_written_total(recorder):
    # batch_size=2 on the fixture recorder; submit 3 ticks so the queue is
    # not a multiple of batch_size and the final flush is a partial batch.
    recorder.submit_tick(make_tick(1), excess_transit_us=0, engine_us=0)
    recorder.submit_tick(make_tick(2), excess_transit_us=0, engine_us=0)
    recorder.submit_tick(make_tick(3), excess_transit_us=0, engine_us=0)

    first = await recorder.flush_once()
    assert first == 2
    second = await recorder.flush_once()
    assert second == 1

    assert recorder.counters["recorder_written_total"] == 3
    rows = list(recorder.conn.execute("SELECT venue_ts_ns FROM ticks"))
    assert len(rows) == 3
