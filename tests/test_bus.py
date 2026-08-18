import asyncio

from marketspike.engine.bus import Bus


async def test_publish_reaches_every_subscriber():
    bus = Bus()
    first = bus.subscribe()
    second = bus.subscribe()
    bus.publish({"type": "tick", "symbol": "BTCUSDT"})
    assert (await first.get())["symbol"] == "BTCUSDT"
    assert (await second.get())["symbol"] == "BTCUSDT"


async def test_slow_subscriber_drops_oldest_and_counts():
    bus = Bus()
    sub = bus.subscribe(maxlen=2)
    for i in range(5):
        bus.publish({"type": "tick", "n": i})
    assert sub.dropped == 3
    assert (await sub.get())["n"] == 3  # oldest dropped, newest retained


async def test_one_slow_subscriber_does_not_starve_another():
    bus = Bus()
    slow = bus.subscribe(maxlen=1)
    fast = bus.subscribe(maxlen=100)
    for i in range(10):
        bus.publish({"n": i})
    assert slow.dropped == 9
    assert fast.dropped == 0


async def test_unsubscribe_stops_delivery():
    bus = Bus()
    sub = bus.subscribe()
    bus.unsubscribe(sub)
    bus.publish({"n": 1})
    with_timeout = asyncio.wait_for(sub.get(), timeout=0.05)
    try:
        await with_timeout
        assert False, "unsubscribed subscriber received a frame"
    except asyncio.TimeoutError:
        pass


def test_sequence_numbers_are_monotonic():
    bus = Bus()
    assert [bus.next_seq() for _ in range(3)] == [1, 2, 3]


async def test_drain_returns_pending_frames_in_fifo_order_and_empties_subscription():
    bus = Bus()
    sub = bus.subscribe(maxlen=10)
    for i in range(4):
        bus.publish({"type": "tick", "n": i})

    drained = sub.drain()

    assert [frame["n"] for frame in drained] == [0, 1, 2, 3]
    assert sub.drain() == []
