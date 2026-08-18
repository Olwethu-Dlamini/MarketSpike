from marketspike.engine.bus import Bus


def test_delivery_is_none_before_any_client_reports():
    assert Bus().delivery_us is None


def test_delivery_is_the_median_of_recent_samples():
    bus = Bus()
    for value in (10_000, 20_000, 30_000):
        bus.record_delivery(value)
    assert bus.delivery_us == 20_000


def test_delivery_window_is_bounded():
    bus = Bus()
    for value in range(1000):
        bus.record_delivery(value)
    assert bus.delivery_us is not None
    assert bus.delivery_us > 900  # old samples evicted, recent ones dominate


def test_negative_samples_are_dropped():
    bus = Bus()
    bus.record_delivery(-5)
    assert bus.delivery_us is None
