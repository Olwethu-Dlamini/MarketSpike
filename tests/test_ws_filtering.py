from marketspike.api.ws import CHANNEL_FOR_TYPE, DEFAULT_CHANNELS, _coerce_int


def test_channel_for_type_covers_all_published_frame_types():
    """Every frame type that SymbolEngine/replay publish must be mapped."""
    required = {
        "tick",
        "latency",
        "regime_change",
        "event_alert",
        "market_state",
        "replay_state",
    }
    assert required <= set(CHANNEL_FOR_TYPE.keys())


def test_unmapped_frame_type_is_deliverable_by_construction():
    """A frame type absent from CHANNEL_FOR_TYPE has no channel to gate on,
    so the pump()'s `channel is None or channel in channels` rule always
    delivers it. Pin the forward-compatibility contract at the data level.
    """
    unmapped_kind = "some_future_frame_type"
    assert unmapped_kind not in CHANNEL_FOR_TYPE

    channel = CHANNEL_FOR_TYPE.get(unmapped_kind)
    channels = set()  # even with nothing subscribed...
    assert channel is None or channel in channels
    assert channel is None  # ...delivery happens because there's no channel to gate on


def test_default_channels_equals_mapped_values():
    assert DEFAULT_CHANNELS == frozenset(CHANNEL_FOR_TYPE.values())


def test_coerce_int_returns_int_for_valid_field():
    assert _coerce_int({"client_send_ns": "123"}, "client_send_ns") == 123
    assert _coerce_int({"client_send_ns": 123}, "client_send_ns") == 123


def test_coerce_int_returns_none_for_missing_or_invalid_field():
    assert _coerce_int({}, "client_send_ns") is None
    assert _coerce_int({"client_send_ns": None}, "client_send_ns") is None
    assert _coerce_int({"client_send_ns": "not-a-number"}, "client_send_ns") is None
    assert _coerce_int({"client_send_ns": [1, 2]}, "client_send_ns") is None
