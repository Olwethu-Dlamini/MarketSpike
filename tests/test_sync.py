from marketspike.clock.sync import SyncFilter, compute_sync


def test_symmetric_path_with_synced_clocks_gives_zero_offset():
    round_trip, offset = compute_sync(
        client_send_ns=0, server_recv_ns=100,
        server_send_ns=110, client_recv_ns=210,
    )
    assert round_trip == 200
    assert offset == 0


def test_offset_recovers_a_known_server_clock_lead():
    # Server clock runs 1000ns ahead; path is symmetric.
    round_trip, offset = compute_sync(
        client_send_ns=0, server_recv_ns=1100,
        server_send_ns=1110, client_recv_ns=210,
    )
    assert round_trip == 200
    assert offset == 1000


def test_server_processing_time_is_excluded_from_round_trip():
    round_trip, _ = compute_sync(
        client_send_ns=0, server_recv_ns=100,
        server_send_ns=5000, client_recv_ns=5100,
    )
    assert round_trip == 200


def test_filter_keeps_the_sample_with_lowest_round_trip():
    filt = SyncFilter(keep=8)
    filt.add(round_trip_ns=900, offset_ns=77)
    filt.add(round_trip_ns=200, offset_ns=42)
    filt.add(round_trip_ns=600, offset_ns=13)
    assert filt.best_offset_ns == 42
    assert filt.best_round_trip_ns == 200


def test_filter_discards_samples_beyond_its_capacity():
    filt = SyncFilter(keep=2)
    filt.add(round_trip_ns=100, offset_ns=1)
    filt.add(round_trip_ns=500, offset_ns=2)
    filt.add(round_trip_ns=400, offset_ns=3)
    assert filt.best_offset_ns == 3


def test_asymmetric_path_offset_reflects_known_ntp_bias():
    # True offset is 0 (clocks synced). Outbound leg takes 50ns, return leg
    # takes 150ns -- an asymmetric path. NTP's offset formula assumes a
    # symmetric path, so with true offset 0 it reports half the leg-duration
    # difference as spurious offset. This is a known bias inherent to the
    # method (unequal transit times cannot be distinguished from clock
    # offset from a single exchange), not a defect in this implementation.
    #
    # client_send=0, server processes instantaneously (recv=send=50),
    # client_recv = 50 (outbound) + 150 (return) = 200.
    round_trip, offset = compute_sync(
        client_send_ns=0, server_recv_ns=50,
        server_send_ns=50, client_recv_ns=200,
    )
    assert round_trip == 200
    # offset = ((50 - 0) + (50 - 200)) // 2 = (50 - 150) // 2 = -50
    assert offset == -50


def test_empty_filter_returns_none_for_both_properties_consistently():
    filt = SyncFilter(keep=8)
    assert filt.best_offset_ns is None
    assert filt.best_round_trip_ns is None

    filt.add(round_trip_ns=500, offset_ns=99)
    filt.add(round_trip_ns=150, offset_ns=7)
    filt.add(round_trip_ns=300, offset_ns=22)

    # Both properties must be drawn from the same underlying best sample.
    assert filt.best_round_trip_ns == 150
    assert filt.best_offset_ns == 7
