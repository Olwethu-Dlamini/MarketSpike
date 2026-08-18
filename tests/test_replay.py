import json
import math

from marketspike.engine.bus import Bus
from marketspike.engine.symbol_state import SymbolEngine
from marketspike.feeds.base import Tick
from marketspike.feeds.replay import read_scenario, write_scenario

SECOND = 1_000_000_000


class FakeRecorder:
    def __init__(self):
        self.ticks, self.regimes = [], []

    def submit_tick(self, tick, excess_transit_us, engine_us):
        self.ticks.append(tick)
        return True

    def submit_regime(self, **kwargs):
        self.regimes.append(kwargs)
        return True


def tick(ts_ns, mid, spread):
    return Tick(
        symbol="EURUSD", venue_ts_ns=ts_ns - 2_000_000, recv_ts_ns=ts_ns,
        bid=mid - spread / 2, ask=mid + spread / 2,
        bid_qty=1_000_000.0, ask_qty=1_000_000.0,
        tradeable=True, source="simulated",
    )


def build_spike_path():
    """Calm, then a violent 20s burst, then calm again."""
    ticks, ts, mid = [], 0, 1.0850
    for _ in range(400):                       # calm: 200s at 2 Hz
        ts += SECOND // 2
        mid *= math.exp(0.000005)
        ticks.append(tick(ts, mid, 0.00013))
    for step in range(200):                    # spike: 20s at 10 Hz
        ts += SECOND // 10
        mid *= math.exp(0.0006 * (1 if step % 2 else -1) + 0.0004)
        ticks.append(tick(ts, mid, 0.00090))
    for _ in range(400):                       # calm again
        ts += SECOND // 2
        mid *= math.exp(0.000005)
        ticks.append(tick(ts, mid, 0.00013))
    return ticks


def test_scenario_round_trips_through_ndjson(tmp_path):
    path = tmp_path / "s.ndjson"
    count = write_scenario(str(path), [tick(SECOND, 1.085, 0.0001)])
    assert count == 1
    rows = read_scenario(str(path))
    assert rows[0]["bid"] == 1.0850 - 0.00005
    assert rows[0]["source"] == "simulated"


def test_scenario_file_is_one_json_object_per_line(tmp_path):
    path = tmp_path / "s.ndjson"
    write_scenario(str(path), [tick(SECOND, 1.085, 0.0001), tick(2 * SECOND, 1.086, 0.0001)])
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[1])["venue_ts_ns"] > json.loads(lines[0])["venue_ts_ns"]


def test_replayed_spike_drives_exactly_one_regime_cycle():
    """The §15.4 integration test: NORMAL -> SPIKE -> NORMAL, once.

    `SymbolEngine` gates regime transitions on `warmup_complete` (both the
    fast and slow volatility horizons must be ready), so the synthetic path
    seeds the engine and runs a long enough calm lead-in -- at the engine's
    default 1s volatility sampling grid -- for the fast horizon to warm
    before the spike segment begins.
    """
    bus = Bus()
    engine = SymbolEngine(
        symbol="EURUSD", bus=bus, recorder=FakeRecorder(),
        tau_fast_s=30.0, tau_slow_s=1800.0, skew_window_s=60.0, ws_max_hz=1000.0,
    )
    engine.seed(1e-11)
    sub = bus.subscribe(maxlen=100000)

    for item in build_spike_path():
        engine.on_tick(item)

    changes = [f for f in sub.drain() if f["type"] == "regime_change"]
    states = [f["to"] for f in changes]
    assert "SPIKE" in states, "spike segment did not raise the regime"
    assert states.count("SPIKE") == 1, "regime flapped: {0}".format(states)
    assert states[-1] == "NORMAL", "regime did not decay after the spike"


def test_replayed_frames_are_labelled_simulated():
    bus = Bus()
    engine = SymbolEngine(
        symbol="EURUSD", bus=bus, recorder=FakeRecorder(),
        tau_fast_s=30.0, tau_slow_s=1800.0, skew_window_s=60.0, ws_max_hz=1000.0,
    )
    sub = bus.subscribe(maxlen=1000)
    engine.on_tick(tick(SECOND, 1.0850, 0.00013))
    published = [f for f in sub.drain() if f["type"] in ("tick", "latency")]
    assert published
    assert all(f["source"] == "simulated" for f in published)
