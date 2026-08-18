import math

from marketspike.engine.bus import Bus
from marketspike.engine.symbol_state import SymbolEngine
from marketspike.feeds.base import Tick

SECOND = 1_000_000_000


class FakeRecorder:
    def __init__(self):
        self.ticks = []
        self.regimes = []

    def submit_tick(self, tick, excess_transit_us, engine_us):
        self.ticks.append((tick, excess_transit_us, engine_us))
        return True

    def submit_regime(self, **kwargs):
        self.regimes.append(kwargs)
        return True


def make_engine(ws_max_hz=1000.0):
    return SymbolEngine(
        symbol="BTCUSDT", bus=Bus(), recorder=FakeRecorder(),
        tau_fast_s=30.0, tau_slow_s=1800.0, skew_window_s=60.0,
        ws_max_hz=ws_max_hz,
    )


def tick_at(ts_ns, mid, spread=0.10, tradeable=True, source="measured"):
    return Tick(
        symbol="BTCUSDT", venue_ts_ns=ts_ns, recv_ts_ns=ts_ns,
        bid=mid - spread / 2, ask=mid + spread / 2,
        bid_qty=1.0, ask_qty=1.0, tradeable=tradeable, source=source,
    )


def test_every_tick_is_recorded():
    engine = make_engine()
    for step in range(5):
        engine.on_tick(tick_at(step * SECOND, 100.0))
    assert len(engine.recorder.ticks) == 5


def test_tick_frames_are_rate_limited_but_recording_is_not():
    engine = make_engine(ws_max_hz=1.0)
    sub = engine.bus.subscribe(maxlen=1000)
    for step in range(20):
        engine.on_tick(tick_at(step * (SECOND // 10), 100.0))  # 10 Hz input
    published = [f for f in sub.drain() if f["type"] == "tick"]
    assert len(engine.recorder.ticks) == 20
    assert 1 <= len(published) <= 4


def test_regime_transition_publishes_a_frame_and_persists_a_row():
    engine = make_engine()
    engine.seed(1e-12)  # tiny baseline so live volatility dwarfs it
    sub = engine.bus.subscribe(maxlen=1000)
    ts, mid = 0, 100.0
    for _ in range(60):
        ts += SECOND
        mid *= math.exp(0.002)
        engine.on_tick(tick_at(ts, mid))
    changes = [f for f in sub.drain() if f["type"] == "regime_change"]
    assert changes, "expected at least one regime transition"
    assert changes[0]["v"] == 1
    assert changes[0]["trigger"] in ("vol_ratio", "spread", "both")
    assert engine.recorder.regimes


def test_snapshot_exposes_current_state():
    engine = make_engine()
    engine.on_tick(tick_at(SECOND, 100.0))
    snap = engine.snapshot()
    assert snap["symbol"] == "BTCUSDT"
    assert snap["regime"] == "NORMAL"
    assert "score" in snap and "spread_z" in snap


def test_untradeable_tick_forces_market_closed_state():
    engine = make_engine()
    closed = Tick(
        symbol="BTCUSDT", venue_ts_ns=SECOND, recv_ts_ns=SECOND,
        bid=100.0, ask=100.1, bid_qty=1.0, ask_qty=1.0,
        tradeable=False, source="measured",
    )
    engine.on_tick(closed)
    assert engine.fsm.state == "MARKET_CLOSED"


def test_market_state_frame_fires_only_on_transition_not_every_untradeable_tick():
    engine = make_engine()
    sub = engine.bus.subscribe(maxlen=1000)
    for step in range(5):
        engine.on_tick(tick_at(SECOND + step * SECOND, 100.0, tradeable=False))
    market_frames = [f for f in sub.drain() if f["type"] == "market_state"]
    assert len(market_frames) == 1
    assert market_frames[0]["tradeable"] is False


def test_market_state_frame_fires_on_reopen_transition():
    engine = make_engine()
    engine.on_tick(tick_at(SECOND, 100.0, tradeable=False))
    sub = engine.bus.subscribe(maxlen=1000)
    engine.on_tick(tick_at(2 * SECOND, 100.0, tradeable=True))
    market_frames = [f for f in sub.drain() if f["type"] == "market_state"]
    assert len(market_frames) == 1
    assert market_frames[0]["tradeable"] is True
    assert engine.fsm.state != "MARKET_CLOSED"


def test_forced_transition_sets_forced_trigger():
    engine = make_engine()
    closed = tick_at(SECOND, 100.0, tradeable=False)
    engine.on_tick(closed)
    assert engine.fsm.last_trigger == "forced"


def test_latency_frame_source_is_simulated_when_tick_source_is_simulated():
    engine = make_engine()
    sub = engine.bus.subscribe(maxlen=1000)
    engine.on_tick(tick_at(SECOND, 100.0, source="simulated"))
    latency_frames = [f for f in sub.drain() if f["type"] == "latency"]
    assert latency_frames
    assert latency_frames[0]["source"] == "simulated"


def test_latency_frame_source_is_estimated_when_tick_source_is_measured():
    engine = make_engine()
    sub = engine.bus.subscribe(maxlen=1000)
    engine.on_tick(tick_at(SECOND, 100.0, source="measured"))
    latency_frames = [f for f in sub.drain() if f["type"] == "latency"]
    assert latency_frames
    assert latency_frames[0]["source"] == "estimated"


def test_no_regime_transition_before_warmup_even_under_violent_price_path():
    """A cold engine (vol never warm) must never evaluate FSM transitions,
    even when the composite score would clearly cross a threshold.

    Every price tick here jumps 50% up or down relative to the previous
    mid, so VolatilityCalc's outlier rejection (> 5% log-return) fires on
    every single update forever -- fast/slow variance is never set, so
    engine.warmup_complete stays False for the whole run. Meanwhile the
    quoted spread (independent of mid, computed as target_bps of the
    *current* wildly-moving mid) ramps from a calm ~5bps baseline to a
    huge 500bps spike, which -- were it not gated -- would push the
    spread-only score comfortably past the 1.5 ELEVATED threshold (the
    spread component alone saturates at 0.4*4.0 = 1.6) and hold it there
    long enough to satisfy the 3s dwell.
    """
    engine = make_engine()
    sub = engine.bus.subscribe(maxlen=1000)

    ts = 0
    mid = 100.0
    max_score_seen = 0.0
    jitter = [4.9, 5.1, 5.0, 5.2, 4.8]
    for step in range(30):
        ts += SECOND
        mid = mid * 1.5 if step % 2 == 0 else mid / 1.5
        target_bps = 500.0 if step >= 25 else jitter[step % len(jitter)]
        spread_dollars = target_bps / 10000.0 * mid
        engine.on_tick(tick_at(ts, mid, spread=spread_dollars))
        max_score_seen = max(max_score_seen, engine.score)
        assert engine.warmup_complete is False
        assert engine.fsm.state == "NORMAL"

    changes = [f for f in sub.drain() if f["type"] == "regime_change"]
    assert changes == []
    assert engine.fsm.state == "NORMAL"
    assert engine.recorder.regimes == []
    # Prove the gating actually mattered: the score did cross the
    # ELEVATED threshold (1.5) and held there for several seconds, so an
    # ungated engine would have transitioned.
    assert max_score_seen >= 1.5


def test_abs_return_5s_is_zero_with_insufficient_history():
    engine = make_engine()
    engine.on_tick(tick_at(0, 100.0))
    engine.on_tick(tick_at(1 * SECOND, 101.0))
    assert engine.abs_return_5s == 0.0


def test_abs_return_5s_computes_log_return_over_five_seconds():
    engine = make_engine()
    engine.on_tick(tick_at(0, 100.0))
    engine.on_tick(tick_at(5 * SECOND, 110.0))
    expected = abs(math.log(110.0 / 100.0))
    assert engine.abs_return_5s == expected


def test_abs_return_5s_buffer_is_bounded_by_age():
    engine = make_engine()
    ts = 0
    for step in range(2000):
        ts += SECOND // 100  # 100 Hz for 20 seconds
        engine.on_tick(tick_at(ts, 100.0 + step * 0.001))
    # Only ~6-7 seconds of history (at 100Hz) should remain, not all 2000.
    assert len(engine._price_history) < 1000
    oldest_ts = engine._price_history[0][0]
    newest_ts = engine._price_history[-1][0]
    assert (newest_ts - oldest_ts) <= 7 * SECOND
