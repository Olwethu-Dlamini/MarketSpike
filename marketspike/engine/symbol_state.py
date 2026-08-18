import math
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

from marketspike.engine.pipeline import PipelineTimer
from marketspike.engine.regime import MARKET_CLOSED, NORMAL, RegimeFSM
from marketspike.engine.scoring import composite_score, dominant_signal
from marketspike.engine.spread import SpreadTracker
from marketspike.engine.volatility import VolatilityPair
from marketspike.feeds.base import Tick

RATE_TAU_S = 5.0

# How much (recv_ts_ns, mid) history to retain for abs_return_5s. Kept a
# little wider than the 5s lookback itself so a sample at (roughly) exactly
# five seconds ago is still present rather than having just aged out --
# bounded by age (not by count) so it can never grow without limit even
# under a very high tick rate.
PRICE_HISTORY_WINDOW_NS = 6_000_000_000
ABS_RETURN_LOOKBACK_NS = 5_000_000_000


class SymbolEngine:
    """Owns all per-symbol state and publishes frames for one instrument.

    Decoupled from transport/storage by construction: `bus` and `recorder`
    are injected, so this module never imports `api/` or `store/` (spec's
    engine/api boundary).
    """

    def __init__(
        self,
        symbol: str,
        bus,
        recorder,
        tau_fast_s: float = 30.0,
        tau_slow_s: float = 1800.0,
        skew_window_s: float = 60.0,
        ws_max_hz: float = 20.0,
        vol_sample_interval_s: float = 1.0,
        event_clock=None,
    ) -> None:
        self.symbol = symbol
        self.bus = bus
        self.recorder = recorder
        self.timer = PipelineTimer(skew_window_s=skew_window_s)
        self.vol = VolatilityPair(tau_fast_s, tau_slow_s, vol_sample_interval_s)
        self.spread = SpreadTracker()
        self.fsm = RegimeFSM()
        self.event_context = "CLEAR"
        self.event_clock = event_clock

        self._min_frame_ns = int(1e9 / ws_max_hz) if ws_max_hz > 0 else 0
        self._last_frame_ns = 0
        self._last_rate_ts_ns: Optional[int] = None

        self.quote_rate_hz = 0.0
        self.v_ratio: Optional[float] = None
        self.spread_z = 0.0
        self.score = 0.0
        self.last_tick: Optional[Tick] = None

        # Rolling (recv_ts_ns, mid) buffer for abs_return_5s -- computed
        # here rather than downstream so the ML feature builder (and Task
        # 17) never risk train/serve skew against this value.
        self._price_history: Deque[Tuple[int, float]] = deque()

    def seed(self, var_per_second: float) -> None:
        self.vol.seed_slow(var_per_second)

    @property
    def warmup_complete(self) -> bool:
        return self.vol.ready

    @property
    def abs_return_5s(self) -> float:
        """abs(ln(mid_now / mid_5s_ago)), or 0.0 with insufficient history.

        `_price_history` is ordered oldest-to-newest, so the first sample
        whose age is >= 5s (walking from the oldest entry forward) is the
        best available approximation of "the price 5 seconds ago".
        """
        if not self._price_history:
            return 0.0
        newest_ts, newest_mid = self._price_history[-1]
        target_ts = newest_ts - ABS_RETURN_LOOKBACK_NS

        candidate: Optional[Tuple[int, float]] = None
        for ts_ns, mid in self._price_history:
            if ts_ns <= target_ts:
                candidate = (ts_ns, mid)
            else:
                break
        if candidate is None:
            return 0.0

        _, old_mid = candidate
        if old_mid <= 0 or newest_mid <= 0:
            return 0.0
        return abs(math.log(newest_mid / old_mid))

    def _record_price(self, ts_ns: int, mid: float) -> None:
        self._price_history.append((ts_ns, mid))
        cutoff = ts_ns - PRICE_HISTORY_WINDOW_NS
        while self._price_history and self._price_history[0][0] < cutoff:
            self._price_history.popleft()

    def _update_quote_rate(self, ts_ns: int) -> None:
        if self._last_rate_ts_ns is None:
            self._last_rate_ts_ns = ts_ns
            return
        dt = (ts_ns - self._last_rate_ts_ns) / 1e9
        self._last_rate_ts_ns = ts_ns
        if dt <= 0:
            return
        instantaneous = 1.0 / dt
        weight = min(1.0, dt / RATE_TAU_S)
        self.quote_rate_hz += weight * (instantaneous - self.quote_rate_hz)

    def _envelope(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        frame = {"v": 1, "seq": self.bus.next_seq(), "server_ts_ns": time.time_ns()}
        frame.update(payload)
        return frame

    def on_tick(self, tick: Tick) -> None:
        excess_us = self.timer.on_receive(tick)
        self._update_quote_rate(tick.recv_ts_ns)

        if not tick.tradeable:
            if self.fsm.state != MARKET_CLOSED:
                self.fsm.force(MARKET_CLOSED, tick.recv_ts_ns)
                self.bus.publish(
                    self._envelope(
                        {
                            "type": "market_state", "symbol": self.symbol,
                            "tradeable": False, "reason": "market_closed",
                            "next_open_ts_ns": None,
                        }
                    )
                )
            self.last_tick = tick
            self.recorder.submit_tick(tick, excess_us, 0)
            return

        if self.fsm.state == MARKET_CLOSED:
            self.fsm.force(NORMAL, tick.recv_ts_ns)
            self.bus.publish(
                self._envelope(
                    {
                        "type": "market_state", "symbol": self.symbol,
                        "tradeable": True, "reason": "market_open",
                        "next_open_ts_ns": None,
                    }
                )
            )

        if self.event_clock is not None:
            previous_context = self.event_context
            self.event_context = self.event_clock.phase(tick.recv_ts_ns, self.symbol)
            if self.event_context != previous_context and self.event_context != "CLEAR":
                event = self.event_clock.relevant(tick.recv_ts_ns, self.symbol)
                if event is not None:
                    self.bus.publish(
                        self._envelope(
                            {
                                "type": "event_alert", "name": event.name,
                                "importance": event.importance,
                                "event_ts_ns": event.event_ts_ns,
                                "seconds_until": int(
                                    (event.event_ts_ns - tick.recv_ts_ns) / 1e9
                                ),
                                "phase": self.event_context,
                                "affects": event.affects,
                            }
                        )
                    )

        self._record_price(tick.recv_ts_ns, tick.mid)
        self.v_ratio = self.vol.update(tick.recv_ts_ns, tick.mid)
        self.spread_z = self.spread.update(tick.recv_ts_ns, tick.spread_bps)
        self.score = composite_score(self.v_ratio, self.spread_z)

        previous = self.fsm.state
        changed: Optional[str] = None
        if self.warmup_complete:
            # SpreadTracker has no readiness signal of its own: a cold
            # engine reports spread_z == 0.0, indistinguishable from a
            # genuinely calm market. That's fail-safe for escalation, but
            # we don't rely on coincidence -- transitions are evaluated
            # (and the FSM's dwell timers only start ticking) once the
            # volatility pair is actually warm.
            trigger = dominant_signal(self.v_ratio, self.spread_z)
            changed = self.fsm.update(tick.recv_ts_ns, self.score, trigger=trigger)

        done_ts_ns = time.time_ns()
        excess_us, engine_us = self.timer.on_processed(tick, done_ts_ns, excess_us)
        self.last_tick = tick
        self.recorder.submit_tick(tick, excess_us, engine_us)

        if changed:
            self.recorder.submit_regime(
                ts_ns=tick.recv_ts_ns, symbol=self.symbol, from_state=previous,
                to_state=changed, score=self.score,
                v_ratio=self.v_ratio or 0.0, spread_z=self.spread_z,
                trigger=self.fsm.last_trigger, event_context=self.event_context,
            )
            self.bus.publish(
                self._envelope(
                    {
                        "type": "regime_change", "symbol": self.symbol,
                        "from": previous, "to": changed, "score": self.score,
                        "v_ratio": self.v_ratio or 0.0, "spread_z": self.spread_z,
                        "event_context": self.event_context,
                        "trigger": self.fsm.last_trigger,
                    }
                )
            )

        # Ticks are recorded at full rate but published at a capped rate: a
        # browser cannot render 100 Hz, and trying inflates delivery latency.
        if tick.recv_ts_ns - self._last_frame_ns < self._min_frame_ns:
            return
        self._last_frame_ns = tick.recv_ts_ns

        self.bus.publish(
            self._envelope(
                {
                    "type": "tick", "symbol": self.symbol,
                    "bid": tick.bid, "ask": tick.ask, "mid": tick.mid,
                    "spread_bps": tick.spread_bps,
                    "spread_pips": tick.spread,
                    "quote_rate_hz": self.quote_rate_hz,
                    "book_imbalance": tick.book_imbalance,
                    "tradeable": tick.tradeable, "source": tick.source,
                }
            )
        )
        p50, p95, p99 = self.timer.total.percentiles(tick.recv_ts_ns)
        self.bus.publish(
            self._envelope(
                {
                    "type": "latency", "symbol": self.symbol,
                    "excess_transit_us": excess_us, "engine_us": engine_us,
                    "delivery_us": None, "p50_us": p50, "p95_us": p95,
                    "p99_us": p99,
                    "source": "simulated" if tick.source == "simulated" else "estimated",
                    "baseline_includes_clock_offset": True,
                }
            )
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "regime": self.fsm.state,
            "since_ns": self.fsm.entered_ns,
            "score": self.score,
            "v_ratio": self.v_ratio,
            "spread_z": self.spread_z,
            "quote_rate_hz": self.quote_rate_hz,
            "event_context": self.event_context,
            "warmup_complete": self.warmup_complete,
            "tradeable": self.last_tick.tradeable if self.last_tick else True,
            "abs_return_5s": self.abs_return_5s,
        }
