import asyncio
import math
import time
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query

from marketspike.api.schemas import SizeRequest
from marketspike.feeds.replay import ReplayAdapter, list_scenarios
from marketspike.risk.instruments import all_instruments, get_instrument
from marketspike.risk.sizing import SizingContext, size_position
from marketspike.risk.slippage import FEATURE_ORDER, fallback_model

router = APIRouter(prefix="/api/v1")

# Cap on how often a running replay publishes its own "replay_state" frame:
# the same rationale as SymbolEngine's tick-frame rate cap (spec §12.3) --
# a browser cannot render progress updates at tick rate and trying to
# inflates delivery latency for no benefit.
REPLAY_STATE_MIN_INTERVAL_NS = 200_000_000


def _state() -> Dict[str, Any]:
    from marketspike.main import STATE

    return STATE


def _unknown_symbol(symbol: str, instance: str) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={
            "type": "/errors/unknown-symbol",
            "title": "Unknown symbol",
            "status": 404,
            "detail": "{0} is not an active symbol".format(symbol),
            "instance": instance,
        },
    )


@router.get("/health")
def health() -> Dict[str, Any]:
    state = _state()
    now_ns = time.time_ns()
    engines = state.get("engines", {})
    adapters = state.get("adapters", {})

    feeds = {}
    for symbol, adapter in adapters.items():
        engine = engines.get(symbol)
        last_tick = engine.last_tick if engine else None
        feeds[symbol] = {
            "venue": adapter.venue,
            "connected": bool(getattr(adapter, "connected", False)),
            "last_tick_age_ms": (
                (now_ns - last_tick.recv_ts_ns) // 1_000_000 if last_tick else None
            ),
            "warmup_complete": bool(engine.warmup_complete) if engine else False,
            "tradeable": bool(last_tick.tradeable) if last_tick else True,
            "reason": None,
        }

    recorder = state.get("recorder")
    bus = state.get("bus")
    counters = dict(recorder.counters) if recorder else {}
    counters["client_dropped_total"] = bus.total_dropped if bus else 0
    counters["feed_dropped_total"] = state.get("feed_dropped_total", 0)

    started_ns = state.get("started_ns") or now_ns
    return {
        "v": 1,
        "status": "ok",
        "uptime_s": int((now_ns - started_ns) / 1e9),
        "feeds": feeds,
        "counters": counters,
        "model": state.get("model_sources", {}),
        "mode": state.get("mode", "live"),
    }


@router.get("/instruments")
def instruments() -> Dict[str, Any]:
    return {
        "v": 1,
        "instruments": [
            {
                "symbol": spec.symbol, "pip_size": spec.pip_size,
                "contract_size": spec.contract_size, "quote_ccy": spec.quote_ccy,
                "min_lot": spec.min_lot, "lot_step": spec.lot_step,
                "margin_rate": spec.margin_rate,
            }
            for spec in all_instruments()
        ],
    }


@router.get("/regime")
def regime(symbol: str = Query(...)) -> Dict[str, Any]:
    engine = _state().get("engines", {}).get(symbol)
    if engine is None:
        raise _unknown_symbol(symbol, "/api/v1/regime")
    return engine.snapshot()


@router.get("/latency/summary")
def latency_summary(symbol: str = Query(...)) -> Dict[str, Any]:
    engine = _state().get("engines", {}).get(symbol)
    if engine is None:
        raise _unknown_symbol(symbol, "/api/v1/latency/summary")

    now_ns = time.time_ns()
    transit = engine.timer.transit.percentiles(now_ns)
    compute = engine.timer.engine.percentiles(now_ns)
    total = engine.timer.total.percentiles(now_ns)
    return {
        "v": 1,
        "symbol": symbol,
        "hops": {
            "excess_transit_us": {"p50": transit[0], "p95": transit[1], "p99": transit[2]},
            "engine_us": {"p50": compute[0], "p95": compute[1], "p99": compute[2]},
        },
        "total_us": {"p50": total[0], "p95": total[1], "p99": total[2]},
        "baseline_includes_clock_offset": True,
        "source": "estimated",
    }


@router.get("/calendar/upcoming")
def calendar_upcoming(
    hours: float = Query(24.0), symbol: str = Query(None)
) -> Dict[str, Any]:
    clock = _state().get("event_clock")
    if clock is None:
        return {"v": 1, "events": []}
    now_ns = time.time_ns()
    return {
        "v": 1,
        "events": [
            {
                "name": event.name, "importance": event.importance,
                "country": event.country, "event_ts_ns": event.event_ts_ns,
                "seconds_until": int((event.event_ts_ns - now_ns) / 1e9),
                "affects": event.affects,
                "confidence": event.confidence,
            }
            for event in clock.upcoming(now_ns, hours, symbol)
        ],
    }


@router.get("/model/card")
def model_card() -> Dict[str, Any]:
    state = _state()
    models = state.get("models", {})
    return {
        "v": 1,
        "models": {
            symbol: {
                "version": model.version,
                "source": model.source,
                "feature_order": model.feature_order,
                "coefficients": model.quantiles,
                "metrics": state.get("model_metrics", {}).get(symbol, {}),
            }
            for symbol, model in models.items()
        },
    }


def _problem(status: int, slug: str, title: str, detail: str, instance: str) -> HTTPException:
    return HTTPException(
        status_code=status,
        detail={
            "type": "/errors/{0}".format(slug), "title": title,
            "status": status, "detail": detail, "instance": instance,
        },
    )


@router.get("/scenarios")
def scenarios() -> Dict[str, Any]:
    return {"v": 1, "scenarios": list_scenarios("scenarios")}


def _publish_replay_state(bus, adapter: ReplayAdapter, mode: str) -> None:
    if bus is None:
        return
    bus.publish(
        {
            "v": 1, "seq": bus.next_seq(), "server_ts_ns": time.time_ns(),
            "type": "replay_state", "mode": mode, "scenario": adapter.scenario,
            "progress_pct": adapter.progress_pct, "source": "simulated",
        }
    )


@router.post("/replay/start")
def replay_start(body: Dict[str, Any]) -> Dict[str, Any]:
    state = _state()
    scenario = body.get("scenario")
    symbol = body.get("symbol", "EURUSD")
    speed = float(body.get("speed", 1.0))

    if not scenario or scenario not in list_scenarios("scenarios"):
        raise _problem(404, "unknown-scenario", "Unknown scenario",
                       "{0} is not available".format(scenario), "/api/v1/replay/start")

    engine = state.get("engines", {}).get(symbol)
    if engine is None:
        raise _problem(404, "unknown-symbol", "Unknown symbol",
                       "{0} is not an active symbol".format(symbol),
                       "/api/v1/replay/start")

    existing = state.get("replay_task")
    if existing is not None:
        existing.cancel()

    path = "scenarios/{0}.ndjson".format(scenario)
    adapter = ReplayAdapter(symbol, path, speed=speed)
    bus = state.get("bus")

    async def drive() -> None:
        last_published_ns = 0
        _publish_replay_state(bus, adapter, "replay")
        try:
            async for tick in adapter.stream():
                engine.on_tick(tick)
                now_ns = time.time_ns()
                if now_ns - last_published_ns >= REPLAY_STATE_MIN_INTERVAL_NS:
                    last_published_ns = now_ns
                    _publish_replay_state(bus, adapter, "replay")
        finally:
            # Reached whether the scenario plays out to completion or the
            # task is cancelled (via /replay/stop or a new /replay/start) --
            # either way the system must not be left reporting mode=="replay"
            # once nothing is actually replaying.
            _publish_replay_state(bus, adapter, "live")
            state["mode"] = "live"
            state["replay_task"] = None
            state["replay_adapter"] = None

    state["mode"] = "replay"
    state["replay_adapter"] = adapter
    state["replay_task"] = asyncio.ensure_future(drive())
    return {"v": 1, "mode": "replay", "scenario": scenario, "symbol": symbol, "speed": speed}


@router.post("/replay/stop")
def replay_stop() -> Dict[str, Any]:
    state = _state()
    task = state.get("replay_task")
    if task is not None:
        task.cancel()
    state["replay_task"] = None
    state["replay_adapter"] = None
    state["mode"] = "live"
    return {"v": 1, "mode": "live"}


def _features(engine, latency_ms: float) -> Dict[str, float]:
    tick = engine.last_tick if engine else None
    v_ratio = (engine.v_ratio if engine else None) or 1.0
    spread_bps = tick.spread_bps if tick else 1.0

    # Real signed seconds-to-event from the economic calendar, falling
    # back to 1800.0 -- the clock's own clipped "no event nearby" sentinel
    # -- when there's no clock or no event relevant to this symbol, rather
    # than 0.0 (which would mean "an event is happening right now" and
    # would train/serve-skew every quote).
    clock = _state().get("event_clock")
    signed_secs_to_event = 1800.0
    if clock is not None and engine is not None and tick is not None:
        if clock.relevant(tick.recv_ts_ns, engine.symbol) is not None:
            signed_secs_to_event = clock.signed_seconds(tick.recv_ts_ns, engine.symbol)

    return {
        "log_v_ratio": math.log(max(v_ratio, 1e-9)),
        "spread_z": engine.spread_z if engine else 0.0,
        "log_spread_bps": math.log(max(spread_bps, 1e-6)),
        "log_latency_ms": math.log(max(latency_ms, 1e-3)),
        "quote_rate_hz": engine.quote_rate_hz if engine else 0.0,
        "book_imbalance": tick.book_imbalance if tick else 0.0,
        "signed_secs_to_event": signed_secs_to_event,
        "in_event_window": 1.0 if (engine and engine.event_context == "EVENT_WINDOW") else 0.0,
        # Read from the engine's rolling price buffer (never hardcoded),
        # so this matches exactly what the ML training path computes.
        "abs_return_5s": engine.abs_return_5s if engine else 0.0,
    }


@router.post("/slippage/predict")
def slippage_predict(body: Dict[str, Any]) -> Dict[str, Any]:
    """What-if slippage prediction for explicit market conditions.

    Unlike /size (which reads live engine state via `_features()`), every
    feature input here comes from the request body, so a client can sweep
    e.g. latency_ms across a range and draw a cost curve without needing a
    live symbol feed. Uses whatever model is currently loaded for the
    symbol (trained if available, else fallback priors) -- the same
    resolution `/size` uses -- so the curve matches what sizing actually
    relies on.
    """
    symbol = body.get("symbol", "")
    state = _state()
    model = state.get("models", {}).get(symbol)
    if model is None:
        try:
            get_instrument(symbol)
        except KeyError:
            raise _problem(404, "unknown-symbol", "Unknown symbol",
                           "{0} is not in the instrument registry".format(symbol),
                           "/api/v1/slippage/predict")
        model = fallback_model(symbol)

    features = {name: 0.0 for name in FEATURE_ORDER}
    features["log_v_ratio"] = math.log(max(float(body.get("v_ratio", 1.0)), 1e-9))
    features["spread_z"] = float(body.get("spread_z", 0.0))
    features["log_spread_bps"] = math.log(max(float(body.get("spread_bps", 1.0)), 1e-6))
    features["log_latency_ms"] = math.log(max(float(body.get("latency_ms", 50.0)), 1e-3))
    features["quote_rate_hz"] = float(body.get("quote_rate_hz", 0.0))
    features["book_imbalance"] = float(body.get("book_imbalance", 0.0))
    features["signed_secs_to_event"] = float(body.get("signed_secs_to_event", 1800.0))
    features["in_event_window"] = float(body.get("in_event_window", 0.0))
    features["abs_return_5s"] = float(body.get("abs_return_5s", 0.0))

    predicted = model.predict_quantiles(features)
    return {
        "v": 1,
        "symbol": symbol,
        "p50_bps": predicted["p50"],
        "p95_bps": predicted["p95"],
        "model_source": model.source,
        "model_version": model.version,
        "inputs_echo": body,
    }


@router.post("/size")
def size(request: SizeRequest) -> Dict[str, Any]:
    if request.risk_pct <= 0 or request.risk_pct > 100:
        raise _problem(422, "invalid-risk", "Invalid risk percentage",
                       "risk_pct must be in (0, 100]", "/api/v1/size")
    if request.stop_distance_price <= 0:
        raise _problem(422, "invalid-stop", "Invalid stop distance",
                       "stop_distance_price must be positive", "/api/v1/size")
    try:
        spec = get_instrument(request.symbol)
    except KeyError:
        raise _problem(404, "unknown-symbol", "Unknown symbol",
                       "{0} is not in the instrument registry".format(request.symbol),
                       "/api/v1/size")

    state = _state()
    engine = state.get("engines", {}).get(request.symbol)
    model = state.get("models", {}).get(request.symbol)
    if model is None:
        model = fallback_model(request.symbol)

    tick = engine.last_tick if engine else None
    price = tick.mid if tick else 1.0
    now_ns = time.time_ns()
    stale = tick is None or (now_ns - tick.recv_ts_ns) > 120 * 1_000_000_000

    if request.assumed_latency_ms is not None:
        latency_ms = float(request.assumed_latency_ms)
        latency_source = "estimated"
    elif engine is not None:
        p50_us = engine.timer.total.percentiles(now_ns)[0]
        latency_ms = p50_us / 1000.0
        latency_source = "measured"
    else:
        latency_ms = 50.0
        latency_source = "estimated"

    features = _features(engine, latency_ms)
    context = SizingContext(
        price=price,
        fx_rate=1.0 if spec.quote_ccy in (request.account_ccy, "USDT") else 1.0,
        fx_assumed=spec.quote_ccy not in (request.account_ccy, "USDT"),
        regime=engine.fsm.state if engine else "UNKNOWN",
        event_context=engine.event_context if engine else "CLEAR",
        latency_ms=latency_ms,
        latency_source=latency_source,
        stale_quote=stale,
        model_source=model.source,
        model_version=model.version,
    )

    predicted = model.predict_quantiles(features)
    result = size_position(
        request, spec,
        predicted["p50"],
        predicted["p95"],
        context,
    )

    recorder = state.get("recorder")
    conn = state.get("conn")
    if conn is not None:
        import json as _json

        conn.execute(
            "INSERT INTO calc_log (ts_ns, symbol, request_json, response_json, "
            "regime, model_version) VALUES (?, ?, ?, ?, ?, ?)",
            (now_ns, request.symbol, _json.dumps(request.model_dump()),
             _json.dumps(result), context.regime, model.version),
        )
        conn.commit()
    return result
