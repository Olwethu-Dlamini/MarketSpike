import time
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query

from marketspike.risk.instruments import all_instruments

router = APIRouter(prefix="/api/v1")


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
