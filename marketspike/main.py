import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from marketspike.api import rest as rest_api
from marketspike.api import ws as ws_api
from marketspike.calendar.clock import EventClock, load_events
from marketspike.config import get_settings
from marketspike.engine.bus import Bus
from marketspike.engine.supervisor import supervise
from marketspike.engine.symbol_state import SymbolEngine
from marketspike.feeds.binance import BinanceAdapter
from marketspike.feeds.oanda import OandaAdapter
from marketspike.risk.slippage import resolve_models
from marketspike.store.db import apply_schema, open_db
from marketspike.store.recorder import Recorder

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

app = FastAPI(title="MarketSpike", version="1.0.0")

# Explicit origins with no credentials. allow_origins=["*"] together with
# allow_credentials=True is rejected by browsers (spec appendix A, item 8).
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", "http://127.0.0.1:3000",
        "http://localhost:5173", "http://127.0.0.1:5173",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.include_router(ws_api.router)
app.include_router(rest_api.router)

STATE: Dict[str, object] = {"started_ns": 0, "adapters": {}, "tasks": []}


def build_adapters(settings) -> Dict[str, object]:
    adapters: Dict[str, object] = {}
    for symbol in settings.symbols:
        if symbol == "BTCUSDT":
            adapters[symbol] = BinanceAdapter(symbol)
        elif symbol == "EURUSD":
            if not (settings.oanda_token and settings.oanda_account_id):
                LOGGER.error(
                    "EURUSD requested but MS_OANDA_TOKEN/MS_OANDA_ACCOUNT_ID "
                    "are unset; symbol will be unavailable"
                )
                continue
            adapters[symbol] = OandaAdapter(
                symbol, settings.oanda_token, settings.oanda_account_id
            )
        else:
            LOGGER.warning("no adapter registered for symbol %s", symbol)
    return adapters


def _load_model_metrics(model_path: str) -> Dict[str, Any]:
    """Read the per-symbol `metrics` block straight out of model.json.

    `resolve_models`/`load_models` (marketspike.risk.slippage) intentionally
    strip everything down to the `SlippageModel` shape needed to serve a
    prediction -- they never carry the evaluation report. `/model/card`
    needs that report too, so it is read here, separately, rather than by
    widening the risk-serving path's return type.
    """
    if not model_path or not os.path.exists(model_path):
        return {}
    try:
        with open(model_path, "r") as handle:
            raw = json.load(handle)
    except (ValueError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    models_section = raw.get("models")
    if not isinstance(models_section, dict):
        return {}
    return {
        symbol: entry.get("metrics", {})
        for symbol, entry in models_section.items()
        if isinstance(entry, dict)
    }


@app.on_event("startup")
async def startup() -> None:
    settings = get_settings()
    conn = open_db(settings.db_path)
    apply_schema(conn)
    recorder = Recorder(conn)
    adapters = build_adapters(settings)
    STATE["bus"] = Bus()
    STATE["mode"] = "live"
    STATE["warmup_complete"] = False

    STATE["started_ns"] = time.time_ns()
    STATE["settings"] = settings
    STATE["conn"] = conn
    STATE["recorder"] = recorder
    STATE["adapters"] = adapters

    event_clock = EventClock(load_events())
    STATE["event_clock"] = event_clock
    conn.executemany(
        "INSERT INTO calendar_events (event_ts_ns, name, importance, country, affects) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (e.event_ts_ns, e.name, e.importance, e.country, ",".join(e.affects))
            for e in load_events()
        ],
    )
    conn.commit()

    engines = {}
    for symbol, adapter in adapters.items():
        engines[symbol] = SymbolEngine(
            symbol=symbol, bus=STATE["bus"], recorder=recorder,
            tau_fast_s=settings.tau_fast_s, tau_slow_s=settings.tau_slow_s,
            skew_window_s=settings.skew_window_s, ws_max_hz=settings.ws_max_hz,
            vol_sample_interval_s=settings.vol_sample_interval_s,
            event_clock=event_clock,
        )
    STATE["engines"] = engines

    models = resolve_models(settings.model_path, list(adapters.keys()))
    STATE["models"] = models
    STATE["model_sources"] = {s: m.source for s, m in models.items()}
    STATE["model_metrics"] = _load_model_metrics(settings.model_path)

    tasks: List[asyncio.Future] = [
        asyncio.ensure_future(supervise("recorder", recorder.run))
    ]
    for symbol, adapter in adapters.items():
        tasks.append(
            asyncio.ensure_future(
                supervise(
                    "feed:{0}".format(symbol),
                    _make_ingest(adapter, engines[symbol], recorder),
                )
            )
        )
    STATE["tasks"] = tasks
    LOGGER.info("started with symbols=%s", list(adapters))


def _make_ingest(adapter, engine: SymbolEngine, recorder: Recorder):
    async def ingest() -> None:
        baseline = await adapter.seed_baseline()
        if baseline:
            engine.seed(baseline)
            LOGGER.info("seeded %s slow variance at %.3e", adapter.symbol, baseline)
        else:
            LOGGER.warning(
                "no baseline for %s; ratios are unreliable until warm",
                adapter.symbol,
            )
        async for tick in adapter.stream():
            engine.on_tick(tick)

    return ingest


@app.on_event("shutdown")
async def shutdown() -> None:
    tasks = list(STATE.get("tasks") or [])
    # The replay driver (started on demand by POST /replay/start) is not
    # part of the startup()-managed task list, so it needs its own explicit
    # cancellation here -- otherwise a replay left running at shutdown would
    # be silently abandoned rather than cancelled and awaited like every
    # other long-lived task.
    replay_task = STATE.get("replay_task")
    if replay_task is not None:
        tasks.append(replay_task)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    recorder = STATE.get("recorder")
    if recorder is not None:
        for _ in range(100):
            written = await recorder.flush_once()
            if not written:
                break
        else:
            LOGGER.warning("recorder queue still non-empty after shutdown drain cap")

    conn = STATE.get("conn")
    if conn is not None:
        conn.close()


if __name__ == "__main__":
    import uvicorn

    # Pass the app by dotted string, not by the `app` object already bound in
    # this module's `__main__` namespace. `python -m marketspike.main` runs
    # this file twice under two different sys.modules keys ('__main__' and,
    # once ws.py's handler does `from marketspike.main import STATE`,
    # 'marketspike.main' too). Handing uvicorn the string means it does that
    # second import itself and serves *that* module's app — the same one the
    # handler's import resolves to — so STATE (with "bus" etc.) lines up.
    # Passing the local `app` object would instead run the '__main__' copy,
    # whose STATE never gets startup()-populated, causing a KeyError at
    # connection time in api/ws.py.
    uvicorn.run("marketspike.main:app", host="0.0.0.0", port=8000)
