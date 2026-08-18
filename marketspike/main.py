import asyncio
import logging
import time
from typing import Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from marketspike.config import get_settings
from marketspike.engine.supervisor import supervise
from marketspike.feeds.binance import BinanceAdapter
from marketspike.feeds.oanda import OandaAdapter
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


@app.on_event("startup")
async def startup() -> None:
    settings = get_settings()
    conn = open_db(settings.db_path)
    apply_schema(conn)
    recorder = Recorder(conn)
    adapters = build_adapters(settings)

    STATE["started_ns"] = time.time_ns()
    STATE["settings"] = settings
    STATE["conn"] = conn
    STATE["recorder"] = recorder
    STATE["adapters"] = adapters

    tasks: List[asyncio.Future] = [
        asyncio.ensure_future(supervise("recorder", recorder.run))
    ]
    for symbol, adapter in adapters.items():
        tasks.append(
            asyncio.ensure_future(
                supervise(
                    "feed:{0}".format(symbol),
                    _make_ingest(adapter, recorder),
                )
            )
        )
    STATE["tasks"] = tasks
    LOGGER.info("started with symbols=%s", list(adapters))


def _make_ingest(adapter, recorder: Recorder):
    async def ingest() -> None:
        async for tick in adapter.stream():
            recorder.submit_tick(tick, excess_transit_us=0, engine_us=0)

    return ingest


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in STATE.get("tasks", []):
        task.cancel()
    conn = STATE.get("conn")
    if conn is not None:
        conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
