import asyncio
import logging
import time
import uuid
from typing import Any, Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from marketspike.clock.sync import SyncFilter, compute_sync

router = APIRouter()

LOGGER = logging.getLogger(__name__)

# Explicit mapping from frame `type` to subscription channel. Keep this in
# sync with every producer (SymbolEngine, replay, etc.) that publishes onto
# the bus — a frame type missing from this map is treated as a
# protocol-level frame (see the filtering rule in `pump()` below), not
# silently dropped.
CHANNEL_FOR_TYPE = {
    "tick": "tick",
    "latency": "latency",
    "regime_change": "regime",
    "event_alert": "event",
    "market_state": "market",
    "replay_state": "replay",
}
DEFAULT_CHANNELS = frozenset(CHANNEL_FOR_TYPE.values())


def _envelope(bus, payload: Dict[str, Any]) -> Dict[str, Any]:
    frame = {"v": 1, "seq": bus.next_seq(), "server_ts_ns": time.time_ns()}
    frame.update(payload)
    return frame


def _coerce_int(message: Dict[str, Any], field: str) -> Any:
    """Return an int for `field`, or a sentinel `None` if missing/invalid."""
    if field not in message:
        return None
    try:
        return int(message[field])
    except (TypeError, ValueError):
        return None


@router.websocket("/ws/v1/stream")
async def stream(websocket: WebSocket) -> None:
    from marketspike.main import STATE

    await websocket.accept()
    bus = STATE["bus"]
    sub = bus.subscribe(maxlen=200)
    pump_task = None
    try:
        sync = SyncFilter(keep=8)
        symbols: Set[str] = set(STATE.get("adapters", {}).keys())
        channels: Set[str] = set(DEFAULT_CHANNELS)

        await websocket.send_json(
            _envelope(
                bus,
                {
                    "type": "hello",
                    "session_id": uuid.uuid4().hex[:8],
                    "server_version": "1.0.0",
                    # Derived live from the engines, not read from a flag set
                    # once at startup -- a static flag would report False forever.
                    "warmup_complete": all(
                        engine.warmup_complete
                        for engine in STATE.get("engines", {}).values()
                    )
                    if STATE.get("engines")
                    else False,
                    "feeds": {
                        name: adapter.venue
                        for name, adapter in STATE.get("adapters", {}).items()
                    },
                    "mode": STATE.get("mode", "live"),
                },
            )
        )

        async def pump() -> None:
            try:
                while True:
                    frame = await sub.get()
                    if frame.get("symbol") and frame["symbol"] not in symbols:
                        continue
                    kind = frame.get("type", "")
                    channel = CHANNEL_FOR_TYPE.get(kind)
                    # A frame type absent from CHANNEL_FOR_TYPE is a
                    # protocol-level frame (e.g. "hello", "error",
                    # "clock_sync_reply") and must not be silently
                    # swallowed by stale filtering logic — always deliver
                    # it. Only mapped (channel-bearing) frame types are
                    # subject to the client's subscription filter.
                    if channel is None or channel in channels:
                        await websocket.send_json(frame)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("ws pump task failed")

        pump_task = asyncio.ensure_future(pump())
        while True:
            message = await websocket.receive_json()
            server_recv_ns = time.time_ns()
            kind = message.get("type")

            if kind == "subscribe":
                symbols = set(message.get("symbols") or symbols)
                channels = set(message.get("channels") or channels)
            elif kind == "unsubscribe":
                symbols -= set(message.get("symbols") or [])
            elif kind == "clock_sync":
                client_send_ns = _coerce_int(message, "client_send_ns")
                if client_send_ns is None:
                    await websocket.send_json(
                        _envelope(
                            bus,
                            {
                                "type": "error",
                                "code": "INVALID_PAYLOAD",
                                "detail": "clock_sync requires integer client_send_ns",
                            },
                        )
                    )
                    continue
                await websocket.send_json(
                    _envelope(
                        bus,
                        {
                            "type": "clock_sync_reply",
                            "client_send_ns": client_send_ns,
                            "server_recv_ns": server_recv_ns,
                            "server_send_ns": time.time_ns(),
                        },
                    )
                )
            elif kind == "ack":
                client_recv_ns = _coerce_int(message, "client_recv_ns")
                if client_recv_ns is None:
                    await websocket.send_json(
                        _envelope(
                            bus,
                            {
                                "type": "error",
                                "code": "INVALID_PAYLOAD",
                                "detail": "ack requires integer client_recv_ns",
                            },
                        )
                    )
                    continue
                client_send_ns = _coerce_int(message, "client_send_ns")
                if client_send_ns is None:
                    client_send_ns = server_recv_ns
                round_trip, offset = compute_sync(
                    client_send_ns=client_send_ns,
                    server_recv_ns=server_recv_ns,
                    server_send_ns=server_recv_ns,
                    client_recv_ns=client_recv_ns,
                )
                sync.add(round_trip, offset)
                best_round_trip_ns = sync.best_round_trip_ns
                if best_round_trip_ns is not None:
                    # One-way delivery is estimated -- not directly
                    # measured -- as half the least-delayed round trip
                    # seen so far (spec §6.3): that sample carries the
                    # least path asymmetry, so it's the best available
                    # proxy for a one-way duration. //2000 combines the
                    # ns -> us conversion with the halving in one step.
                    bus.record_delivery(max(0, best_round_trip_ns // 2000))
            else:
                # Unknown message types are ignored, never fatal (spec §12.3).
                await websocket.send_json(
                    _envelope(
                        bus,
                        {
                            "type": "error",
                            "code": "UNKNOWN_MESSAGE_TYPE",
                            "detail": "ignored message type {0!r}".format(kind),
                        },
                    )
                )
    except WebSocketDisconnect:
        pass
    finally:
        if pump_task is not None:
            pump_task.cancel()
            try:
                await pump_task
            except asyncio.CancelledError:
                pass
        bus.unsubscribe(sub)
