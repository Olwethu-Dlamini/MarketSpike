import asyncio
import time
import uuid
from typing import Any, Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from marketspike.clock.sync import SyncFilter, compute_sync

router = APIRouter()


def _envelope(bus, payload: Dict[str, Any]) -> Dict[str, Any]:
    frame = {"v": 1, "seq": bus.next_seq(), "server_ts_ns": time.time_ns()}
    frame.update(payload)
    return frame


@router.websocket("/ws/v1/stream")
async def stream(websocket: WebSocket) -> None:
    from marketspike.main import STATE

    await websocket.accept()
    bus = STATE["bus"]
    sub = bus.subscribe(maxlen=200)
    sync = SyncFilter(keep=8)
    symbols: Set[str] = set(STATE.get("adapters", {}).keys())
    channels: Set[str] = {"tick", "regime", "latency", "event"}

    await websocket.send_json(
        _envelope(
            bus,
            {
                "type": "hello",
                "session_id": uuid.uuid4().hex[:8],
                "server_version": "1.0.0",
                "warmup_complete": bool(STATE.get("warmup_complete", False)),
                "feeds": {
                    name: adapter.venue
                    for name, adapter in STATE.get("adapters", {}).items()
                },
                "mode": STATE.get("mode", "live"),
            },
        )
    )

    async def pump() -> None:
        while True:
            frame = await sub.get()
            if frame.get("symbol") and frame["symbol"] not in symbols:
                continue
            kind = frame.get("type", "")
            channel = "regime" if kind == "regime_change" else kind.split("_")[0]
            if channel in channels or kind in ("hello", "market_state", "replay_state"):
                await websocket.send_json(frame)

    pump_task = asyncio.ensure_future(pump())
    try:
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
                await websocket.send_json(
                    _envelope(
                        bus,
                        {
                            "type": "clock_sync_reply",
                            "client_send_ns": int(message["client_send_ns"]),
                            "server_recv_ns": server_recv_ns,
                            "server_send_ns": time.time_ns(),
                        },
                    )
                )
            elif kind == "ack":
                round_trip, offset = compute_sync(
                    client_send_ns=int(message.get("client_send_ns", server_recv_ns)),
                    server_recv_ns=server_recv_ns,
                    server_send_ns=server_recv_ns,
                    client_recv_ns=int(message["client_recv_ns"]),
                )
                sync.add(round_trip, offset)
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
        pump_task.cancel()
        bus.unsubscribe(sub)
