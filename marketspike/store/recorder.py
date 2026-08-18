import asyncio
import logging
import sqlite3
from collections import deque
from typing import Any, Deque, Dict, List, Tuple

from marketspike.feeds.base import Tick

LOGGER = logging.getLogger(__name__)

TICK_SQL = (
    "INSERT INTO ticks (symbol, venue_ts_ns, recv_ts_ns, bid, ask, bid_qty, "
    "ask_qty, excess_transit_us, engine_us, tradeable, source) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
REGIME_SQL = (
    "INSERT INTO regime_events (ts_ns, symbol, from_state, to_state, score, "
    "v_ratio, spread_z, trigger, event_context) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


class Recorder:
    """Drains a bounded queue into SQLite on a thread executor.

    The engine never awaits disk. When the queue is full, rows are dropped and
    counted rather than applying backpressure to the data path (spec §11.1).
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        max_queue: int = 10000,
        batch_size: int = 500,
        flush_interval_s: float = 0.25,
    ) -> None:
        self.conn = conn
        self._max_queue = max_queue
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._queue: Deque[Tuple[str, Tuple[Any, ...]]] = deque()
        self._lock = asyncio.Lock()
        self.counters: Dict[str, int] = {
            "recorder_dropped_total": 0,
            "recorder_written_total": 0,
            "recorder_write_failed_total": 0,
        }

    def _submit(self, kind: str, params: Tuple[Any, ...]) -> bool:
        if len(self._queue) >= self._max_queue:
            self.counters["recorder_dropped_total"] += 1
            return False
        self._queue.append((kind, params))
        return True

    def submit_tick(self, tick: Tick, excess_transit_us: int, engine_us: int) -> bool:
        return self._submit(
            "tick",
            (
                tick.symbol, tick.venue_ts_ns, tick.recv_ts_ns, tick.bid, tick.ask,
                tick.bid_qty, tick.ask_qty, excess_transit_us, engine_us,
                1 if tick.tradeable else 0, tick.source,
            ),
        )

    def submit_regime(
        self, ts_ns: int, symbol: str, from_state: str, to_state: str,
        score: float, v_ratio: float, spread_z: float, trigger: str,
        event_context: str,
    ) -> bool:
        return self._submit(
            "regime",
            (ts_ns, symbol, from_state, to_state, score, v_ratio, spread_z,
             trigger, event_context),
        )

    def _drain_batch(self) -> Dict[str, List[Tuple[Any, ...]]]:
        batch: Dict[str, List[Tuple[Any, ...]]] = {"tick": [], "regime": []}
        for _ in range(min(self._batch_size, len(self._queue))):
            kind, params = self._queue.popleft()
            batch[kind].append(params)
        return batch

    def _write(self, batch: Dict[str, List[Tuple[Any, ...]]]) -> int:
        written = 0
        if batch["tick"]:
            self.conn.executemany(TICK_SQL, batch["tick"])
            written += len(batch["tick"])
        if batch["regime"]:
            self.conn.executemany(REGIME_SQL, batch["regime"])
            written += len(batch["regime"])
        if written:
            self.conn.commit()
        return written

    async def flush_once(self) -> int:
        async with self._lock:
            if not self._queue:
                return 0
            batch = self._drain_batch()
            batch_len = sum(len(rows) for rows in batch.values())
            loop = asyncio.get_event_loop()
            try:
                written = await loop.run_in_executor(None, self._write, batch)
            except Exception:
                LOGGER.exception(
                    "recorder write failed; dropping %d rows", batch_len
                )
                self.counters["recorder_write_failed_total"] += batch_len
                return 0
            self.counters["recorder_written_total"] += written
            return written

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval_s)
            try:
                while self._queue:
                    await self.flush_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("recorder run() loop encountered an error")
