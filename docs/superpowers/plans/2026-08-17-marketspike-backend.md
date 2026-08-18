# MarketSpike Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the MarketSpike backend — a FastAPI service that ingests live BTCUSDT and EURUSD quotes, measures real pipeline latency with clock-skew correction, detects volatility regimes, and returns slippage-aware position sizes over a frozen REST/WebSocket contract.

**Architecture:** One FastAPI process, one asyncio event loop. Feed adapters normalise venue messages to a common `Tick` and push through a bounded queue into a per-symbol engine (latency timing, volatility, spread, regime, slippage). The engine publishes to an in-process `Bus` consumed by the WebSocket endpoint, and to a separate recorder queue drained to SQLite on a thread executor so disk never blocks the data path.

**Tech Stack:** Python 3.8+, FastAPI, uvicorn, `websockets`, `httpx`, Pydantic v2, SQLite (WAL), scikit-learn (offline training only), pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-08-17-marketspike-design.md` — section references below (§N) point into it.

## Global Constraints

- **Python 3.8+ compatible syntax.** Use `Optional[X]`, `Dict[K, V]`, `List[X]` from `typing` — never `X | None` or builtin generics. No `slots=True` on dataclasses, no `match`, no `asyncio.TaskGroup`. This code must run on the EOL 3.8 currently installed and unchanged on 3.11+.
- **Every latency and tick value carries `source`** ∈ `{"measured", "estimated", "simulated"}` (§2.4, §6.5). No synthetic value is ever emitted without it.
- **Disk I/O never blocks the data path** (§2.4, §11.1). Nothing in `engine/` or `feeds/` may `await` a database call or import `sqlite3`.
- **Module boundaries:** `engine/` never imports `api/`. `risk/` never imports `feeds/`. `sqlite3` is imported only inside `store/`.
- **Timestamps are integer nanoseconds since epoch** everywhere — never float, never text (§11.4).
- **Money is integer minor units; prices are float** (§11.4).
- **API schema version is `1`.** Every WebSocket frame carries `"v": 1`. Changes within v1 are additive only (§12).
- **Volatility is always variance-per-second** in both horizons (§7.1). Normalising per-horizon introduces a factor-of-60 error.
- **Lot sizes always round DOWN** to `lot_step` (§10.4).
- Commit after every task. Conventional commit prefixes (`feat:`, `test:`, `fix:`, `chore:`).

---

## File Structure

| Path | Responsibility |
|---|---|
| `marketspike/config.py` | Env-var settings, single `Settings` instance |
| `marketspike/feeds/base.py` | `Tick` dataclass, `FeedAdapter` protocol, `rfc3339_to_ns` |
| `marketspike/feeds/binance.py` | Binance `bookTicker` WebSocket adapter + kline seeding |
| `marketspike/feeds/oanda.py` | OANDA v20 chunked-NDJSON adapter + candle seeding |
| `marketspike/feeds/replay.py` | File-driven adapter emitting `source="simulated"` |
| `marketspike/clock/skew.py` | `SkewEstimator` — sliding-window minimum transit floor |
| `marketspike/clock/sync.py` | `compute_sync` — NTP four-timestamp offset/round-trip |
| `marketspike/engine/pipeline.py` | `PipelineTimer` — hop stamping, percentile aggregation |
| `marketspike/engine/volatility.py` | `VolatilityCalc` — time-weighted EWMA variance rate |
| `marketspike/engine/spread.py` | `SpreadTracker` — rolling median/MAD z-score |
| `marketspike/engine/regime.py` | `RegimeFSM` — hysteresis + dwell state machine |
| `marketspike/engine/scoring.py` | `composite_score` — combines V ratio and spread z |
| `marketspike/engine/bus.py` | `Bus` — async fan-out with per-subscriber bounded queues |
| `marketspike/engine/supervisor.py` | `supervise` — restart-with-backoff task wrapper |
| `marketspike/engine/symbol_state.py` | `SymbolEngine` — per-symbol composition of the above |
| `marketspike/calendar/clock.py` | `EventClock` — phase and signed seconds to release |
| `marketspike/calendar/static_events.json` | Curated high-impact release schedule |
| `marketspike/risk/instruments.py` | Instrument registry + `instruments.json` |
| `marketspike/risk/slippage.py` | `SlippageModel` — dot-product inference, fallback load |
| `marketspike/risk/sizing.py` | `size_position` — the core calculation |
| `marketspike/store/schema.sql` | Full DDL |
| `marketspike/store/db.py` | Connection management, pragmas, schema bootstrap |
| `marketspike/store/recorder.py` | Bounded queue → batched executor writes |
| `marketspike/store/queries.py` | Read-only query layer |
| `marketspike/api/schemas.py` | All Pydantic models — single source of truth |
| `marketspike/api/rest.py` | REST routes |
| `marketspike/api/ws.py` | WebSocket endpoint |
| `marketspike/ml/features.py` | Feature builder with structural leakage guard |
| `marketspike/ml/train.py` | Quantile regression fit → `model.json` |
| `marketspike/ml/evaluate.py` | Pinball loss, coverage, per-regime breakdown |
| `marketspike/main.py` | App assembly, lifespan, task startup |

---

## Phase 0 — Foundation (spec §11, §12; hours 0–2)

The highest-leverage work of the build: it publishes the contract and unblocks the frontend.

### Task 1: Project scaffold, config, and database bootstrap

**Files:**
- Create: `pyproject.toml`, `requirements.txt`, `.gitignore`, `.env.example`
- Create: `marketspike/__init__.py`, `marketspike/config.py`
- Create: `marketspike/store/__init__.py`, `marketspike/store/schema.sql`, `marketspike/store/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Settings` (attributes `symbols: List[str]`, `db_path: str`, `oanda_token: Optional[str]`, `oanda_account_id: Optional[str]`, `tau_fast_s: float`, `tau_slow_s: float`, `skew_window_s: float`, `ws_max_hz: float`, `model_path: str`, `max_tick_age_hours: int`); `get_settings() -> Settings`; `open_db(path: str, read_only: bool = False) -> sqlite3.Connection`; `apply_schema(conn) -> None`; `SCHEMA_VERSION: int = 1`

- [ ] **Step 1: Create `requirements.txt`**

```
fastapi==0.110.0
uvicorn[standard]==0.27.1
websockets==11.0.3
httpx==0.26.0
pydantic==2.6.4
pytest==8.0.2
pytest-asyncio==0.23.5
scikit-learn==1.3.2
numpy==1.24.4
```

- [ ] **Step 2: Create `.gitignore`**

```
__pycache__/
*.py[cod]
.venv/
venv/
.env
*.db
*.db-wal
*.db-shm
.pytest_cache/
model.json
scenarios/*.ndjson
```

- [ ] **Step 3: Create `.env.example`**

```
MS_SYMBOLS=BTCUSDT,EURUSD
MS_OANDA_TOKEN=
MS_OANDA_ACCOUNT_ID=
MS_DB_PATH=./marketspike.db
MS_TAU_FAST_S=30
MS_TAU_SLOW_S=1800
MS_SKEW_WINDOW_S=60
MS_WS_MAX_HZ=20
MS_MODEL_PATH=./model.json
MS_MAX_TICK_AGE_HOURS=0
```

- [ ] **Step 4: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "marketspike"
version = "1.0.0"
requires-python = ">=3.8"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 5: Create `marketspike/config.py`**

```python
import os
from dataclasses import dataclass, field
from typing import List, Optional


def _split(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class Settings:
    symbols: List[str] = field(default_factory=list)
    db_path: str = "./marketspike.db"
    oanda_token: Optional[str] = None
    oanda_account_id: Optional[str] = None
    tau_fast_s: float = 30.0
    tau_slow_s: float = 1800.0
    skew_window_s: float = 60.0
    ws_max_hz: float = 20.0
    model_path: str = "./model.json"
    max_tick_age_hours: int = 0


def get_settings() -> Settings:
    return Settings(
        symbols=_split(os.getenv("MS_SYMBOLS", "BTCUSDT,EURUSD")),
        db_path=os.getenv("MS_DB_PATH", "./marketspike.db"),
        oanda_token=os.getenv("MS_OANDA_TOKEN") or None,
        oanda_account_id=os.getenv("MS_OANDA_ACCOUNT_ID") or None,
        tau_fast_s=float(os.getenv("MS_TAU_FAST_S", "30")),
        tau_slow_s=float(os.getenv("MS_TAU_SLOW_S", "1800")),
        skew_window_s=float(os.getenv("MS_SKEW_WINDOW_S", "60")),
        ws_max_hz=float(os.getenv("MS_WS_MAX_HZ", "20")),
        model_path=os.getenv("MS_MODEL_PATH", "./model.json"),
        max_tick_age_hours=int(os.getenv("MS_MAX_TICK_AGE_HOURS", "0")),
    )
```

- [ ] **Step 6: Create `marketspike/store/schema.sql`**

Copy the DDL from spec §11.3 verbatim — all eight tables plus indexes. It is already written there; do not retype it from memory, copy it.

- [ ] **Step 7: Write the failing test — `tests/test_db.py`**

```python
import sqlite3
from marketspike.store.db import open_db, apply_schema, SCHEMA_VERSION


def test_apply_schema_creates_all_tables(tmp_path):
    conn = open_db(str(tmp_path / "t.db"))
    apply_schema(conn)
    names = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {
        "ticks", "regime_events", "client_latency", "calc_log",
        "training_samples", "model_registry", "calendar_events", "schema_version",
    } <= names


def test_apply_schema_is_idempotent(tmp_path):
    conn = open_db(str(tmp_path / "t.db"))
    apply_schema(conn)
    apply_schema(conn)
    rows = [tuple(row) for row in conn.execute("SELECT version FROM schema_version")]
    assert rows == [(SCHEMA_VERSION,)]


def test_wal_mode_enabled(tmp_path):
    conn = open_db(str(tmp_path / "t.db"))
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
```

- [ ] **Step 8: Run test to verify it fails**

Run: `python -m pytest tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.store.db'`

- [ ] **Step 9: Create `marketspike/store/db.py`**

```python
import os
import sqlite3

SCHEMA_VERSION = 1
_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def open_db(path: str, read_only: bool = False) -> sqlite3.Connection:
    if read_only:
        conn = sqlite3.connect(
            "file:{0}?mode=ro".format(path), uri=True, check_same_thread=False
        )
    else:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -64000")
    conn.row_factory = sqlite3.Row
    return conn


def apply_schema(conn: sqlite3.Connection) -> None:
    with open(_SCHEMA_PATH, "r") as handle:
        conn.executescript(handle.read())
    conn.execute("DELETE FROM schema_version")
    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    conn.commit()
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `python -m pytest tests/test_db.py -v`
Expected: 3 passed

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml requirements.txt .gitignore .env.example marketspike tests
git commit -m "feat: project scaffold, settings, and SQLite schema bootstrap"
```

---

### Task 2: Frozen API schemas and published contract

This task is what unblocks the frontend. It ships **before** any engine code exists.

**Files:**
- Create: `marketspike/api/__init__.py`, `marketspike/api/schemas.py`
- Create: `scripts/export_contract.py`
- Create: `docs/api/examples/*.json` (generated)
- Test: `tests/test_contract.py`

**Interfaces:**
- Consumes: nothing
- Produces: Pydantic models `TickFrame`, `LatencyFrame`, `RegimeChangeFrame`, `EventAlertFrame`, `MarketStateFrame`, `HelloFrame`, `ReplayStateFrame`, `ErrorFrame`, `ClockSyncReply`, `SizeRequest`, `SizeResponse`, `HealthResponse`, `Instrument`. All frames carry `v: int = 1`, `type: str`, `seq: int`, `server_ts_ns: int`.

- [ ] **Step 1: Create `marketspike/api/schemas.py`**

```python
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

SCHEMA_V = 1
Source = str  # "measured" | "estimated" | "simulated"


class Frame(BaseModel):
    v: int = SCHEMA_V
    type: str
    seq: int
    server_ts_ns: int


class HelloFrame(Frame):
    type: str = "hello"
    session_id: str
    server_version: str
    warmup_complete: bool
    feeds: Dict[str, str]
    mode: str


class TickFrame(Frame):
    type: str = "tick"
    symbol: str
    bid: float
    ask: float
    mid: float
    spread_bps: float
    spread_pips: float
    quote_rate_hz: float
    book_imbalance: float
    tradeable: bool
    source: Source


class LatencyFrame(Frame):
    type: str = "latency"
    symbol: str
    excess_transit_us: int
    engine_us: int
    delivery_us: Optional[int] = None
    p50_us: int
    p95_us: int
    p99_us: int
    source: Source
    baseline_includes_clock_offset: bool = True


class RegimeChangeFrame(Frame):
    type: str = "regime_change"
    symbol: str
    from_state: str = Field(alias="from")
    to_state: str = Field(alias="to")
    score: float
    v_ratio: float
    spread_z: float
    event_context: str
    trigger: str

    model_config = {"populate_by_name": True}


class EventAlertFrame(Frame):
    type: str = "event_alert"
    name: str
    importance: str
    event_ts_ns: int
    seconds_until: int
    phase: str
    affects: List[str]


class MarketStateFrame(Frame):
    type: str = "market_state"
    symbol: str
    tradeable: bool
    reason: str
    next_open_ts_ns: Optional[int] = None


class ReplayStateFrame(Frame):
    type: str = "replay_state"
    mode: str
    scenario: str
    progress_pct: float
    source: Source = "simulated"


class ClockSyncReply(Frame):
    type: str = "clock_sync_reply"
    client_send_ns: int
    server_recv_ns: int
    server_send_ns: int


class ErrorFrame(Frame):
    type: str = "error"
    code: str
    detail: str


class Instrument(BaseModel):
    symbol: str
    pip_size: float
    contract_size: float
    quote_ccy: str
    min_lot: float
    lot_step: float
    margin_rate: float


class SizeRequest(BaseModel):
    symbol: str
    account_balance_minor: int
    account_ccy: str = "USD"
    risk_pct: float
    stop_distance_price: float
    direction: str = "buy"
    quantile: str = "p95"
    free_margin_minor: int
    assumed_latency_ms: Optional[float] = None


class SizeResponse(BaseModel):
    naive_lot_size: float
    recommended_lot_size: float
    overexposure_pct: float
    slippage_p50_pips: float
    slippage_p95_pips: float
    stop_distance_pips: float
    effective_adverse_pips: float
    actual_risk_amount_minor: int
    actual_risk_pct: float
    required_margin_minor: int
    capped_by: Optional[str] = None
    fx_assumed: bool = False
    stale_quote: bool = False
    model_source: str
    model_version: str
    regime_at_calc: str
    event_context: str
    latency_used_ms: float
    latency_source: Source
    warnings: List[str] = Field(default_factory=list)
    inputs_echo: Dict[str, Any] = Field(default_factory=dict)


class FeedHealth(BaseModel):
    venue: str
    connected: bool
    last_tick_age_ms: Optional[int] = None
    warmup_complete: bool = False
    tradeable: bool = True
    reason: Optional[str] = None


class HealthResponse(BaseModel):
    v: int = SCHEMA_V
    status: str
    uptime_s: int
    feeds: Dict[str, FeedHealth]
    counters: Dict[str, int]
    model: Dict[str, str]
    mode: str
```

- [ ] **Step 2: Write the failing contract test — `tests/test_contract.py`**

```python
import json
import pathlib

import pytest

from marketspike.api import schemas

EXAMPLES = pathlib.Path("docs/api/examples")

MODEL_FOR_TYPE = {
    "hello": schemas.HelloFrame,
    "tick": schemas.TickFrame,
    "latency": schemas.LatencyFrame,
    "regime_change": schemas.RegimeChangeFrame,
    "event_alert": schemas.EventAlertFrame,
    "market_state": schemas.MarketStateFrame,
    "replay_state": schemas.ReplayStateFrame,
    "clock_sync_reply": schemas.ClockSyncReply,
    "error": schemas.ErrorFrame,
}


def test_examples_directory_is_populated():
    assert list(EXAMPLES.glob("*.json")), "run scripts/export_contract.py"


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("frame_*.json")))
def test_frame_examples_validate(path):
    payload = json.loads(path.read_text())
    model = MODEL_FOR_TYPE[payload["type"]]
    parsed = model.model_validate(payload)
    assert parsed.v == 1


def test_size_example_validates():
    req = json.loads((EXAMPLES / "size_request.json").read_text())
    res = json.loads((EXAMPLES / "size_response.json").read_text())
    schemas.SizeRequest.model_validate(req)
    parsed = schemas.SizeResponse.model_validate(res)
    assert parsed.recommended_lot_size <= parsed.naive_lot_size
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/test_contract.py -v`
Expected: FAIL — `test_examples_directory_is_populated` fails, parametrised tests collect zero cases

- [ ] **Step 4: Create `scripts/export_contract.py`**

```python
"""Generate docs/api/openapi.json and docs/api/examples/*.json from the models."""
import json
import pathlib

from marketspike.api import schemas

OUT = pathlib.Path("docs/api")
EX = OUT / "examples"

FRAMES = {
    "hello": schemas.HelloFrame(
        seq=1, server_ts_ns=1723891200000000000, session_id="a1b2",
        server_version="1.0.0", warmup_complete=False,
        feeds={"BTCUSDT": "binance", "EURUSD": "oanda"}, mode="live",
    ),
    "tick": schemas.TickFrame(
        seq=2, server_ts_ns=1723891200100000000, symbol="EURUSD",
        bid=1.08512, ask=1.08525, mid=1.085185, spread_bps=1.20,
        spread_pips=1.3, quote_rate_hz=6.4, book_imbalance=-0.13,
        tradeable=True, source="measured",
    ),
    "latency": schemas.LatencyFrame(
        seq=3, server_ts_ns=1723891200200000000, symbol="EURUSD",
        excess_transit_us=4100, engine_us=180, delivery_us=19400,
        p50_us=21000, p95_us=68000, p99_us=141000, source="estimated",
    ),
    "regime_change": schemas.RegimeChangeFrame(
        seq=4, server_ts_ns=1723891200300000000, symbol="EURUSD",
        from_state="ELEVATED", to_state="SPIKE", score=3.1, v_ratio=4.8,
        spread_z=6.2, event_context="EVENT_WINDOW", trigger="vol_ratio",
    ),
    "event_alert": schemas.EventAlertFrame(
        seq=5, server_ts_ns=1723891200400000000, name="US CPI (YoY)",
        importance="high", event_ts_ns=1723891800000000000,
        seconds_until=1800, phase="PRE_EVENT", affects=["EURUSD", "BTCUSDT"],
    ),
    "market_state": schemas.MarketStateFrame(
        seq=6, server_ts_ns=1723891200500000000, symbol="EURUSD",
        tradeable=False, reason="market_closed",
        next_open_ts_ns=1723921200000000000,
    ),
    "replay_state": schemas.ReplayStateFrame(
        seq=7, server_ts_ns=1723891200600000000, mode="replay",
        scenario="cpi_2026_07_11", progress_pct=34.2,
    ),
    "clock_sync_reply": schemas.ClockSyncReply(
        seq=8, server_ts_ns=1723891200700000000,
        client_send_ns=1723891200123456789,
        server_recv_ns=1723891200141902311,
        server_send_ns=1723891200141998042,
    ),
    "error": schemas.ErrorFrame(
        seq=9, server_ts_ns=1723891200800000000, code="UNKNOWN_SYMBOL",
        detail="GBPJPY is not in the instrument registry",
    ),
}

SIZE_REQUEST = schemas.SizeRequest(
    symbol="EURUSD", account_balance_minor=1000000, account_ccy="USD",
    risk_pct=1.0, stop_distance_price=0.0020, direction="buy",
    quantile="p95", free_margin_minor=1000000, assumed_latency_ms=None,
)

SIZE_RESPONSE = schemas.SizeResponse(
    naive_lot_size=0.50, recommended_lot_size=0.38, overexposure_pct=31.6,
    slippage_p50_pips=1.4, slippage_p95_pips=6.2, stop_distance_pips=20.0,
    effective_adverse_pips=26.2, actual_risk_amount_minor=9956,
    actual_risk_pct=0.9956, required_margin_minor=137296, capped_by=None,
    fx_assumed=False, stale_quote=False, model_source="trained",
    model_version="eurusd-2026-08-17T04:12Z", regime_at_calc="SPIKE",
    event_context="EVENT_WINDOW", latency_used_ms=63.2,
    latency_source="measured", warnings=[],
    inputs_echo=SIZE_REQUEST.model_dump(),
)


def main() -> None:
    EX.mkdir(parents=True, exist_ok=True)
    for name, frame in FRAMES.items():
        path = EX / "frame_{0}.json".format(name)
        path.write_text(json.dumps(frame.model_dump(by_alias=True), indent=2))
    (EX / "size_request.json").write_text(
        json.dumps(SIZE_REQUEST.model_dump(), indent=2)
    )
    (EX / "size_response.json").write_text(
        json.dumps(SIZE_RESPONSE.model_dump(), indent=2)
    )
    print("wrote {0} examples to {1}".format(len(FRAMES) + 2, EX))


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Generate the examples**

Run: `python scripts/export_contract.py`
Expected: `wrote 11 examples to docs/api/examples`

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/test_contract.py -v`
Expected: 11 passed (1 directory check + 9 frame cases + 1 size case)

- [ ] **Step 7: Write the frontend handoff note — `docs/api/README.md`**

```markdown
# MarketSpike API — v1 (FROZEN)

Build against `examples/*.json`. Every payload here is literal and validated
in CI by `tests/test_contract.py`.

- REST base: `http://localhost:8000/api/v1`
- WebSocket:  `ws://localhost:8000/ws/v1/stream`

## Rules

1. Every frame carries `"v": 1`. Changes within v1 are **additive only**.
2. **Ignore unknown fields.** Adding a field is never a breaking change.
3. **`source` must be surfaced in the UI.** Any value other than `"measured"`
   MUST be visibly badged. `"simulated"` means replay data.
4. `regime_change` fires only on transition — not per tick.
5. Errors never close the socket.
6. REST errors are RFC 7807 problem details.

## Client → server frames

    {"type":"subscribe","symbols":["BTCUSDT","EURUSD"],
     "channels":["tick","regime","latency","event"]}
    {"type":"unsubscribe","symbols":["EURUSD"]}
    {"type":"clock_sync","client_send_ns":1723891200123456789}
    {"type":"ack","seq":4471,"client_recv_ns":1723891200987654321}
```

- [ ] **Step 8: Commit and push — the frontend is now unblocked**

```bash
git add marketspike/api scripts docs/api tests/test_contract.py
git commit -m "feat: freeze v1 API schemas and publish contract examples"
git push
```

---

## Phase 1 — Feeds and recorder (spec §3, §5.1, §11; hours 2–5)

Recording starts here and never stops. Training data is the only deliverable that cannot be compressed by working harder.

### Task 3: Tick model, feed protocol, and Binance adapter

**Files:**
- Create: `marketspike/feeds/__init__.py`, `marketspike/feeds/base.py`, `marketspike/feeds/binance.py`
- Test: `tests/test_feeds_binance.py`

**Interfaces:**
- Consumes: `Settings` from Task 1
- Produces: `Tick` (frozen dataclass: `symbol: str`, `venue_ts_ns: int`, `recv_ts_ns: int`, `bid: float`, `ask: float`, `bid_qty: float`, `ask_qty: float`, `tradeable: bool`, `source: str`; properties `mid: float`, `spread: float`); `rfc3339_to_ns(str) -> int`; `FeedAdapter` protocol with `symbol`, `venue`, `async stream()`, `async seed_baseline()`; `BinanceAdapter(symbol)`; `parse_book_ticker(raw: Dict, recv_ts_ns: int) -> Optional[Tick]`; `variance_per_second_from_closes(closes: List[float], interval_s: float) -> float`

- [ ] **Step 1: Write the failing test — `tests/test_feeds_binance.py`**

```python
import math

from marketspike.feeds.base import rfc3339_to_ns
from marketspike.feeds.binance import parse_book_ticker, variance_per_second_from_closes

FRAME = {
    "stream": "btcusdt@bookTicker",
    "data": {
        "u": 400900217, "s": "BTCUSDT", "E": 1723891200123,
        "b": "63120.50", "B": "1.234", "a": "63121.90", "A": "0.876",
    },
}


def test_parse_book_ticker_maps_all_fields():
    tick = parse_book_ticker(FRAME, recv_ts_ns=1723891200_200_000_000)
    assert tick.symbol == "BTCUSDT"
    assert tick.venue_ts_ns == 1723891200123 * 1_000_000
    assert tick.bid == 63120.50
    assert tick.ask == 63121.90
    assert tick.bid_qty == 1.234
    assert tick.source == "measured"
    assert tick.tradeable is True


def test_parse_book_ticker_computes_mid_and_spread():
    tick = parse_book_ticker(FRAME, recv_ts_ns=1)
    assert tick.mid == (63120.50 + 63121.90) / 2
    assert abs(tick.spread - 1.40) < 1e-9


def test_parse_book_ticker_ignores_non_tick_frames():
    assert parse_book_ticker({"result": None, "id": 1}, recv_ts_ns=1) is None


def test_rfc3339_to_ns_keeps_nanosecond_precision():
    assert rfc3339_to_ns("2026-08-17T14:23:01.123456789Z") % 1_000_000_000 == 123456789


def test_rfc3339_to_ns_handles_absent_fraction():
    assert rfc3339_to_ns("2026-08-17T14:23:01Z") % 1_000_000_000 == 0


def test_variance_per_second_normalises_by_interval():
    # Constant 1% move each minute -> per-minute variance is (ln 1.01)^2.
    closes = [100.0 * (1.01 ** i) for i in range(11)]
    var_s = variance_per_second_from_closes(closes, interval_s=60.0)
    assert abs(var_s - (math.log(1.01) ** 2) / 60.0) < 1e-12
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_feeds_binance.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.feeds'`

- [ ] **Step 3: Create `marketspike/feeds/base.py`**

```python
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, List, Optional

try:
    from typing import Protocol
except ImportError:  # pragma: no cover - Python 3.7 fallback
    Protocol = object  # type: ignore


@dataclass(frozen=True)
class Tick:
    symbol: str
    venue_ts_ns: int
    recv_ts_ns: int
    bid: float
    ask: float
    bid_qty: float
    ask_qty: float
    tradeable: bool
    source: str

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def spread_bps(self) -> float:
        mid = self.mid
        return 0.0 if mid <= 0 else (self.spread / mid) * 10000.0

    @property
    def book_imbalance(self) -> float:
        total = self.bid_qty + self.ask_qty
        return 0.0 if total <= 0 else (self.bid_qty - self.ask_qty) / total


def rfc3339_to_ns(value: str) -> int:
    """Parse an RFC3339 timestamp to integer nanoseconds.

    datetime cannot represent nanoseconds, so the fractional part is handled
    separately rather than through strptime.
    """
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1]
    head, _, frac = text.partition(".")
    moment = datetime.strptime(head, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    total = int(moment.timestamp()) * 1_000_000_000
    if frac:
        total += int(frac.ljust(9, "0")[:9])
    return total


class FeedAdapter(Protocol):
    symbol: str
    venue: str

    def stream(self) -> AsyncIterator[Tick]:
        """Yield normalised ticks until cancelled.

        Must not raise on transient network failure — reconnect internally
        with backoff.
        """
        ...

    async def seed_baseline(self) -> Optional[float]:
        """Return initial slow-horizon variance per second, or None."""
        ...
```

- [ ] **Step 4: Create `marketspike/feeds/binance.py`**

```python
import asyncio
import json
import math
import random
import time
from typing import AsyncIterator, Dict, List, Optional

import httpx
import websockets

from marketspike.feeds.base import Tick

WS_URL = "wss://stream.binance.com:9443/stream?streams={0}@bookTicker"
KLINES_URL = "https://api.binance.com/api/v3/klines"


def parse_book_ticker(raw: Dict, recv_ts_ns: int) -> Optional[Tick]:
    data = raw.get("data", raw)
    if "s" not in data or "b" not in data or "E" not in data:
        return None
    return Tick(
        symbol=data["s"],
        venue_ts_ns=int(data["E"]) * 1_000_000,
        recv_ts_ns=recv_ts_ns,
        bid=float(data["b"]),
        ask=float(data["a"]),
        bid_qty=float(data["B"]),
        ask_qty=float(data["A"]),
        tradeable=True,
        source="measured",
    )


def variance_per_second_from_closes(closes: List[float], interval_s: float) -> float:
    """Mean squared log return divided by the bar interval (§7.2)."""
    if len(closes) < 2 or interval_s <= 0:
        return 0.0
    returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    if not returns:
        return 0.0
    return (sum(r * r for r in returns) / len(returns)) / interval_s


class BinanceAdapter:
    venue = "binance"

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.connected = False

    async def seed_baseline(self) -> Optional[float]:
        params = {"symbol": self.symbol, "interval": "1m", "limit": 1440}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(KLINES_URL, params=params)
                response.raise_for_status()
                closes = [float(row[4]) for row in response.json()]
        except Exception:
            return None
        return variance_per_second_from_closes(closes, interval_s=60.0)

    async def stream(self) -> AsyncIterator[Tick]:
        url = WS_URL.format(self.symbol.lower())
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as socket:
                    self.connected = True
                    backoff = 1.0
                    async for message in socket:
                        recv_ts_ns = time.time_ns()
                        tick = parse_book_ticker(json.loads(message), recv_ts_ns)
                        if tick is not None:
                            yield tick
            except asyncio.CancelledError:
                raise
            except Exception:
                self.connected = False
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30.0)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_feeds_binance.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add marketspike/feeds tests/test_feeds_binance.py
git commit -m "feat: Tick model, feed protocol, and Binance bookTicker adapter"
```

---

### Task 4: OANDA adapter with market-closed handling

**Files:**
- Create: `marketspike/feeds/oanda.py`
- Test: `tests/test_feeds_oanda.py`

**Interfaces:**
- Consumes: `Tick`, `rfc3339_to_ns` from Task 3
- Produces: `parse_price(raw: Dict, recv_ts_ns: int) -> Optional[Tick]`; `OandaAdapter(symbol, token, account_id)` implementing `FeedAdapter`

- [ ] **Step 1: Write the failing test — `tests/test_feeds_oanda.py`**

```python
from marketspike.feeds.oanda import parse_price

PRICE = {
    "type": "PRICE",
    "time": "2026-08-17T14:23:01.123456789Z",
    "instrument": "EUR_USD",
    "bids": [{"price": "1.08512", "liquidity": 10000000}],
    "asks": [{"price": "1.08525", "liquidity": 10000000}],
    "status": "tradeable",
    "tradeable": True,
}


def test_parse_price_normalises_symbol_and_prices():
    tick = parse_price(PRICE, recv_ts_ns=1723891200_200_000_000)
    assert tick.symbol == "EURUSD"
    assert tick.bid == 1.08512
    assert tick.ask == 1.08525
    assert tick.bid_qty == 10000000.0
    assert tick.source == "measured"


def test_parse_price_preserves_nanosecond_venue_time():
    tick = parse_price(PRICE, recv_ts_ns=1)
    assert tick.venue_ts_ns % 1_000_000_000 == 123456789


def test_parse_price_skips_heartbeats():
    assert parse_price({"type": "HEARTBEAT", "time": "2026-08-17T14:23:01Z"}, 1) is None


def test_parse_price_marks_untradeable_when_market_closed():
    closed = dict(PRICE, tradeable=False, status="non-tradeable")
    assert parse_price(closed, recv_ts_ns=1).tradeable is False


def test_parse_price_returns_none_when_book_side_missing():
    assert parse_price(dict(PRICE, bids=[]), recv_ts_ns=1) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_feeds_oanda.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.feeds.oanda'`

- [ ] **Step 3: Create `marketspike/feeds/oanda.py`**

```python
import asyncio
import json
import random
import time
from typing import AsyncIterator, Dict, List, Optional

import httpx

from marketspike.feeds.base import Tick, rfc3339_to_ns
from marketspike.feeds.binance import variance_per_second_from_closes

STREAM_URL = "https://stream-fxpractice.oanda.com/v3/accounts/{0}/pricing/stream"
CANDLES_URL = "https://api-fxpractice.oanda.com/v3/instruments/{0}/candles"


def _to_instrument(symbol: str) -> str:
    return symbol[:3] + "_" + symbol[3:] if "_" not in symbol else symbol


def parse_price(raw: Dict, recv_ts_ns: int) -> Optional[Tick]:
    if raw.get("type") != "PRICE":
        return None
    bids: List[Dict] = raw.get("bids") or []
    asks: List[Dict] = raw.get("asks") or []
    if not bids or not asks:
        return None
    return Tick(
        symbol=raw["instrument"].replace("_", ""),
        venue_ts_ns=rfc3339_to_ns(raw["time"]),
        recv_ts_ns=recv_ts_ns,
        bid=float(bids[0]["price"]),
        ask=float(asks[0]["price"]),
        bid_qty=float(bids[0].get("liquidity", 0.0)),
        ask_qty=float(asks[0].get("liquidity", 0.0)),
        tradeable=bool(raw.get("tradeable", True)),
        source="measured",
    )


class OandaAdapter:
    venue = "oanda"

    def __init__(self, symbol: str, token: str, account_id: str) -> None:
        self.symbol = symbol
        self.instrument = _to_instrument(symbol)
        self._token = token
        self._account_id = account_id
        self.connected = False

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer {0}".format(self._token)}

    async def seed_baseline(self) -> Optional[float]:
        params = {"price": "M", "granularity": "M1", "count": 1440}
        url = CANDLES_URL.format(self.instrument)
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, params=params, headers=self._headers())
                response.raise_for_status()
                candles = response.json().get("candles", [])
                closes = [
                    float(c["mid"]["c"]) for c in candles if c.get("complete")
                ]
        except Exception:
            return None
        return variance_per_second_from_closes(closes, interval_s=60.0)

    async def stream(self) -> AsyncIterator[Tick]:
        url = STREAM_URL.format(self._account_id)
        params = {"instruments": self.instrument}
        backoff = 1.0
        while True:
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream(
                        "GET", url, params=params, headers=self._headers()
                    ) as response:
                        response.raise_for_status()
                        self.connected = True
                        backoff = 1.0
                        async for line in response.aiter_lines():
                            if not line.strip():
                                continue
                            recv_ts_ns = time.time_ns()
                            tick = parse_price(json.loads(line), recv_ts_ns)
                            if tick is not None:
                                yield tick
            except asyncio.CancelledError:
                raise
            except Exception:
                self.connected = False
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30.0)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_feeds_oanda.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/feeds/oanda.py tests/test_feeds_oanda.py
git commit -m "feat: OANDA v20 pricing-stream adapter with market-closed detection"
```

---

### Task 5: Recorder — bounded queue, batched writes, drop counters

**Files:**
- Create: `marketspike/store/recorder.py`
- Test: `tests/test_recorder.py`

**Interfaces:**
- Consumes: `open_db`, `apply_schema` from Task 1; `Tick` from Task 3
- Produces: `Recorder(conn, max_queue=10000, batch_size=500, flush_interval_s=0.25)` with `submit_tick(tick, excess_transit_us, engine_us) -> bool`, `submit_regime(...) -> bool`, `async run()`, `async flush_once()`, `counters: Dict[str, int]`

- [ ] **Step 1: Write the failing test — `tests/test_recorder.py`**

```python
import pytest

from marketspike.feeds.base import Tick
from marketspike.store.db import apply_schema, open_db
from marketspike.store.recorder import Recorder


def make_tick(ts=1):
    return Tick(
        symbol="BTCUSDT", venue_ts_ns=ts, recv_ts_ns=ts + 1000,
        bid=100.0, ask=100.2, bid_qty=1.0, ask_qty=2.0,
        tradeable=True, source="measured",
    )


@pytest.fixture
def recorder(tmp_path):
    conn = open_db(str(tmp_path / "r.db"))
    apply_schema(conn)
    return Recorder(conn, max_queue=4, batch_size=2)


async def test_flush_persists_submitted_ticks(recorder):
    recorder.submit_tick(make_tick(1), excess_transit_us=10, engine_us=5)
    recorder.submit_tick(make_tick(2), excess_transit_us=20, engine_us=6)
    await recorder.flush_once()
    rows = list(recorder.conn.execute("SELECT venue_ts_ns, excess_transit_us FROM ticks"))
    assert [(r[0], r[1]) for r in rows] == [(1, 10), (2, 20)]


async def test_queue_overflow_drops_and_counts(recorder):
    for i in range(10):
        recorder.submit_tick(make_tick(i), excess_transit_us=0, engine_us=0)
    assert recorder.counters["recorder_dropped_total"] == 6


async def test_submit_returns_false_when_dropped(recorder):
    results = [
        recorder.submit_tick(make_tick(i), excess_transit_us=0, engine_us=0)
        for i in range(6)
    ]
    assert results[:4] == [True, True, True, True]
    assert results[4:] == [False, False]


async def test_regime_events_persist(recorder):
    recorder.submit_regime(
        ts_ns=5, symbol="EURUSD", from_state="NORMAL", to_state="ELEVATED",
        score=1.6, v_ratio=2.1, spread_z=1.2, trigger="vol_ratio",
        event_context="CLEAR",
    )
    await recorder.flush_once()
    row = recorder.conn.execute(
        "SELECT to_state, trigger FROM regime_events"
    ).fetchone()
    assert row[0] == "ELEVATED"
    assert row[1] == "vol_ratio"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_recorder.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.store.recorder'`

- [ ] **Step 3: Create `marketspike/store/recorder.py`**

```python
import asyncio
import sqlite3
from collections import deque
from typing import Any, Deque, Dict, List, Tuple

from marketspike.feeds.base import Tick

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
        self.counters: Dict[str, int] = {
            "recorder_dropped_total": 0,
            "recorder_written_total": 0,
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
        if not self._queue:
            return 0
        batch = self._drain_batch()
        loop = asyncio.get_event_loop()
        written = await loop.run_in_executor(None, self._write, batch)
        self.counters["recorder_written_total"] += written
        return written

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval_s)
            while self._queue:
                await self.flush_once()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_recorder.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/store/recorder.py tests/test_recorder.py
git commit -m "feat: batched SQLite recorder with bounded queue and drop counters"
```

---

### Task 6: Task supervisor and runnable app skeleton

**Files:**
- Create: `marketspike/engine/__init__.py`, `marketspike/engine/supervisor.py`
- Create: `marketspike/main.py`
- Test: `tests/test_supervisor.py`

**Interfaces:**
- Consumes: `get_settings`, `open_db`, `apply_schema`, `Recorder`, `BinanceAdapter`, `OandaAdapter`
- Produces: `async supervise(name: str, factory, on_error=None, max_backoff_s=30.0)`; FastAPI `app` in `marketspike/main.py`

- [ ] **Step 1: Write the failing test — `tests/test_supervisor.py`**

```python
import asyncio

import pytest

from marketspike.engine.supervisor import supervise


async def test_supervise_restarts_after_failure():
    attempts = {"count": 0}

    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("boom")
        await asyncio.sleep(10)

    task = asyncio.ensure_future(supervise("flaky", flaky, max_backoff_s=0.01))
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert attempts["count"] >= 3


async def test_supervise_reports_errors_to_callback():
    seen = []

    async def always_fails():
        raise ValueError("nope")

    task = asyncio.ensure_future(
        supervise("bad", always_fails, on_error=seen.append, max_backoff_s=0.01)
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen and isinstance(seen[0], ValueError)


async def test_supervise_propagates_cancellation_without_restart():
    started = {"count": 0}

    async def long_running():
        started["count"] += 1
        await asyncio.sleep(10)

    task = asyncio.ensure_future(supervise("long", long_running))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert started["count"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_supervisor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine'`

- [ ] **Step 3: Create `marketspike/engine/supervisor.py`**

```python
import asyncio
import logging
import random
from typing import Awaitable, Callable, Optional

LOGGER = logging.getLogger(__name__)


async def supervise(
    name: str,
    factory: Callable[[], Awaitable[None]],
    on_error: Optional[Callable[[BaseException], None]] = None,
    max_backoff_s: float = 30.0,
) -> None:
    """Run a coroutine forever, restarting it with backoff on failure.

    A bare asyncio.Task that raises dies silently — the feed stops and nothing
    is logged (spec §14.1). Every long-lived task runs under this wrapper.
    """
    backoff = 0.5
    while True:
        try:
            await factory()
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except BaseException as error:  # noqa: BLE001 - deliberate catch-all
            LOGGER.exception("task %s failed; restarting", name)
            if on_error is not None:
                on_error(error)
            await asyncio.sleep(min(backoff, max_backoff_s) + random.random() * 0.1)
            backoff = min(backoff * 2, max_backoff_s)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_supervisor.py -v`
Expected: 3 passed

- [ ] **Step 5: Create `marketspike/main.py`**

```python
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
```

- [ ] **Step 6: Verify the app boots and records live BTCUSDT ticks**

Run: `MS_SYMBOLS=BTCUSDT python -m marketspike.main`
Expected: log line `started with symbols=['BTCUSDT']`, no traceback. Leave it running for 30 seconds, stop with Ctrl-C, then confirm rows landed:

Run: `sqlite3 marketspike.db "SELECT count(*), min(bid), max(ask) FROM ticks;"`
Expected: a non-zero count and plausible BTC prices

If the count is zero, check Binance reachability from this network before proceeding — this is the hour-0 verification called for in spec §18.

- [ ] **Step 7: Commit**

```bash
git add marketspike/engine marketspike/main.py tests/test_supervisor.py
git commit -m "feat: task supervisor and runnable ingest app skeleton"
```

---

## Phase 2 — Latency measurement (spec §6; hours 5–8)

This phase replaces the original draft's fabricated latency (`sleep(0.5)` elapsed plus a random number) with three genuinely measured hops.

### Task 7: Skew estimator — sliding-window minimum transit floor

**Files:**
- Create: `marketspike/clock/__init__.py`, `marketspike/clock/skew.py`
- Test: `tests/test_skew.py`

**Interfaces:**
- Consumes: nothing
- Produces: `SkewEstimator(window_s: float = 60.0)` with `update(venue_ts_ns: int, recv_ts_ns: int) -> int` returning **excess transit in microseconds**, and property `floor_ns: Optional[int]`

- [ ] **Step 1: Write the failing test — `tests/test_skew.py`**

```python
from marketspike.clock.skew import SkewEstimator

SECOND = 1_000_000_000


def test_first_sample_has_zero_excess():
    est = SkewEstimator(window_s=60.0)
    # Venue clock is 5s behind ours; that offset is unmeasurable and must
    # not appear as latency.
    assert est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND) == 0


def test_excess_is_measured_above_the_running_floor():
    est = SkewEstimator(window_s=60.0)
    est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND)            # raw 5.000s
    excess = est.update(venue_ts_ns=SECOND, recv_ts_ns=6 * SECOND + 3_000_000)
    assert excess == 3000  # 3ms above floor, in microseconds


def test_a_faster_sample_lowers_the_floor_and_never_goes_negative():
    est = SkewEstimator(window_s=60.0)
    est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND)
    excess = est.update(venue_ts_ns=SECOND, recv_ts_ns=SECOND + 2 * SECOND)
    assert excess == 0
    assert est.floor_ns == 2 * SECOND


def test_stale_samples_leave_the_window():
    est = SkewEstimator(window_s=1.0)
    est.update(venue_ts_ns=0, recv_ts_ns=1 * SECOND)              # fast, raw 1s
    # 10s later the fast sample has expired, so this slow one becomes the floor.
    excess = est.update(venue_ts_ns=6 * SECOND, recv_ts_ns=11 * SECOND)
    assert excess == 0
    assert est.floor_ns == 5 * SECOND


def test_clock_drift_backwards_is_clamped_not_negative():
    est = SkewEstimator(window_s=60.0)
    est.update(venue_ts_ns=0, recv_ts_ns=5 * SECOND)
    assert est.update(venue_ts_ns=10 * SECOND, recv_ts_ns=10 * SECOND) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_skew.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.clock'`

- [ ] **Step 3: Create `marketspike/clock/skew.py`**

```python
from collections import deque
from typing import Deque, Optional, Tuple


class SkewEstimator:
    """Reports transit latency *in excess of* a rolling baseline.

    Absolute one-way transit cannot be measured against a venue clock: the
    observed difference is skew plus transit, and the two are inseparable from
    a single sample. Subtracting the window minimum cancels the skew term and
    leaves queueing above baseline (spec §6.2).

    Uses a monotonic deque so the minimum is O(1) amortised rather than O(n)
    per tick — this runs on the hot path of a latency product.
    """

    def __init__(self, window_s: float = 60.0) -> None:
        self._window_ns = int(window_s * 1_000_000_000)
        self._mono: Deque[Tuple[int, int]] = deque()

    @property
    def floor_ns(self) -> Optional[int]:
        return self._mono[0][1] if self._mono else None

    def update(self, venue_ts_ns: int, recv_ts_ns: int) -> int:
        raw = recv_ts_ns - venue_ts_ns

        cutoff = recv_ts_ns - self._window_ns
        while self._mono and self._mono[0][0] < cutoff:
            self._mono.popleft()

        while self._mono and self._mono[-1][1] >= raw:
            self._mono.pop()
        self._mono.append((recv_ts_ns, raw))

        excess_ns = raw - self._mono[0][1]
        return max(0, excess_ns // 1000)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_skew.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/clock tests/test_skew.py
git commit -m "feat: skew-cancelling excess-transit estimator"
```

---

### Task 8: Pipeline timer and rolling latency percentiles

**Files:**
- Create: `marketspike/engine/pipeline.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `SkewEstimator` from Task 7
- Produces: `percentile(sorted_values: List[int], q: float) -> int`; `LatencyAggregator(window_s: float = 300.0)` with `add(ts_ns, value_us)` and `percentiles(ts_ns) -> Tuple[int, int, int]`; `PipelineTimer(skew_window_s)` with `on_receive(tick) -> int` and `on_processed(tick, done_ts_ns) -> Tuple[int, int]`

- [ ] **Step 1: Write the failing test — `tests/test_pipeline.py`**

```python
from marketspike.engine.pipeline import LatencyAggregator, PipelineTimer, percentile
from marketspike.feeds.base import Tick

SECOND = 1_000_000_000


def make_tick(venue_ts_ns, recv_ts_ns):
    return Tick(
        symbol="BTCUSDT", venue_ts_ns=venue_ts_ns, recv_ts_ns=recv_ts_ns,
        bid=100.0, ask=100.1, bid_qty=1.0, ask_qty=1.0,
        tradeable=True, source="measured",
    )


def test_percentile_interpolates_between_samples():
    assert percentile([10, 20, 30, 40], 0.5) == 25


def test_percentile_of_empty_series_is_zero():
    assert percentile([], 0.95) == 0


def test_aggregator_reports_ordered_percentiles():
    agg = LatencyAggregator(window_s=300.0)
    for i in range(1, 101):
        agg.add(ts_ns=i * 1_000_000, value_us=i)
    p50, p95, p99 = agg.percentiles(ts_ns=100 * 1_000_000)
    assert p50 < p95 < p99
    assert p50 == 50


def test_aggregator_evicts_samples_outside_the_window():
    agg = LatencyAggregator(window_s=1.0)
    agg.add(ts_ns=0, value_us=9999)
    agg.add(ts_ns=5 * SECOND, value_us=10)
    assert agg.percentiles(ts_ns=5 * SECOND) == (10, 10, 10)


def test_engine_time_is_exact_and_needs_no_correction():
    timer = PipelineTimer(skew_window_s=60.0)
    tick = make_tick(venue_ts_ns=0, recv_ts_ns=5 * SECOND)
    timer.on_receive(tick)
    excess_us, engine_us = timer.on_processed(tick, done_ts_ns=5 * SECOND + 250_000)
    assert engine_us == 250
    assert excess_us == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine.pipeline'`

- [ ] **Step 3: Create `marketspike/engine/pipeline.py`**

```python
import math
from collections import deque
from typing import Deque, List, Tuple

from marketspike.clock.skew import SkewEstimator
from marketspike.feeds.base import Tick


def percentile(sorted_values: List[int], q: float) -> int:
    """Linear-interpolated percentile, matching numpy's default method."""
    if not sorted_values:
        return 0
    position = (len(sorted_values) - 1) * q
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return int(sorted_values[low])
    lower = sorted_values[low]
    upper = sorted_values[high]
    return int(lower + (upper - lower) * (position - low))


class LatencyAggregator:
    """Rolling-window percentiles.

    Percentiles, not means: a 20ms mean with a 400ms p99 is a materially
    different trading environment from a 20ms mean with a 25ms p99, and the
    mean cannot distinguish them (spec §6.4).

    Sorting happens on read, not on write, because frames are emitted at a few
    hertz while ticks arrive at tens of hertz.
    """

    def __init__(self, window_s: float = 300.0) -> None:
        self._window_ns = int(window_s * 1_000_000_000)
        self._samples: Deque[Tuple[int, int]] = deque()

    def add(self, ts_ns: int, value_us: int) -> None:
        self._samples.append((ts_ns, value_us))
        self._evict(ts_ns)

    def _evict(self, now_ns: int) -> None:
        cutoff = now_ns - self._window_ns
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def percentiles(self, ts_ns: int) -> Tuple[int, int, int]:
        self._evict(ts_ns)
        values = sorted(value for _, value in self._samples)
        return (
            percentile(values, 0.50),
            percentile(values, 0.95),
            percentile(values, 0.99),
        )


class PipelineTimer:
    """Stamps the three measurable hops for one symbol (spec §6.1)."""

    def __init__(self, skew_window_s: float = 60.0, agg_window_s: float = 300.0) -> None:
        self._skew = SkewEstimator(window_s=skew_window_s)
        self.transit = LatencyAggregator(window_s=agg_window_s)
        self.engine = LatencyAggregator(window_s=agg_window_s)
        self.total = LatencyAggregator(window_s=agg_window_s)
        self._last_excess_us = 0

    def on_receive(self, tick: Tick) -> int:
        self._last_excess_us = self._skew.update(tick.venue_ts_ns, tick.recv_ts_ns)
        return self._last_excess_us

    def on_processed(self, tick: Tick, done_ts_ns: int) -> Tuple[int, int]:
        # Same machine, same clock: exact, no correction needed.
        engine_us = max(0, (done_ts_ns - tick.recv_ts_ns) // 1000)
        excess_us = self._last_excess_us
        self.transit.add(tick.recv_ts_ns, excess_us)
        self.engine.add(tick.recv_ts_ns, engine_us)
        self.total.add(tick.recv_ts_ns, excess_us + engine_us)
        return excess_us, engine_us
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pipeline.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/engine/pipeline.py tests/test_pipeline.py
git commit -m "feat: pipeline hop timing and rolling latency percentiles"
```

---

### Task 9: NTP-style client clock synchronisation

**Files:**
- Create: `marketspike/clock/sync.py`
- Test: `tests/test_sync.py`

**Interfaces:**
- Consumes: nothing
- Produces: `compute_sync(client_send_ns, server_recv_ns, server_send_ns, client_recv_ns) -> Tuple[int, int]` returning `(round_trip_ns, offset_ns)`; `SyncFilter(keep: int = 8)` with `add(round_trip_ns, offset_ns)` and property `best_offset_ns: Optional[int]`, `best_round_trip_ns: Optional[int]`

- [ ] **Step 1: Write the failing test — `tests/test_sync.py`**

```python
from marketspike.clock.sync import SyncFilter, compute_sync


def test_symmetric_path_with_synced_clocks_gives_zero_offset():
    round_trip, offset = compute_sync(
        client_send_ns=0, server_recv_ns=100,
        server_send_ns=110, client_recv_ns=210,
    )
    assert round_trip == 200
    assert offset == 0


def test_offset_recovers_a_known_server_clock_lead():
    # Server clock runs 1000ns ahead; path is symmetric.
    round_trip, offset = compute_sync(
        client_send_ns=0, server_recv_ns=1100,
        server_send_ns=1110, client_recv_ns=210,
    )
    assert round_trip == 200
    assert offset == 1000


def test_server_processing_time_is_excluded_from_round_trip():
    round_trip, _ = compute_sync(
        client_send_ns=0, server_recv_ns=100,
        server_send_ns=5000, client_recv_ns=5100,
    )
    assert round_trip == 200


def test_filter_keeps_the_sample_with_lowest_round_trip():
    filt = SyncFilter(keep=8)
    filt.add(round_trip_ns=900, offset_ns=77)
    filt.add(round_trip_ns=200, offset_ns=42)
    filt.add(round_trip_ns=600, offset_ns=13)
    assert filt.best_offset_ns == 42
    assert filt.best_round_trip_ns == 200


def test_filter_discards_samples_beyond_its_capacity():
    filt = SyncFilter(keep=2)
    filt.add(round_trip_ns=100, offset_ns=1)
    filt.add(round_trip_ns=500, offset_ns=2)
    filt.add(round_trip_ns=400, offset_ns=3)
    assert filt.best_offset_ns == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_sync.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.clock.sync'`

- [ ] **Step 3: Create `marketspike/clock/sync.py`**

```python
from collections import deque
from typing import Deque, Optional, Tuple


def compute_sync(
    client_send_ns: int,
    server_recv_ns: int,
    server_send_ns: int,
    client_recv_ns: int,
) -> Tuple[int, int]:
    """Standard NTP four-timestamp exchange (spec §6.3).

    Unlike the venue path, both endpoints cooperate here, so absolute offset
    and round-trip are separately recoverable.
    """
    round_trip = (client_recv_ns - client_send_ns) - (server_send_ns - server_recv_ns)
    offset = ((server_recv_ns - client_send_ns) + (server_send_ns - client_recv_ns)) // 2
    return round_trip, offset


class SyncFilter:
    """Keeps the least-delayed recent sample.

    The sample with the lowest round-trip carries the least path asymmetry and
    therefore the least offset error — NTP's own clock filter.
    """

    def __init__(self, keep: int = 8) -> None:
        self._samples: Deque[Tuple[int, int]] = deque(maxlen=keep)

    def add(self, round_trip_ns: int, offset_ns: int) -> None:
        self._samples.append((round_trip_ns, offset_ns))

    def _best(self) -> Optional[Tuple[int, int]]:
        return min(self._samples) if self._samples else None

    @property
    def best_offset_ns(self) -> Optional[int]:
        best = self._best()
        return best[1] if best else None

    @property
    def best_round_trip_ns(self) -> Optional[int]:
        best = self._best()
        return best[0] if best else None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sync.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/clock/sync.py tests/test_sync.py
git commit -m "feat: NTP-style client clock sync with minimum-delay filter"
```

---

### Task 10: Bus fan-out and the WebSocket endpoint

**Files:**
- Create: `marketspike/engine/bus.py`, `marketspike/api/ws.py`
- Modify: `marketspike/main.py` (register the WS router)
- Test: `tests/test_bus.py`

**Interfaces:**
- Consumes: `compute_sync`, `SyncFilter` from Task 9
- Produces: `Subscription` with `push(frame) -> bool`, `async get() -> Dict`, `dropped: int`; `Bus()` with `subscribe(maxlen=200) -> Subscription`, `unsubscribe(sub)`, `publish(frame) -> None`, `next_seq() -> int`, `total_dropped -> int`

- [ ] **Step 1: Write the failing test — `tests/test_bus.py`**

```python
import asyncio

from marketspike.engine.bus import Bus


async def test_publish_reaches_every_subscriber():
    bus = Bus()
    first = bus.subscribe()
    second = bus.subscribe()
    bus.publish({"type": "tick", "symbol": "BTCUSDT"})
    assert (await first.get())["symbol"] == "BTCUSDT"
    assert (await second.get())["symbol"] == "BTCUSDT"


async def test_slow_subscriber_drops_oldest_and_counts():
    bus = Bus()
    sub = bus.subscribe(maxlen=2)
    for i in range(5):
        bus.publish({"type": "tick", "n": i})
    assert sub.dropped == 3
    assert (await sub.get())["n"] == 3  # oldest dropped, newest retained


async def test_one_slow_subscriber_does_not_starve_another():
    bus = Bus()
    slow = bus.subscribe(maxlen=1)
    fast = bus.subscribe(maxlen=100)
    for i in range(10):
        bus.publish({"n": i})
    assert slow.dropped == 9
    assert fast.dropped == 0


async def test_unsubscribe_stops_delivery():
    bus = Bus()
    sub = bus.subscribe()
    bus.unsubscribe(sub)
    bus.publish({"n": 1})
    with_timeout = asyncio.wait_for(sub.get(), timeout=0.05)
    try:
        await with_timeout
        assert False, "unsubscribed subscriber received a frame"
    except asyncio.TimeoutError:
        pass


def test_sequence_numbers_are_monotonic():
    bus = Bus()
    assert [bus.next_seq() for _ in range(3)] == [1, 2, 3]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_bus.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine.bus'`

- [ ] **Step 3: Create `marketspike/engine/bus.py`**

```python
import asyncio
from collections import deque
from typing import Any, Deque, Dict, List


class Subscription:
    """A bounded per-client mailbox.

    One slow browser must not add latency for every other client, so each
    subscriber drops its own oldest frames rather than blocking the publisher
    (spec §4.2).
    """

    def __init__(self, maxlen: int = 200) -> None:
        self._queue: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._event = asyncio.Event()
        self.dropped = 0

    def push(self, frame: Dict[str, Any]) -> bool:
        overflowed = len(self._queue) == self._queue.maxlen
        if overflowed:
            self.dropped += 1
        self._queue.append(frame)
        self._event.set()
        return not overflowed

    async def get(self) -> Dict[str, Any]:
        while not self._queue:
            self._event.clear()
            await self._event.wait()
        return self._queue.popleft()


class Bus:
    """In-process fan-out.

    Publishing is synchronous and non-blocking. Swapping this for Redis
    pub/sub is a one-file change (spec §4.1).
    """

    def __init__(self) -> None:
        self._subscribers: List[Subscription] = []
        self._seq = 0

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def subscribe(self, maxlen: int = 200) -> Subscription:
        sub = Subscription(maxlen=maxlen)
        self._subscribers.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subscribers:
            self._subscribers.remove(sub)

    def publish(self, frame: Dict[str, Any]) -> None:
        for sub in self._subscribers:
            sub.push(frame)

    @property
    def total_dropped(self) -> int:
        return sum(sub.dropped for sub in self._subscribers)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_bus.py -v`
Expected: 5 passed

- [ ] **Step 5: Create `marketspike/api/ws.py`**

```python
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
```

- [ ] **Step 6: Wire the bus and router into `marketspike/main.py`**

Add these imports beside the existing ones:

```python
from marketspike.api import ws as ws_api
from marketspike.engine.bus import Bus
```

Add this line immediately after the `app.add_middleware(...)` call:

```python
app.include_router(ws_api.router)
```

Add this line inside `startup()`, immediately after `adapters = build_adapters(settings)`:

```python
    STATE["bus"] = Bus()
    STATE["mode"] = "live"
    STATE["warmup_complete"] = False
```

- [ ] **Step 7: Verify the socket accepts a client and sends `hello`**

Run: `MS_SYMBOLS=BTCUSDT python -m marketspike.main` in one terminal, then in another:

```bash
python - <<'PY'
import asyncio, json, websockets
async def main():
    async with websockets.connect("ws://localhost:8000/ws/v1/stream") as ws:
        print(json.loads(await ws.recv()))
asyncio.get_event_loop().run_until_complete(main())
PY
```

Expected: a dict with `'type': 'hello'`, `'v': 1`, and `'feeds': {'BTCUSDT': 'binance'}`

- [ ] **Step 8: Commit**

```bash
git add marketspike/engine/bus.py marketspike/api/ws.py marketspike/main.py tests/test_bus.py
git commit -m "feat: bus fan-out and v1 WebSocket endpoint with clock sync"
```

---

## Phase 3 — Volatility and regime detection (spec §7; hours 8–12)

This phase replaces the draft's `random.choice()` regime assignment, which reassigned state every 500ms and produced three transitions per second.

### Task 11: Time-weighted EWMA volatility

**Files:**
- Create: `marketspike/engine/volatility.py`
- Test: `tests/test_volatility.py`

**Interfaces:**
- Consumes: nothing
- Produces: `VolatilityCalc(tau_s: float)` with `seed(var_per_second: float)`, `update(ts_ns: int, mid: float) -> Optional[float]`, properties `variance: Optional[float]`, `sigma: Optional[float]`, `ready: bool`; `VolatilityPair(tau_fast_s, tau_slow_s)` with `update(ts_ns, mid) -> Optional[float]` returning the ratio `V`, and `seed_slow(var_per_second)`

- [ ] **Step 1: Write the failing test — `tests/test_volatility.py`**

```python
import math

from marketspike.engine.volatility import VolatilityCalc, VolatilityPair

SECOND = 1_000_000_000


def drive(calc, dt_s, log_return, steps, start_ts=0, start_mid=100.0):
    ts, mid = start_ts, start_mid
    for _ in range(steps):
        ts += int(dt_s * SECOND)
        mid *= math.exp(log_return)
        calc.update(ts, mid)
    return calc


def test_variance_rate_is_invariant_to_sampling_density():
    """A path sampled 10x more often, with returns scaled by sqrt(10), is the
    same underlying process and must produce the same variance rate."""
    fast_sampled = drive(VolatilityCalc(tau_s=30.0), 0.1, 0.001, steps=3000)
    slow_sampled = drive(
        VolatilityCalc(tau_s=30.0), 1.0, 0.001 * math.sqrt(10), steps=300
    )
    a = fast_sampled.variance
    b = slow_sampled.variance
    assert abs(a - b) / b < 0.01


def test_variance_rate_matches_the_analytic_value():
    calc = drive(VolatilityCalc(tau_s=30.0), 0.1, 0.001, steps=3000)
    expected = (0.001 ** 2) / 0.1  # r^2 / dt
    assert abs(calc.variance - expected) / expected < 0.01


def test_seed_makes_the_calculator_ready_immediately():
    calc = VolatilityCalc(tau_s=1800.0)
    assert calc.ready is False
    calc.seed(1e-5)
    assert calc.ready is True
    assert calc.sigma == math.sqrt(1e-5)


def test_sub_millisecond_duplicate_updates_are_ignored():
    calc = VolatilityCalc(tau_s=30.0)
    calc.seed(1e-5)
    calc.update(SECOND, 100.0)
    before = calc.variance
    calc.update(SECOND + 100_000, 500.0)  # 0.1ms later, absurd price
    assert calc.variance == before


def test_implausible_returns_are_rejected_as_bad_prints():
    calc = VolatilityCalc(tau_s=30.0)
    calc.seed(1e-5)
    calc.update(SECOND, 100.0)
    before = calc.variance
    calc.update(2 * SECOND, 100.0 * math.exp(0.5))  # 50% jump
    assert calc.variance == before


def test_ratio_is_one_when_both_horizons_agree():
    pair = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0)
    pair.seed_slow(1e-5)
    ratio = drive_pair(pair)
    assert ratio is not None and abs(ratio - 1.0) < 0.05


def drive_pair(pair):
    ts, mid, ratio = 0, 100.0, None
    for _ in range(3000):
        ts += int(0.1 * SECOND)
        mid *= math.exp(0.001)
        ratio = pair.update(ts, mid)
    return ratio
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_volatility.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine.volatility'`

- [ ] **Step 3: Create `marketspike/engine/volatility.py`**

```python
import math
from typing import Optional

MIN_DT_S = 1e-3
MAX_ABS_RETURN = 0.05


class VolatilityCalc:
    """Time-weighted EWMA of variance **per second** (spec §7.1).

    Quote updates arrive irregularly and arrive faster during volatility, so a
    tick-count EWMA double-counts spikes: the quantity being measured alters
    the sampling rate of the measurement. Decaying on elapsed time and
    normalising r^2 by dt removes that dependence.

    Both horizons must use this same per-second normalisation. Normalising by
    tau/dt instead would express each horizon in variance-per-its-own-horizon,
    and the fast/slow ratio would silently carry a factor of tau_fast/tau_slow.
    """

    def __init__(self, tau_s: float) -> None:
        self._tau = tau_s
        self._variance: Optional[float] = None
        self._last_ts_ns: Optional[int] = None
        self._last_mid: Optional[float] = None
        self.rejected = 0

    def seed(self, var_per_second: float) -> None:
        self._variance = var_per_second

    @property
    def variance(self) -> Optional[float]:
        return self._variance

    @property
    def sigma(self) -> Optional[float]:
        if self._variance is None or self._variance < 0:
            return None
        return math.sqrt(self._variance)

    @property
    def ready(self) -> bool:
        return self._variance is not None

    def update(self, ts_ns: int, mid: float) -> Optional[float]:
        if mid <= 0:
            return self._variance
        if self._last_ts_ns is None or self._last_mid is None:
            self._last_ts_ns, self._last_mid = ts_ns, mid
            return self._variance

        dt = (ts_ns - self._last_ts_ns) / 1e9
        if dt < MIN_DT_S:
            return self._variance

        log_return = math.log(mid / self._last_mid)
        self._last_ts_ns, self._last_mid = ts_ns, mid

        if abs(log_return) > MAX_ABS_RETURN:
            self.rejected += 1
            return self._variance

        rate = (log_return * log_return) / dt
        decay = math.exp(-dt / self._tau)
        if self._variance is None:
            self._variance = rate
        else:
            self._variance = decay * self._variance + (1.0 - decay) * rate
        return self._variance


class VolatilityPair:
    """Fast and slow horizons sharing one update, yielding the ratio V."""

    def __init__(self, tau_fast_s: float, tau_slow_s: float) -> None:
        self.fast = VolatilityCalc(tau_fast_s)
        self.slow = VolatilityCalc(tau_slow_s)

    def seed_slow(self, var_per_second: float) -> None:
        self.slow.seed(var_per_second)

    @property
    def ready(self) -> bool:
        return self.fast.ready and self.slow.ready

    def update(self, ts_ns: int, mid: float) -> Optional[float]:
        self.fast.update(ts_ns, mid)
        self.slow.update(ts_ns, mid)
        fast_sigma = self.fast.sigma
        slow_sigma = self.slow.sigma
        if not fast_sigma or not slow_sigma:
            return None
        return fast_sigma / slow_sigma
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_volatility.py -v`
Expected: 6 passed (the helper `drive_pair` is not collected as a test)

- [ ] **Step 5: Commit**

```bash
git add marketspike/engine/volatility.py tests/test_volatility.py
git commit -m "feat: time-weighted dual-horizon EWMA volatility"
```

---

### Task 12: Robust spread z-score

**Files:**
- Create: `marketspike/engine/spread.py`
- Test: `tests/test_spread.py`

**Interfaces:**
- Consumes: nothing
- Produces: `median(sorted_values: List[float]) -> float`; `SpreadTracker(window_s=3600.0, recompute_interval_s=5.0)` with `update(ts_ns, spread_bps) -> float`, `z(spread_bps) -> float`, properties `median_bps`, `mad_bps`

- [ ] **Step 1: Write the failing test — `tests/test_spread.py`**

```python
from marketspike.engine.spread import SpreadTracker, median

SECOND = 1_000_000_000


def feed(tracker, values, start_ts=0, step_s=1.0):
    ts = start_ts
    result = 0.0
    for value in values:
        result = tracker.update(ts, value)
        ts += int(step_s * SECOND)
    return result


def test_median_of_even_length_series_averages_the_middle_pair():
    assert median([1.0, 2.0, 3.0, 4.0]) == 2.5


def test_median_of_empty_series_is_zero():
    assert median([]) == 0.0


def test_z_score_uses_scaled_mad():
    tracker = SpreadTracker(window_s=3600.0, recompute_interval_s=0.0)
    feed(tracker, [1.0, 1.1, 1.2, 1.3, 1.4])
    assert abs(tracker.median_bps - 1.2) < 1e-9
    assert abs(tracker.mad_bps - 0.1) < 1e-9
    assert abs(tracker.z(1.7) - (0.5 / (1.4826 * 0.1))) < 1e-6


def test_median_and_mad_resist_a_large_outlier():
    tracker = SpreadTracker(window_s=3600.0, recompute_interval_s=0.0)
    feed(tracker, [1.0, 1.1, 1.2, 1.3, 1.4])
    baseline_median = tracker.median_bps
    feed(tracker, [500.0], start_ts=10 * SECOND)
    assert abs(tracker.median_bps - baseline_median) < 0.15


def test_zero_dispersion_yields_zero_rather_than_dividing_by_zero():
    tracker = SpreadTracker(window_s=3600.0, recompute_interval_s=0.0)
    feed(tracker, [1.2, 1.2, 1.2, 1.2])
    assert tracker.mad_bps == 0.0
    assert tracker.z(9.9) == 0.0


def test_samples_outside_the_window_are_evicted():
    tracker = SpreadTracker(window_s=2.0, recompute_interval_s=0.0)
    feed(tracker, [50.0, 50.0, 50.0])
    feed(tracker, [1.0, 1.0, 1.0], start_ts=100 * SECOND)
    assert abs(tracker.median_bps - 1.0) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_spread.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine.spread'`

- [ ] **Step 3: Create `marketspike/engine/spread.py`**

```python
from collections import deque
from typing import Deque, List, Optional, Tuple

MAD_TO_SIGMA = 1.4826


def median(sorted_values: List[float]) -> float:
    count = len(sorted_values)
    if count == 0:
        return 0.0
    middle = count // 2
    if count % 2 == 1:
        return float(sorted_values[middle])
    return (sorted_values[middle - 1] + sorted_values[middle]) / 2.0


class SpreadTracker:
    """Rolling robust z-score of quoted spread (spec §7.3).

    Spread distributions are fat-tailed, so mean and standard deviation are the
    wrong estimators — the outliers are the signal, and they would inflate the
    very scale used to detect them. Median and MAD are unaffected.

    The 1.4826 factor makes MAD a consistent estimator of sigma under
    normality, keeping z on the familiar scale.
    """

    def __init__(self, window_s: float = 3600.0, recompute_interval_s: float = 5.0) -> None:
        self._window_ns = int(window_s * 1_000_000_000)
        self._recompute_ns = int(recompute_interval_s * 1_000_000_000)
        self._samples: Deque[Tuple[int, float]] = deque()
        self._median: Optional[float] = None
        self._mad: Optional[float] = None
        self._last_recompute_ns: Optional[int] = None

    @property
    def median_bps(self) -> float:
        return self._median if self._median is not None else 0.0

    @property
    def mad_bps(self) -> float:
        return self._mad if self._mad is not None else 0.0

    def update(self, ts_ns: int, spread_bps: float) -> float:
        self._samples.append((ts_ns, spread_bps))
        cutoff = ts_ns - self._window_ns
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

        due = (
            self._last_recompute_ns is None
            or (ts_ns - self._last_recompute_ns) >= self._recompute_ns
        )
        if due:
            self._recompute()
            self._last_recompute_ns = ts_ns
        return self.z(spread_bps)

    def _recompute(self) -> None:
        values = sorted(value for _, value in self._samples)
        self._median = median(values)
        deviations = sorted(abs(value - self._median) for value in values)
        self._mad = median(deviations)

    def z(self, spread_bps: float) -> float:
        if self._median is None or not self._mad:
            return 0.0
        return (spread_bps - self._median) / (MAD_TO_SIGMA * self._mad)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_spread.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/engine/spread.py tests/test_spread.py
git commit -m "feat: outlier-robust spread z-score via median and MAD"
```

---

### Task 13: Composite score and the regime state machine

This is the highest-value test target in the build: pure logic, trivially testable, and the exact feature that was broken in the draft.

**Files:**
- Create: `marketspike/engine/scoring.py`, `marketspike/engine/regime.py`
- Test: `tests/test_regime.py`

**Interfaces:**
- Consumes: nothing
- Produces: `composite_score(v_ratio: Optional[float], spread_z: float) -> float`; `Transition` dataclass (`to`, `threshold`, `direction`, `dwell_s`); `TRANSITIONS: Dict[str, List[Transition]]`; `RegimeFSM(initial="NORMAL")` with `update(ts_ns: int, score: float) -> Optional[str]`, attributes `state: str`, `entered_ns: Optional[int]`, `last_trigger: str`

- [ ] **Step 1: Write the failing test — `tests/test_regime.py`**

```python
import math

import pytest

from marketspike.engine.regime import RegimeFSM
from marketspike.engine.scoring import composite_score

SECOND = 1_000_000_000


def test_score_is_zero_when_volatility_matches_baseline_and_spread_is_normal():
    assert composite_score(v_ratio=1.0, spread_z=0.0) == 0.0


def test_score_combines_both_signals_with_documented_weights():
    # log2(4) = 2 -> 0.6*2 = 1.2 ; z=4 -> clamp(4/2)=2 -> 0.4*2 = 0.8
    assert abs(composite_score(v_ratio=4.0, spread_z=4.0) - 2.0) < 1e-9


def test_score_clamps_extreme_inputs():
    assert composite_score(v_ratio=1e9, spread_z=1e9) == pytest.approx(4.0)


def test_score_handles_missing_ratio_during_warmup():
    assert composite_score(v_ratio=None, spread_z=2.0) == pytest.approx(0.4)


def test_transition_requires_the_dwell_period_to_elapse():
    fsm = RegimeFSM()
    assert fsm.update(0, 2.0) is None            # above 1.5 but dwell not met
    assert fsm.update(2 * SECOND, 2.0) is None   # still under 3s
    assert fsm.update(3 * SECOND, 2.0) == "ELEVATED"
    assert fsm.state == "ELEVATED"


def test_dwell_timer_resets_when_the_condition_lapses():
    fsm = RegimeFSM()
    fsm.update(0, 2.0)
    fsm.update(2 * SECOND, 1.0)                  # condition lost, timer resets
    assert fsm.update(4 * SECOND, 2.0) is None   # only 0s of dwell so far
    assert fsm.state == "NORMAL"


def test_score_oscillating_around_a_threshold_does_not_flap():
    """The defect this FSM exists to prevent: the draft reassigned regime
    every 500ms and produced three transitions per second."""
    fsm = RegimeFSM()
    transitions = []
    for step in range(400):
        ts = step * (SECOND // 2)               # every 500ms
        score = 1.5 + (0.2 if step % 2 == 0 else -0.2)
        changed = fsm.update(ts, score)
        if changed:
            transitions.append(changed)
    assert transitions == []
    assert fsm.state == "NORMAL"


def test_full_spike_cycle_with_asymmetric_exit_dwell():
    fsm = RegimeFSM()
    ts = 0
    for _ in range(10):
        ts += SECOND
        fsm.update(ts, 3.5)
    assert fsm.state == "SPIKE"

    # Exit requires 10s below 2.0 — entry took 2s, exit deliberately does not.
    ts += SECOND
    assert fsm.update(ts, 1.0) is None
    ts += 5 * SECOND
    assert fsm.update(ts, 1.0) is None
    ts += 6 * SECOND
    assert fsm.update(ts, 1.0) == "ELEVATED"


def test_entered_timestamp_is_recorded_on_transition():
    fsm = RegimeFSM()
    for step in range(5):
        fsm.update(step * SECOND, 2.0)
    assert fsm.entered_ns == 3 * SECOND
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_regime.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine.regime'`

- [ ] **Step 3: Create `marketspike/engine/scoring.py`**

```python
import math
from typing import Optional

VOL_WEIGHT = 0.6
SPREAD_WEIGHT = 0.4
MAX_COMPONENT = 4.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def composite_score(v_ratio: Optional[float], spread_z: float) -> float:
    """Combine realised volatility and quoted spread into a 0-4 score (§7.4).

    Two signals rather than one because they diverge: volatility can rise on
    thin genuine movement without spread widening, and spread can widen on
    liquidity withdrawal before price moves. Either alone yields false
    negatives.
    """
    if v_ratio is None or v_ratio <= 0:
        vol_component = 0.0
    else:
        vol_component = _clamp(math.log(v_ratio, 2), 0.0, MAX_COMPONENT)
    spread_component = _clamp(spread_z / 2.0, 0.0, MAX_COMPONENT)
    return VOL_WEIGHT * vol_component + SPREAD_WEIGHT * spread_component
```

- [ ] **Step 4: Create `marketspike/engine/regime.py`**

```python
from dataclasses import dataclass
from typing import Dict, List, Optional

NORMAL = "NORMAL"
ELEVATED = "ELEVATED"
SPIKE = "SPIKE"
MARKET_CLOSED = "MARKET_CLOSED"


@dataclass(frozen=True)
class Transition:
    to: str
    threshold: float
    direction: str  # "above" or "below"
    dwell_s: float


# Entry thresholds sit above exit thresholds (hysteresis) and exit dwell
# exceeds entry dwell. The asymmetry is deliberate: failing to warn a trader
# of a spike costs real money, while a regime that lingers ten seconds too
# long costs nothing (spec §7.5).
TRANSITIONS: Dict[str, List[Transition]] = {
    NORMAL: [Transition(ELEVATED, 1.5, "above", 3.0)],
    ELEVATED: [
        Transition(SPIKE, 2.8, "above", 2.0),
        Transition(NORMAL, 1.1, "below", 15.0),
    ],
    SPIKE: [Transition(ELEVATED, 2.0, "below", 10.0)],
}


class RegimeFSM:
    def __init__(self, initial: str = NORMAL, transitions: Optional[Dict] = None) -> None:
        self._transitions = transitions or TRANSITIONS
        self.state = initial
        self.entered_ns: Optional[int] = None
        self.last_trigger = ""
        self._since: Dict[str, int] = {}

    def force(self, state: str, ts_ns: int) -> None:
        """Used for MARKET_CLOSED, which is driven by tradeability not price."""
        self.state = state
        self.entered_ns = ts_ns
        self._since.clear()

    def update(self, ts_ns: int, score: float) -> Optional[str]:
        """Return the new state if a transition fired, else None."""
        for transition in self._transitions.get(self.state, []):
            if transition.direction == "above":
                met = score >= transition.threshold
            else:
                met = score < transition.threshold

            if not met:
                self._since.pop(transition.to, None)
                continue

            started = self._since.setdefault(transition.to, ts_ns)
            if (ts_ns - started) >= transition.dwell_s * 1_000_000_000:
                self.state = transition.to
                self.entered_ns = ts_ns
                self.last_trigger = (
                    "vol_ratio" if transition.direction == "above" else "decay"
                )
                self._since.clear()
                return transition.to
        return None
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_regime.py -v`
Expected: 9 passed

- [ ] **Step 6: Commit**

```bash
git add marketspike/engine/scoring.py marketspike/engine/regime.py tests/test_regime.py
git commit -m "feat: composite score and hysteretic regime state machine"
```

---

### Task 14: Per-symbol engine, regime persistence, and the regime endpoint

**Files:**
- Create: `marketspike/engine/symbol_state.py`, `marketspike/api/rest.py`
- Modify: `marketspike/main.py` (replace `_make_ingest`, register the REST router)
- Test: `tests/test_symbol_engine.py`

**Interfaces:**
- Consumes: `PipelineTimer`, `VolatilityPair`, `SpreadTracker`, `RegimeFSM`, `composite_score`, `Bus`, `Recorder`
- Produces: `SymbolEngine(symbol, bus, recorder, tau_fast_s, tau_slow_s, skew_window_s, ws_max_hz)` with `seed(var_per_second)`, `on_tick(tick) -> None`, `snapshot() -> Dict[str, Any]`, attributes `quote_rate_hz`, `v_ratio`, `spread_z`, `score`, `fsm`, `last_tick`

- [ ] **Step 1: Write the failing test — `tests/test_symbol_engine.py`**

```python
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


def tick_at(ts_ns, mid, spread=0.10):
    return Tick(
        symbol="BTCUSDT", venue_ts_ns=ts_ns, recv_ts_ns=ts_ns,
        bid=mid - spread / 2, ask=mid + spread / 2,
        bid_qty=1.0, ask_qty=1.0, tradeable=True, source="measured",
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
    published = [f for f in list(sub._queue) if f["type"] == "tick"]
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
    changes = [f for f in list(sub._queue) if f["type"] == "regime_change"]
    assert changes, "expected at least one regime transition"
    assert changes[0]["v"] == 1
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_symbol_engine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.engine.symbol_state'`

- [ ] **Step 3: Create `marketspike/engine/symbol_state.py`**

```python
import time
from typing import Any, Dict, Optional

from marketspike.engine.pipeline import PipelineTimer
from marketspike.engine.regime import MARKET_CLOSED, NORMAL, RegimeFSM
from marketspike.engine.scoring import composite_score
from marketspike.engine.spread import SpreadTracker
from marketspike.engine.volatility import VolatilityPair
from marketspike.feeds.base import Tick

RATE_TAU_S = 5.0


class SymbolEngine:
    """Owns all per-symbol state and publishes frames for one instrument."""

    def __init__(
        self,
        symbol: str,
        bus,
        recorder,
        tau_fast_s: float = 30.0,
        tau_slow_s: float = 1800.0,
        skew_window_s: float = 60.0,
        ws_max_hz: float = 20.0,
    ) -> None:
        self.symbol = symbol
        self.bus = bus
        self.recorder = recorder
        self.timer = PipelineTimer(skew_window_s=skew_window_s)
        self.vol = VolatilityPair(tau_fast_s, tau_slow_s)
        self.spread = SpreadTracker()
        self.fsm = RegimeFSM()
        self.event_context = "CLEAR"

        self._min_frame_ns = int(1e9 / ws_max_hz) if ws_max_hz > 0 else 0
        self._last_frame_ns = 0
        self._last_rate_ts_ns: Optional[int] = None

        self.quote_rate_hz = 0.0
        self.v_ratio: Optional[float] = None
        self.spread_z = 0.0
        self.score = 0.0
        self.last_tick: Optional[Tick] = None

    def seed(self, var_per_second: float) -> None:
        self.vol.seed_slow(var_per_second)

    @property
    def warmup_complete(self) -> bool:
        return self.vol.ready

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

        self.v_ratio = self.vol.update(tick.recv_ts_ns, tick.mid)
        self.spread_z = self.spread.update(tick.recv_ts_ns, tick.spread_bps)
        self.score = composite_score(self.v_ratio, self.spread_z)

        previous = self.fsm.state
        changed = self.fsm.update(tick.recv_ts_ns, self.score)

        done_ts_ns = time.time_ns()
        excess_us, engine_us = self.timer.on_processed(tick, done_ts_ns)
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
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_symbol_engine.py -v`
Expected: 5 passed

- [ ] **Step 5: Create `marketspike/api/rest.py` with health, regime, and latency routes**

```python
import time
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v1")


def _state() -> Dict[str, Any]:
    from marketspike.main import STATE

    return STATE


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


@router.get("/regime")
def regime(symbol: str = Query(...)) -> Dict[str, Any]:
    engine = _state().get("engines", {}).get(symbol)
    if engine is None:
        raise HTTPException(
            status_code=404,
            detail={
                "type": "/errors/unknown-symbol",
                "title": "Unknown symbol",
                "status": 404,
                "detail": "{0} is not an active symbol".format(symbol),
                "instance": "/api/v1/regime",
            },
        )
    return engine.snapshot()


@router.get("/latency/summary")
def latency_summary(symbol: str = Query(...)) -> Dict[str, Any]:
    engine = _state().get("engines", {}).get(symbol)
    if engine is None:
        raise HTTPException(
            status_code=404,
            detail={
                "type": "/errors/unknown-symbol",
                "title": "Unknown symbol",
                "status": 404,
                "detail": "{0} is not an active symbol".format(symbol),
                "instance": "/api/v1/latency/summary",
            },
        )
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
```

- [ ] **Step 6: Wire engines into `marketspike/main.py`**

Add these imports:

```python
from marketspike.api import rest as rest_api
from marketspike.engine.symbol_state import SymbolEngine
```

Add beside the existing `app.include_router(ws_api.router)` line:

```python
app.include_router(rest_api.router)
```

Replace the whole `_make_ingest` function with this version, which drives the engine rather than writing straight to the recorder:

```python
def _make_ingest(adapter, engine, recorder: Recorder):
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
```

Then, inside `startup()`, replace the feed-task loop with:

```python
    engines = {}
    for symbol, adapter in adapters.items():
        engines[symbol] = SymbolEngine(
            symbol=symbol, bus=STATE["bus"], recorder=recorder,
            tau_fast_s=settings.tau_fast_s, tau_slow_s=settings.tau_slow_s,
            skew_window_s=settings.skew_window_s, ws_max_hz=settings.ws_max_hz,
        )
    STATE["engines"] = engines

    for symbol, adapter in adapters.items():
        tasks.append(
            asyncio.ensure_future(
                supervise(
                    "feed:{0}".format(symbol),
                    _make_ingest(adapter, engines[symbol], recorder),
                )
            )
        )
```

- [ ] **Step 7: Verify against the live feed**

Run: `MS_SYMBOLS=BTCUSDT python -m marketspike.main`, wait 30 seconds, then:

```bash
curl -s localhost:8000/api/v1/regime?symbol=BTCUSDT
curl -s localhost:8000/api/v1/latency/summary?symbol=BTCUSDT
curl -s localhost:8000/api/v1/health
```

Expected: `regime` returns `"warmup_complete": true` and a `v_ratio` near 1.0 on a quiet market; `latency/summary` returns non-zero `engine_us` percentiles; `health` shows `connected: true` and a small `last_tick_age_ms`.

A `v_ratio` far from 1.0 on a calm market means the kline seeding units are wrong — check `variance_per_second_from_closes` divides by 60.

- [ ] **Step 8: Commit**

```bash
git add marketspike/engine/symbol_state.py marketspike/api/rest.py marketspike/main.py tests/test_symbol_engine.py
git commit -m "feat: per-symbol engine with regime frames, persistence, and REST reads"
```

---

## Phase 4 — Risk engine (spec §10; hours 12–14)

At the end of this phase the application is end-to-end functional: live data in, slippage-aware size out. Everything after this makes it more credible, not more complete.

### Task 15: Instrument registry

**Files:**
- Create: `marketspike/risk/__init__.py`, `marketspike/risk/instruments.json`, `marketspike/risk/instruments.py`
- Test: `tests/test_instruments.py`

**Interfaces:**
- Consumes: nothing
- Produces: `InstrumentSpec` dataclass (`symbol`, `pip_size`, `contract_size`, `quote_ccy`, `min_lot`, `lot_step`, `margin_rate`) with `pip_value(fx_rate: float) -> float`; `REGISTRY: Dict[str, InstrumentSpec]`; `get_instrument(symbol) -> InstrumentSpec` raising `KeyError`; `all_instruments() -> List[InstrumentSpec]`

- [ ] **Step 1: Write the failing test — `tests/test_instruments.py`**

```python
import pytest

from marketspike.risk.instruments import all_instruments, get_instrument


def test_eurusd_pip_value_is_ten_dollars_per_standard_lot():
    spec = get_instrument("EURUSD")
    assert spec.pip_value(fx_rate=1.0) == pytest.approx(10.0)


def test_usdjpy_uses_a_two_decimal_pip_and_needs_conversion():
    spec = get_instrument("USDJPY")
    assert spec.pip_size == 0.01
    assert spec.quote_ccy == "JPY"
    # 0.01 * 100000 = 1000 JPY per pip; at 0.0067 USD/JPY that is $6.70.
    assert spec.pip_value(fx_rate=0.0067) == pytest.approx(6.70)


def test_btcusdt_is_not_forced_into_forex_conventions():
    spec = get_instrument("BTCUSDT")
    assert spec.contract_size == 1
    assert spec.lot_step == 0.0001


def test_unknown_symbol_raises():
    with pytest.raises(KeyError):
        get_instrument("GBPJPY")


def test_registry_exposes_every_instrument():
    symbols = {spec.symbol for spec in all_instruments()}
    assert {"EURUSD", "USDJPY", "XAUUSD", "BTCUSDT"} <= symbols
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_instruments.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.risk'`

- [ ] **Step 3: Create `marketspike/risk/instruments.json`**

```json
{
  "EURUSD":  {"pip_size": 0.0001, "contract_size": 100000, "quote_ccy": "USD",
              "min_lot": 0.01, "lot_step": 0.01, "margin_rate": 0.0333},
  "USDJPY":  {"pip_size": 0.01,   "contract_size": 100000, "quote_ccy": "JPY",
              "min_lot": 0.01, "lot_step": 0.01, "margin_rate": 0.0333},
  "XAUUSD":  {"pip_size": 0.01,   "contract_size": 100,    "quote_ccy": "USD",
              "min_lot": 0.01, "lot_step": 0.01, "margin_rate": 0.05},
  "BTCUSDT": {"pip_size": 1.0,    "contract_size": 1,      "quote_ccy": "USDT",
              "min_lot": 0.0001, "lot_step": 0.0001, "margin_rate": 0.10}
}
```

- [ ] **Step 4: Create `marketspike/risk/instruments.py`**

```python
import json
import os
from dataclasses import dataclass
from typing import Dict, List

_PATH = os.path.join(os.path.dirname(__file__), "instruments.json")


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str
    pip_size: float
    contract_size: float
    quote_ccy: str
    min_lot: float
    lot_step: float
    margin_rate: float

    def pip_value(self, fx_rate: float) -> float:
        """Account-currency value of one pip on one lot.

        Derived, never stored: the draft hardcoded 10.0, which is a
        USD-quoted-major assumption presented as a universal constant.
        """
        return self.pip_size * self.contract_size * fx_rate


def _load() -> Dict[str, InstrumentSpec]:
    with open(_PATH, "r") as handle:
        raw = json.load(handle)
    return {
        symbol: InstrumentSpec(symbol=symbol, **fields)
        for symbol, fields in raw.items()
    }


REGISTRY: Dict[str, InstrumentSpec] = _load()


def get_instrument(symbol: str) -> InstrumentSpec:
    return REGISTRY[symbol]


def all_instruments() -> List[InstrumentSpec]:
    return list(REGISTRY.values())
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_instruments.py -v`
Expected: 5 passed

- [ ] **Step 6: Add the `/instruments` route to `marketspike/api/rest.py`**

Add the import at the top:

```python
from marketspike.risk.instruments import all_instruments
```

Add the route:

```python
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
```

- [ ] **Step 7: Commit**

```bash
git add marketspike/risk tests/test_instruments.py marketspike/api/rest.py
git commit -m "feat: instrument registry with derived pip value"
```

---

### Task 16: Slippage model inference with fallback coefficients

**Files:**
- Create: `marketspike/risk/slippage.py`
- Test: `tests/test_slippage.py`

**Interfaces:**
- Consumes: nothing
- Produces: `FEATURE_ORDER: List[str]`; `SlippageModel(symbol, quantiles, version, source)` with `predict_bps(features: Dict[str, float], quantile: str) -> float`; `load_models(path: str) -> Dict[str, SlippageModel]`; `fallback_model(symbol: str) -> SlippageModel`

- [ ] **Step 1: Write the failing test — `tests/test_slippage.py`**

```python
import json

from marketspike.risk.slippage import (
    FEATURE_ORDER, fallback_model, load_models,
)

FEATURES = {name: 0.0 for name in FEATURE_ORDER}


def test_prediction_is_the_intercept_when_all_features_are_zero():
    model = fallback_model("EURUSD")
    assert model.predict_bps(FEATURES, "p50") == model.quantiles["p50"]["intercept"]


def test_p95_never_falls_below_p50_on_the_fallback():
    model = fallback_model("EURUSD")
    features = dict(FEATURES, log_v_ratio=1.5, spread_z=3.0, log_latency_ms=5.0)
    assert model.predict_bps(features, "p95") >= model.predict_bps(features, "p50")


def test_prediction_is_clamped_at_zero():
    model = fallback_model("EURUSD")
    features = dict(FEATURES, log_v_ratio=-1000.0)
    assert model.predict_bps(features, "p50") >= 0.0


def test_missing_features_default_to_zero_rather_than_raising():
    model = fallback_model("EURUSD")
    assert model.predict_bps({}, "p95") >= 0.0


def test_fallback_declares_its_provenance():
    assert fallback_model("EURUSD").source == "fallback_coefficients"


def test_loading_a_trained_file_marks_the_model_as_trained(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({
        "models": {
            "EURUSD": {
                "version": "eurusd-test",
                "feature_order": FEATURE_ORDER,
                "quantiles": {
                    "p50": {"intercept": 1.0, "coefficients": [0.0] * len(FEATURE_ORDER)},
                    "p95": {"intercept": 4.0, "coefficients": [0.0] * len(FEATURE_ORDER)},
                },
            }
        }
    }))
    models = load_models(str(path))
    assert models["EURUSD"].source == "trained"
    assert models["EURUSD"].predict_bps(FEATURES, "p95") == 4.0


def test_missing_file_yields_no_models_rather_than_raising():
    assert load_models("/nonexistent/model.json") == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_slippage.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.risk.slippage'`

- [ ] **Step 3: Create `marketspike/risk/slippage.py`**

```python
import json
import os
from typing import Any, Dict, List

# Order is part of the persisted model format. Never reorder without bumping
# the model version — coefficients are positional.
FEATURE_ORDER: List[str] = [
    "log_v_ratio",
    "spread_z",
    "log_spread_bps",
    "log_latency_ms",
    "quote_rate_hz",
    "book_imbalance",
    "signed_secs_to_event",
    "in_event_window",
    "abs_return_5s",
]

# Hand-set priors used until a model is trained. They are deliberately
# conservative and are always reported as "fallback_coefficients" so the demo
# never degrades silently (spec §9.10).
_FALLBACK: Dict[str, Dict[str, Any]] = {
    "p50": {
        "intercept": 0.60,
        "coefficients": [0.10, 0.05, 0.50, 0.05, 0.0, 0.0, 0.0, 0.20, 0.0],
    },
    "p95": {
        "intercept": 1.50,
        "coefficients": [0.80, 0.35, 0.90, 0.30, 0.0, 0.0, 0.0, 1.20, 0.0],
    },
}


class SlippageModel:
    """Linear quantile regression served as a dot product.

    Linear is the right choice here: it trains in seconds, the coefficients are
    interpretable, and inference needs no ML runtime at all — which means
    nothing extra to install on demo day (spec §9.5).
    """

    def __init__(
        self,
        symbol: str,
        quantiles: Dict[str, Dict[str, Any]],
        version: str,
        source: str,
        feature_order: List[str] = None,
    ) -> None:
        self.symbol = symbol
        self.quantiles = quantiles
        self.version = version
        self.source = source
        self.feature_order = feature_order or FEATURE_ORDER

    def predict_bps(self, features: Dict[str, float], quantile: str) -> float:
        spec = self.quantiles.get(quantile)
        if spec is None:
            return 0.0
        total = float(spec["intercept"])
        coefficients = spec["coefficients"]
        for index, name in enumerate(self.feature_order):
            if index >= len(coefficients):
                break
            total += coefficients[index] * float(features.get(name, 0.0))
        # A negative predicted cost is meaningless; clamp rather than emit it.
        return max(0.0, total)


def fallback_model(symbol: str) -> SlippageModel:
    return SlippageModel(
        symbol=symbol,
        quantiles={q: dict(spec) for q, spec in _FALLBACK.items()},
        version="fallback-v1",
        source="fallback_coefficients",
    )


def load_models(path: str) -> Dict[str, SlippageModel]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as handle:
            raw = json.load(handle)
    except (ValueError, OSError):
        return {}

    models: Dict[str, SlippageModel] = {}
    for symbol, entry in (raw.get("models") or {}).items():
        quantiles = entry.get("quantiles") or {}
        if not quantiles:
            continue
        models[symbol] = SlippageModel(
            symbol=symbol,
            quantiles=quantiles,
            version=entry.get("version", "unknown"),
            source="trained",
            feature_order=entry.get("feature_order") or FEATURE_ORDER,
        )
    return models


def resolve_models(path: str, symbols: List[str]) -> Dict[str, SlippageModel]:
    """Trained model where available, fallback everywhere else."""
    trained = load_models(path)
    return {
        symbol: trained.get(symbol) or fallback_model(symbol) for symbol in symbols
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_slippage.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/risk/slippage.py tests/test_slippage.py
git commit -m "feat: quantile slippage inference with declared fallback coefficients"
```

---

### Task 17: Position sizing and the `/size` endpoint

The core deliverable. `overexposure_pct` is the number the demo turns on.

**Files:**
- Create: `marketspike/risk/sizing.py`
- Modify: `marketspike/api/rest.py` (add `/size`), `marketspike/main.py` (load models at startup)
- Test: `tests/test_sizing.py`

**Interfaces:**
- Consumes: `InstrumentSpec`, `SlippageModel`, `SizeRequest`/`SizeResponse` schemas
- Produces: `round_down_to_step(value: float, step: float) -> float`; `SizingContext` dataclass (`price`, `fx_rate`, `fx_assumed`, `regime`, `event_context`, `latency_ms`, `latency_source`, `stale_quote`, `model_source`, `model_version`); `size_position(request, spec, slippage_p50_bps, slippage_p95_bps, context) -> Dict[str, Any]`

- [ ] **Step 1: Write the failing test — `tests/test_sizing.py`**

```python
import pytest

from marketspike.api.schemas import SizeRequest
from marketspike.risk.instruments import get_instrument
from marketspike.risk.sizing import SizingContext, round_down_to_step, size_position

EURUSD = get_instrument("EURUSD")


def context(**overrides):
    base = dict(
        price=1.0850, fx_rate=1.0, fx_assumed=False, regime="SPIKE",
        event_context="EVENT_WINDOW", latency_ms=63.2, latency_source="measured",
        stale_quote=False, model_source="trained", model_version="eurusd-test",
    )
    base.update(overrides)
    return SizingContext(**base)


def request(**overrides):
    base = dict(
        symbol="EURUSD", account_balance_minor=1000000, account_ccy="USD",
        risk_pct=1.0, stop_distance_price=0.0020, direction="buy",
        quantile="p95", free_margin_minor=1000000, assumed_latency_ms=None,
    )
    base.update(overrides)
    return SizeRequest(**base)


# slippage_pips = bps * price / (10000 * pip_size) = bps * price for EURUSD.
P50_BPS = 1.4 / 1.0850   # -> 1.4 pips
P95_BPS = 6.2 / 1.0850   # -> 6.2 pips


def test_round_down_never_rounds_up():
    assert round_down_to_step(0.3817, 0.01) == pytest.approx(0.38)
    assert round_down_to_step(0.3899, 0.01) == pytest.approx(0.38)


def test_round_down_is_stable_on_exact_multiples():
    assert round_down_to_step(0.38, 0.01) == pytest.approx(0.38)


def test_worked_example_from_the_spec():
    """Spec §10.6: $10,000 at 1% risk, 20-pip stop, 6.2-pip p95 slippage."""
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    assert result["stop_distance_pips"] == pytest.approx(20.0)
    assert result["slippage_p95_pips"] == pytest.approx(6.2, abs=1e-6)
    assert result["effective_adverse_pips"] == pytest.approx(26.2, abs=1e-6)
    assert result["naive_lot_size"] == pytest.approx(0.50)
    assert result["recommended_lot_size"] == pytest.approx(0.38)
    assert result["overexposure_pct"] == pytest.approx(31.6, abs=0.05)
    assert result["actual_risk_amount_minor"] == 9956
    assert result["actual_risk_pct"] == pytest.approx(0.9956, abs=1e-4)
    assert result["required_margin_minor"] == 137296


def test_actual_risk_is_below_target_because_of_round_down():
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    assert result["actual_risk_amount_minor"] < 10000


def test_p50_quantile_gives_a_larger_size_than_p95():
    p50 = size_position(request(quantile="p50"), EURUSD, P50_BPS, P95_BPS, context())
    p95 = size_position(request(quantile="p95"), EURUSD, P50_BPS, P95_BPS, context())
    assert p50["recommended_lot_size"] > p95["recommended_lot_size"]


def test_insufficient_margin_caps_the_size_and_says_so():
    result = size_position(
        request(free_margin_minor=50000),  # $500 free margin
        EURUSD, P50_BPS, P95_BPS, context(),
    )
    assert result["capped_by"] == "margin"
    assert result["recommended_lot_size"] < 0.38
    assert result["required_margin_minor"] <= 50000


def test_high_risk_warns_but_does_not_block():
    result = size_position(request(risk_pct=8.0), EURUSD, P50_BPS, P95_BPS, context())
    assert "HIGH_RISK_PCT" in result["warnings"]
    assert result["recommended_lot_size"] > 0


def test_size_below_minimum_lot_returns_zero_and_flags_it():
    result = size_position(
        request(account_balance_minor=1000),  # $10 account
        EURUSD, P50_BPS, P95_BPS, context(),
    )
    assert result["recommended_lot_size"] == 0.0
    assert "BELOW_MIN_LOT" in result["warnings"]


def test_assumed_fx_rate_is_surfaced():
    result = size_position(
        request(), EURUSD, P50_BPS, P95_BPS, context(fx_assumed=True)
    )
    assert result["fx_assumed"] is True


def test_response_echoes_inputs_and_context():
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    assert result["inputs_echo"]["symbol"] == "EURUSD"
    assert result["regime_at_calc"] == "SPIKE"
    assert result["latency_source"] == "measured"
    assert result["model_source"] == "trained"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_sizing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.risk.sizing'`

- [ ] **Step 3: Create `marketspike/risk/sizing.py`**

```python
import math
from dataclasses import dataclass
from typing import Any, Dict, List

from marketspike.api.schemas import SizeRequest
from marketspike.risk.instruments import InstrumentSpec

HIGH_RISK_PCT = 5.0


def round_down_to_step(value: float, step: float) -> float:
    """Round toward zero on the lot grid.

    Always down: rounding a risk-limited quantity up breaches the risk budget
    the user specified, which defeats the calculation (spec §10.4). The epsilon
    absorbs binary representation error so an exact multiple does not fall to
    the step below.
    """
    if step <= 0:
        return value
    return round(math.floor(value / step + 1e-9) * step, 10)


@dataclass
class SizingContext:
    price: float
    fx_rate: float
    fx_assumed: bool
    regime: str
    event_context: str
    latency_ms: float
    latency_source: str
    stale_quote: bool
    model_source: str
    model_version: str


def _bps_to_pips(bps: float, price: float, pip_size: float) -> float:
    return (bps / 10000.0) * price / pip_size


def size_position(
    request: SizeRequest,
    spec: InstrumentSpec,
    slippage_p50_bps: float,
    slippage_p95_bps: float,
    context: SizingContext,
) -> Dict[str, Any]:
    warnings: List[str] = []
    if request.risk_pct > HIGH_RISK_PCT:
        warnings.append("HIGH_RISK_PCT")

    balance = request.account_balance_minor / 100.0
    risk_budget = balance * (request.risk_pct / 100.0)

    pip_value = spec.pip_value(context.fx_rate)
    stop_pips = request.stop_distance_price / spec.pip_size

    p50_pips = _bps_to_pips(slippage_p50_bps, context.price, spec.pip_size)
    p95_pips = _bps_to_pips(slippage_p95_bps, context.price, spec.pip_size)
    chosen_pips = p95_pips if request.quantile == "p95" else p50_pips
    effective_pips = stop_pips + chosen_pips

    naive_lots = risk_budget / (stop_pips * pip_value)
    raw_lots = risk_budget / (effective_pips * pip_value)
    lots = round_down_to_step(raw_lots, spec.lot_step)

    capped_by = None
    free_margin = request.free_margin_minor / 100.0
    margin_per_lot = spec.contract_size * context.price * spec.margin_rate * context.fx_rate
    if margin_per_lot > 0:
        max_by_margin = round_down_to_step(free_margin / margin_per_lot, spec.lot_step)
        if max_by_margin < lots:
            lots = max_by_margin
            capped_by = "margin"

    if lots < spec.min_lot:
        lots = 0.0
        warnings.append("BELOW_MIN_LOT")

    actual_risk = lots * effective_pips * pip_value
    required_margin = lots * margin_per_lot
    overexposure = (
        ((naive_lots - lots) / lots * 100.0) if lots > 0 else 0.0
    )

    return {
        "naive_lot_size": round(naive_lots, 4),
        "recommended_lot_size": lots,
        "overexposure_pct": round(overexposure, 2),
        "slippage_p50_pips": round(p50_pips, 4),
        "slippage_p95_pips": round(p95_pips, 4),
        "stop_distance_pips": round(stop_pips, 4),
        "effective_adverse_pips": round(effective_pips, 4),
        "actual_risk_amount_minor": int(round(actual_risk * 100)),
        "actual_risk_pct": (actual_risk / balance * 100.0) if balance else 0.0,
        "required_margin_minor": int(round(required_margin * 100)),
        "capped_by": capped_by,
        "fx_assumed": context.fx_assumed,
        "stale_quote": context.stale_quote,
        "model_source": context.model_source,
        "model_version": context.model_version,
        "regime_at_calc": context.regime,
        "event_context": context.event_context,
        "latency_used_ms": context.latency_ms,
        "latency_source": context.latency_source,
        "warnings": warnings,
        "inputs_echo": request.model_dump(),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_sizing.py -v`
Expected: 10 passed

If `test_worked_example_from_the_spec` fails on `required_margin_minor`, recompute by hand: `0.38 × 100000 × 1.0850 × 0.0333 = 1372.959`, which rounds to `137296` minor units. Do not adjust the assertion to match a buggy implementation.

- [ ] **Step 5: Add the `/size` route to `marketspike/api/rest.py`**

Add the imports:

```python
import math

from marketspike.api.schemas import SizeRequest
from marketspike.risk.instruments import get_instrument
from marketspike.risk.sizing import SizingContext, size_position
from marketspike.risk.slippage import FEATURE_ORDER
```

Add the route:

```python
def _problem(status: int, slug: str, title: str, detail: str, instance: str):
    return HTTPException(
        status_code=status,
        detail={
            "type": "/errors/{0}".format(slug), "title": title,
            "status": status, "detail": detail, "instance": instance,
        },
    )


def _features(engine, latency_ms: float) -> Dict[str, float]:
    tick = engine.last_tick if engine else None
    v_ratio = (engine.v_ratio if engine else None) or 1.0
    spread_bps = tick.spread_bps if tick else 1.0
    return {
        "log_v_ratio": math.log(max(v_ratio, 1e-9)),
        "spread_z": engine.spread_z if engine else 0.0,
        "log_spread_bps": math.log(max(spread_bps, 1e-6)),
        "log_latency_ms": math.log(max(latency_ms, 1e-3)),
        "quote_rate_hz": engine.quote_rate_hz if engine else 0.0,
        "book_imbalance": tick.book_imbalance if tick else 0.0,
        "signed_secs_to_event": 0.0,
        "in_event_window": 1.0 if (engine and engine.event_context == "EVENT_WINDOW") else 0.0,
        "abs_return_5s": 0.0,
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
        from marketspike.risk.slippage import fallback_model

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

    result = size_position(
        request, spec,
        model.predict_bps(features, "p50"),
        model.predict_bps(features, "p95"),
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
```

- [ ] **Step 6: Load models at startup in `marketspike/main.py`**

Add the import:

```python
from marketspike.risk.slippage import resolve_models
```

Add inside `startup()`, immediately after `STATE["engines"] = engines`:

```python
    models = resolve_models(settings.model_path, list(adapters.keys()))
    STATE["models"] = models
    STATE["model_sources"] = {s: m.source for s, m in models.items()}
```

- [ ] **Step 7: Verify against the running service**

Run: `MS_SYMBOLS=BTCUSDT python -m marketspike.main`, wait 30 seconds, then:

```bash
curl -s -X POST localhost:8000/api/v1/size \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","account_balance_minor":1000000,"risk_pct":1.0,
       "stop_distance_price":250.0,"free_margin_minor":1000000}'
```

Expected: JSON containing `recommended_lot_size` strictly less than `naive_lot_size`, a positive `overexposure_pct`, `model_source: "fallback_coefficients"`, and `latency_source: "measured"`.

**The application is now end-to-end functional.** Everything after this point improves credibility, not completeness.

- [ ] **Step 8: Commit and push**

```bash
git add marketspike/risk/sizing.py marketspike/api/rest.py marketspike/main.py tests/test_sizing.py
git commit -m "feat: slippage-aware position sizing endpoint"
git push
```

---

## Phase 5 — Economic calendar (spec §8; hours 20–24)

Regime is derived from price and is therefore backward-looking by construction. It cannot tell a trader that a print is thirty minutes away — only the calendar can.

### Task 18: Event clock, phases, and alerts

**Files:**
- Create: `marketspike/calendar/__init__.py`, `marketspike/calendar/static_events.json`, `marketspike/calendar/clock.py`
- Modify: `marketspike/engine/symbol_state.py` (set `event_context` per tick), `marketspike/api/rest.py` (add `/calendar/upcoming`), `marketspike/main.py` (build the clock, load rows)
- Test: `tests/test_calendar.py`

**Interfaces:**
- Consumes: nothing
- Produces: `CalendarEvent` dataclass (`name`, `importance`, `country`, `event_ts_ns`, `affects`); `load_events(path: str) -> List[CalendarEvent]`; `EventClock(events)` with `relevant(now_ns, symbol) -> Optional[CalendarEvent]`, `signed_seconds(now_ns, symbol) -> float`, `phase(now_ns, symbol) -> str`, `upcoming(now_ns, hours, symbol=None) -> List[CalendarEvent]`; constants `PRE_EVENT_LEAD_S = 1800`, `EVENT_ENTER_S = 60`, `EVENT_EXIT_S = 900`

- [ ] **Step 1: Write the failing test — `tests/test_calendar.py`**

```python
import pytest

from marketspike.calendar.clock import CalendarEvent, EventClock

SECOND = 1_000_000_000
EVENT_TS = 1_000_000 * SECOND

CPI = CalendarEvent(
    name="US CPI (YoY)", importance="high", country="US",
    event_ts_ns=EVENT_TS, affects=["EURUSD", "BTCUSDT"],
)
CLOCK = EventClock([CPI])


def test_far_before_the_release_is_clear():
    assert CLOCK.phase(EVENT_TS - 7200 * SECOND, "EURUSD") == "CLEAR"


def test_thirty_minutes_before_is_pre_event():
    assert CLOCK.phase(EVENT_TS - 1799 * SECOND, "EURUSD") == "PRE_EVENT"


def test_thirty_seconds_before_is_the_event_window():
    assert CLOCK.phase(EVENT_TS - 30 * SECOND, "EURUSD") == "EVENT_WINDOW"


def test_five_minutes_after_is_still_the_event_window():
    assert CLOCK.phase(EVENT_TS + 300 * SECOND, "EURUSD") == "EVENT_WINDOW"


def test_an_hour_after_is_clear_again():
    assert CLOCK.phase(EVENT_TS + 3600 * SECOND, "EURUSD") == "CLEAR"


def test_symbols_the_event_does_not_affect_stay_clear():
    assert CLOCK.phase(EVENT_TS - 30 * SECOND, "XAUUSD") == "CLEAR"


def test_signed_seconds_are_negative_before_and_positive_after():
    assert CLOCK.signed_seconds(EVENT_TS - 600 * SECOND, "EURUSD") == pytest.approx(-600)
    assert CLOCK.signed_seconds(EVENT_TS + 600 * SECOND, "EURUSD") == pytest.approx(600)


def test_signed_seconds_are_clipped_to_the_feature_range():
    assert CLOCK.signed_seconds(EVENT_TS - 99999 * SECOND, "EURUSD") == -1800.0
    assert CLOCK.signed_seconds(EVENT_TS + 99999 * SECOND, "EURUSD") == 1800.0


def test_upcoming_filters_by_horizon_and_symbol():
    now = EVENT_TS - 3600 * SECOND
    assert CLOCK.upcoming(now, hours=2, symbol="EURUSD") == [CPI]
    assert CLOCK.upcoming(now, hours=2, symbol="XAUUSD") == []
    assert CLOCK.upcoming(now, hours=0.5, symbol="EURUSD") == []


def test_nearest_event_wins_when_two_are_close():
    later = CalendarEvent(
        name="FOMC", importance="high", country="US",
        event_ts_ns=EVENT_TS + 1200 * SECOND, affects=["EURUSD"],
    )
    clock = EventClock([CPI, later])
    assert clock.relevant(EVENT_TS + 1100 * SECOND, "EURUSD").name == "FOMC"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_calendar.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.calendar'`

- [ ] **Step 3: Create `marketspike/calendar/static_events.json`**

Populate with real high-impact US releases. BLS publishes the full annual schedule in advance, so these dates are known and static — replace with the actual dates covering your hackathon window before shipping.

```json
{
  "events": [
    {"name": "US Non-Farm Payrolls", "importance": "high", "country": "US",
     "event_ts": "2026-09-04T12:30:00Z", "affects": ["EURUSD", "BTCUSDT"]},
    {"name": "US CPI (YoY)", "importance": "high", "country": "US",
     "event_ts": "2026-09-10T12:30:00Z", "affects": ["EURUSD", "BTCUSDT"]},
    {"name": "FOMC Rate Decision", "importance": "high", "country": "US",
     "event_ts": "2026-09-16T18:00:00Z", "affects": ["EURUSD", "BTCUSDT"]},
    {"name": "US PPI (MoM)", "importance": "medium", "country": "US",
     "event_ts": "2026-09-11T12:30:00Z", "affects": ["EURUSD"]},
    {"name": "US Non-Farm Payrolls", "importance": "high", "country": "US",
     "event_ts": "2026-10-02T12:30:00Z", "affects": ["EURUSD", "BTCUSDT"]},
    {"name": "US CPI (YoY)", "importance": "high", "country": "US",
     "event_ts": "2026-10-13T12:30:00Z", "affects": ["EURUSD", "BTCUSDT"]}
  ]
}
```

- [ ] **Step 4: Create `marketspike/calendar/clock.py`**

```python
import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

from marketspike.feeds.base import rfc3339_to_ns

PRE_EVENT_LEAD_S = 1800
EVENT_ENTER_S = 60
EVENT_EXIT_S = 900

CLEAR = "CLEAR"
PRE_EVENT = "PRE_EVENT"
EVENT_WINDOW = "EVENT_WINDOW"

_PATH = os.path.join(os.path.dirname(__file__), "static_events.json")


@dataclass(frozen=True)
class CalendarEvent:
    name: str
    importance: str
    country: str
    event_ts_ns: int
    affects: List[str] = field(default_factory=list)


def load_events(path: Optional[str] = None) -> List[CalendarEvent]:
    with open(path or _PATH, "r") as handle:
        raw = json.load(handle)
    return [
        CalendarEvent(
            name=entry["name"],
            importance=entry.get("importance", "medium"),
            country=entry.get("country", ""),
            event_ts_ns=rfc3339_to_ns(entry["event_ts"]),
            affects=list(entry.get("affects") or []),
        )
        for entry in raw.get("events", [])
    ]


class EventClock:
    """Forward-looking context that price-derived regime cannot supply (§7.6)."""

    def __init__(self, events: List[CalendarEvent]) -> None:
        self._events = sorted(events, key=lambda event: event.event_ts_ns)

    def _for_symbol(self, symbol: str) -> List[CalendarEvent]:
        return [event for event in self._events if symbol in event.affects]

    def relevant(self, now_ns: int, symbol: str) -> Optional[CalendarEvent]:
        candidates = self._for_symbol(symbol)
        if not candidates:
            return None
        return min(candidates, key=lambda event: abs(now_ns - event.event_ts_ns))

    def signed_seconds(self, now_ns: int, symbol: str) -> float:
        event = self.relevant(now_ns, symbol)
        if event is None:
            return float(PRE_EVENT_LEAD_S)
        delta = (now_ns - event.event_ts_ns) / 1e9
        return max(-float(PRE_EVENT_LEAD_S), min(float(PRE_EVENT_LEAD_S), delta))

    def phase(self, now_ns: int, symbol: str) -> str:
        event = self.relevant(now_ns, symbol)
        if event is None:
            return CLEAR
        delta = (now_ns - event.event_ts_ns) / 1e9
        if -EVENT_ENTER_S <= delta <= EVENT_EXIT_S:
            return EVENT_WINDOW
        if -PRE_EVENT_LEAD_S <= delta < -EVENT_ENTER_S:
            return PRE_EVENT
        return CLEAR

    def upcoming(
        self, now_ns: int, hours: float, symbol: Optional[str] = None
    ) -> List[CalendarEvent]:
        horizon_ns = now_ns + int(hours * 3600 * 1e9)
        return [
            event
            for event in self._events
            if now_ns <= event.event_ts_ns <= horizon_ns
            and (symbol is None or symbol in event.affects)
        ]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_calendar.py -v`
Expected: 10 passed

- [ ] **Step 6: Feed event context into `SymbolEngine`**

In `marketspike/engine/symbol_state.py`, add `event_clock=None` as the final keyword argument of `__init__` and store it:

```python
        self.event_clock = event_clock
```

Then inside `on_tick`, immediately after the `if self.fsm.state == MARKET_CLOSED:` block and before `self.v_ratio = ...`, add:

```python
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
```

- [ ] **Step 7: Add the `/calendar/upcoming` route to `marketspike/api/rest.py`**

```python
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
            }
            for event in clock.upcoming(now_ns, hours, symbol)
        ],
    }
```

- [ ] **Step 8: Build the clock in `marketspike/main.py`**

Add the import:

```python
from marketspike.calendar.clock import EventClock, load_events
```

Inside `startup()`, immediately before the `engines = {}` line:

```python
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
```

Then pass it into each engine by adding `event_clock=event_clock,` to the `SymbolEngine(...)` constructor call.

- [ ] **Step 9: Commit**

```bash
git add marketspike/calendar marketspike/engine/symbol_state.py marketspike/api/rest.py marketspike/main.py tests/test_calendar.py
git commit -m "feat: economic calendar with pre-event and event-window context"
```

---

## Phase 6 — Slippage model training (spec §9; hours 24–28)

### Task 19: Feature builder with a structural leakage guard

**Files:**
- Create: `marketspike/ml/__init__.py`, `marketspike/ml/features.py`
- Test: `tests/test_features.py`

**Interfaces:**
- Consumes: `FEATURE_ORDER` from Task 16, `EventClock` from Task 18
- Produces: `TickRow` dataclass (`ts_ns`, `mid`, `spread_bps`, `book_imbalance`, `quote_rate_hz`, `v_ratio`, `spread_z`, `abs_return_5s`, `latency_ms`, `regime`); `LeakageError`; `build_sample(history, target, delta_ms, direction, event_clock, symbol) -> Sample`; `Sample` dataclass (`t_ns`, `symbol`, `features`, `delta_ms`, `direction`, `cost_bps`, `regime`); `build_dataset(rows, delta_ms, event_clock, symbol) -> List[Sample]`

- [ ] **Step 1: Write the failing test — `tests/test_features.py`**

```python
import pytest

from marketspike.calendar.clock import CalendarEvent, EventClock
from marketspike.ml.features import (
    LeakageError, TickRow, build_dataset, build_sample,
)
from marketspike.risk.slippage import FEATURE_ORDER

SECOND = 1_000_000_000
CLOCK = EventClock([
    CalendarEvent("US CPI", "high", "US", 500 * SECOND, ["EURUSD"])
])


def row(ts_ns, mid, spread_bps=2.0):
    return TickRow(
        ts_ns=ts_ns, mid=mid, spread_bps=spread_bps, book_imbalance=0.1,
        quote_rate_hz=5.0, v_ratio=1.5, spread_z=0.5, abs_return_5s=0.001,
        latency_ms=40.0, regime="NORMAL",
    )


def test_target_before_decision_time_is_rejected():
    history = [row(1000 * SECOND, 100.0)]
    with pytest.raises(LeakageError):
        build_sample(history, row(999 * SECOND, 100.0), 50.0, 1, CLOCK, "EURUSD")


def test_target_at_exactly_decision_time_is_rejected():
    history = [row(1000 * SECOND, 100.0)]
    with pytest.raises(LeakageError):
        build_sample(history, row(1000 * SECOND, 100.0), 50.0, 1, CLOCK, "EURUSD")


def test_features_use_only_the_decision_time_row():
    history = [row(1000 * SECOND, 100.0, spread_bps=2.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0, spread_bps=99.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    # log_spread_bps must reflect 2.0, never the target's 99.0.
    assert sample.features["log_spread_bps"] == pytest.approx(0.6931, abs=1e-3)


def test_every_declared_feature_is_populated():
    history = [row(1000 * SECOND, 100.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    assert set(sample.features) == set(FEATURE_ORDER)


def test_flat_market_cost_is_the_half_spread():
    history = [row(1000 * SECOND, 100.0, spread_bps=4.0)]
    target = row(1000 * SECOND + 50_000_000, 100.0, spread_bps=4.0)
    sample = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    assert sample.cost_bps == pytest.approx(2.0)


def test_adverse_drift_raises_cost_for_a_buy_and_lowers_it_for_a_sell():
    history = [row(1000 * SECOND, 100.0, spread_bps=4.0)]
    target = row(1000 * SECOND + 50_000_000, 100.1, spread_bps=4.0)  # +10 bps
    buy = build_sample(history, target, 50.0, 1, CLOCK, "EURUSD")
    sell = build_sample(history, target, 50.0, -1, CLOCK, "EURUSD")
    assert buy.cost_bps > sell.cost_bps
    assert (buy.cost_bps + sell.cost_bps) / 2 == pytest.approx(2.0, abs=1e-6)


def test_dataset_emits_both_directions_for_each_decision_point():
    rows = [row(i * 100 * 1_000_000, 100.0 + i * 0.01) for i in range(20)]
    samples = build_dataset(rows, delta_ms=200.0, event_clock=CLOCK, symbol="EURUSD")
    assert samples
    assert len(samples) % 2 == 0
    directions = {s.direction for s in samples}
    assert directions == {1, -1}


def test_dataset_never_emits_a_sample_whose_target_precedes_its_features():
    rows = [row(i * 100 * 1_000_000, 100.0 + i * 0.01) for i in range(20)]
    samples = build_dataset(rows, delta_ms=200.0, event_clock=CLOCK, symbol="EURUSD")
    for sample in samples:
        assert sample.t_ns + sample.delta_ms * 1_000_000 <= rows[-1].ts_ns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_features.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.ml'`

- [ ] **Step 3: Create `marketspike/ml/features.py`**

```python
import math
from dataclasses import dataclass
from typing import Dict, List, Optional

from marketspike.risk.slippage import FEATURE_ORDER


class LeakageError(ValueError):
    """Raised when a target observation is not strictly after decision time.

    This is the one defect that would make the model look excellent and be
    worthless, so it is enforced structurally rather than by convention
    (spec §9.4).
    """


@dataclass(frozen=True)
class TickRow:
    ts_ns: int
    mid: float
    spread_bps: float
    book_imbalance: float
    quote_rate_hz: float
    v_ratio: float
    spread_z: float
    abs_return_5s: float
    latency_ms: float
    regime: str


@dataclass(frozen=True)
class Sample:
    t_ns: int
    symbol: str
    features: Dict[str, float]
    delta_ms: float
    direction: int
    cost_bps: float
    regime: str


def _features_at(decision: TickRow, event_clock, symbol: str, latency_ms: float) -> Dict[str, float]:
    signed = event_clock.signed_seconds(decision.ts_ns, symbol) if event_clock else 1800.0
    phase = event_clock.phase(decision.ts_ns, symbol) if event_clock else "CLEAR"
    values = {
        "log_v_ratio": math.log(max(decision.v_ratio, 1e-9)),
        "spread_z": decision.spread_z,
        "log_spread_bps": math.log(max(decision.spread_bps, 1e-6)),
        "log_latency_ms": math.log(max(latency_ms, 1e-3)),
        "quote_rate_hz": decision.quote_rate_hz,
        "book_imbalance": decision.book_imbalance,
        "signed_secs_to_event": signed,
        "in_event_window": 1.0 if phase == "EVENT_WINDOW" else 0.0,
        "abs_return_5s": decision.abs_return_5s,
    }
    return {name: values[name] for name in FEATURE_ORDER}


def build_sample(
    history: List[TickRow],
    target: TickRow,
    delta_ms: float,
    direction: int,
    event_clock,
    symbol: str,
) -> Sample:
    """Implementation shortfall against arrival price (spec §9.1).

    `history` holds observations up to and including decision time; `target`
    is the single observation at t + delta. They are separate arguments so a
    caller cannot accidentally reach forward when constructing features.
    """
    decision = history[-1]
    if target.ts_ns <= decision.ts_ns:
        raise LeakageError(
            "target ts {0} must be strictly after decision ts {1}".format(
                target.ts_ns, decision.ts_ns
            )
        )

    half_spread_bps = (target.spread_bps / 2.0) * (target.mid / decision.mid)
    drift_bps = (target.mid - decision.mid) / decision.mid * 10000.0
    cost_bps = half_spread_bps + direction * drift_bps

    return Sample(
        t_ns=decision.ts_ns,
        symbol=symbol,
        features=_features_at(decision, event_clock, symbol, decision.latency_ms),
        delta_ms=delta_ms,
        direction=direction,
        cost_bps=cost_bps,
        regime=decision.regime,
    )


def build_dataset(
    rows: List[TickRow],
    delta_ms: float,
    event_clock,
    symbol: str,
    stride: int = 1,
) -> List[Sample]:
    """Emit both directions per decision point.

    Trade direction is unknown at feature time, and over a horizon of tens of
    milliseconds the assumption of no directional edge is well founded. The
    consequence is that p50 lands near the half spread while p95 captures the
    adverse tail (spec §9.2).
    """
    delta_ns = int(delta_ms * 1_000_000)
    samples: List[Sample] = []
    target_index = 0

    for index in range(0, len(rows), stride):
        decision = rows[index]
        wanted = decision.ts_ns + delta_ns
        if target_index < index:
            target_index = index
        while target_index < len(rows) and rows[target_index].ts_ns < wanted:
            target_index += 1
        if target_index >= len(rows):
            break
        target = rows[target_index]
        if target.ts_ns <= decision.ts_ns:
            continue
        for direction in (1, -1):
            samples.append(
                build_sample([decision], target, delta_ms, direction, event_clock, symbol)
            )
    return samples
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_features.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/ml tests/test_features.py
git commit -m "feat: leakage-guarded feature builder for slippage training"
```

---

### Task 20: Train, evaluate, and publish the model card

**Files:**
- Create: `marketspike/ml/evaluate.py`, `marketspike/ml/train.py`
- Modify: `marketspike/api/rest.py` (add `/model/card`)
- Test: `tests/test_evaluate.py`

**Interfaces:**
- Consumes: `Sample`, `build_dataset`, `FEATURE_ORDER`
- Produces: `pinball_loss(actuals, predictions, tau) -> float`; `coverage(actuals, predictions) -> float`; `baseline_predictions(samples) -> List[float]`; `evaluate(samples, predictions_by_quantile) -> Dict[str, Any]`; `train_symbol(samples, symbol) -> Dict[str, Any]`; `main()` CLI writing `model.json`

- [ ] **Step 1: Write the failing test — `tests/test_evaluate.py`**

```python
import pytest

from marketspike.ml.evaluate import coverage, pinball_loss


def test_pinball_loss_is_zero_for_a_perfect_forecast():
    assert pinball_loss([1.0, 2.0, 3.0], [1.0, 2.0, 3.0], tau=0.5) == 0.0


def test_pinball_loss_penalises_under_prediction_more_at_high_tau():
    under = pinball_loss([10.0], [8.0], tau=0.95)
    over = pinball_loss([10.0], [12.0], tau=0.95)
    assert under > over


def test_pinball_loss_is_symmetric_at_the_median():
    assert pinball_loss([10.0], [8.0], tau=0.5) == pytest.approx(
        pinball_loss([10.0], [12.0], tau=0.5)
    )


def test_coverage_counts_exceedances():
    actuals = [1.0, 2.0, 3.0, 4.0]
    predictions = [5.0, 5.0, 5.0, 5.0]
    assert coverage(actuals, predictions) == 0.0
    assert coverage(actuals, [0.0, 0.0, 5.0, 5.0]) == pytest.approx(0.5)


def test_coverage_of_empty_series_is_zero():
    assert coverage([], []) == 0.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_evaluate.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'marketspike.ml.evaluate'`

- [ ] **Step 3: Create `marketspike/ml/evaluate.py`**

```python
from typing import Any, Dict, List

from marketspike.ml.features import Sample


def pinball_loss(actuals: List[float], predictions: List[float], tau: float) -> float:
    if not actuals:
        return 0.0
    total = 0.0
    for actual, predicted in zip(actuals, predictions):
        error = actual - predicted
        total += max(tau * error, (tau - 1.0) * error)
    return total / len(actuals)


def coverage(actuals: List[float], predictions: List[float]) -> float:
    """Empirical exceedance rate — the calibration check for a quantile.

    A well-calibrated p95 is exceeded about 5% of the time. Report the real
    number even when it comes out at 7% (spec §9.7).
    """
    if not actuals:
        return 0.0
    exceeded = sum(1 for a, p in zip(actuals, predictions) if a > p)
    return exceeded / len(actuals)


def baseline_predictions(samples: List[Sample]) -> List[float]:
    """The assumption every retail calculator makes: you pay the half spread
    and slippage is zero (spec §9.6)."""
    import math

    return [math.exp(s.features["log_spread_bps"]) / 2.0 for s in samples]


def evaluate(
    samples: List[Sample], predictions_by_quantile: Dict[str, List[float]]
) -> Dict[str, Any]:
    actuals = [s.cost_bps for s in samples]
    base = baseline_predictions(samples)

    report: Dict[str, Any] = {"n_rows": len(samples), "quantiles": {}, "by_regime": {}}

    for quantile, predictions in predictions_by_quantile.items():
        tau = 0.95 if quantile == "p95" else 0.50
        model_loss = pinball_loss(actuals, predictions, tau)
        base_loss = pinball_loss(actuals, base, tau)
        improvement = (
            (base_loss - model_loss) / base_loss * 100.0 if base_loss > 0 else 0.0
        )
        report["quantiles"][quantile] = {
            "tau": tau,
            "pinball_model": model_loss,
            "pinball_baseline": base_loss,
            "improvement_pct": improvement,
            "coverage": coverage(actuals, predictions),
        }

    # The decisive breakdown: the baseline is adequate in calm markets and
    # catastrophically wrong during a print (spec §9.7).
    regimes = sorted({s.regime for s in samples})
    for regime in regimes:
        indices = [i for i, s in enumerate(samples) if s.regime == regime]
        if not indices:
            continue
        regime_actuals = [actuals[i] for i in indices]
        regime_base = [base[i] for i in indices]
        entry: Dict[str, Any] = {"n_rows": len(indices)}
        for quantile, predictions in predictions_by_quantile.items():
            tau = 0.95 if quantile == "p95" else 0.50
            regime_predictions = [predictions[i] for i in indices]
            model_loss = pinball_loss(regime_actuals, regime_predictions, tau)
            base_loss = pinball_loss(regime_actuals, regime_base, tau)
            entry[quantile] = {
                "pinball_model": model_loss,
                "pinball_baseline": base_loss,
                "improvement_pct": (
                    (base_loss - model_loss) / base_loss * 100.0 if base_loss > 0 else 0.0
                ),
                "coverage": coverage(regime_actuals, regime_predictions),
            }
        report["by_regime"][regime] = entry

    return report
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_evaluate.py -v`
Expected: 5 passed

- [ ] **Step 5: Create `marketspike/ml/train.py`**

```python
"""Fit linear quantile regressions from recorded ticks and write model.json.

Usage:
    python -m marketspike.ml.train --db marketspike.db --out model.json
"""
import argparse
import json
import math
import sqlite3
import time
from typing import Any, Dict, List, Optional

from marketspike.calendar.clock import EventClock, load_events
from marketspike.ml.evaluate import evaluate
from marketspike.ml.features import Sample, TickRow, build_dataset
from marketspike.risk.slippage import FEATURE_ORDER

TICK_QUERY = (
    "SELECT recv_ts_ns, bid, ask, bid_qty, ask_qty, excess_transit_us, engine_us "
    "FROM ticks WHERE symbol = ? AND tradeable = 1 ORDER BY recv_ts_ns"
)
REGIME_QUERY = "SELECT ts_ns, to_state FROM regime_events WHERE symbol = ? ORDER BY ts_ns"


def load_rows(conn: sqlite3.Connection, symbol: str) -> List[TickRow]:
    regimes = list(conn.execute(REGIME_QUERY, (symbol,)))
    regime_index = 0
    current_regime = "NORMAL"

    rows: List[TickRow] = []
    previous_mid: Optional[float] = None
    previous_ts: Optional[int] = None
    recent: List[Any] = []

    for record in conn.execute(TICK_QUERY, (symbol,)):
        ts_ns = record[0]
        bid, ask = record[1], record[2]
        mid = (bid + ask) / 2.0
        if mid <= 0:
            continue
        spread_bps = (ask - bid) / mid * 10000.0
        total_qty = (record[3] or 0.0) + (record[4] or 0.0)
        imbalance = (
            ((record[3] or 0.0) - (record[4] or 0.0)) / total_qty if total_qty else 0.0
        )

        while regime_index < len(regimes) and regimes[regime_index][0] <= ts_ns:
            current_regime = regimes[regime_index][1]
            regime_index += 1

        rate = 0.0
        if previous_ts is not None and ts_ns > previous_ts:
            rate = 1e9 / (ts_ns - previous_ts)

        recent.append((ts_ns, mid))
        cutoff = ts_ns - 5 * 1_000_000_000
        while recent and recent[0][0] < cutoff:
            recent.pop(0)
        abs_return_5s = (
            abs(math.log(mid / recent[0][1])) if recent and recent[0][1] > 0 else 0.0
        )

        latency_ms = ((record[5] or 0) + (record[6] or 0)) / 1000.0

        rows.append(
            TickRow(
                ts_ns=ts_ns, mid=mid, spread_bps=spread_bps,
                book_imbalance=imbalance, quote_rate_hz=rate,
                v_ratio=1.0, spread_z=0.0, abs_return_5s=abs_return_5s,
                latency_ms=max(latency_ms, 1.0), regime=current_regime,
            )
        )
        previous_mid, previous_ts = mid, ts_ns

    return _attach_volatility(rows)


def _attach_volatility(rows: List[TickRow]) -> List[TickRow]:
    """Recompute V ratio and spread z offline with the same estimators."""
    from marketspike.engine.spread import SpreadTracker
    from marketspike.engine.volatility import VolatilityPair

    vol = VolatilityPair(tau_fast_s=30.0, tau_slow_s=1800.0)
    spread = SpreadTracker(recompute_interval_s=5.0)
    enriched: List[TickRow] = []
    for row in rows:
        ratio = vol.update(row.ts_ns, row.mid)
        z = spread.update(row.ts_ns, row.spread_bps)
        enriched.append(
            TickRow(
                ts_ns=row.ts_ns, mid=row.mid, spread_bps=row.spread_bps,
                book_imbalance=row.book_imbalance, quote_rate_hz=row.quote_rate_hz,
                v_ratio=ratio or 1.0, spread_z=z, abs_return_5s=row.abs_return_5s,
                latency_ms=row.latency_ms, regime=row.regime,
            )
        )
    return enriched


def fit_quantiles(samples: List[Sample]) -> Dict[str, Dict[str, Any]]:
    import numpy as np
    from sklearn.linear_model import QuantileRegressor

    matrix = np.array(
        [[s.features[name] for name in FEATURE_ORDER] for s in samples], dtype=float
    )
    target = np.array([s.cost_bps for s in samples], dtype=float)

    fitted: Dict[str, Dict[str, Any]] = {}
    for label, tau in (("p50", 0.5), ("p95", 0.95)):
        model = QuantileRegressor(quantile=tau, alpha=1e-4, solver="highs")
        model.fit(matrix, target)
        fitted[label] = {
            "intercept": float(model.intercept_),
            "coefficients": [float(c) for c in model.coef_],
        }
    return fitted


def predict(fitted: Dict[str, Dict[str, Any]], samples: List[Sample]) -> Dict[str, List[float]]:
    output: Dict[str, List[float]] = {}
    for label, spec in fitted.items():
        coefficients = spec["coefficients"]
        output[label] = [
            max(
                0.0,
                spec["intercept"]
                + sum(
                    coefficients[i] * s.features[name]
                    for i, name in enumerate(FEATURE_ORDER)
                ),
            )
            for s in samples
        ]
    return output


def train_symbol(
    conn: sqlite3.Connection, symbol: str, delta_ms: float, clock: EventClock
) -> Optional[Dict[str, Any]]:
    rows = load_rows(conn, symbol)
    if len(rows) < 200:
        print("{0}: only {1} rows, skipping".format(symbol, len(rows)))
        return None

    samples = build_dataset(rows, delta_ms=delta_ms, event_clock=clock, symbol=symbol)
    if len(samples) < 200:
        print("{0}: only {1} samples, skipping".format(symbol, len(samples)))
        return None

    # Time-ordered split. Never random: shuffling a time series leaks the
    # future into the past and inflates every metric (spec §9.7).
    samples.sort(key=lambda s: s.t_ns)
    cut = int(len(samples) * 0.7)
    train, test = samples[:cut], samples[cut:]

    fitted = fit_quantiles(train)
    report = evaluate(test, predict(fitted, test))

    version = "{0}-{1}".format(
        symbol.lower(), time.strftime("%Y-%m-%dT%H:%MZ", time.gmtime())
    )
    print("{0}: {1} train / {2} test rows".format(symbol, len(train), len(test)))
    for label, stats in report["quantiles"].items():
        print(
            "  {0}: pinball {1:.4f} vs baseline {2:.4f} ({3:+.1f}%), coverage {4:.3f}".format(
                label, stats["pinball_model"], stats["pinball_baseline"],
                stats["improvement_pct"], stats["coverage"],
            )
        )

    return {
        "version": version,
        "trained_at_ns": time.time_ns(),
        "feature_order": FEATURE_ORDER,
        "quantiles": fitted,
        "metrics": report,
        "n_rows": len(train),
        "latency_coef_source": "fitted",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="marketspike.db")
    parser.add_argument("--out", default="model.json")
    parser.add_argument("--symbols", default="BTCUSDT,EURUSD")
    parser.add_argument("--delta-ms", type=float, default=60.0)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    clock = EventClock(load_events())
    models: Dict[str, Any] = {}
    for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        entry = train_symbol(conn, symbol, args.delta_ms, clock)
        if entry:
            models[symbol] = entry

        conn.execute(
            "INSERT OR REPLACE INTO model_registry (version, symbol, trained_at_ns, "
            "coefficients_json, metrics_json, n_rows, is_active) VALUES (?,?,?,?,?,?,1)",
            (entry["version"], symbol, entry["trained_at_ns"],
             json.dumps(entry["quantiles"]), json.dumps(entry["metrics"]),
             entry["n_rows"]),
        ) if entry else None
    conn.commit()

    with open(args.out, "w") as handle:
        json.dump({"models": models}, handle, indent=2)
    print("wrote {0} model(s) to {1}".format(len(models), args.out))


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Add the `/model/card` route to `marketspike/api/rest.py`**

```python
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
```

- [ ] **Step 7: Train against recorded data and confirm the model beats the baseline**

Run: `python -m marketspike.ml.train --db marketspike.db --symbols BTCUSDT --out model.json`

Expected: per-quantile lines showing `pinball` below `baseline` with a positive improvement percentage, and `wrote 1 model(s) to model.json`.

If improvement is negative, do not ship it as trained — the fallback is the honest choice, and `/model/card` will say so. If `n_rows` is under 200, the recorder has not run long enough; keep it running.

- [ ] **Step 8: Restart and confirm the trained model is picked up**

Run: `MS_SYMBOLS=BTCUSDT python -m marketspike.main`, then `curl -s localhost:8000/api/v1/model/card`

Expected: `"source": "trained"` and a `version` matching the training run.

- [ ] **Step 9: Commit**

```bash
git add marketspike/ml/train.py marketspike/ml/evaluate.py marketspike/api/rest.py tests/test_evaluate.py
git commit -m "feat: quantile regression training, evaluation, and model card"
```

---

## Phase 7 — Replay (spec §17, §18; hours 28–32)

Core scope, not stretch. A hackathon weekend very likely contains no genuine high-impact release, and the forex market may be closed the entire time. The replay path is what makes the demo deterministic.

### Task 21: Replay adapter, scenario capture, and the end-to-end integration test

**Files:**
- Create: `marketspike/feeds/replay.py`, `scripts/capture_scenario.py`
- Modify: `marketspike/api/rest.py` (add `/scenarios`, `/replay/start`, `/replay/stop`), `marketspike/main.py` (replay task control)
- Test: `tests/test_replay.py`

**Interfaces:**
- Consumes: `Tick`, `SymbolEngine`, `Bus`
- Produces: `write_scenario(path, ticks) -> int`; `read_scenario(path) -> List[Dict]`; `ReplayAdapter(symbol, path, speed=1.0)` with `stream()`, `seed_baseline()`, `progress_pct`; `list_scenarios(directory) -> List[str]`

- [ ] **Step 1: Write the failing test — `tests/test_replay.py`**

```python
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
    """The §15.4 integration test: NORMAL -> SPIKE -> NORMAL, once."""
    bus = Bus()
    engine = SymbolEngine(
        symbol="EURUSD", bus=bus, recorder=FakeRecorder(),
        tau_fast_s=30.0, tau_slow_s=1800.0, skew_window_s=60.0, ws_max_hz=1000.0,
    )
    engine.seed(1e-11)
    sub = bus.subscribe(maxlen=100000)

    for item in build_spike_path():
        engine.on_tick(item)

    changes = [f for f in list(sub._queue) if f["type"] == "regime_change"]
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
    published = [f for f in list(sub._queue) if f["type"] in ("tick", "latency")]
    assert published
    assert all(f["source"] == "simulated" for f in published)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_replay.py -v`
Expected: FAIL with `ImportError: cannot import name 'read_scenario'`

- [ ] **Step 3: Create `marketspike/feeds/replay.py`**

```python
import asyncio
import glob
import json
import os
import time
from typing import AsyncIterator, Dict, List, Optional

from marketspike.feeds.base import Tick

FIELDS = (
    "symbol", "venue_ts_ns", "recv_ts_ns", "bid", "ask",
    "bid_qty", "ask_qty", "tradeable", "source",
)


def write_scenario(path: str, ticks: List[Tick]) -> int:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w") as handle:
        for tick in ticks:
            handle.write(
                json.dumps(
                    {
                        "symbol": tick.symbol,
                        "venue_ts_ns": tick.venue_ts_ns,
                        "recv_ts_ns": tick.recv_ts_ns,
                        "bid": tick.bid, "ask": tick.ask,
                        "bid_qty": tick.bid_qty, "ask_qty": tick.ask_qty,
                        "tradeable": tick.tradeable,
                        "source": "simulated",
                    }
                )
                + "\n"
            )
    return len(ticks)


def read_scenario(path: str) -> List[Dict]:
    rows: List[Dict] = []
    with open(path, "r") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def list_scenarios(directory: str = "scenarios") -> List[str]:
    if not os.path.isdir(directory):
        return []
    return sorted(
        os.path.splitext(os.path.basename(p))[0]
        for p in glob.glob(os.path.join(directory, "*.ndjson"))
    )


class ReplayAdapter:
    """Emits recorded ticks through the identical engine code path.

    Demo mode is not a branch in the engine, it is a different adapter — which
    is what makes the replay trustworthy: the same logic runs, differing only
    in the `source` field (spec §5.1).
    """

    venue = "replay"

    def __init__(self, symbol: str, path: str, speed: float = 1.0) -> None:
        self.symbol = symbol
        self.path = path
        self.speed = max(speed, 0.01)
        self.connected = False
        self.progress_pct = 0.0
        self.scenario = os.path.splitext(os.path.basename(path))[0]

    async def seed_baseline(self) -> Optional[float]:
        return None

    async def stream(self) -> AsyncIterator[Tick]:
        rows = read_scenario(self.path)
        if not rows:
            return
        self.connected = True
        base_ns = rows[0]["recv_ts_ns"]
        started_ns = time.time_ns()

        for index, row in enumerate(rows):
            offset_ns = int((row["recv_ts_ns"] - base_ns) / self.speed)
            due_ns = started_ns + offset_ns
            delay_s = (due_ns - time.time_ns()) / 1e9
            if delay_s > 0:
                await asyncio.sleep(delay_s)

            now_ns = time.time_ns()
            transit_ns = max(0, row["recv_ts_ns"] - row["venue_ts_ns"])
            self.progress_pct = (index + 1) / len(rows) * 100.0

            yield Tick(
                symbol=self.symbol,
                venue_ts_ns=now_ns - transit_ns,
                recv_ts_ns=now_ns,
                bid=row["bid"], ask=row["ask"],
                bid_qty=row.get("bid_qty", 0.0), ask_qty=row.get("ask_qty", 0.0),
                tradeable=bool(row.get("tradeable", True)),
                source="simulated",
            )
        self.connected = False
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_replay.py -v`
Expected: 4 passed

If `test_replayed_spike_drives_exactly_one_regime_cycle` reports more than one SPIKE, the dwell or hysteresis constants in `engine/regime.py` have been altered — the FSM, not the test, is wrong.

- [ ] **Step 5: Create `scripts/capture_scenario.py`**

```python
"""Build a replay scenario, either from recorded ticks or OANDA history.

    # From your own recording, by time range:
    python scripts/capture_scenario.py from-db --db marketspike.db \
        --symbol BTCUSDT --start-ns 0 --end-ns 9999999999999999999 \
        --out scenarios/btc_spike.ndjson

    # From real OANDA history around a past release:
    python scripts/capture_scenario.py from-oanda --symbol EURUSD \
        --from 2026-07-11T12:00:00Z --to 2026-07-11T13:00:00Z \
        --out scenarios/cpi_2026_07_11.ndjson
"""
import argparse
import json
import os

import httpx

from marketspike.feeds.base import Tick, rfc3339_to_ns
from marketspike.feeds.replay import write_scenario

CANDLES_URL = "https://api-fxpractice.oanda.com/v3/instruments/{0}/candles"


def from_db(args) -> None:
    import sqlite3

    conn = sqlite3.connect(args.db)
    rows = conn.execute(
        "SELECT symbol, venue_ts_ns, recv_ts_ns, bid, ask, bid_qty, ask_qty, tradeable "
        "FROM ticks WHERE symbol = ? AND recv_ts_ns BETWEEN ? AND ? ORDER BY recv_ts_ns",
        (args.symbol, args.start_ns, args.end_ns),
    )
    ticks = [
        Tick(
            symbol=r[0], venue_ts_ns=r[1], recv_ts_ns=r[2], bid=r[3], ask=r[4],
            bid_qty=r[5] or 0.0, ask_qty=r[6] or 0.0, tradeable=bool(r[7]),
            source="simulated",
        )
        for r in rows
    ]
    print("captured {0} ticks -> {1}".format(write_scenario(args.out, ticks), args.out))


def from_oanda(args) -> None:
    """S5 bid/ask candles become one synthetic tick per bar close.

    Real spreads from a real release, at 5-second resolution. Coarser than
    live ticks, but the widening pattern is genuine (spec §3.3).
    """
    token = os.environ.get("MS_OANDA_TOKEN")
    if not token:
        raise SystemExit("MS_OANDA_TOKEN is not set")

    instrument = args.symbol[:3] + "_" + args.symbol[3:]
    response = httpx.get(
        CANDLES_URL.format(instrument),
        params={
            "price": "BA", "granularity": "S5",
            "from": getattr(args, "from"), "to": args.to,
        },
        headers={"Authorization": "Bearer {0}".format(token)},
        timeout=30.0,
    )
    response.raise_for_status()

    ticks = []
    for candle in response.json().get("candles", []):
        if not candle.get("complete"):
            continue
        ts_ns = rfc3339_to_ns(candle["time"])
        ticks.append(
            Tick(
                symbol=args.symbol,
                venue_ts_ns=ts_ns - 2_000_000,
                recv_ts_ns=ts_ns,
                bid=float(candle["bid"]["c"]),
                ask=float(candle["ask"]["c"]),
                bid_qty=1_000_000.0, ask_qty=1_000_000.0,
                tradeable=True, source="simulated",
            )
        )
    print("captured {0} ticks -> {1}".format(write_scenario(args.out, ticks), args.out))


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    db = sub.add_parser("from-db")
    db.add_argument("--db", default="marketspike.db")
    db.add_argument("--symbol", required=True)
    db.add_argument("--start-ns", type=int, required=True)
    db.add_argument("--end-ns", type=int, required=True)
    db.add_argument("--out", required=True)
    db.set_defaults(func=from_db)

    oanda = sub.add_parser("from-oanda")
    oanda.add_argument("--symbol", default="EURUSD")
    oanda.add_argument("--from", dest="from", required=True)
    oanda.add_argument("--to", required=True)
    oanda.add_argument("--out", required=True)
    oanda.set_defaults(func=from_oanda)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Add replay control routes to `marketspike/api/rest.py`**

Add the import:

```python
import asyncio

from marketspike.feeds.replay import ReplayAdapter, list_scenarios
```

Add the routes:

```python
@router.get("/scenarios")
def scenarios() -> Dict[str, Any]:
    return {"v": 1, "scenarios": list_scenarios("scenarios")}


@router.post("/replay/start")
def replay_start(body: Dict[str, Any]) -> Dict[str, Any]:
    from marketspike.engine.supervisor import supervise

    state = _state()
    scenario = body.get("scenario")
    symbol = body.get("symbol", "EURUSD")
    speed = float(body.get("speed", 1.0))

    path = "scenarios/{0}.ndjson".format(scenario)
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

    adapter = ReplayAdapter(symbol, path, speed=speed)

    async def drive() -> None:
        async for tick in adapter.stream():
            engine.on_tick(tick)

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
```

- [ ] **Step 7: Rehearse the demo path end to end**

```bash
mkdir -p scenarios
MS_SYMBOLS=BTCUSDT python -m marketspike.main &
sleep 120                                    # accumulate live ticks
python scripts/capture_scenario.py from-db --db marketspike.db --symbol BTCUSDT \
  --start-ns 0 --end-ns 99999999999999999999 --out scenarios/btc_live.ndjson
curl -s localhost:8000/api/v1/scenarios
curl -s -X POST localhost:8000/api/v1/replay/start \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"btc_live","symbol":"BTCUSDT","speed":10.0}'
```

Expected: `/scenarios` lists `btc_live`; after starting replay, WebSocket tick frames carry `"source": "simulated"` and `/api/v1/health` reports `"mode": "replay"`.

- [ ] **Step 8: Run the whole suite**

Run: `python -m pytest -q`
Expected: all tests pass, zero failures. Record the count — it is the number quoted in the README.

- [ ] **Step 9: Commit and push**

```bash
git add marketspike/feeds/replay.py scripts/capture_scenario.py marketspike/api/rest.py tests/test_replay.py
git commit -m "feat: replay adapter, scenario capture, and end-to-end regime integration test"
git push
```

---

## Plan self-review

Checked against the spec after writing. Findings and resolutions:

**Spec coverage.** Every numbered spec section maps to a task:

| Spec | Task |
|---|---|
| §3 feeds, §5.1 protocol | 3, 4 |
| §4.2 backpressure | 5 (recorder), 10 (bus) |
| §6 latency | 7, 8, 9 |
| §7 volatility and regime | 11, 12, 13, 14 |
| §8 calendar | 18 |
| §9 slippage model | 16, 19, 20 |
| §10 sizing | 15, 17 |
| §11 storage | 1, 5 |
| §12 API contract | 2, 10, 14, 15, 17, 18, 20, 21 |
| §13 configuration | 1 |
| §14 resilience | 6 (supervisor), 4 (market closed), 16 (model fallback) |
| §15 tests | distributed across all tasks; §15.4 integration lands in Task 21 |
| §17 demo | 21 |

**Gaps found and closed during review:**

1. **`/slippage/predict` (spec §12.1) had no task.** Added as Task 22 below — it is small, and the UI needs it to draw a slippage curve.
2. **Client-side `delivery_us` is emitted as `null`.** The `ack` handshake collects round-trip and offset in Task 10, but the value is never fed back into the latency frame. Closed as Task 23 below.
3. **`fx_rate` is hardcoded to 1.0 in Task 17's route.** Correct for EURUSD and BTCUSDT with a USD account — the only two symbols shipping — and `fx_assumed` is set truthfully for anything else. Documented as a known limitation rather than built out, since USDJPY and XAUUSD are registry entries, not active feeds.

**Type consistency.** Verified `SymbolEngine.on_tick` signature against both call sites (Task 14 ingest, Task 21 replay driver); `Recorder.submit_tick`/`submit_regime` against the `FakeRecorder` used in tests; `SlippageModel.predict_bps` against Task 17's caller; `FEATURE_ORDER` identical across `risk/slippage.py`, `ml/features.py`, and `ml/train.py`.

**Placeholder scan.** No TBDs. Every code step carries complete code.

---

### Task 22: Slippage prediction endpoint

**Files:**
- Modify: `marketspike/api/rest.py`
- Test: `tests/test_predict_route.py`

**Interfaces:**
- Consumes: `SlippageModel`, `_features` from Task 17
- Produces: `POST /api/v1/slippage/predict`

- [ ] **Step 1: Write the failing test — `tests/test_predict_route.py`**

```python
from fastapi.testclient import TestClient

from marketspike.api import rest
from marketspike.risk.slippage import fallback_model


def client_with_state(monkeypatch):
    from fastapi import FastAPI

    import marketspike.main as main

    main.STATE.clear()
    main.STATE.update({
        "engines": {}, "adapters": {}, "models": {"EURUSD": fallback_model("EURUSD")},
        "started_ns": 0, "mode": "live",
    })
    app = FastAPI()
    app.include_router(rest.router)
    return TestClient(app)


def test_predict_returns_both_quantiles(monkeypatch):
    client = client_with_state(monkeypatch)
    response = client.post(
        "/api/v1/slippage/predict",
        json={"symbol": "EURUSD", "spread_bps": 2.0, "v_ratio": 3.0,
              "latency_ms": 80.0, "spread_z": 4.0},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["p95_bps"] >= body["p50_bps"] >= 0.0
    assert body["model_source"] == "fallback_coefficients"


def test_predict_rejects_unknown_symbol(monkeypatch):
    client = client_with_state(monkeypatch)
    response = client.post("/api/v1/slippage/predict", json={"symbol": "GBPJPY"})
    assert response.status_code == 404


def test_higher_latency_predicts_higher_cost(monkeypatch):
    client = client_with_state(monkeypatch)
    base = {"symbol": "EURUSD", "spread_bps": 2.0, "v_ratio": 1.0, "spread_z": 0.0}
    slow = client.post("/api/v1/slippage/predict", json=dict(base, latency_ms=500.0))
    fast = client.post("/api/v1/slippage/predict", json=dict(base, latency_ms=5.0))
    assert slow.json()["p95_bps"] > fast.json()["p95_bps"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_predict_route.py -v`
Expected: FAIL with 404 on `/api/v1/slippage/predict`

- [ ] **Step 3: Add the route to `marketspike/api/rest.py`**

```python
@router.post("/slippage/predict")
def slippage_predict(body: Dict[str, Any]) -> Dict[str, Any]:
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
        from marketspike.risk.slippage import fallback_model

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

    return {
        "v": 1,
        "symbol": symbol,
        "p50_bps": model.predict_bps(features, "p50"),
        "p95_bps": model.predict_bps(features, "p95"),
        "model_source": model.source,
        "model_version": model.version,
        "inputs_echo": body,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_predict_route.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add marketspike/api/rest.py tests/test_predict_route.py
git commit -m "feat: slippage prediction endpoint for what-if curves"
```

---

### Task 23: Feed client delivery latency back into the stream

Closes the gap where `delivery_us` was always `null` despite the handshake collecting the data.

**Files:**
- Modify: `marketspike/api/ws.py`, `marketspike/engine/symbol_state.py`
- Test: `tests/test_delivery_latency.py`

**Interfaces:**
- Consumes: `compute_sync`, `SyncFilter`
- Produces: `Bus.record_delivery(delivery_us: int)` and `Bus.delivery_us` (median of recent client samples)

- [ ] **Step 1: Write the failing test — `tests/test_delivery_latency.py`**

```python
from marketspike.engine.bus import Bus


def test_delivery_is_none_before_any_client_reports():
    assert Bus().delivery_us is None


def test_delivery_is_the_median_of_recent_samples():
    bus = Bus()
    for value in (10_000, 20_000, 30_000):
        bus.record_delivery(value)
    assert bus.delivery_us == 20_000


def test_delivery_window_is_bounded():
    bus = Bus()
    for value in range(1000):
        bus.record_delivery(value)
    assert bus.delivery_us is not None
    assert bus.delivery_us > 900  # old samples evicted, recent ones dominate
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_delivery_latency.py -v`
Expected: FAIL with `AttributeError: 'Bus' object has no attribute 'delivery_us'`

- [ ] **Step 3: Add delivery tracking to `marketspike/engine/bus.py`**

Add to the imports at the top of the file:

```python
from statistics import median as _median
```

Add to `Bus.__init__`:

```python
        self._delivery: Deque[int] = deque(maxlen=64)
```

Add these members to `Bus`:

```python
    def record_delivery(self, delivery_us: int) -> None:
        if delivery_us >= 0:
            self._delivery.append(delivery_us)

    @property
    def delivery_us(self) -> Any:
        if not self._delivery:
            return None
        return int(_median(self._delivery))
```

- [ ] **Step 4: Report it from the WebSocket `ack` handler in `marketspike/api/ws.py`**

Replace the `elif kind == "ack":` branch with:

```python
            elif kind == "ack":
                round_trip, offset = compute_sync(
                    client_send_ns=int(message.get("client_send_ns", server_recv_ns)),
                    server_recv_ns=server_recv_ns,
                    server_send_ns=server_recv_ns,
                    client_recv_ns=int(message["client_recv_ns"]),
                )
                sync.add(round_trip, offset)
                best = sync.best_round_trip_ns
                if best is not None:
                    # One-way delivery is half the least-delayed round trip.
                    bus.record_delivery(max(0, best // 2000))
```

- [ ] **Step 5: Use it in the latency frame in `marketspike/engine/symbol_state.py`**

Replace `"delivery_us": None,` in the latency frame payload with:

```python
                    "delivery_us": self.bus.delivery_us,
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/test_delivery_latency.py -v`
Expected: 3 passed

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest -q`
Expected: all tests pass

- [ ] **Step 8: Commit and push**

```bash
git add marketspike/engine/bus.py marketspike/api/ws.py marketspike/engine/symbol_state.py tests/test_delivery_latency.py
git commit -m "feat: report measured client delivery latency on the stream"
git push
```

---

## Known limitations

State these plainly rather than letting a judge find them:

1. **FX conversion is identity.** Correct for the two shipping symbols (EURUSD and BTCUSDT against a USD account). USDJPY and XAUUSD exist in the registry but have no live feed; any request for them sets `fx_assumed: true`.
2. **Venue transit is relative, not absolute.** `excess_transit_us` is measured above a rolling baseline that contains an unmeasurable clock offset. This is a property of the problem, not a shortcut — see §6.2.
3. **`v_ratio` and `spread_z` are recomputed offline during training** rather than read from the tick row, because they are engine state rather than persisted columns. The same estimator classes are used, so values match, but a mid-session restart resets the EWMA and produces a discontinuity in the training features around that point.
4. **The EURUSD model may be trained on S5 candles** if the market is closed all weekend, which cannot identify the latency coefficient. Spec §9.9 defines the partial-pooling fallback; the model card must record it.
5. **No authentication.** Out of scope per §2.2; do not expose the service beyond localhost.

---

## Definition of done

- [ ] `python -m pytest -q` passes with zero failures
- [ ] `/health` reports both configured feeds connected and warm
- [ ] `/regime` shows transitions on real data, with no flapping over a 10-minute observation
- [ ] `/size` returns `overexposure_pct` above 25% during a replayed spike
- [ ] `/model/card` reports `source: "trained"` with positive improvement over baseline, broken out by regime
- [ ] Replay reproduces a volatility event end to end with `source: "simulated"` on every frame
- [ ] The frontend consumes the contract with no backend changes requested
- [ ] Demo rehearsed start to finish three times
