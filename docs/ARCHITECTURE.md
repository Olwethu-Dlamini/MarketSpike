# How the backend works

For a developer joining the project. Read this before changing anything — several design choices look arbitrary until you know what they're defending against.

The user-facing guide is [`USAGE.md`](USAGE.md); the full quantitative rationale is [`design/design-spec.md`](design/design-spec.md). This document is about the *code*.

---

## 1. The shape of it

One process, one asyncio event loop, three concurrent activities:

```
  [event loop thread]                          [executor threads]

  feed adapter ──▶ SymbolEngine.on_tick()      recorder ──▶ SQLite
        │               │        │                              ▲
        │               │        └──▶ recorder queue ───────────┘
        │               ▼
        │             Bus  ──▶ WebSocket clients
        │
        └── one supervised task per symbol      FastAPI routes (sync def)
                                                      │
                                                      └──▶ read engine state
```

Three rules explain most of the code:

1. **The engine never awaits disk or network.** It hands rows to a bounded queue and returns.
2. **Every long-lived task is supervised.** A bare `asyncio.Task` that raises dies silently.
3. **REST routes run on a *different thread* to the engine.** This is the subtlest one — see §6.

---

## 2. Following a single tick

Start at `marketspike/main.py`, `_make_ingest()`. That's the whole ingest loop:

```python
baseline = await adapter.seed_baseline()      # 24h of klines → warm slow horizon
engine.seed(baseline)
async for tick in adapter.stream():           # adapter reconnects internally
    engine.on_tick(tick)
```

`on_tick` (`engine/symbol_state.py`) then runs, in order:

| Step | What | Why it's there |
|---|---|---|
| `timer.on_receive(tick)` | Skew-corrected transit | Must happen first — it timestamps arrival |
| tradeability check | `MARKET_CLOSED` transition | Weekend forex; fires once per transition, not per tick |
| `vol.update()` | Volatility ratio `V` | Gated to ~1 sample/sec inside `VolatilityPair` |
| `spread.update()` | Robust spread z-score | Median/MAD, recomputed every ~5s |
| `composite_score()` | Blends the two signals | Bounded 0–4 |
| `fsm.update()` | Regime transition | **Only if `warmup_complete`** |
| `timer.on_processed()` | Engine compute time | Exact — same clock |
| `recorder.submit_tick()` | Persist | Non-blocking; drops if full |
| `bus.publish()` | Frames to clients | Rate-capped; recording is not |

**The recording and publishing rates are deliberately independent.** Every tick is recorded; frames are capped at `MS_WS_MAX_HZ` (default 20). A browser can't render 100 Hz, and trying inflates the very delivery latency this system measures.

---

## 3. Module map, and the boundaries that are enforced

```
feeds/      venue adapters → normalised Tick        imports nothing from the project
clock/      skew estimator, NTP sync                leaf, no project imports
engine/     timing, volatility, spread, regime, bus imports feeds/ + clock/ only
calendar/   event phases                            leaf
risk/       instruments, slippage, sizing           imports api/schemas only
store/      SQLite schema + recorder                the only place sqlite3 appears*
ml/         features, training, evaluation          imports risk/ + calendar/
api/        schemas, REST routes, WebSocket         imports everything
```

\* `ml/train.py` also imports `sqlite3` — it's an offline CLI reading the database directly, not part of the serving path.

**`engine/` must never import `api/` or `store/`.** `SymbolEngine` receives `bus` and `recorder` by injection, untyped. That's what lets the entire engine be tested with a five-line fake recorder and no database. If you find yourself wanting to import a route or a connection into `engine/`, the dependency is pointing the wrong way.

There is no framework enforcing this — just discipline and a grep. Adding a violating import will not fail the tests.

---

## 4. Things that look wrong but aren't

Each of these cost real debugging time. Please don't "fix" them.

**`uvicorn.run("marketspike.main:app", ...)` — a string, not the object.**
`python -m marketspike.main` loads this module as `__main__`. When `api/ws.py` later does `from marketspike.main import STATE`, Python loads it *again* under its real name, producing two independent `STATE` dicts. Uvicorn would serve the `__main__` copy (where `startup()` ran) while the WebSocket handler reads the other, empty one — `KeyError: 'bus'` on every connection. Passing the import string makes uvicorn do that second import itself, so both resolve to the same module.

**`api/ws.py` imports `marketspike.main` *inside* the handler function.**
Module-level would be a circular import. The placement is deliberate.

**Binance subscribes to two streams.**
`bookTicker` carries no venue timestamp — verified against the live venue, its keys are `A B a b s u`. `depth@100ms` does carry `E`. The adapter subscribes to both and reads depth frames *solely* for their timestamp, discarding everything else. No order book is maintained. Transit latency is a property of the connection, not of an individual quote, so a 10 Hz timing sample on the same socket characterises the path.

**`LatencyAggregator.add()` clamps timestamps to a running maximum.**
Front-only eviction assumes non-decreasing timestamps. Clamping makes that true by construction, keeping eviction O(1) on a hot path where an O(n) rescan would inflate the `engine_us` figure being reported.

**`log₂(V)` clamps to zero below `V = 1`.**
Not a bug. The volatility signal is meant to contribute only when volatility exceeds its own trailing baseline. Live `V` on BTCUSDT ranges roughly 0.23–1.28 in ordinary conditions.

**`p95 = max(p95_raw, p50_raw)` in `risk/slippage.py`.**
Quantile crossing — a known artefact of fitting quantiles independently. It occurred in 3% of training samples. Without the repair, the *conservative* sizing path could recommend a larger position than the typical one.

**Fallback slippage priors are split by asset class.**
BTCUSDT's spread is ~0.00156 bps against EURUSD's ~1.2 bps — roughly 770× tighter in relative terms. A single set of coefficients drove crypto predictions negative, clamping to zero and making `overexposure_pct` read 0.

---

## 5. Concurrency, and the bug it caused in production

**FastAPI runs `def` routes in a worker threadpool; `async def` routes run on the event loop.** Every route here is `def`, so every route runs on a *different thread* to the engine.

That is deliberate — `/size` performs a synchronous SQLite write, and running request handling on the event loop would make it contend with tick processing.

But it means **any route reading an engine collection races with the engine mutating it.** This shipped, and produced consistent HTTP 500s in production:

```
RuntimeError: deque mutated during iteration
```

It never appeared locally. The deployed instance is in Frankfurt, close to Binance, and sees ~127 ticks/sec against ~16 locally — 8× the race window. Locally it failed 2 requests in 400; hosted, it failed every time.

Guarded collections, each with a `threading.Lock`:

- `LatencyAggregator._samples` — `add()` and `percentiles()`
- `SymbolEngine._price_history` — `_record_price()` and `abs_return_5s`

`SpreadTracker._samples` and `Bus._delivery` are deliberately *unguarded*: no route reaches them (`/regime` reads only the cached scalar `spread_z`).

**If you add a route that touches a new engine collection, it needs a lock.** Nothing will warn you. The regression test in `tests/` needs 50,000 iterations across 2 writers and 4 readers to detect this class of bug reliably — a casual test will pass on broken code.

---

## 6. Common changes

**Add a venue.** Implement the `FeedAdapter` protocol in `feeds/base.py`: `symbol`, `venue`, `connected`, `async stream()`, `async seed_baseline()`. `stream()` must reconnect internally with backoff and re-raise `asyncio.CancelledError` before any broad `except`, or shutdown hangs. Normalise to `Tick`; the engine never learns which transport it came from. Register in `main.py::build_adapters`.

**Add an instrument.** Append to `risk/instruments.json`. Keys must match `InstrumentSpec` field names exactly — it's constructed with `**fields`, so a typo is a `TypeError` at import, taking down the whole app. Pip value is *derived* (`pip_size × contract_size × fx`), never stored.

**Add a WebSocket frame type.** Add it to `CHANNEL_FOR_TYPE` in `api/ws.py`. Unmapped types are delivered unconditionally, bypassing subscription filtering — that's intentional forward-compatibility, but it means a new frame silently becomes undisableable if you forget.

**Add a model feature.** `FEATURE_ORDER` in `risk/slippage.py` is part of the persisted format — coefficients are positional, so reordering silently invalidates every trained model. Append only, and retrain. The serving path (`api/rest.py::_features`) and the training path (`ml/features.py`) must compute it identically or you introduce train/serve skew.

**Change a tuned constant.** FSM thresholds, dwell times, EWMA horizons and scoring weights are mutation-tested: zeroing every dwell fails 6 of 11 regime tests; mis-normalising volatility fails 3. If you change one and nothing fails, be suspicious of the test rather than pleased.

---

## 7. Testing philosophy

241+ tests, concentrated where being wrong is both plausible and invisible.

Several exist because an earlier version of the suite **passed while the thing it tested was broken**:

- A Binance fixture fabricated an `E` field the venue never sends. 49 tests passed with the entire data path dead.
- A contract test collected its cases at import time, so it would have gone green with every example file deleted.
- An un-standardisation test re-derived the formula inline instead of calling the function, asserting an identity that held by construction.

**The lesson: a test that cannot fail is worse than no test**, because it advertises coverage that doesn't exist.

Where a failure mode isn't obvious, tests are mutation-checked: break the code, confirm the test fails, restore. If you add a test for something subtle, do the same — and say so in the commit message.

```bash
python -m pytest -q                        # all
python -m pytest tests/test_regime.py -v   # one module
```

---

## 8. Where the numbers came from

Claims in the README are measured, not projected. If you change the relevant code, re-measure rather than editing the prose:

| Claim | Source |
|---|---|
| p50 cost = half the spread | 23,292 recorded ticks, Δ=60 ms, both directions |
| p95 = 265× the median | same |
| latency p50 1,034 µs / p95 72,238 µs | live WebSocket session |
| model +5.2% at p95, −2.2% at p50 | 240,294 training samples, time-ordered split |
| `bookTicker` has no `E` | direct probe of the live venue |
| ~127 ticks/s hosted vs ~16 local | `recorder_written_total` over `uptime_s` |

The p50 result being *worse* than baseline is expected and documented — the p50 target **is** the half-spread, so the baseline is provably optimal there. Don't "fix" it.
