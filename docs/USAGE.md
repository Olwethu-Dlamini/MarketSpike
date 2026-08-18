# Using the MarketSpike backend

A practical guide to running the service and calling every endpoint. **Every response shown here was captured from a live running instance** — none of it is invented.

---

## 1. Install and run

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .                                        # makes `marketspike` importable anywhere

MS_SYMBOLS=BTCUSDT python -m marketspike.main
```

Listens on `http://localhost:8000`. BTCUSDT needs **no API key**.

> Always start it with `python -m marketspike.main`. The entrypoint passes uvicorn the import string `"marketspike.main:app"` deliberately — launching the app object directly causes Python to load the module twice under two names, giving the WebSocket handler a second, never-initialised state dict.

**Adding EURUSD** requires a free OANDA *practice* account:

```bash
export MS_OANDA_TOKEN=...
export MS_OANDA_ACCOUNT_ID=...
MS_SYMBOLS=BTCUSDT,EURUSD python -m marketspike.main
```

Without credentials, EURUSD is logged and skipped; BTCUSDT is unaffected. Note that forex is closed Fri 21:00 → Sun 21:00 UTC, when EURUSD reports `MARKET_CLOSED` — a normal state, not an error.

### Is it ready?

Two different things, easy to confuse:

- **`warmup_complete`** flips `true` within a couple of seconds. It means both volatility horizons hold an estimate — the slow one is seeded from 24 h of klines at boot, the fast one after its first sample.
- **Convergence** takes longer. The fast horizon is a 30-second EWMA, so allow roughly **150 seconds** before `v_ratio` and the regime score are stable enough to trust.

```bash
curl -s localhost:8000/api/v1/health
curl -s "localhost:8000/api/v1/regime?symbol=BTCUSDT"
```

---

## 2. Health — check this first

```bash
curl -s localhost:8000/api/v1/health
```

```json
{
  "v": 1, "status": "ok", "uptime_s": 130,
  "feeds": {
    "BTCUSDT": {
      "venue": "binance", "connected": true, "last_tick_age_ms": 29,
      "warmup_complete": true, "tradeable": true, "reason": null
    }
  },
  "counters": {
    "recorder_dropped_total": 0, "recorder_written_total": 7338,
    "recorder_write_failed_total": 0, "client_dropped_total": 0,
    "feed_dropped_total": 0
  },
  "model": {"BTCUSDT": "trained"},
  "mode": "live"
}
```

| Field | What to watch for |
|---|---|
| `last_tick_age_ms` | Should stay in the tens of ms for BTCUSDT. Growing means the feed stalled. |
| `recorder_dropped_total` | Non-zero means the queue overflowed and training rows were lost. |
| `recorder_write_failed_total` | Non-zero means SQLite writes are failing — check disk. |
| `model` | `trained` or `fallback_coefficients`, per symbol. |
| `mode` | `live` or `replay`. |

---

## 3. Sizing a position — the main endpoint

```bash
curl -s -X POST localhost:8000/api/v1/size \
  -H 'Content-Type: application/json' \
  -d '{
    "symbol": "BTCUSDT",
    "account_balance_minor": 1000000,
    "risk_pct": 1.0,
    "stop_distance_price": 250.0,
    "free_margin_minor": 1000000
  }'
```

**Money is always integer minor units.** `1000000` is $10,000.00. This avoids float rounding on ledger quantities; market *prices* stay float because they are measurements, not balances.

```json
{
  "naive_lot_size": 0.4,
  "recommended_lot_size": 0.3997,
  "overexposure_pct": 0.08,
  "slippage_p50_pips": 0.0092,
  "slippage_p95_pips": 0.1449,
  "stop_distance_pips": 250.0,
  "effective_adverse_pips": 250.1449,
  "actual_risk_amount_minor": 9998,
  "actual_risk_pct": 0.9998290373119006,
  "required_margin_minor": 257231,
  "capped_by": null,
  "fx_assumed": false,
  "stale_quote": false,
  "model_source": "trained",
  "model_version": "btcusdt-2026-08-18T10:27Z",
  "regime_at_calc": "NORMAL",
  "event_context": "CLEAR",
  "latency_used_ms": 0.806,
  "latency_source": "measured"
}
```

### Reading the response

- **`naive_lot_size`** — what a conventional calculator returns: stop distance only.
- **`recommended_lot_size`** — stop distance **plus** predicted slippage, margin-checked, rounded **down** to the lot step. Never rounded up: rounding a risk-limited quantity up breaches the budget you asked for.
- **`overexposure_pct`** — `(naive − recommended) / recommended × 100`. The headline number. Small in calm markets by design; it widens during a volatility event.
- **`actual_risk_amount_minor`** — real risk **at the rounded size**, not the requested figure. Here $99.98 against a requested $100.00; the shortfall is the round-down being honest.
- **`capped_by`** — `"margin"` if free margin limited the size, else `null`.
- **`model_source`** — `trained` or `fallback_coefficients`. Never presented as fitted when it is not.
- **`latency_source`** — `measured` when derived from live pipeline timing.

### Optional request fields

| Field | Default | Effect |
|---|---|---|
| `account_ccy` | `"USD"` | Sets `fx_assumed: true` when conversion is unavailable |
| `direction` | `"buy"` | `"buy"` or `"sell"` |
| `quantile` | `"p95"` | `"p50"` for typical cost, `"p95"` for conservative sizing |
| `assumed_latency_ms` | `null` | Override measured latency — this is the what-if lever |

`assumed_latency_ms` lets you ask *"what if my broker were 200 ms slower?"* and see the size shrink.

### Errors

Validation failures return **RFC 7807 problem details**:

```json
{"detail": {
  "type": "/errors/unknown-symbol", "title": "Unknown symbol",
  "status": 404, "detail": "NOPE is not an active symbol",
  "instance": "/api/v1/regime"
}}
```

`risk_pct` outside `(0, 100]` or a non-positive stop returns 422. `risk_pct > 5` succeeds but adds `HIGH_RISK_PCT` to `warnings` — it warns, it does not block.

---

## 4. What-if slippage curves

```bash
curl -s -X POST localhost:8000/api/v1/slippage/predict \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","spread_bps":0.0016,"v_ratio":1.0,"latency_ms":60}'
```

```json
{
  "v": 1, "symbol": "BTCUSDT",
  "p50_bps": 0.001571196356210536,
  "p95_bps": 0.025154082857391083,
  "model_source": "trained",
  "model_version": "btcusdt-2026-08-18T10:27Z",
  "inputs_echo": {"symbol":"BTCUSDT","spread_bps":0.0016,"v_ratio":1.0,"latency_ms":60}
}
```

Any of the nine features can be supplied; omitted ones default sensibly. Sweep one to draw a curve — raising `latency_ms` from 60 to 500 moves p95 from 0.02515 to 0.03349 bps.

`p95 >= p50` is **guaranteed** by a quantile-crossing repair at inference, so a conservative estimate can never come back below the typical one.

---

## 5. Market state

```bash
curl -s "localhost:8000/api/v1/regime?symbol=BTCUSDT"
```

```json
{
  "symbol": "BTCUSDT", "regime": "NORMAL", "since_ns": null,
  "score": 0.2952176197065112,
  "v_ratio": 0.5170384963133665,
  "spread_z": 1.4760880985325562,
  "quote_rate_hz": 59.67916935834643,
  "event_context": "CLEAR",
  "warmup_complete": true, "tradeable": true,
  "abs_return_5s": 3.061049128989501e-05
}
```

`regime` is `NORMAL` / `ELEVATED` / `SPIKE` / `MARKET_CLOSED`. `event_context` is `CLEAR` / `PRE_EVENT` / `EVENT_WINDOW` and is **independent** of regime — regime is price-derived and backward-looking, so only the calendar can tell you a print is thirty minutes away. `ELEVATED + PRE_EVENT` is the state worth acting on.

`v_ratio` below 1.0 means current volatility is under its own trailing baseline, and clamps the volatility term to zero by design.

```bash
curl -s "localhost:8000/api/v1/latency/summary?symbol=BTCUSDT"
```

Returns p50/p95/p99 per hop. Percentiles, never means — a 1 ms median with a 72 ms p95 is a different environment from a 1 ms median with a 2 ms p95, and a mean cannot distinguish them.

---

## 6. Reference data

```bash
curl -s localhost:8000/api/v1/instruments              # build symbol pickers from this
curl -s "localhost:8000/api/v1/calendar/upcoming?hours=720"
curl -s localhost:8000/api/v1/model/card
```

Calendar events carry `confidence` — `confirmed` or `estimated` — so a guessed date is never presented like a sourced one.

`model/card` reports version, provenance, pinball loss against baseline, empirical coverage, and the per-regime breakdown.

---

## 7. WebSocket stream

Connect to `ws://localhost:8000/ws/v1/stream`. Every frame carries `v`, `type`, `seq`, `server_ts_ns`.

```python
import asyncio, json, time, websockets

async def main():
    async with websockets.connect("ws://localhost:8000/ws/v1/stream") as ws:
        hello = json.loads(await ws.recv())
        print("connected:", hello["feeds"], "mode:", hello["mode"])

        await ws.send(json.dumps({
            "type": "subscribe",
            "symbols": ["BTCUSDT"],
            "channels": ["tick", "latency", "regime"],
        }))

        # Optional: clock sync, so the server can measure delivery latency
        c0 = time.time_ns()
        await ws.send(json.dumps({"type": "clock_sync", "client_send_ns": c0}))

        while True:
            frame = json.loads(await ws.recv())
            if frame["type"] == "clock_sync_reply":
                await ws.send(json.dumps({
                    "type": "ack",
                    "client_send_ns": c0,
                    "client_recv_ns": time.time_ns(),
                }))
            elif frame["type"] == "tick":
                print(frame["symbol"], frame["mid"], frame["source"])

asyncio.run(main())
```

### Real frames

```json
{"v":1,"seq":2011,"server_ts_ns":1787053750485872086,"type":"hello",
 "session_id":"262b8540","server_version":"1.0.0","warmup_complete":true,
 "feeds":{"BTCUSDT":"binance"},"mode":"live"}

{"v":1,"seq":2013,"server_ts_ns":1787053750674445676,"type":"tick",
 "symbol":"BTCUSDT","bid":64358.09,"ask":64358.1,"mid":64358.095,
 "spread_bps":0.001553806091065509,"spread_pips":0.010000000002037268,
 "quote_rate_hz":47.44720910248157,"book_imbalance":0.6549232608124551,
 "tradeable":true,"source":"measured"}

{"v":1,"seq":2014,"server_ts_ns":1787053750679040839,"type":"latency",
 "symbol":"BTCUSDT","excess_transit_us":546,"engine_us":179,"delivery_us":605,
 "p50_us":1154,"p95_us":3417,"p99_us":87441,
 "source":"estimated","baseline_includes_clock_offset":true}
```

### Client rules

1. **Ignore unknown fields.** Adding a field is never a breaking change within `v: 1`.
2. **Badge anything where `source` is not `"measured"`.** `estimated` means derived (skew-corrected transit, delivery from the handshake); `simulated` means replay. This is contractual — nothing synthetic may be shown as measured.
3. **`regime_change` fires only on a transition**, never per tick. Ticks arrive at up to 20/s; regimes change a handful of times an hour.
4. **The server sets the tick cadence.** Frames are capped at `MS_WS_MAX_HZ` (default 20). Recording still happens at full rate.
5. **Errors do not close the socket.** A malformed message gets an `error` frame; the connection stays open.

Channels: `tick`, `regime`, `latency`, `event`, `market`, `replay`. Omitting one suppresses those frames.

Full schemas and literal examples: [`docs/api/README.md`](api/README.md) and [`docs/api/examples/`](api/examples/).

---

## 8. Recording and training

The service records every tick it sees. Training data is the one thing you cannot generate faster by working harder, so start early.

```bash
# Keep the database outside the repo so `git clean` can't destroy it
export MS_DB_PATH=$HOME/marketspike-live.db
MS_SYMBOLS=BTCUSDT python -m marketspike.main
```

```bash
python -m marketspike.ml.train --db $MS_DB_PATH --symbols BTCUSDT --out model.json
```

Prints pinball loss against the baseline, empirical coverage, a per-regime breakdown, and the quantile-crossing fraction. Restart the service to pick the model up; `/model/card` will report `source: "trained"`.

Fitting uses gradient descent on pinball loss — 343k samples in about 49 seconds. **If the model does not beat the baseline, keep the fallback.** The card declares which is in use either way.

---

## 9. Replay

```bash
python scripts/capture_scenario.py from-db --db $MS_DB_PATH --symbol BTCUSDT \
  --start-ns 0 --end-ns 9223372036854775807 --out scenarios/btc_live.ndjson

curl -s localhost:8000/api/v1/scenarios

curl -s -X POST localhost:8000/api/v1/replay/start \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"btc_live","symbol":"BTCUSDT","speed":10.0}'

curl -s -X POST localhost:8000/api/v1/replay/stop
```

Replay drives the **same engine code path** as live data — it is a different adapter, not a branch in the engine. Every replayed frame carries `source: "simulated"` and `/health` reports `mode: "replay"`.

---

## 10. Configuration

All optional except OANDA credentials. See `.env.example`.

| Variable | Default | Purpose |
|---|---|---|
| `MS_SYMBOLS` | `BTCUSDT,EURUSD` | active symbols |
| `MS_OANDA_TOKEN` / `MS_OANDA_ACCOUNT_ID` | — | required for EURUSD |
| `MS_DB_PATH` | `./marketspike.db` | tick database |
| `MS_MODEL_PATH` | `./model.json` | fitted coefficients |
| `MS_TAU_FAST_S` / `MS_TAU_SLOW_S` | `30` / `1800` | volatility horizons |
| `MS_VOL_SAMPLE_INTERVAL_S` | `1.0` | volatility sampling cadence |
| `MS_SKEW_WINDOW_S` | `60` | transit-floor window |
| `MS_WS_MAX_HZ` | `20` | per-symbol frame cap |
| `MS_MAX_TICK_AGE_HOURS` | `0` (off) | tick retention |

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `KeyError: 'bus'` on WebSocket connect | App launched so the module loaded twice | Use `python -m marketspike.main` |
| No ticks, `connected: false` | Venue unreachable | `curl -sI https://api.binance.com/api/v3/ping`. The supervisor retries with backoff. |
| EURUSD absent from `/health` | Credentials unset | Check the startup log for the OANDA error line |
| EURUSD `MARKET_CLOSED` | Weekend or out of session | Normal state, not an error |
| `v_ratio` swinging wildly | Fast EWMA not converged | Wait ~150 s (5× the 30 s horizon) |
| `overexposure_pct` near 0 | Calm regime, wide stop | Correct. Use a tighter stop or start a replay spike |
| `model_source: fallback_coefficients` | No model loaded | Train one, or accept the priors — they are declared |
| `recorder_dropped_total` climbing | Queue overflow | Rows are dropped deliberately rather than delaying the data path |

Running the tests:

```bash
python -m pytest -q          # 241 tests
```
