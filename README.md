# MarketSpike

**Slippage-aware position sizing for high-impact market events.**

Live market data → measured pipeline latency → volatility regime detection → a position size that accounts for what a conventional calculator ignores.

---

## The problem

Every retail position-size calculator computes:

```
lots = (balance × risk%) / (stop_loss_pips × pip_value)
```

That formula assumes the trader is filled at the price they saw, at the moment they saw it. Both assumptions fail hardest during high-impact economic releases — NFP, CPI, FOMC — which is precisely when position sizing matters most. Spreads widen, quote latency spikes, and the fill lands at a materially worse price than the one the decision was based on.

**MarketSpike measures the two costs that formula omits** — execution latency and expected slippage — and returns a size that accounts for both. The gap between the two answers is reported as `overexposure_pct`: how much more risk the conventional calculator would have handed you.

### Why latency and slippage belong in the same tool

They are not two features stapled together. Measured pipeline latency **Δ is an input feature to the slippage model**:

> A trader decides at time `t`, looking at price `mid_t`. Their order reaches the venue at `t + Δ`. The cost they pay is a function of what the market did during Δ.

Latency is not a vanity metric displayed beside the calculator. It is a term *in* the calculator.

---

## Status

| | |
|---|---|
| **Tests** | 241 passing |
| **Feeds** | BTCUSDT (Binance, live-verified) · EURUSD (OANDA v20, implemented — needs credentials) |
| **Endpoints** | 11 REST + 1 WebSocket, all live-verified |
| **Model** | Trained on 240,294 samples from 178,718 recorded ticks |
| **Python** | 3.8+ compatible (developed on 3.11) |

---

## Measured results

Every number below came from the shipping code path against real market data, not from a model or a script that reimplements it.

### The cost decomposition is empirically correct

The design decomposes fill cost as *half-spread + adverse drift*, and assumes a trader with no directional edge over a ~60 ms horizon. That predicts the **median** cost should land on the half-spread while the **95th percentile** captures the adverse tail. Measured over 23,292 recorded BTCUSDT ticks at Δ = 60 ms, both trade directions:

```
BTCUSDT spread                p50 = 0.00156 bps
realised cost at Δ = 60 ms    p50 = 0.00078 bps   ← exactly half the spread
                              p95 = 0.20642 bps   ← 265× the median
                              p99 = 0.82701 bps
```

The p50 landing on the half-spread to the digit is the raw data agreeing with the model's structure. **The 265× gap between typical and tail cost is the entire argument for sizing off p95 rather than the median.**

### Latency is measured, and the tail is what matters

From a live WebSocket session:

```
engine_us            137        (exact — one machine, one clock)
excess_transit_us      0        (calm market; skew-cancelled, see below)
delivery_us          580        (NTP-style handshake, half least-delayed RTT)
total p50          1,034 µs
total p95         72,238 µs     ← 70× the median
```

A 1 ms median with a 72 ms p95 is a completely different trading environment from a 1 ms median with a 2 ms p95, and **a mean cannot tell them apart.** That is why every latency figure here is a percentile.

### The model beats the baseline only where it should

The baseline is `cost = current half-spread` — precisely what every retail calculator implicitly assumes ("you pay the spread, slippage is zero"). Trained on 240,294 samples with a time-ordered 70/30 split:

| Quantile | Pinball (model) | Pinball (baseline) | Change | Coverage | Nominal |
|---|---|---|---|---|---|
| p50 | 0.0128024 | 0.0125212 | **−2.2%** | 0.181 | 0.50 |
| p95 | 0.0118221 | 0.0124754 | **+5.2%** | 0.048 | 0.05 |

**Read this carefully, because the p50 result is not a failure.** The p50 target *is* the half-spread, so the baseline is definitionally optimal at τ = 0.5 — the model can only tie it or lose slightly to it, and here it loses 2.2% to regularisation. The model earns its keep **only in the tail**, at p95.

That is a stronger claim than "retail calculators are wrong". It is: *they are provably right about typical cost and blind to the case that hurts you.*

p95 coverage of **0.048 against a nominal 0.05** means the 95th-percentile prediction is exceeded 4.8% of the time — it is doing what a 95th percentile claims to do.

The p50 coverage of 0.181 looks badly off but is an artefact of price discreteness: ~98.8% of consecutive BTCUSDT ticks show *zero* mid change over 60 ms, so the cost distribution has a large point mass at exactly the half-spread. `actual > predicted` is rare by construction.

**Quantile crossing:** 3.0% of training samples produce a raw `p95 < p50`. This is a known artefact of fitting quantiles independently, and it is repaired at inference (`p95 = max(p95_raw, p50_raw)`) rather than hidden.

---

## Quick start

Requires Python 3.8+ and network access. No API key is needed for BTCUSDT.

```bash
git clone https://github.com/Olwethu-Dlamini/MarketSpike.git
cd MarketSpike

python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .

MS_SYMBOLS=BTCUSDT python -m marketspike.main
```

The service listens on `http://localhost:8000`.

**Two different readiness notions, worth not confusing.** `warmup_complete` flips to `true` within a couple of seconds — it means both volatility horizons hold an estimate (the slow one is seeded from klines at boot, the fast one after its first sample). What takes longer is the 30-second fast EWMA *converging* to a stable value: allow roughly **150 seconds** before `v_ratio` and the regime score are trustworthy. Check both:

```bash
curl -s localhost:8000/api/v1/regime?symbol=BTCUSDT
```

Then ask for a size:

```bash
curl -s -X POST localhost:8000/api/v1/size \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","account_balance_minor":1000000,"risk_pct":1.0,
       "stop_distance_price":250.0,"free_margin_minor":1000000}'
```

### Adding EURUSD

EURUSD needs a free OANDA **practice** account (no funding required):

```bash
export MS_OANDA_TOKEN=...
export MS_OANDA_ACCOUNT_ID=...
MS_SYMBOLS=BTCUSDT,EURUSD python -m marketspike.main
```

Without credentials the symbol is logged and skipped; BTCUSDT is unaffected.

> **The forex market is closed at weekends** (Fri 21:00 → Sun 21:00 UTC). EURUSD will show a `MARKET_CLOSED` state and no ticks. That is why BTCUSDT exists in this project: it streams 24/7 and guarantees the service has something live to show.

---

## Architecture

One FastAPI process, one asyncio event loop. Feed adapters normalise venue messages into a common `Tick`; a per-symbol engine derives latency, volatility, spread and regime; frames fan out over a bus to the WebSocket while a separate recorder drains to SQLite on a thread executor.

```
   Binance WS            OANDA HTTP stream          Replay file
   BTCUSDT · 24/7        EURUSD · weekdays          recorded event
        │                        │                        │
        └────────────────────────┼────────────────────────┘
                                 ▼
                    FeedAdapter → Tick(venue_ts, recv_ts, bid, ask, …)
                                 │  bounded queue, drop-oldest
                                 ▼
        ┌──────────────────── SymbolEngine ────────────────────┐
        │  PipelineTimer    hop timing, skew-corrected         │
        │  VolatilityPair   dual-horizon time-weighted EWMA    │
        │  SpreadTracker    rolling median / MAD z-score       │
        │  RegimeFSM        hysteresis + asymmetric dwell      │
        │  EventClock       phase & seconds-to-release         │
        └───────┬──────────────────────────────────┬───────────┘
                ▼ Bus (async fan-out)              ▼ recorder queue (batched)
        WebSocket /ws/v1/stream            SQLite WAL
        REST      /api/v1/*                  │
                                             ▼ offline
                                   ml/train.py → model.json → risk/slippage.py
```

**The governing invariant: disk never touches the data path.** A latency product that stalls on `fsync` is measuring its own defect. The engine hands rows to a bounded queue and moves on; when the queue is full, rows are **dropped and counted** rather than applying backpressure. Losing training rows during a spike is recoverable; adding queueing delay during a spike corrupts the exact measurement the spike exists to test. Every drop counter is exposed on `/health`.

Module boundaries are enforced and verified: `engine/` never imports `api/` or `store/`; `risk/` imports neither `feeds/` nor `engine/`; `sqlite3` appears only in `store/` (plus the offline trainer CLI).

---

## The quantitative method

### Volatility — time-weighted, not tick-weighted

Quote updates arrive irregularly, and they arrive *faster* during volatility. A tick-count EWMA therefore double-counts spikes: the quantity being measured alters the sampling rate of the measurement. So decay on elapsed time and normalise by `Δt`, giving variance **per second**:

```
Δt   = t − t_prev                        [seconds]
r    = ln(mid_t / mid_prev)
λ    = exp(−Δt / τ)
σ²_t = λ·σ²_{t−1} + (1 − λ)·(r² / Δt)    [variance per second]
```

Two horizons — `τ_fast = 30 s`, `τ_slow = 30 min` — both in the same per-second units, giving a unitless ratio `V = σ_fast / σ_slow` that equals 1 when current volatility matches baseline.

**Cold start:** a 30-minute EWMA needs ~2.5 hours to mean anything. At boot the slow horizon is seeded from 24 h of 1-minute klines (one REST call), so the engine is warm on tick one.

Observed `V` on live BTCUSDT ranges roughly **0.23–1.28** under ordinary conditions. Values below 1.0 clamp the volatility term to zero **by design** — the signal contributes only when volatility exceeds its own trailing baseline.

### Spread — robust, because the outliers are the signal

Spread distributions are fat-tailed. Mean and standard deviation would let the outliers inflate the very scale used to detect them, muting detection exactly when it matters. Median and MAD are unaffected:

```
z = (spread_bps − median_60m) / (1.4826 · MAD_60m)
```

The 1.4826 makes MAD a consistent estimator of σ under normality, keeping `z` on the familiar scale.

### Regime — hysteresis with deliberately asymmetric dwell

```
score = 0.6 · clamp(log₂ V, 0, 4)  +  0.4 · clamp(z / 2, 0, 4)      →  0 … 4
```

Two independent signals, because they diverge: volatility can rise on thin genuine movement without spread widening, and spread can widen on liquidity withdrawal before price moves.

| Transition | Trigger | Min dwell |
|---|---|---|
| NORMAL → ELEVATED | score ≥ 1.5 | 3 s |
| ELEVATED → SPIKE | score ≥ 2.8 | 2 s |
| SPIKE → ELEVATED | score < 2.0 | 10 s |
| ELEVATED → NORMAL | score < 1.1 | 15 s |

Entry thresholds sit above exit thresholds, and **exit dwell is longer than entry dwell**. The asymmetry is deliberate: failing to warn a trader of a spike costs them money, while a regime that lingers ten seconds too long costs nothing.

`regime_change` frames report `trigger` — which signal actually dominated the score, not merely the transition direction.

### Latency — three hops, honestly labelled

`receive → engine done` is **exact**: one machine, one monotonic clock.

`venue → local receive` crosses a foreign clock. Absolute one-way transit is *unmeasurable* — the observed difference is skew plus transit and one sample cannot separate them. So the system never claims absolute transit. It tracks a rolling **minimum** of the raw difference (the least-queued sample ≈ the constant skew-plus-baseline) and reports only the **excess above that floor**, which cancels the skew term and leaves queueing and congestion — the part that actually spikes during a release. Against live Binance the raw value sits at ~179 ms with a ~2.7 ms spread: the 179 ms is the uninteresting constant, the 2.7 ms is the signal.

`engine → client` **can** be handshaked, so it uses the standard NTP four-timestamp exchange with a minimum-delay filter over the last 8 samples.

> **Binance `bookTicker` carries no venue timestamp.** Verified empirically: its keys are `A B a b s u`. `depth@100ms` does carry `E`. The adapter therefore subscribes to **both** on one socket, reading depth frames *solely* for their timestamp. This is legitimate rather than a workaround: transit latency is a property of the connection, not of an individual quote, and both streams traverse the same TCP connection.

### Position sizing

```
risk_budget   = balance × risk_pct / 100
adverse_price = stop_distance + slippage(τ)
value_per_pt  = contract_size × fx(quote_ccy → account_ccy)

lots = round_DOWN( risk_budget / (adverse_price × value_per_pt), lot_step )
```

**Pip value is derived, never stored** — `pip_size × contract_size × fx`. A hardcoded `10.0` is a USD-quoted-major assumption; for USDJPY it is $6.70 per pip per lot, and anyone sizing USDJPY off `10.0` is 49% over their stated risk before slippage enters the picture.

**Lot size always rounds down.** Rounding a risk-limited quantity up breaches the budget the user specified. `actual_risk_amount_minor` is then recomputed *at the rounded size* and returned, so the response shows real risk rather than requested risk.

### Slippage — linear quantile regression

The target is **implementation shortfall against arrival price**:

```
cost_bps = half_spread_bps(at t+Δ, scaled to decision mid) + direction · drift_bps
```

Each observation is emitted twice, once per direction, since over tens of milliseconds the assumption of no directional edge is well founded.

Nine features, all strictly at or before `t`: `log_v_ratio`, `spread_z`, `log_spread_bps`, **`log_latency_ms`**, `quote_rate_hz`, `book_imbalance`, `signed_secs_to_event`, `in_event_window`, `abs_return_5s`.

**Leakage is prevented structurally**, not by convention: `build_sample(history, target, …)` takes the two windows as separate arguments so a caller cannot reach forward, and raises `LeakageError` when `target.ts_ns <= decision.ts_ns`.

Linear was chosen deliberately: coefficients stay interpretable, training is fast, overfitting at nine features is structurally hard, and **inference is a dot product** — the model ships as a ~3 KB JSON and runs inline with no ML runtime, so there is nothing extra to install to serve it.

Fitting uses batch gradient descent on pinball loss with Polyak–Ruppert tail averaging. `sklearn.QuantileRegressor` was measured at ~O(n^1.7) — 5.9 s at 8k samples, ~3.5 min at 54k, extrapolating to **~6 hours at 1M rows** — which made the intended overnight-record-then-train workflow impossible. Gradient descent fits 343k samples in **49 seconds** and is better calibrated (coverage 0.048 vs 0.017 against nominal 0.05), at the cost of ~9% higher pinball loss than the LP solve.

---

## API

**How to run and call the backend, with real captured responses: [`docs/USAGE.md`](docs/USAGE.md).**

Full contract, message schemas and literal example payloads: [`docs/api/README.md`](docs/api/README.md) and [`docs/api/examples/`](docs/api/examples/).

### REST — `/api/v1`

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | liveness, warm-up, drop counters, per-feed status |
| GET | `/instruments` | instrument registry — build symbol pickers from this |
| GET | `/regime?symbol=` | state, score, `v_ratio`, `spread_z`, event context |
| GET | `/latency/summary?symbol=` | p50/p95/p99 per hop |
| POST | `/size` | slippage-aware position size |
| POST | `/slippage/predict` | p50/p95 for explicit what-if conditions |
| GET | `/calendar/upcoming?hours=` | upcoming releases, with date `confidence` |
| GET | `/model/card` | version, provenance, metrics vs baseline, per-regime |
| GET | `/scenarios` | available replay scenarios |
| POST | `/replay/start` · `/replay/stop` | demo control |

Errors are RFC 7807 problem details.

### WebSocket — `/ws/v1/stream`

Every frame carries `v`, `type`, `seq`, `server_ts_ns`. Frame types: `hello`, `tick`, `latency`, `regime_change`, `event_alert`, `market_state`, `replay_state`, `clock_sync_reply`, `error`. Subscribable channels: `tick`, `regime`, `latency`, `event`, `market`, `replay`.

### The honesty rule

**Every latency and tick value carries a `source` field** — `measured`, `estimated`, or `simulated`. Skew-corrected transit is `estimated` because it is relative to a rolling baseline. Replay frames are `simulated`. **Nothing synthetic is ever presented as measured**, and the frontend is contractually required to badge anything that is not `measured`.

The same principle applies elsewhere: `/size` reports `model_source` (`trained` vs `fallback_coefficients`), `fx_assumed`, and `stale_quote`; calendar events carry `confidence` (`confirmed` vs `estimated`).

---

## Configuration

Environment variables, all optional except OANDA credentials. See `.env.example`.

| Variable | Default | Purpose |
|---|---|---|
| `MS_SYMBOLS` | `BTCUSDT,EURUSD` | active symbols |
| `MS_OANDA_TOKEN` | — | required for EURUSD |
| `MS_OANDA_ACCOUNT_ID` | — | required for EURUSD |
| `MS_DB_PATH` | `./marketspike.db` | tick database |
| `MS_MODEL_PATH` | `./model.json` | fitted coefficients |
| `MS_TAU_FAST_S` | `30` | fast volatility horizon |
| `MS_TAU_SLOW_S` | `1800` | slow volatility horizon |
| `MS_VOL_SAMPLE_INTERVAL_S` | `1.0` | volatility sampling cadence |
| `MS_SKEW_WINDOW_S` | `60` | transit-floor window |
| `MS_WS_MAX_HZ` | `20` | per-symbol frame cap |
| `MS_MAX_TICK_AGE_HOURS` | `0` (off) | tick retention |

---

## Training a model

The service runs on hand-set fallback priors until a model is trained, and says so via `model_source: "fallback_coefficients"` in every response.

```bash
# 1. Record. Longer is better; the tail matters most.
MS_SYMBOLS=BTCUSDT python -m marketspike.main

# 2. Fit (seconds, not hours).
python -m marketspike.ml.train --db marketspike.db --symbols BTCUSDT --out model.json

# 3. Restart. /model/card now reports source: "trained".
```

Training reports pinball loss against the baseline, empirical coverage, a per-regime breakdown, and the quantile-crossing fraction. **If the model does not beat the baseline, ship the fallback** — `/model/card` will say which is in use either way.

---

## Replay

A hackathon weekend may contain no high-impact release at all, and forex may be closed throughout. Replay makes the demo deterministic.

```bash
# Capture from your own recording
python scripts/capture_scenario.py from-db --db marketspike.db --symbol BTCUSDT \
  --start-ns 0 --end-ns 9223372036854775807 --out scenarios/btc_live.ndjson

# Or from real OANDA history around a past release (needs credentials)
python scripts/capture_scenario.py from-oanda --symbol EURUSD \
  --from 2026-07-11T12:00:00Z --to 2026-07-11T13:00:00Z --out scenarios/cpi.ndjson

curl -s -X POST localhost:8000/api/v1/replay/start \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"btc_live","symbol":"BTCUSDT","speed":10.0}'
```

Replay emits the same `Tick` type through the same engine code path — demo mode is a different *adapter*, not a branch in the engine. That is what makes it trustworthy: identical logic runs, differing only in the `source` field.

---

## Testing

```bash
python -m pytest -q          # 241 tests
```

Coverage is concentrated where being wrong is both plausible and invisible:

- **Leakage guard** — asserts no feature timestamp exceeds its target timestamp.
- **Regime anti-flapping** — 400 updates oscillating around a threshold must produce zero transitions. Zeroing every dwell time fails 6 of 11 tests, so the mechanism is load-bearing rather than incidental.
- **Sliding-window minimum** — validated against an independent brute-force `min()`, plus explicit tests pinning the eviction boundary at exactly the window edge.
- **Volatility normalisation** — injecting the `τ/Δt` mis-normalisation (a factor-of-60 error) fails 3 tests.
- **Un-standardisation round-trip** — dropping either the mean term or the σ division fails the test.
- **Quantile ordering** — swept across the realistic input range for both asset classes.
- **Solver equivalence** — gradient descent within 2% of sklearn's LP solve on held-out data.
- **Replay integration** — a synthetic spike must drive NORMAL → SPIKE → NORMAL exactly once.

Several of these exist because an earlier version of the test suite passed while the thing it tested was broken. Where a test's failure mode was not obvious, it was mutation-checked: break the code, confirm the test fails, restore.

---

## Known limitations

Stated plainly rather than left to be discovered.

1. **FX conversion is identity.** Correct for the two shipping symbols (EURUSD and BTCUSDT against a USD account). USDJPY and XAUUSD exist in the registry but have no live feed; any request for them sets `fx_assumed: true`.
2. **Venue transit is relative, not absolute** — `excess_transit_us` is measured above a rolling baseline containing an unmeasurable clock offset. This is a property of the problem, not a shortcut.
3. **No SPIKE regime in the recorded data.** 178,718 ticks were captured across a calm session; only NORMAL and ELEVATED occurred. The per-regime model breakdown therefore has one populated row, and the comparison that best demonstrates the thesis needs a genuine volatility event in the recording.
4. **One calendar date is `estimated`** and the BLS Sep–Nov 2026 schedule may have shifted following a shutdown. A wrong date fails *silently* — the event context simply never fires. Verify against bls.gov before relying on it.
5. **EURUSD is implemented but not live-verified** — no OANDA credentials were available. The adapter is unit-tested offline against real payload shapes.
6. **No authentication.** Do not expose the service beyond localhost.
7. **`calendar_events` rows are re-inserted on every restart** without dedup. The table is currently write-only, so nothing reads the duplicates.

---

## Repository layout

```
marketspike/
├── feeds/        adapters: binance, oanda, replay + Tick / FeedAdapter
├── clock/        skew estimator (venue), NTP sync (client)
├── engine/       pipeline timing, volatility, spread, regime, bus, supervisor
├── calendar/     event clock + curated release schedule
├── risk/         instrument registry, slippage inference, position sizing
├── store/        SQLite schema, batched recorder
├── ml/           feature builder (leakage-guarded), trainer, evaluation
├── api/          frozen v1 schemas, REST routes, WebSocket
└── main.py       app assembly, supervised task startup

docs/
├── api/          frozen contract + literal example payloads
├── USAGE.md      how to run and call the backend
├── DEMO.md       demo script and pre-flight checklist
└── design/       design spec and implementation plan
```

Design rationale and the full quantitative derivation: [`docs/design/design-spec.md`](docs/design/design-spec.md).

---

## Licence

MIT.
