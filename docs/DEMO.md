# MarketSpike — Demo Guide

Everything needed to run the demo, plus the pre-flight checks that stop it failing on stage.

---

## Pre-flight checklist

Work down this list **before** the event, not on the morning.

### T-minus days

- [ ] **Register a free OANDA practice account** and export `MS_OANDA_TOKEN` / `MS_OANDA_ACCOUNT_ID`. This is the only external dependency with a signup delay. Without it EURUSD is skipped — the demo still runs on BTCUSDT, but you lose the forex half of the pitch.
- [ ] **Verify the calendar dates** in `marketspike/calendar/static_events.json` against [bls.gov](https://www.bls.gov/schedule/) and the [Fed's FOMC calendar](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm). One PPI entry is marked `confidence: "estimated"`, and the Sep–Nov 2026 BLS schedule may have shifted after a shutdown. **A wrong date fails silently** — the event context simply never fires, and no test will catch it.
- [ ] **Confirm Binance reachability from the venue network**, not just from home. `curl -sI https://api.binance.com/api/v3/ping` is enough. If it is blocked, the Kraken adapter moves from stretch to necessary.
- [ ] **Rehearse the full script three times.** The steps below take five minutes; the failure modes reveal themselves on the second run, not the first.

### T-minus hours

- [ ] **Start the recorder and leave it running.** This is the single highest-leverage thing you can do:
      ```bash
      MS_SYMBOLS=BTCUSDT,EURUSD python -m marketspike.main
      ```
      Training data is the only deliverable that cannot be compressed by working harder. Run it through a scheduled release if at all possible — see *The SPIKE problem* below.
- [ ] **Write the database outside the repo** so a `git clean` cannot destroy it:
      `export MS_DB_PATH=$HOME/marketspike-live.db`
- [ ] **Capture a replay scenario** as soon as you have data, so the demo has a deterministic fallback:
      ```bash
      python scripts/capture_scenario.py from-db --db $MS_DB_PATH --symbol BTCUSDT \
        --start-ns 0 --end-ns 9223372036854775807 --out scenarios/btc_live.ndjson
      ```
- [ ] **Train a model** and confirm `/model/card` reports `source: "trained"`. If it does not beat the baseline, ship the fallback — the card declares which is in use either way.

### T-minus minutes

- [ ] Service started **at least 150 seconds** before you present. `warmup_complete` must be `true`:
      `curl -s localhost:8000/api/v1/regime?symbol=BTCUSDT`
- [ ] `/health` shows every configured feed `connected: true` and `recorder_dropped_total: 0`.
- [ ] `/scenarios` lists your replay file.
- [ ] Frontend connected and receiving `tick` frames.

---

## The SPIKE problem — read this first

Across the entire build session the market stayed calm: **178,718 ticks recorded, zero SPIKE regimes.** Only NORMAL and ELEVATED occurred.

This matters because the comparison that makes the thesis land — the baseline being adequate in calm markets and badly wrong during a print — needs a genuine volatility event in the data. Without one, the per-regime breakdown on `/model/card` has a single populated row and `overexposure_pct` stays under 1%.

**Three ways to solve it, in order of preference:**

1. **Record through a real release.** Check the calendar, start the recorder well before a scheduled NFP or CPI print, and let it run. This gives you genuine SPIKE data and the strongest possible version of the demo.
2. **Capture historical OANDA data around a past release.** Real spreads from a real event, replayable on demand:
   ```bash
   python scripts/capture_scenario.py from-oanda --symbol EURUSD \
     --from 2026-07-11T12:00:00Z --to 2026-07-11T13:00:00Z \
     --out scenarios/cpi_2026_07_11.ndjson
   ```
3. **Fall back to the synthetic spike.** The replay integration test constructs one; it proves the mechanism works but it is not real market data, and you should say so if asked.

Whichever you use, **replay frames carry `source: "simulated"`** and the UI badges them. Do not present replayed data as live.

---

## The five-minute script

### 1. Open on live data — establish credibility

Show BTCUSDT (and EURUSD if the market is open) streaming, with the latency waterfall breathing.

> *"These are real ticks from Binance, and this latency is genuinely measured — note the badge. Every number in this application declares its own provenance: `measured`, `estimated`, or `simulated`. Nothing synthetic is ever shown as measured."*

Point at the latency percentiles. Live values from a verified session:

```
engine        137 µs      exact — one machine, one clock
delivery      580 µs      NTP handshake with the browser
total p50   1,034 µs
total p95  72,238 µs      ← 70× the median
```

> *"A one-millisecond median with a seventy-two millisecond p95 is a completely different trading environment from a one-millisecond median with a two-millisecond p95 — and a mean cannot tell them apart. That's why nothing here reports an average."*

### 2. Size a position in a calm market — set the trap

Call `/size` with the regime showing NORMAL.

```
naive_lot_size        0.4
recommended_lot_size  0.3997
overexposure_pct      0.08
```

> *"Right now the gap is nearly nothing. This is exactly why nobody notices the problem exists — in a quiet market the conventional formula is basically right."*

### 3. Trigger the event — show the regime change

Start the replay of your captured spike, or let a live release hit.

```bash
curl -s -X POST localhost:8000/api/v1/replay/start \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"cpi_2026_07_11","symbol":"EURUSD","speed":10.0}'
```

Watch NORMAL → ELEVATED → SPIKE, spread widening, latency p99 blowing out.

> *"The regime state machine has hysteresis and deliberately asymmetric dwell times — three seconds to escalate, fifteen to stand down. Missing a spike costs a trader money; a warning that lingers ten seconds too long costs nothing."*

If asked how it differs from a naive detector: an earlier draft of this project assigned regime with `random.choice()` on every tick and produced three transitions per second. The test suite now asserts that 400 updates oscillating around a threshold produce **zero** transitions, and zeroing every dwell time fails six of eleven tests.

### 4. Recalculate — the payoff

Call `/size` again inside the spike. The gap widens sharply.

> *"Every retail calculator just handed this trader materially more risk than they asked for — at the exact moment it's most dangerous. That's the whole product in one number."*

### 5. The evidence — why believe the slippage figure

Open `/model/card`.

> *"The baseline we're beating isn't a straw man. It's `cost = the current half-spread` — literally what every retail calculator assumes when it ignores slippage."*

Then the measured validation, from 23,292 real ticks:

```
BTCUSDT spread              p50 = 0.00156 bps
realised cost at Δ=60ms     p50 = 0.00078 bps   ← exactly half the spread
                            p95 = 0.20642 bps   ← 265× the median
```

> *"The model decomposes fill cost as half-spread plus adverse drift, and assumes a trader with no directional edge over sixty milliseconds. That predicts the median cost should land on the half-spread — and measured against real data, it lands there to the digit. The 265× gap between typical and tail cost is the entire reason you should size off the 95th percentile."*

And the honest framing of the metrics:

```
p50   −2.2%  vs baseline     coverage 0.181
p95   +5.2%  vs baseline     coverage 0.048  (nominal 0.05)
```

> *"We only beat the baseline at p95, and we very slightly lose to it at p50. That's not a weakness — the p50 target IS the half-spread, so the baseline is provably optimal there. The claim isn't 'retail calculators are bad at arithmetic'. It's that they're right about the average case and blind to the case that hurts you."*

### 6. The what-if — latency as a risk parameter

```bash
curl -s -X POST localhost:8000/api/v1/slippage/predict \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTCUSDT","spread_bps":0.0016,"v_ratio":1.0,"latency_ms":500}'
```

Compare against `latency_ms: 60`. Verified live: p95 rises from 0.02515 to 0.03349 bps.

> *"Measured latency is an input feature to the cost model, not a metric displayed beside it. Your broker's speed is a risk parameter — and nobody shows you that."*

---

## Questions you should expect

**"How do you know your latency is real?"**
Three hops, each labelled honestly. `receive → engine` is exact — one machine, one monotonic clock. `engine → browser` uses a standard NTP four-timestamp handshake with a minimum-delay filter. `venue → receive` is the interesting one: absolute one-way transit against a venue clock is *unmeasurable*, because the observed difference is skew plus transit and one sample cannot separate them. So we never claim it. We track a rolling minimum — the least-queued sample, approximating the constant offset — and report only the **excess above that floor**, which cancels skew and leaves queueing. Against live Binance the raw figure sits at ~179 ms with a ~2.7 ms spread: the 179 ms is an uninteresting constant, the 2.7 ms is the signal.

**"Binance doesn't publish an event timestamp on bookTicker."**
Correct, and we verified that empirically — its keys are `A B a b s u`. `depth@100ms` does carry `E`, so we subscribe to both on one socket and read depth frames *solely* for their timestamp. That is legitimate rather than a workaround: transit latency is a property of the connection, not of an individual quote, and both streams traverse the same TCP connection to the same endpoint.

**"Why crypto if the pitch is about NFP?"**
Because hackathons are judged at weekends and **the forex market is closed at weekends**. An EURUSD-only demo on a Sunday shows a flat line and zero ticks. BTCUSDT is the availability guarantee — 24/7, no API key, no jurisdiction problem. EURUSD carries the argument; BTCUSDT guarantees there is something to show.

**"Is the model overfitted?"**
Nine features, linear, with a time-ordered 70/30 split — never random, because shuffling a time series leaks the future into the past. Leakage is prevented *structurally*: the feature builder takes the pre-decision window and the post-Δ target as separate arguments, so a caller cannot reach forward, and it raises `LeakageError` if the target is not strictly after the decision.

**"Why not a neural network?"**
Linear keeps the coefficients interpretable — we can tell you the cost-per-doubling-of-latency and point at the number. It also means **inference is a dot product**: the model ships as a 3 KB JSON and runs inline with no ML runtime, so there is nothing to install to serve it.

**"What's not finished?"**
See *Known limitations* in the README. Briefly: FX conversion is identity (correct for the two shipping symbols, flagged via `fx_assumed` otherwise), EURUSD is implemented but not live-verified for want of credentials, there is no auth, and the recorded data contains no SPIKE regime.

---

## If something breaks on stage

| Symptom | Cause | Response |
|---|---|---|
| `warmup_complete: false` | Started under ~150 s ago | The fast horizon is a 30 s EWMA. Talk through the architecture while it warms. |
| `v_ratio` near zero, regime stuck NORMAL | Genuinely calm market | Expected. `log₂(V)` clamps below V=1 by design — the signal fires only above baseline. Switch to replay. |
| EURUSD shows `MARKET_CLOSED` | Weekend or out of session | Normal operating state, not an error. Present BTCUSDT. |
| `overexposure_pct` near 0 | Calm regime, wide stop | Correct behaviour. Use a tighter stop, or trigger the replay spike. |
| `model_source: fallback_coefficients` | No trained model loaded | Honest and fine — say the priors are hand-set and the card declares it. |
| No `tick` frames on the socket | Feed disconnected | `curl /health`. The supervisor reconnects with backoff; drop counters show whether anything was lost. |

---

## Reference commands

```bash
# Run
MS_SYMBOLS=BTCUSDT python -m marketspike.main

# Tests
python -m pytest -q                                    # 241 tests

# Train
python -m marketspike.ml.train --db marketspike.db --symbols BTCUSDT --out model.json

# Capture a scenario
python scripts/capture_scenario.py from-db --db marketspike.db --symbol BTCUSDT \
  --start-ns 0 --end-ns 9223372036854775807 --out scenarios/btc_live.ndjson

# Replay
curl -s -X POST localhost:8000/api/v1/replay/start -H 'Content-Type: application/json' \
  -d '{"scenario":"btc_live","symbol":"BTCUSDT","speed":10.0}'
curl -s -X POST localhost:8000/api/v1/replay/stop

# Health
curl -s localhost:8000/api/v1/health
curl -s "localhost:8000/api/v1/regime?symbol=BTCUSDT"
curl -s localhost:8000/api/v1/model/card
```
