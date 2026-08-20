# MarketSpike — 3-minute video transcript

Spoken word count: **~490 → about 3:05** at a 155–165 wpm delivery (3:22 if you read slowly at
145). Bracketed lines are screen cues and are not spoken. If you need a hard 3:00, cut the
venue-to-receive sentence in *How it works* — it is the one passage the argument survives losing.

Record against the live service — <https://marketspike.onrender.com> — so nothing depends on a
local process staying up. Load a captured spike scenario before you hit record; the pre-flight
checklist is in `DEMO.md`.

---

## 0:00 — The problem

*[Instrument panel, BTCUSDT streaming, latency waterfall moving]*

> Every position-size calculator a retail trader has used computes the same thing: risk budget,
> divided by stop distance times pip value. That formula assumes you get filled at the price you
> saw, at the moment you saw it.
>
> Both fail hardest during a release — payrolls, CPI, FOMC — when sizing matters most. Spreads
> widen, quotes arrive late, and the fill lands worse than the price you decided on.

## 0:20 — What this is

> MarketSpike measures the two costs that formula leaves out — execution latency and expected
> slippage — and returns a size that accounts for both. The gap between the two answers is one
> number: **overexposure percent**. The extra risk the conventional calculator hands you.

## 0:35 — How it works

*[Cut to the architecture diagram]*

> Live ticks arrive from Binance. Each one gets a single engine pass: timestamp its arrival,
> update two volatility horizons, compute a robust spread z-score, blend those into one score,
> and run a regime state machine — normal, elevated, spike.
>
> Latency is measured in three hops, each labelled honestly. Receive-to-engine is exact — one
> machine, one clock. Engine-to-browser uses an NTP-style handshake. Venue-to-receive can't be
> measured at all — what you see is clock skew plus transit, inseparable from one sample — so we
> never claim it. We report the excess above a rolling minimum, which cancels the skew.
>
> And this is what makes it one tool instead of two: that measured latency is an **input feature**
> to the slippage model. You decide at time *t*, your order lands at *t* plus delta, and what you
> pay depends on what the market did in between. Latency is a term inside the calculator, not a
> metric beside it.

## 1:25 — The demo

*[Call `/size` with the regime showing NORMAL]*

> Calm market. Naive size, four-tenths of a lot. Ours, essentially the same — overexposure under
> a tenth of a percent. This is why nobody notices the problem: in a quiet market the old formula
> is basically right.

*[Start the replay; regime climbs NORMAL → ELEVATED → SPIKE]*

> Now a release hits. Spread widens, latency p99 blows out, the regime escalates.

*[Call `/size` again]*

> Same trader, same risk budget, same stop — and the gap opens. Every retail calculator just
> handed this trader more risk than they asked for, at the moment it's most dangerous.

## 2:00 — Why believe it

*[Open `/model/card`]*

> The baseline we beat is *cost equals the current half-spread* — what a calculator assumes when
> it ignores slippage.
>
> Over twenty-three thousand real ticks, median realised cost lands on the half-spread to the
> digit — exactly what the model's structure predicts. The ninety-fifth percentile is two hundred
> and sixty-five times the median, and that gap is the whole argument for sizing off the tail.
>
> We beat the baseline only in that tail. At p50 we lose to it slightly — the p50 target *is* the
> half-spread, so the baseline is provably optimal there. The claim was never that these
> calculators are bad at arithmetic. It's that they're right about the average case and blind to
> the case that hurts you.

## 2:45 — Close

> Every number here declares its provenance: measured, estimated, or simulated. Nothing synthetic
> is shown as measured. And the model ships as three kilobytes of JSON — inference is a dot
> product, with nothing to install.
>
> That's MarketSpike.
