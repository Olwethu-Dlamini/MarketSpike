# MarketSpike — 3-minute video transcript

Spoken lines only are read aloud; bracketed lines are screen cues.

**Pacing, honestly: the full read is 528 words — about 3:12 at 165 wpm, 3:24 at a normal 155.**
This version trades some length for technical density. Dropping the two passages marked
*[cut-1]* and *[cut-2]* below (52 words) puts it at 476 — 3:04 at 155, 2:58 at 160 — and the
argument is unaffected by either. Recount with the snippet under *Pacing* after any edit.

Record against the live service — <https://marketspike.onrender.com> — so nothing rides on a
local process staying up, and load a captured spike scenario before you hit record. The
pre-flight checklist lives in `DEMO.md`.

Tone: talking to another engineer who trades, not to a judging panel. Say the numbers plainly;
they do the work on their own.

---

## 0:00 — The problem

*[Instrument panel, BTCUSDT streaming, latency waterfall moving]*

> Quick tour of MarketSpike, and the one number it exists to produce.
>
> Every retail position-size calculator computes this: lots equals risk budget, over stop
> distance times pip value. Clean formula — and it quietly assumes you're filled at the price on
> your screen, the instant you saw it. Both halves break during a CPI or payrolls print, which is
> exactly when your size matters.

## 0:20 — The fix, in one line

> So we put the missing term back. Effective stop equals your stop **plus predicted slippage at
> the ninety-fifth percentile**, and we size off that. Same risk budget, honest denominator. The
> distance between the two answers is `overexposure_pct` — the extra risk the old formula handed
> you.

## 0:40 — How it works

*[Cut to the architecture diagram]*

> Ticks come off Binance's WebSocket into one engine pass each. Two EWMA variance horizons —
> thirty seconds fast, eighteen hundred slow, normalised per second so the ratio `V` is
> dimensionless. A spread z-score off median and MAD, so one bad quote can't move it. Those blend
> into a score bounded zero to four, driving a regime machine with real hysteresis: enter elevated
> above 1.5 after three seconds, leave below 1.1 after fifteen. Asymmetric on purpose — missing a
> spike costs a trader money, a warning that lingers costs nothing.
>
> Latency is three hops, each labelled. Receive-to-engine is exact: one machine, one monotonic
> clock. Engine-to-browser is a four-timestamp NTP handshake. Venue-to-receive we refuse to
> claim — what you see is skew plus transit, and one sample can't split them — so we report only
> the excess above a rolling minimum.
>
> Then nine features into linear quantile regression on pinball loss: log v-ratio, spread z, log
> spread, **log latency**, quote rate, book imbalance, signed seconds to the nearest event, an
> in-event-window flag, and a five-second absolute return. Notice latency in the feature vector —
> your broker's speed is an input to the cost model, not an ornament on the dashboard.

## 1:35 — The demo

*[Call `/size`, regime NORMAL]*

> Calm market. Naive, four-tenths of a lot; ours, essentially identical; overexposure
> eight-hundredths of a percent. Nothing to see — that's the point. On a quiet tape the old
> formula is basically right, which is why nobody believes there's a problem.

*[Start the replay; NORMAL → ELEVATED → SPIKE]*

> Now replay a print. Spread widens, p99 latency blows out, the regime steps up.

*[Call `/size` again]*

> Size again, and the gap opens.
>
> *[cut-2]* What comes back is floored to the instrument's lot step and re-checked against free
> margin, so `actual_risk` is computed at the size you can really trade — not the one you asked
> for.

## 2:10 — Why believe the slippage number

*[Open `/model/card`]*

> The baseline is `cost = current half-spread` — literally the assumption we're replacing. Over
> twenty-three thousand real ticks, median realised cost lands on the half-spread to the digit;
> p95 is two hundred and sixty-five times that.
>
> We beat it by five percent at p95 and lose two at p50 — and losing at p50 is the correct
> result: the p50 target *is* the half-spread, so the baseline is optimal there by construction.
> The split is time-ordered, never shuffled, and the feature builder raises `LeakageError` if the
> target isn't strictly after the decision. *[cut-1]*

## 2:45 — Close

> Every number carries its provenance — measured, estimated, or simulated. An untrained model
> says `fallback_coefficients` out loud instead of pretending. And it all ships as three
> kilobytes of JSON, so inference is a dot product with nothing to install.
>
> That's MarketSpike. Thanks for watching.

---

## Pacing

Recount after any edit:

```bash
python3 - <<'PY'
import re
spoken = [l[1:] for l in open('docs/VIDEO-SCRIPT.md') if l.startswith('>')]
w = len(re.findall(r"[\w'’%-]+", " ".join(spoken)))
print(w, "words →", round(w/155, 2), "min at 155 wpm")
PY
```

If you overrun, cut in this order — each is self-contained, and the argument survives all three:

1. `[cut-1]` the `LeakageError` sentence — 20 words,
2. `[cut-2]` the lot-step / free-margin sentence — 32 words,
3. the venue-to-receive hop — 28 words. Losing this one costs the most: it is the sharpest
   evidence that the project declines to claim things it can't measure.
