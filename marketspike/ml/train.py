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
from marketspike.config import get_settings
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
        previous_ts = ts_ns

    return _attach_volatility(rows)


def _attach_volatility(rows: List[TickRow]) -> List[TickRow]:
    """Recompute V ratio and spread z offline with the same estimators.

    The volatility pair is constructed with the same tau/gate settings the
    live engine uses (spec: train/serve skew). `VolatilityPair`'s sampling
    gate lives inside the estimator specifically so this offline recompute
    samples identically to live serving -- passing a different interval
    here would reintroduce that skew.
    """
    from marketspike.engine.spread import SpreadTracker
    from marketspike.engine.volatility import VolatilityPair

    settings = get_settings()
    vol = VolatilityPair(
        tau_fast_s=settings.tau_fast_s,
        tau_slow_s=settings.tau_slow_s,
        min_sample_interval_s=settings.vol_sample_interval_s,
    )
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


def quantile_crossing_fraction(
    fitted: Dict[str, Dict[str, Any]], samples: List[Sample]
) -> float:
    """Fraction of rows where the *raw* fitted p95 falls below the raw p50.

    Quantile crossing is a known artefact of fitting quantiles
    independently -- it already bit the hand-set fallback priors (see
    risk/slippage.py). `SlippageModel.predict_quantiles` repairs the
    ordering at serving time (`p95 = max(p95_raw, p50_raw)`), so a crossing
    row is not a serving bug -- but a model that crosses frequently is a
    signal the fit itself is poor, and the model card should say so rather
    than let the serving-time repair mask it silently.
    """
    if not samples or "p50" not in fitted or "p95" not in fitted:
        return 0.0
    p50 = fitted["p50"]
    p95 = fitted["p95"]
    crossed = 0
    for s in samples:
        p50_raw = p50["intercept"] + sum(
            p50["coefficients"][i] * s.features[name]
            for i, name in enumerate(FEATURE_ORDER)
        )
        p95_raw = p95["intercept"] + sum(
            p95["coefficients"][i] * s.features[name]
            for i, name in enumerate(FEATURE_ORDER)
        )
        if p95_raw < p50_raw:
            crossed += 1
    return crossed / len(samples)


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

    # Quantile-crossing check on the training set (spec amendment): a model
    # that crosses often is a signal the fit is poor, distinct from the
    # serving-time repair that masks individual crossed rows.
    crossing_frac = quantile_crossing_fraction(fitted, train)
    report["quantile_crossing_frac"] = crossing_frac
    if crossing_frac > 0:
        print(
            "{0}: p95 < p50 (raw, pre-repair) on {1:.1%} of training rows".format(
                symbol, crossing_frac
            )
        )

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

        if entry:
            conn.execute(
                "INSERT OR REPLACE INTO model_registry (version, symbol, trained_at_ns, "
                "coefficients_json, metrics_json, n_rows, is_active) VALUES (?,?,?,?,?,?,1)",
                (entry["version"], symbol, entry["trained_at_ns"],
                 json.dumps(entry["quantiles"]), json.dumps(entry["metrics"]),
                 entry["n_rows"]),
            )
    conn.commit()

    with open(args.out, "w") as handle:
        json.dump({"models": models}, handle, indent=2)
    print("wrote {0} model(s) to {1}".format(len(models), args.out))


if __name__ == "__main__":
    main()
