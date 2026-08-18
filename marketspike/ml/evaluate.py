import math
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
