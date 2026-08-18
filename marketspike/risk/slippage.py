import json
import logging
import os
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)

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
#
# The intercept gap between p50 and p95 (3.00) is deliberately wide so that
# the two curves stay ordered (p95 >= p50) across the realistic input range
# without relying on the predict_quantiles() crossing guard: specifically for
# log_v_ratio down to -3.0 and spread_z down to -5.0 (even in combination —
# the worst case, log_v_ratio=-3.0 AND spread_z=-5.0 simultaneously, leaves a
# margin of 0.25 bps). p95 is still meaningfully more sensitive than p50 to
# volatility (log_v_ratio coefficient 0.60 vs 0.10) and spread (0.30 vs 0.05).
# The predict_quantiles() max() repair remains as a backstop for combinations
# outside that range, and for trained models fitted independently per quantile.
_FALLBACK: Dict[str, Dict[str, Any]] = {
    "p50": {
        "intercept": 0.60,
        "coefficients": [0.10, 0.05, 0.50, 0.05, 0.0, 0.0, 0.0, 0.20, 0.0],
    },
    "p95": {
        "intercept": 3.60,
        "coefficients": [0.60, 0.30, 0.90, 0.30, 0.0, 0.0, 0.0, 1.20, 0.0],
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
        self._warned_unknown_quantiles = set()
        self._warn_on_coefficient_length_mismatch()

    def _warn_on_coefficient_length_mismatch(self) -> None:
        # Checked once, at load time, rather than per-prediction — a mismatch
        # is a property of the model file, not of any individual request, so
        # warning here can't spam the hot path.
        for quantile, spec in self.quantiles.items():
            if not isinstance(spec, dict):
                continue
            coefficients = spec.get("coefficients")
            if coefficients is None:
                continue
            if len(coefficients) != len(self.feature_order):
                LOGGER.warning(
                    "slippage model %s (%s) quantile %r has %d coefficient(s) "
                    "but feature_order has %d entries; extra coefficients are "
                    "ignored and missing ones are treated as zero",
                    self.symbol, self.version, quantile,
                    len(coefficients), len(self.feature_order),
                )

    def _warn_unknown_quantile(self, quantile: str) -> None:
        if quantile in self._warned_unknown_quantiles:
            return
        self._warned_unknown_quantiles.add(quantile)
        LOGGER.warning(
            "slippage model %s (%s) has no quantile %r; returning 0.0",
            self.symbol, self.version, quantile,
        )

    def _raw_score(self, features: Dict[str, float], quantile: str) -> Optional[float]:
        spec = self.quantiles.get(quantile)
        if spec is None:
            self._warn_unknown_quantile(quantile)
            return None
        total = float(spec["intercept"])
        coefficients = spec["coefficients"]
        for index, name in enumerate(self.feature_order):
            if index >= len(coefficients):
                break
            total += coefficients[index] * float(features.get(name, 0.0))
        return total

    def predict_quantiles(self, features: Dict[str, float]) -> Dict[str, float]:
        """Return both quantiles with p95 >= p50 guaranteed.

        Fitting p50 and p95 independently (whether hand-set priors or two
        separately-trained regressions) offers no guarantee that the p95
        curve stays above the p50 curve pointwise — a well-known artefact
        called "quantile crossing". Rather than trust the coefficients to
        never cross, we repair the ordering at inference time by taking
        p95 = max(p95_raw, p50_raw). This protects trained models (Task 20)
        exactly the same way it protects today's fallback priors.
        """
        p50_raw = self._raw_score(features, "p50")
        p95_raw = self._raw_score(features, "p95")
        # A negative predicted cost is meaningless; clamp rather than emit it.
        p50 = max(0.0, p50_raw) if p50_raw is not None else 0.0
        p95_clamped = max(0.0, p95_raw) if p95_raw is not None else 0.0
        p95 = max(p95_clamped, p50)  # quantile-crossing repair
        return {"p50": p50, "p95": p95}

    def predict_bps(self, features: Dict[str, float], quantile: str) -> float:
        if quantile in ("p50", "p95"):
            return self.predict_quantiles(features)[quantile]
        raw = self._raw_score(features, quantile)
        if raw is None:
            return 0.0
        # A negative predicted cost is meaningless; clamp rather than emit it.
        return max(0.0, raw)


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

    if not isinstance(raw, dict):
        return {}
    models_section = raw.get("models")
    if not isinstance(models_section, dict):
        return {}

    models: Dict[str, SlippageModel] = {}
    for symbol, entry in models_section.items():
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
