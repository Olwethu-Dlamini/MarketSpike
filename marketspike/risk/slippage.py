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
