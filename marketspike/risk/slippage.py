import json
import logging
import os
from typing import Any, Dict, List, Optional

from marketspike.risk.instruments import REGISTRY

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

# Hand-set priors used until a model is trained per symbol (Task 20). They
# are deliberately conservative and are always reported as
# "fallback_coefficients" so the demo never degrades silently (spec §9.10).
#
# A single linear-in-log(spread) model cannot serve both FX and crypto: FX
# spreads run ~1.2 bps while BTCUSDT's spread is ~0.00156 bps (~770x
# tighter in relative terms), so log_spread_bps for crypto sits around -6.46
# — a value the FX-tuned priors were never calibrated against, and which
# drove both quantiles negative and into the max(0.0, ...) clamp (the
# BTCUSDT "recommended == naive lot size" defect). Priors are therefore kept
# per asset class; fallback_model() picks the right set for the symbol.
#
# fx: the original, unretouched coefficients — EURUSD behaviour is
# unchanged. The intercept gap between p50 and p95 (3.00) is deliberately
# wide so that the two curves stay ordered (p95 >= p50) across the
# realistic input range without relying on the predict_quantiles() crossing
# guard: specifically for log_v_ratio down to -3.0 and spread_z down to
# -5.0 (even in combination — the worst case, log_v_ratio=-3.0 AND
# spread_z=-5.0 simultaneously, leaves a margin of 0.25 bps). p95 is still
# meaningfully more sensitive than p50 to volatility (log_v_ratio
# coefficient 0.60 vs 0.10) and spread (0.30 vs 0.05). The
# predict_quantiles() max() repair remains as a backstop for combinations
# outside that range, and for trained models fitted independently per
# quantile.
#
# crypto: calibrated against 23,292 recorded BTCUSDT ticks (implementation
# shortfall vs arrival price at 60ms), measured p50=0.00078 bps (exactly
# half the spread, as designed), p95=0.20642 bps, p99=0.82701 bps. At the
# typical vector (log_v_ratio=log(0.25)=-1.386, spread_z=0.0,
# log_spread_bps=log(0.00156)=-6.463, log_latency_ms=log(60)=4.094):
#   - Non-spread coefficients and the p50 intercept are the FX p50 set
#     scaled by s = 1/769.23 (the measured FX/BTC spread ratio), because
#     these features are already dimensionless log-ratios/z-scores and the
#     only reason their FX-tuned magnitudes are too large for crypto is the
#     ~770x smaller overall cost scale.
#   - log_spread_bps gets its own small positive coefficient (0.000013)
#     rather than a scaled-down 0.50: log_spread_bps itself is ~-6.46 for
#     crypto vs ~+0.18 for FX, so naively scaling the FX coefficient would
#     let this one term dominate and swing the prediction negative again.
#   - p95's non-spread coefficients keep the FX p95/p50 sensitivity ratio
#     (6x for log_v_ratio/spread_z/log_latency_ms/in_event_window, 1.8x for
#     log_spread_bps); its intercept (0.206056) is then solved exactly so
#     predict_bps at the typical vector reproduces the measured p95
#     (0.20642) — crypto's p95/p50 ratio (~265x) is far fatter than FX's
#     (6x), reflecting a thin-book tail risk that isn't simply proportional
#     to the median spread cost, so it cannot come from the same scale
#     factor as p50.
#   - Solved with scripts/tune fallback exercise (see task-17 report); not
#     fitted to full precision, only to the right order of magnitude.
_FALLBACK: Dict[str, Dict[str, Dict[str, Any]]] = {
    "fx": {
        "p50": {
            "intercept": 0.60,
            "coefficients": [0.10, 0.05, 0.50, 0.05, 0.0, 0.0, 0.0, 0.20, 0.0],
        },
        "p95": {
            "intercept": 3.60,
            "coefficients": [0.60, 0.30, 0.90, 0.30, 0.0, 0.0, 0.0, 1.20, 0.0],
        },
    },
    "crypto": {
        "p50": {
            "intercept": 0.00078,
            "coefficients": [
                0.00013, 0.000065, 0.000013, 0.000065,
                0.0, 0.0, 0.0, 0.00026, 0.0,
            ],
        },
        "p95": {
            "intercept": 0.206056,
            "coefficients": [
                0.00078, 0.00039, 0.000023, 0.00039,
                0.0, 0.0, 0.0, 0.00156, 0.0,
            ],
        },
    },
}

# Symbols whose quote currency isn't in the instrument registry (or which
# aren't registered at all) still need an asset-class guess. USDT/USDC are
# unambiguous crypto stablecoin quotes; a bare "USD" suffix is NOT included
# here because it also matches FX pairs like EURUSD.
_CRYPTO_QUOTE_CCYS = frozenset({"USDT", "USDC"})
_CRYPTO_SUFFIXES = ("USDT", "USDC")


def _asset_class(symbol: str) -> str:
    # marketspike.risk.instruments imports only json/os/dataclasses/types/
    # typing and never imports slippage (or anything that does), so this
    # import is not circular — checked by reading instruments.py before
    # adding it here. Prefer its quote_ccy when the symbol is registered,
    # since that's an explicit, curated signal rather than a guess; fall
    # back to a symbol-suffix rule for anything not (yet) in the registry.
    instrument = REGISTRY.get(symbol)
    if instrument is not None:
        return "crypto" if instrument.quote_ccy in _CRYPTO_QUOTE_CCYS else "fx"
    return "crypto" if symbol.endswith(_CRYPTO_SUFFIXES) else "fx"


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
    asset_class = _asset_class(symbol)
    return SlippageModel(
        symbol=symbol,
        quantiles={q: dict(spec) for q, spec in _FALLBACK[asset_class].items()},
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
