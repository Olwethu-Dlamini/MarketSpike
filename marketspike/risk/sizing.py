import math
from dataclasses import dataclass
from typing import Any, Dict, List

from marketspike.api.schemas import SizeRequest
from marketspike.risk.instruments import InstrumentSpec

HIGH_RISK_PCT = 5.0


def round_down_to_step(value: float, step: float) -> float:
    """Round toward zero on the lot grid.

    Always down: rounding a risk-limited quantity up breaches the risk budget
    the user specified, which defeats the calculation (spec §10.4). The epsilon
    absorbs binary representation error so an exact multiple does not fall to
    the step below.
    """
    if step <= 0:
        return value
    return round(math.floor(value / step + 1e-9) * step, 10)


@dataclass
class SizingContext:
    price: float
    fx_rate: float
    fx_assumed: bool
    regime: str
    event_context: str
    latency_ms: float
    latency_source: str
    stale_quote: bool
    model_source: str
    model_version: str


def _bps_to_pips(bps: float, price: float, pip_size: float) -> float:
    return (bps / 10000.0) * price / pip_size


def size_position(
    request: SizeRequest,
    spec: InstrumentSpec,
    slippage_p50_bps: float,
    slippage_p95_bps: float,
    context: SizingContext,
) -> Dict[str, Any]:
    warnings: List[str] = []
    if request.risk_pct > HIGH_RISK_PCT:
        warnings.append("HIGH_RISK_PCT")

    balance = request.account_balance_minor / 100.0
    risk_budget = balance * (request.risk_pct / 100.0)

    pip_value = spec.pip_value(context.fx_rate)
    stop_pips = request.stop_distance_price / spec.pip_size

    p50_pips = _bps_to_pips(slippage_p50_bps, context.price, spec.pip_size)
    p95_pips = _bps_to_pips(slippage_p95_bps, context.price, spec.pip_size)
    chosen_pips = p95_pips if request.quantile == "p95" else p50_pips
    effective_pips = stop_pips + chosen_pips

    naive_lots = risk_budget / (stop_pips * pip_value)
    raw_lots = risk_budget / (effective_pips * pip_value)
    lots = round_down_to_step(raw_lots, spec.lot_step)

    capped_by = None
    free_margin = request.free_margin_minor / 100.0
    margin_per_lot = spec.contract_size * context.price * spec.margin_rate * context.fx_rate
    if margin_per_lot > 0:
        max_by_margin = round_down_to_step(free_margin / margin_per_lot, spec.lot_step)
        if max_by_margin < lots:
            lots = max_by_margin
            capped_by = "margin"

    if lots < spec.min_lot:
        lots = 0.0
        warnings.append("BELOW_MIN_LOT")

    # actual_risk is recomputed at the rounded-down size, never the raw
    # request: after flooring to the lot step, true risk sits strictly
    # below the requested target, and the caller must see that real
    # figure rather than the naive risk_budget they asked for (spec §10.4).
    actual_risk = lots * effective_pips * pip_value
    required_margin = lots * margin_per_lot
    overexposure = (
        ((naive_lots - lots) / lots * 100.0) if lots > 0 else 0.0
    )

    return {
        "naive_lot_size": round(naive_lots, 4),
        "recommended_lot_size": lots,
        "overexposure_pct": round(overexposure, 2),
        "slippage_p50_pips": round(p50_pips, 4),
        "slippage_p95_pips": round(p95_pips, 4),
        "stop_distance_pips": round(stop_pips, 4),
        "effective_adverse_pips": round(effective_pips, 4),
        "actual_risk_amount_minor": int(round(actual_risk * 100)),
        "actual_risk_pct": (actual_risk / balance * 100.0) if balance else 0.0,
        "required_margin_minor": int(round(required_margin * 100)),
        "capped_by": capped_by,
        "fx_assumed": context.fx_assumed,
        "stale_quote": context.stale_quote,
        "model_source": context.model_source,
        "model_version": context.model_version,
        "regime_at_calc": context.regime,
        "event_context": context.event_context,
        "latency_used_ms": context.latency_ms,
        "latency_source": context.latency_source,
        "warnings": warnings,
        "inputs_echo": request.model_dump(),
    }
