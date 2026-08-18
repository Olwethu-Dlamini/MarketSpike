import pytest

from marketspike.api.schemas import SizeResponse, SizeRequest
from marketspike.risk.instruments import get_instrument
from marketspike.risk.sizing import SizingContext, round_down_to_step, size_position

EURUSD = get_instrument("EURUSD")


def context(**overrides):
    base = dict(
        price=1.0850, fx_rate=1.0, fx_assumed=False, regime="SPIKE",
        event_context="EVENT_WINDOW", latency_ms=63.2, latency_source="measured",
        stale_quote=False, model_source="trained", model_version="eurusd-test",
    )
    base.update(overrides)
    return SizingContext(**base)


def request(**overrides):
    base = dict(
        symbol="EURUSD", account_balance_minor=1000000, account_ccy="USD",
        risk_pct=1.0, stop_distance_price=0.0020, direction="buy",
        quantile="p95", free_margin_minor=1000000, assumed_latency_ms=None,
    )
    base.update(overrides)
    return SizeRequest(**base)


# slippage_pips = bps * price / (10000 * pip_size) = bps * price for EURUSD.
P50_BPS = 1.4 / 1.0850   # -> 1.4 pips
P95_BPS = 6.2 / 1.0850   # -> 6.2 pips


def test_round_down_never_rounds_up():
    assert round_down_to_step(0.3817, 0.01) == pytest.approx(0.38)
    assert round_down_to_step(0.3899, 0.01) == pytest.approx(0.38)


def test_round_down_is_stable_on_exact_multiples():
    assert round_down_to_step(0.38, 0.01) == pytest.approx(0.38)


def test_worked_example_from_the_spec():
    """Spec §10.6: $10,000 at 1% risk, 20-pip stop, 6.2-pip p95 slippage."""
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    assert result["stop_distance_pips"] == pytest.approx(20.0)
    assert result["slippage_p95_pips"] == pytest.approx(6.2, abs=1e-6)
    assert result["effective_adverse_pips"] == pytest.approx(26.2, abs=1e-6)
    assert result["naive_lot_size"] == pytest.approx(0.50)
    assert result["recommended_lot_size"] == pytest.approx(0.38)
    assert result["overexposure_pct"] == pytest.approx(31.6, abs=0.05)
    assert result["actual_risk_amount_minor"] == 9956
    assert result["actual_risk_pct"] == pytest.approx(0.9956, abs=1e-4)
    assert result["required_margin_minor"] == 137296


def test_actual_risk_is_below_target_because_of_round_down():
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    assert result["actual_risk_amount_minor"] < 10000


def test_p50_quantile_gives_a_larger_size_than_p95():
    p50 = size_position(request(quantile="p50"), EURUSD, P50_BPS, P95_BPS, context())
    p95 = size_position(request(quantile="p95"), EURUSD, P50_BPS, P95_BPS, context())
    assert p50["recommended_lot_size"] > p95["recommended_lot_size"]


def test_insufficient_margin_caps_the_size_and_says_so():
    result = size_position(
        request(free_margin_minor=50000),  # $500 free margin
        EURUSD, P50_BPS, P95_BPS, context(),
    )
    assert result["capped_by"] == "margin"
    assert result["recommended_lot_size"] < 0.38
    assert result["required_margin_minor"] <= 50000


def test_high_risk_warns_but_does_not_block():
    result = size_position(request(risk_pct=8.0), EURUSD, P50_BPS, P95_BPS, context())
    assert "HIGH_RISK_PCT" in result["warnings"]
    assert result["recommended_lot_size"] > 0


def test_size_below_minimum_lot_returns_zero_and_flags_it():
    result = size_position(
        request(account_balance_minor=1000),  # $10 account
        EURUSD, P50_BPS, P95_BPS, context(),
    )
    assert result["recommended_lot_size"] == 0.0
    assert "BELOW_MIN_LOT" in result["warnings"]


def test_assumed_fx_rate_is_surfaced():
    result = size_position(
        request(), EURUSD, P50_BPS, P95_BPS, context(fx_assumed=True)
    )
    assert result["fx_assumed"] is True


def test_response_echoes_inputs_and_context():
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    assert result["inputs_echo"]["symbol"] == "EURUSD"
    assert result["regime_at_calc"] == "SPIKE"
    assert result["latency_source"] == "measured"
    assert result["model_source"] == "trained"


def test_overexposure_pct_is_positive_whenever_slippage_is_positive():
    """The demo's headline number must never read 0 when slippage exists.

    Uses a realistic spike-like p95 slippage (well above the calm-market
    P95_BPS fixture) to make sure overexposure is strictly positive, not
    just non-negative by coincidence of rounding.
    """
    spike_p95_bps = 25.0 / 1.0850  # -> 25 pips, a spike-scenario slippage
    result = size_position(request(), EURUSD, P50_BPS, spike_p95_bps, context())
    assert result["overexposure_pct"] > 0.0


def test_response_validates_against_the_frozen_schema():
    result = size_position(request(), EURUSD, P50_BPS, P95_BPS, context())
    validated = SizeResponse.model_validate(result)
    assert validated.recommended_lot_size == pytest.approx(0.38)
