import json
import os

import pytest

from marketspike.risk.instruments import (
    InstrumentSpec,
    _PATH,
    all_instruments,
    get_instrument,
)


def test_eurusd_pip_value_is_ten_dollars_per_standard_lot():
    spec = get_instrument("EURUSD")
    assert spec.pip_value(fx_rate=1.0) == pytest.approx(10.0)


def test_usdjpy_uses_a_two_decimal_pip_and_needs_conversion():
    spec = get_instrument("USDJPY")
    assert spec.pip_size == 0.01
    assert spec.quote_ccy == "JPY"
    # 0.01 * 100000 = 1000 JPY per pip; at 0.0067 USD/JPY that is $6.70.
    assert spec.pip_value(fx_rate=0.0067) == pytest.approx(6.70)


def test_btcusdt_is_not_forced_into_forex_conventions():
    spec = get_instrument("BTCUSDT")
    assert spec.contract_size == 1
    assert spec.lot_step == 0.0001


def test_unknown_symbol_raises():
    with pytest.raises(KeyError):
        get_instrument("GBPJPY")


def test_registry_exposes_every_instrument():
    symbols = {spec.symbol for spec in all_instruments()}
    assert {"EURUSD", "USDJPY", "XAUUSD", "BTCUSDT"} <= symbols


def test_pip_value_is_derived_not_stored():
    """pip_value must be computed from fx_rate, not read from a stored field.

    Guards against a future edit re-introducing a cached/hardcoded pip
    value: (1) the result must scale linearly with fx_rate, and (2) the
    JSON source of truth must not carry a "pip_value" key at all.
    """
    spec = get_instrument("EURUSD")
    base = spec.pip_value(fx_rate=1.0)
    doubled = spec.pip_value(fx_rate=2.0)
    assert doubled == pytest.approx(base * 2.0)
    tripled = spec.pip_value(fx_rate=3.0)
    assert tripled == pytest.approx(base * 3.0)

    with open(_PATH, "r") as handle:
        raw = json.load(handle)
    for symbol, fields in raw.items():
        assert "pip_value" not in fields, (
            "{0} has a stored pip_value; it must be derived".format(symbol)
        )


def test_every_registry_entry_is_a_valid_positive_instrument_spec():
    """Guard against a typo in instruments.json silently producing nonsense
    sizing: every entry must load into a complete InstrumentSpec with
    strictly positive size/step fields and a margin_rate in (0, 1]."""
    with open(_PATH, "r") as handle:
        raw = json.load(handle)

    assert raw, "instruments.json must not be empty"

    required_fields = {
        "pip_size",
        "contract_size",
        "quote_ccy",
        "min_lot",
        "lot_step",
        "margin_rate",
    }

    for symbol, fields in raw.items():
        assert set(fields.keys()) == required_fields, symbol

        spec = InstrumentSpec(symbol=symbol, **fields)
        assert isinstance(spec, InstrumentSpec)

        assert spec.pip_size > 0, symbol
        assert spec.contract_size > 0, symbol
        assert spec.min_lot > 0, symbol
        assert spec.lot_step > 0, symbol
        assert 0 < spec.margin_rate <= 1, symbol
        assert isinstance(spec.quote_ccy, str) and spec.quote_ccy, symbol
