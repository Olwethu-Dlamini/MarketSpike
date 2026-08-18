from marketspike.config import Settings
from marketspike.main import build_adapters


def test_single_binance_symbol_yields_binance_adapter():
    settings = Settings(symbols=["BTCUSDT"])
    adapters = build_adapters(settings)
    assert list(adapters) == ["BTCUSDT"]
    assert adapters["BTCUSDT"].venue == "binance"


def test_eurusd_with_credentials_yields_oanda_adapter():
    settings = Settings(
        symbols=["EURUSD"],
        oanda_token="tok",
        oanda_account_id="acct",
    )
    adapters = build_adapters(settings)
    assert list(adapters) == ["EURUSD"]
    assert adapters["EURUSD"].venue == "oanda"


def test_eurusd_without_credentials_is_skipped_but_other_symbols_are_not():
    settings = Settings(symbols=["EURUSD", "BTCUSDT"])
    adapters = build_adapters(settings)
    assert "EURUSD" not in adapters
    assert "BTCUSDT" in adapters
    assert adapters["BTCUSDT"].venue == "binance"


def test_unrecognised_symbol_is_skipped_not_raised():
    settings = Settings(symbols=["DOGEUSD"])
    adapters = build_adapters(settings)
    assert adapters == {}
