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


# --- Generic, registry-driven routing (no code change to add a symbol) ---


def test_ethusdt_resolves_to_binance_via_registry_alone():
    """ETHUSDT is not special-cased anywhere in build_adapters -- it routes
    to Binance purely because its instruments.json entry has quote_ccy
    "USDT". This is the whole point of the task: adding a symbol is a
    config change to instruments.json, not a code change here."""
    settings = Settings(symbols=["ETHUSDT"])
    adapters = build_adapters(settings)
    assert list(adapters) == ["ETHUSDT"]
    assert adapters["ETHUSDT"].venue == "binance"


def test_gbpusd_resolves_to_oanda_via_registry_alone():
    """Same argument as ETHUSDT above, but for the OANDA side: GBPUSD's
    quote_ccy "USD" is not a crypto quote, so it routes to OandaAdapter with
    no GBPUSD-specific branch in build_adapters."""
    settings = Settings(
        symbols=["GBPUSD"],
        oanda_token="tok",
        oanda_account_id="acct",
    )
    adapters = build_adapters(settings)
    assert list(adapters) == ["GBPUSD"]
    assert adapters["GBPUSD"].venue == "oanda"


def test_gbpusd_without_credentials_is_skipped():
    settings = Settings(symbols=["GBPUSD"])
    adapters = build_adapters(settings)
    assert adapters == {}
