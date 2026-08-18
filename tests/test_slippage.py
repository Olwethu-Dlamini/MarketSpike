import json

from marketspike.risk.slippage import (
    FEATURE_ORDER, SlippageModel, fallback_model, load_models, resolve_models,
)

FEATURES = {name: 0.0 for name in FEATURE_ORDER}


def test_prediction_is_the_intercept_when_all_features_are_zero():
    model = fallback_model("EURUSD")
    assert model.predict_bps(FEATURES, "p50") == model.quantiles["p50"]["intercept"]


def test_p95_never_falls_below_p50_on_the_fallback():
    model = fallback_model("EURUSD")
    features = dict(FEATURES, log_v_ratio=1.5, spread_z=3.0, log_latency_ms=5.0)
    assert model.predict_bps(features, "p95") >= model.predict_bps(features, "p50")


def test_prediction_is_clamped_at_zero():
    model = fallback_model("EURUSD")
    features = dict(FEATURES, log_v_ratio=-1000.0)
    assert model.predict_bps(features, "p50") >= 0.0


def test_missing_features_default_to_zero_rather_than_raising():
    model = fallback_model("EURUSD")
    assert model.predict_bps({}, "p95") >= 0.0


def test_fallback_declares_its_provenance():
    assert fallback_model("EURUSD").source == "fallback_coefficients"


def test_loading_a_trained_file_marks_the_model_as_trained(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({
        "models": {
            "EURUSD": {
                "version": "eurusd-test",
                "feature_order": FEATURE_ORDER,
                "quantiles": {
                    "p50": {"intercept": 1.0, "coefficients": [0.0] * len(FEATURE_ORDER)},
                    "p95": {"intercept": 4.0, "coefficients": [0.0] * len(FEATURE_ORDER)},
                },
            }
        }
    }))
    models = load_models(str(path))
    assert models["EURUSD"].source == "trained"
    assert models["EURUSD"].predict_bps(FEATURES, "p95") == 4.0


def test_missing_file_yields_no_models_rather_than_raising():
    assert load_models("/nonexistent/model.json") == {}


def test_unreadable_file_yields_no_models_rather_than_raising(tmp_path):
    path = tmp_path / "model.json"
    path.write_text("{ this is not valid json")
    assert load_models(str(path)) == {}


def test_model_entry_with_empty_quantiles_is_skipped(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({
        "models": {
            "EURUSD": {
                "version": "broken",
                "feature_order": FEATURE_ORDER,
                "quantiles": {},
            }
        }
    }))
    assert load_models(str(path)) == {}


def test_more_coefficients_than_feature_order_does_not_crash(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({
        "models": {
            "BTCUSDT": {
                "version": "extra-coeffs",
                "feature_order": FEATURE_ORDER,
                "quantiles": {
                    "p50": {
                        "intercept": 1.0,
                        "coefficients": [0.0] * (len(FEATURE_ORDER) + 5),
                    },
                },
            }
        }
    }))
    models = load_models(str(path))
    # Loop breaks at the shorter length (FEATURE_ORDER); extra coefficients
    # are silently ignored rather than raising.
    assert models["BTCUSDT"].predict_bps(FEATURES, "p50") == 1.0


def test_fewer_coefficients_than_feature_order_does_not_crash(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({
        "models": {
            "BTCUSDT": {
                "version": "short-coeffs",
                "feature_order": FEATURE_ORDER,
                "quantiles": {
                    "p50": {
                        "intercept": 1.0,
                        "coefficients": [1.0, 1.0],
                    },
                },
            }
        }
    }))
    models = load_models(str(path))
    features = dict(FEATURES)
    features[FEATURE_ORDER[0]] = 2.0
    features[FEATURE_ORDER[1]] = 3.0
    features[FEATURE_ORDER[2]] = 100.0  # no coefficient for this — must not IndexError
    # 1.0 (intercept) + 1.0*2.0 + 1.0*3.0 = 6.0
    assert models["BTCUSDT"].predict_bps(features, "p50") == 6.0


def test_predict_bps_is_a_dot_product_with_known_arithmetic():
    coefficients = [0.0] * len(FEATURE_ORDER)
    target_index = FEATURE_ORDER.index("log_latency_ms")
    coefficients[target_index] = 0.31
    model = SlippageModel(
        symbol="EURUSD",
        quantiles={
            "p50": {"intercept": 2.0, "coefficients": coefficients},
        },
        version="known-arithmetic",
        source="trained",
    )
    features = dict(FEATURES, log_latency_ms=4.0)
    # 2.0 + 0.31 * 4.0 = 3.24
    assert model.predict_bps(features, "p50") == 3.24


def test_resolve_models_returns_mixed_provenance(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({
        "models": {
            "BTCUSDT": {
                "version": "btc-trained",
                "feature_order": FEATURE_ORDER,
                "quantiles": {
                    "p50": {"intercept": 1.0, "coefficients": [0.0] * len(FEATURE_ORDER)},
                    "p95": {"intercept": 4.0, "coefficients": [0.0] * len(FEATURE_ORDER)},
                },
            }
        }
    }))
    models = resolve_models(str(path), ["EURUSD", "BTCUSDT"])
    assert models["BTCUSDT"].source == "trained"
    assert models["EURUSD"].source == "fallback_coefficients"
