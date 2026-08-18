"""Tests for GET /api/v1/model/card (spec Task 20, review finding 3).

Uses FastAPI's TestClient without the `with` context manager: Starlette
only runs `@app.on_event("startup"/"shutdown")` handlers for a TestClient
used as a context manager, so a plain `TestClient(app)` instance never
triggers `marketspike.main.startup()` -- no adapters, no feeds, no network.
`marketspike.main.STATE` is populated directly with a known SlippageModel
instead, keeping the test fully offline.
"""
from fastapi.testclient import TestClient

from marketspike.main import STATE, app
from marketspike.risk.slippage import FEATURE_ORDER, SlippageModel


def test_model_card_reports_source_and_coefficients_for_known_model():
    quantiles = {
        "p50": {"intercept": 0.5, "coefficients": [0.1] * len(FEATURE_ORDER)},
        "p95": {"intercept": 2.0, "coefficients": [0.3] * len(FEATURE_ORDER)},
    }
    model = SlippageModel(
        symbol="BTCUSDT",
        quantiles=quantiles,
        version="btcusdt-2026-08-17T00:00Z",
        source="trained",
        feature_order=FEATURE_ORDER,
    )

    original_models = STATE.get("models")
    original_metrics = STATE.get("model_metrics")
    STATE["models"] = {"BTCUSDT": model}
    STATE["model_metrics"] = {
        "BTCUSDT": {"quantiles": {"p50": {"pinball_model": 0.01}}}
    }
    try:
        client = TestClient(app)
        response = client.get("/api/v1/model/card")

        assert response.status_code == 200
        body = response.json()
        assert body["v"] == 1
        card = body["models"]["BTCUSDT"]
        assert card["source"] == "trained"
        assert card["version"] == "btcusdt-2026-08-17T00:00Z"
        assert card["feature_order"] == FEATURE_ORDER
        assert card["coefficients"] == quantiles
        assert card["metrics"] == STATE["model_metrics"]["BTCUSDT"]
    finally:
        if original_models is None:
            STATE.pop("models", None)
        else:
            STATE["models"] = original_models
        if original_metrics is None:
            STATE.pop("model_metrics", None)
        else:
            STATE["model_metrics"] = original_metrics


def test_model_card_is_empty_when_no_models_loaded():
    original_models = STATE.get("models")
    STATE["models"] = {}
    try:
        client = TestClient(app)
        response = client.get("/api/v1/model/card")

        assert response.status_code == 200
        assert response.json() == {"v": 1, "models": {}}
    finally:
        if original_models is None:
            STATE.pop("models", None)
        else:
            STATE["models"] = original_models
