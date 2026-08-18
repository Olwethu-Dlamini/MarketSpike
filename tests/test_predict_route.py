"""Tests for POST /api/v1/slippage/predict (Task 22).

Follows tests/test_model_card.py's approach: FastAPI's TestClient is used
without the `with` context manager, so Starlette never runs
`marketspike.main`'s startup lifespan handler (no adapters, no feeds, no
network) and `marketspike.main.STATE` can be populated directly with a
known model, keeping the test fully offline.
"""
from fastapi.testclient import TestClient

from marketspike.main import STATE, app
from marketspike.risk.slippage import fallback_model


def _client_with_fallback_model():
    original_models = STATE.get("models")
    STATE["models"] = {"EURUSD": fallback_model("EURUSD")}
    return TestClient(app), original_models


def _restore(original_models):
    if original_models is None:
        STATE.pop("models", None)
    else:
        STATE["models"] = original_models


def test_predict_returns_both_quantiles():
    client, original_models = _client_with_fallback_model()
    try:
        response = client.post(
            "/api/v1/slippage/predict",
            json={
                "symbol": "EURUSD", "spread_bps": 2.0, "v_ratio": 3.0,
                "latency_ms": 80.0, "spread_z": 4.0,
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["p95_bps"] >= body["p50_bps"] >= 0.0
        assert body["model_source"] == "fallback_coefficients"
        assert body["model_version"] == "fallback-v1"
    finally:
        _restore(original_models)


def test_predict_rejects_unknown_symbol():
    client, original_models = _client_with_fallback_model()
    try:
        response = client.post(
            "/api/v1/slippage/predict", json={"symbol": "GBPJPY"}
        )
        assert response.status_code == 404
        detail = response.json()["detail"]
        assert detail["status"] == 404
        assert detail["type"] == "/errors/unknown-symbol"
    finally:
        _restore(original_models)


def test_higher_latency_predicts_higher_cost():
    client, original_models = _client_with_fallback_model()
    try:
        base = {"symbol": "EURUSD", "spread_bps": 2.0, "v_ratio": 1.0, "spread_z": 0.0}
        slow = client.post(
            "/api/v1/slippage/predict", json=dict(base, latency_ms=500.0)
        )
        fast = client.post(
            "/api/v1/slippage/predict", json=dict(base, latency_ms=5.0)
        )
        assert slow.json()["p95_bps"] > fast.json()["p95_bps"]
    finally:
        _restore(original_models)
