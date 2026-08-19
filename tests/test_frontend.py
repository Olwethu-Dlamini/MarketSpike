"""Tests for the instrument panel served by the API service itself.

Follows tests/test_predict_route.py's approach: TestClient is used without the
`with` context manager, so Starlette never runs marketspike.main's startup
lifespan handler -- no adapters, no feeds, no network.
"""
from fastapi.testclient import TestClient

from marketspike.main import FRONTEND_DIR, app

client = TestClient(app)


def test_root_serves_the_instrument_panel():
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "MARKETSPIKE" in response.text


def test_static_mount_serves_the_page_bytes_unchanged():
    response = client.get("/static/index.html")
    assert response.status_code == 200
    assert response.text == (FRONTEND_DIR / "index.html").read_text()


def test_serving_the_page_does_not_shadow_the_api():
    # The page is bound to exactly "/" and the assets to "/static", so an API
    # path can never be swallowed by either. This is the regression guard for
    # someone later mounting StaticFiles at "/" instead.
    response = client.get("/api/v1/instruments")
    assert response.status_code == 200
    assert response.json()["v"] == 1


def test_panel_defaults_to_its_own_origin():
    # The deployed page must connect without anyone retyping a URL: it probes
    # location.origin before falling back to the local dev address.
    page = (FRONTEND_DIR / "index.html").read_text()
    assert "location.origin" in page
    assert 'const LOCAL_API_BASE = "http://localhost:8000";' in page
