"""Tests for the instrument panel served by the API service itself.

Follows tests/test_predict_route.py's approach: TestClient is used without the
`with` context manager, so Starlette never runs marketspike.main's startup
lifespan handler -- no adapters, no feeds, no network.
"""
from fastapi.testclient import TestClient

from marketspike.main import INDEX_HTML, app

client = TestClient(app)


def test_root_serves_the_instrument_panel():
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "MARKETSPIKE" in response.text


def test_root_serves_the_page_bytes_unchanged():
    assert client.get("/").text == INDEX_HTML.read_text()


def test_serving_the_page_does_not_shadow_the_api():
    # The page is bound to exactly "/", so no API path can be swallowed by it.
    # This is the regression guard for someone later mounting StaticFiles at
    # "/" -- which would match every unclaimed path, including future routes.
    response = client.get("/api/v1/instruments")
    assert response.status_code == 200
    assert response.json()["v"] == 1


def test_repo_root_is_not_published_over_http():
    # index.html is served by an explicit route, not by a StaticFiles mount
    # over the repo root: nothing else in the repository is reachable.
    for path in ("/render.yaml", "/model.json", "/marketspike/config.py",
                 "/static/index.html", "/.git/config"):
        assert client.get(path).status_code == 404, path
