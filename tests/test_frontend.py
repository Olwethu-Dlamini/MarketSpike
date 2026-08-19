"""Tests for the instrument panel served by the API service itself.

Follows tests/test_predict_route.py's approach: TestClient is used without the
`with` context manager, so Starlette never runs marketspike.main's startup
lifespan handler -- no adapters, no feeds, no network.
"""
from fastapi.testclient import TestClient

from marketspike.main import INDEX_HTML, PANEL_DEFAULT_API_BASE, app

client = TestClient(app)


def test_root_serves_the_instrument_panel():
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "MARKETSPIKE" in response.text


def test_page_on_disk_still_carries_the_marker_that_gets_rewritten():
    # The rewrite in main.py is a literal substitution, so it fails silently if
    # the frontend author changes the default. Fail here instead, loudly, with
    # the reason: whoever edits that string must update PANEL_DEFAULT_API_BASE.
    page = INDEX_HTML.read_text(encoding="utf-8")
    assert page.count(PANEL_DEFAULT_API_BASE) == 2, (
        "index.html no longer hardcodes {0} exactly twice (the `api` input's "
        "value attribute and state.apiBase); update PANEL_DEFAULT_API_BASE in "
        "marketspike/main.py to match".format(PANEL_DEFAULT_API_BASE)
    )


def test_served_page_points_at_the_origin_it_was_requested_on():
    # TestClient requests arrive as http://testserver, so that is the origin the
    # served copy must default to -- not the localhost address on disk.
    body = client.get("/").text
    assert body.count("http://testserver") == 2
    assert PANEL_DEFAULT_API_BASE not in body


def test_https_deployment_gets_an_https_base_through_the_proxy():
    # Render terminates TLS and forwards over plain HTTP, so the scheme has to
    # come from the forwarded header. Getting this wrong yields an http:// base
    # on an https:// page: mixed content, plus ws:// instead of wss://.
    body = client.get(
        "/",
        headers={"x-forwarded-proto": "https", "host": "marketspike.onrender.com"},
    ).text
    assert body.count("https://marketspike.onrender.com") == 2
    assert "http://marketspike.onrender.com" not in body


def test_proxy_chain_uses_the_client_facing_scheme():
    body = client.get("/", headers={"x-forwarded-proto": "https,http"}).text
    assert "https://testserver" in body
    assert "http://testserver" not in body


def test_served_page_is_not_cached_by_shared_caches():
    # The body depends on the request's Host, so a shared cache must not hand
    # one visitor's copy to someone who arrived on a different hostname.
    assert client.get("/").headers["cache-control"] == "no-store"


def test_the_page_is_otherwise_untouched():
    # Only the API base is rewritten. Everything else -- markup, styles, script
    # -- is served exactly as the frontend author wrote it.
    on_disk = INDEX_HTML.read_text(encoding="utf-8")
    served = client.get("/").text
    assert served == on_disk.replace(PANEL_DEFAULT_API_BASE, "http://testserver")


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
