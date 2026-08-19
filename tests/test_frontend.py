"""Tests for the instrument panel served by the API service itself.

Follows tests/test_predict_route.py's approach: TestClient is used without the
`with` context manager, so Starlette never runs marketspike.main's startup
lifespan handler -- no adapters, no feeds, no network.
"""
import yaml
from fastapi.testclient import TestClient

from marketspike.config import _split
from marketspike.main import (
    INDEX_HTML,
    PANEL_DEFAULT_API_BASE,
    app,
)

client = TestClient(app)


def test_root_serves_the_instrument_panel():
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "MARKETSPIKE" in response.text


def test_page_on_disk_still_carries_the_marker_that_gets_rewritten():
    # The rewrite in main.py is a literal substitution, so it fails silently if
    # the frontend's default changes. Fail here instead, loudly, with the
    # reason: whoever edits that string must update PANEL_DEFAULT_API_BASE.
    page = INDEX_HTML.read_text(encoding="utf-8")
    assert page.count(PANEL_DEFAULT_API_BASE) == 2, (
        "index.html no longer hardcodes {0} exactly twice (the `api` input's "
        "value attribute and state.apiBase); update PANEL_DEFAULT_API_BASE in "
        "marketspike/main.py to match".format(PANEL_DEFAULT_API_BASE)
    )


def test_the_file_defaults_to_the_live_service_not_localhost():
    # Copies of this file that no backend serves -- the GitHub Pages build of
    # the repo root, or one opened straight off disk -- get no rewrite, so the
    # value committed here is the one they use. It must be the live service.
    page = INDEX_HTML.read_text(encoding="utf-8")
    assert "localhost" not in page
    assert page.count("https://marketspike.onrender.com") == 2


def test_deployed_cors_allow_list_covers_the_pages_copy():
    # The GitHub Pages copy of index.html calls this API cross-origin, so its
    # origin needs an entry in MS_CORS_ORIGINS or every request fails preflight
    # and that copy shows preview mode. render.yaml is what sets it in
    # production, so assert the deployed value parses and contains the origin.
    render_yaml = (INDEX_HTML.parent / "render.yaml").read_text(encoding="utf-8")
    blueprint = yaml.safe_load(render_yaml)
    env = {
        entry["key"]: entry.get("value")
        for entry in blueprint["services"][0]["envVars"]
    }
    assert "https://olwethu-dlamini.github.io" in _split(env["MS_CORS_ORIGINS"])


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


def _render_size_result_body() -> str:
    """The body of the panel's renderSizeResult(), with // comments stripped.

    Scoped to that one function because `res` is a local name reused by other
    handlers (/instruments, /slippage/predict) and by fetchJSON, and comments
    quote the old field names on purpose.
    """
    page = INDEX_HTML.read_text(encoding="utf-8")
    start = page.index("function renderSizeResult(")
    end = page.index("\n  }", start)
    body = page[start:end]
    return "\n".join(
        line for line in body.split("\n") if not line.strip().startswith("//")
    )


def test_panel_reads_the_lot_field_the_api_actually_returns():
    """Guard the bug this replaced: the panel read `res.lots`, which
    POST /api/v1/size has never returned, so its headline number -- the lot
    size, the one figure the panel exists to show -- rendered as an em dash on
    every live request. Assert it names the real fields, not the invented one.
    """
    body = _render_size_result_body()
    assert "res.recommended_lot_size" in body
    assert "res.naive_lot_size" in body
    assert "res.lots" not in body


def test_size_request_carries_the_symbol_and_direction():
    # Direction changes the answer (a sell is sized off a different slippage
    # quantile than a buy), and symbol selects the instrument spec that turns a
    # risk budget into lots. Both must reach the endpoint.
    page = INDEX_HTML.read_text(encoding="utf-8")
    body = page.split('"/api/v1/size"', 1)[1][:600]
    assert "symbol: state.symbol" in body
    assert "direction: state.direction" in body


def test_panel_field_names_match_the_size_response_schema():
    """Every `res.<field>` the panel reads must exist in the response model, so
    a rename on the backend fails here rather than silently rendering dashes.
    """
    import re

    from marketspike.api import schemas

    read = set(re.findall(r"\bres\.([a-z_][a-z0-9_]*)\b", _render_size_result_body()))
    model = next(
        cls for name, cls in vars(schemas).items()
        if name.lower().startswith("size") and hasattr(cls, "model_fields")
        and "recommended_lot_size" in cls.model_fields
    )
    unknown = read - set(model.model_fields)
    assert not unknown, "panel reads fields absent from {0}: {1}".format(
        model.__name__, sorted(unknown)
    )


def test_tape_is_capped_to_what_the_strip_can_show():
    """The footer 'crackle': the tape kept 40 rows in a 230px column-reverse box
    with overflow hidden and inserted one per frame at the feed's rate (20/s),
    so every tick reflowed 40 rows directly above the footer. Assert the cap is
    small, the draw rate is decoupled from the feed, and the box is contained.
    """
    page = INDEX_HTML.read_text(encoding="utf-8")
    assert "const TAPE_ROWS = 12;" in page
    assert "const TAPE_HZ" in page
    assert "contain:content; overflow-anchor:none;" in page
    # The row cap must be enforced against TAPE_ROWS, not a literal 40.
    assert "strip.children.length > TAPE_ROWS" in page
    assert "strip.children.length > 40" not in page
