import json
import pathlib

import pytest

from marketspike.api import schemas

EXAMPLES = pathlib.Path("docs/api/examples")

MODEL_FOR_TYPE = {
    "hello": schemas.HelloFrame,
    "tick": schemas.TickFrame,
    "latency": schemas.LatencyFrame,
    "regime_change": schemas.RegimeChangeFrame,
    "event_alert": schemas.EventAlertFrame,
    "market_state": schemas.MarketStateFrame,
    "replay_state": schemas.ReplayStateFrame,
    "clock_sync_reply": schemas.ClockSyncReply,
    "error": schemas.ErrorFrame,
}


def test_examples_directory_is_populated():
    assert list(EXAMPLES.glob("*.json")), "run scripts/export_contract.py"


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("frame_*.json")))
def test_frame_examples_validate(path):
    payload = json.loads(path.read_text())
    model = MODEL_FOR_TYPE[payload["type"]]
    parsed = model.model_validate(payload)
    assert parsed.v == 1


def test_size_example_validates():
    req = json.loads((EXAMPLES / "size_request.json").read_text())
    res = json.loads((EXAMPLES / "size_response.json").read_text())
    schemas.SizeRequest.model_validate(req)
    parsed = schemas.SizeResponse.model_validate(res)
    assert parsed.recommended_lot_size <= parsed.naive_lot_size
