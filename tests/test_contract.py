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
    frame_files = sorted(EXAMPLES.glob("frame_*.json"))
    expected_count = len(MODEL_FOR_TYPE)
    assert len(frame_files) == expected_count, (
        f"Expected {expected_count} frame examples, found {len(frame_files)}. "
        f"Run scripts/export_contract.py"
    )
    expected_names = {f"frame_{name}.json" for name in MODEL_FOR_TYPE.keys()}
    actual_names = {f.name for f in frame_files}
    assert actual_names == expected_names, (
        f"Frame filenames mismatch. Expected {expected_names}, got {actual_names}"
    )
    # Also verify the two non-frame examples exist
    assert (EXAMPLES / "size_request.json").exists(), "Missing size_request.json"
    assert (EXAMPLES / "size_response.json").exists(), "Missing size_response.json"


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
