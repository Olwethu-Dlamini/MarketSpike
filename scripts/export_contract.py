"""Generate example payloads in docs/api/examples/*.json from the models."""
import json
import pathlib

from marketspike.api import schemas

OUT = pathlib.Path("docs/api")
EX = OUT / "examples"

FRAMES = {
    "hello": schemas.HelloFrame(
        seq=1, server_ts_ns=1723891200000000000, session_id="a1b2",
        server_version="1.0.0", warmup_complete=False,
        feeds={"BTCUSDT": "binance", "EURUSD": "oanda"}, mode="live",
    ),
    "tick": schemas.TickFrame(
        seq=2, server_ts_ns=1723891200100000000, symbol="EURUSD",
        bid=1.08512, ask=1.08525, mid=1.085185, spread_bps=1.20,
        spread_pips=1.3, quote_rate_hz=6.4, book_imbalance=-0.13,
        tradeable=True, source="measured",
    ),
    "latency": schemas.LatencyFrame(
        seq=3, server_ts_ns=1723891200200000000, symbol="EURUSD",
        excess_transit_us=4100, engine_us=180, delivery_us=19400,
        p50_us=21000, p95_us=68000, p99_us=141000, source="estimated",
    ),
    "regime_change": schemas.RegimeChangeFrame(
        seq=4, server_ts_ns=1723891200300000000, symbol="EURUSD",
        from_state="ELEVATED", to_state="SPIKE", score=3.1, v_ratio=4.8,
        spread_z=6.2, event_context="EVENT_WINDOW", trigger="vol_ratio",
    ),
    "event_alert": schemas.EventAlertFrame(
        seq=5, server_ts_ns=1723891200400000000, name="US CPI (YoY)",
        importance="high", event_ts_ns=1723891800000000000,
        seconds_until=1800, phase="PRE_EVENT", affects=["EURUSD", "BTCUSDT"],
    ),
    "market_state": schemas.MarketStateFrame(
        seq=6, server_ts_ns=1723891200500000000, symbol="EURUSD",
        tradeable=False, reason="market_closed",
        next_open_ts_ns=1723921200000000000,
    ),
    "replay_state": schemas.ReplayStateFrame(
        seq=7, server_ts_ns=1723891200600000000, mode="replay",
        scenario="cpi_2026_07_11", progress_pct=34.2,
    ),
    "clock_sync_reply": schemas.ClockSyncReply(
        seq=8, server_ts_ns=1723891200700000000,
        client_send_ns=1723891200123456789,
        server_recv_ns=1723891200141902311,
        server_send_ns=1723891200141998042,
    ),
    "error": schemas.ErrorFrame(
        seq=9, server_ts_ns=1723891200800000000, code="UNKNOWN_SYMBOL",
        detail="GBPJPY is not in the instrument registry",
    ),
}

SIZE_REQUEST = schemas.SizeRequest(
    symbol="EURUSD", account_balance_minor=1000000, account_ccy="USD",
    risk_pct=1.0, stop_distance_price=0.0020, direction="buy",
    quantile="p95", free_margin_minor=1000000, assumed_latency_ms=None,
)

SIZE_RESPONSE = schemas.SizeResponse(
    naive_lot_size=0.50, recommended_lot_size=0.38, overexposure_pct=31.6,
    slippage_p50_pips=1.4, slippage_p95_pips=6.2, stop_distance_pips=20.0,
    effective_adverse_pips=26.2, actual_risk_amount_minor=9956,
    actual_risk_pct=0.9956, required_margin_minor=137296, capped_by=None,
    fx_assumed=False, stale_quote=False, model_source="trained",
    model_version="eurusd-2026-08-17T04:12Z", regime_at_calc="SPIKE",
    event_context="EVENT_WINDOW", latency_used_ms=63.2,
    latency_source="measured", warnings=[],
    inputs_echo=SIZE_REQUEST.model_dump(),
)


def main() -> None:
    EX.mkdir(parents=True, exist_ok=True)
    for name, frame in FRAMES.items():
        path = EX / "frame_{0}.json".format(name)
        path.write_text(json.dumps(frame.model_dump(by_alias=True), indent=2))
    (EX / "size_request.json").write_text(
        json.dumps(SIZE_REQUEST.model_dump(), indent=2)
    )
    (EX / "size_response.json").write_text(
        json.dumps(SIZE_RESPONSE.model_dump(), indent=2)
    )
    print("wrote {0} examples to {1}".format(len(FRAMES) + 2, EX))


if __name__ == "__main__":
    main()
