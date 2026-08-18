# MarketSpike API — v1 (FROZEN)

Build against `examples/*.json`. Every payload here is literal and validated
in CI by `tests/test_contract.py`.

- REST base: `http://localhost:8000/api/v1`
- WebSocket:  `ws://localhost:8000/ws/v1/stream`

## Rules

1. Every frame carries `"v": 1`. Changes within v1 are **additive only**.
2. **Ignore unknown fields.** Adding a field is never a breaking change.
3. **`source` must be surfaced in the UI.** Any value other than `"measured"`
   MUST be visibly badged. `"simulated"` means replay data.
4. `regime_change` fires only on transition — not per tick.
5. Errors never close the socket.
6. REST errors are RFC 7807 problem details.

## Client → server frames

    {"type":"subscribe","symbols":["BTCUSDT","EURUSD"],
     "channels":["tick","regime","latency","event","market","replay"]}
    {"type":"unsubscribe","symbols":["EURUSD"]}
    {"type":"clock_sync","client_send_ns":1723891200123456789}
    {"type":"ack","seq":4471,"client_recv_ns":1723891200987654321}

The full channel set is `tick`, `regime`, `latency`, `event`, `market`, and
`replay` (the last two gate `market_state` and `replay_state` frames).
Omitting a channel from `subscribe` now suppresses frames on that channel —
including `market`/`replay`, which used to be delivered unconditionally.
Frame types with no channel (e.g. `hello`, `clock_sync_reply`, `error`) are
always delivered and are unaffected by this filter.
