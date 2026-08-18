"""Build a replay scenario, either from recorded ticks or OANDA history.

    # From your own recording, by time range:
    python scripts/capture_scenario.py from-db --db marketspike.db \
        --symbol BTCUSDT --start-ns 0 --end-ns 9999999999999999999 \
        --out scenarios/btc_spike.ndjson

    # From real OANDA history around a past release:
    python scripts/capture_scenario.py from-oanda --symbol EURUSD \
        --from 2026-07-11T12:00:00Z --to 2026-07-11T13:00:00Z \
        --out scenarios/cpi_2026_07_11.ndjson
"""
import argparse
import os

import httpx

from marketspike.feeds.base import Tick, rfc3339_to_ns
from marketspike.feeds.replay import write_scenario

CANDLES_URL = "https://api-fxpractice.oanda.com/v3/instruments/{0}/candles"


def from_db(args) -> None:
    import sqlite3

    conn = sqlite3.connect(args.db)
    rows = conn.execute(
        "SELECT symbol, venue_ts_ns, recv_ts_ns, bid, ask, bid_qty, ask_qty, tradeable "
        "FROM ticks WHERE symbol = ? AND recv_ts_ns BETWEEN ? AND ? ORDER BY recv_ts_ns",
        (args.symbol, args.start_ns, args.end_ns),
    )
    ticks = [
        Tick(
            symbol=r[0], venue_ts_ns=r[1], recv_ts_ns=r[2], bid=r[3], ask=r[4],
            bid_qty=r[5] or 0.0, ask_qty=r[6] or 0.0, tradeable=bool(r[7]),
            source="simulated",
        )
        for r in rows
    ]
    print("captured {0} ticks -> {1}".format(write_scenario(args.out, ticks), args.out))


def from_oanda(args) -> None:
    """S5 bid/ask candles become one synthetic tick per bar close.

    Real spreads from a real release, at 5-second resolution. Coarser than
    live ticks, but the widening pattern is genuine (spec §3.3).

    Requires MS_OANDA_TOKEN in the environment. Not exercised in this task
    (no credentials available in this environment) -- implemented faithfully
    against the OANDA v3 candles API but left unrun.
    """
    token = os.environ.get("MS_OANDA_TOKEN")
    if not token:
        raise SystemExit("MS_OANDA_TOKEN is not set")

    instrument = args.symbol[:3] + "_" + args.symbol[3:]
    response = httpx.get(
        CANDLES_URL.format(instrument),
        params={
            "price": "BA", "granularity": "S5",
            "from": getattr(args, "from"), "to": args.to,
        },
        headers={"Authorization": "Bearer {0}".format(token)},
        timeout=30.0,
    )
    response.raise_for_status()

    ticks = []
    for candle in response.json().get("candles", []):
        if not candle.get("complete"):
            continue
        ts_ns = rfc3339_to_ns(candle["time"])
        ticks.append(
            Tick(
                symbol=args.symbol,
                venue_ts_ns=ts_ns - 2_000_000,
                recv_ts_ns=ts_ns,
                bid=float(candle["bid"]["c"]),
                ask=float(candle["ask"]["c"]),
                bid_qty=1_000_000.0, ask_qty=1_000_000.0,
                tradeable=True, source="simulated",
            )
        )
    print("captured {0} ticks -> {1}".format(write_scenario(args.out, ticks), args.out))


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    db = sub.add_parser("from-db")
    db.add_argument("--db", default="marketspike.db")
    db.add_argument("--symbol", required=True)
    db.add_argument("--start-ns", type=int, required=True)
    db.add_argument("--end-ns", type=int, required=True)
    db.add_argument("--out", required=True)
    db.set_defaults(func=from_db)

    oanda = sub.add_parser("from-oanda")
    oanda.add_argument("--symbol", default="EURUSD")
    oanda.add_argument("--from", dest="from", required=True)
    oanda.add_argument("--to", required=True)
    oanda.add_argument("--out", required=True)
    oanda.set_defaults(func=from_oanda)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
