from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

SCHEMA_V = 1
Source = str  # "measured" | "estimated" | "simulated"


class Frame(BaseModel):
    v: int = SCHEMA_V
    type: str
    seq: int
    server_ts_ns: int


class HelloFrame(Frame):
    type: str = "hello"
    session_id: str
    server_version: str
    warmup_complete: bool
    feeds: Dict[str, str]
    mode: str


class TickFrame(Frame):
    type: str = "tick"
    symbol: str
    bid: float
    ask: float
    mid: float
    spread_bps: float
    spread_pips: float
    quote_rate_hz: float
    book_imbalance: float
    tradeable: bool
    source: Source


class LatencyFrame(Frame):
    type: str = "latency"
    symbol: str
    excess_transit_us: int
    engine_us: int
    delivery_us: Optional[int] = None
    p50_us: int
    p95_us: int
    p99_us: int
    source: Source
    baseline_includes_clock_offset: bool = True


class RegimeChangeFrame(Frame):
    type: str = "regime_change"
    symbol: str
    from_state: str = Field(alias="from")
    to_state: str = Field(alias="to")
    score: float
    v_ratio: float
    spread_z: float
    event_context: str
    trigger: str

    model_config = {"populate_by_name": True}


class EventAlertFrame(Frame):
    type: str = "event_alert"
    name: str
    importance: str
    event_ts_ns: int
    seconds_until: int
    phase: str
    affects: List[str]


class MarketStateFrame(Frame):
    type: str = "market_state"
    symbol: str
    tradeable: bool
    reason: str
    next_open_ts_ns: Optional[int] = None


class ReplayStateFrame(Frame):
    type: str = "replay_state"
    mode: str
    scenario: str
    progress_pct: float
    source: Source = "simulated"


class ClockSyncReply(Frame):
    type: str = "clock_sync_reply"
    client_send_ns: int
    server_recv_ns: int
    server_send_ns: int


class ErrorFrame(Frame):
    type: str = "error"
    code: str
    detail: str


class Instrument(BaseModel):
    symbol: str
    pip_size: float
    contract_size: float
    quote_ccy: str
    min_lot: float
    lot_step: float
    margin_rate: float


class SizeRequest(BaseModel):
    symbol: str
    account_balance_minor: int
    account_ccy: str = "USD"
    risk_pct: float
    stop_distance_price: float
    direction: str = "buy"
    quantile: str = "p95"
    free_margin_minor: int
    assumed_latency_ms: Optional[float] = None


class SizeResponse(BaseModel):
    naive_lot_size: float
    recommended_lot_size: float
    overexposure_pct: float
    slippage_p50_pips: float
    slippage_p95_pips: float
    stop_distance_pips: float
    effective_adverse_pips: float
    actual_risk_amount_minor: int
    actual_risk_pct: float
    required_margin_minor: int
    capped_by: Optional[str] = None
    fx_assumed: bool = False
    stale_quote: bool = False
    model_source: str
    model_version: str
    regime_at_calc: str
    event_context: str
    latency_used_ms: float
    latency_source: Source
    warnings: List[str] = Field(default_factory=list)
    inputs_echo: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"protected_namespaces": ()}


class FeedHealth(BaseModel):
    venue: str
    connected: bool
    last_tick_age_ms: Optional[int] = None
    warmup_complete: bool = False
    tradeable: bool = True
    reason: Optional[str] = None


class HealthResponse(BaseModel):
    v: int = SCHEMA_V
    status: str
    uptime_s: int
    feeds: Dict[str, FeedHealth]
    counters: Dict[str, int]
    model: Dict[str, str]
    mode: str

    model_config = {"protected_namespaces": ()}
