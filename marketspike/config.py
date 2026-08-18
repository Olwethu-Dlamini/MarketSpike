import os
from dataclasses import dataclass, field
from typing import List, Optional


def _split(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class Settings:
    symbols: List[str] = field(default_factory=list)
    db_path: str = "./marketspike.db"
    oanda_token: Optional[str] = None
    oanda_account_id: Optional[str] = None
    tau_fast_s: float = 30.0
    tau_slow_s: float = 1800.0
    vol_sample_interval_s: float = 1.0
    skew_window_s: float = 60.0
    ws_max_hz: float = 20.0
    model_path: str = "./model.json"
    max_tick_age_hours: int = 0


def get_settings() -> Settings:
    return Settings(
        symbols=_split(os.getenv("MS_SYMBOLS", "BTCUSDT,EURUSD")),
        db_path=os.getenv("MS_DB_PATH", "./marketspike.db"),
        oanda_token=os.getenv("MS_OANDA_TOKEN") or None,
        oanda_account_id=os.getenv("MS_OANDA_ACCOUNT_ID") or None,
        tau_fast_s=float(os.getenv("MS_TAU_FAST_S", "30")),
        tau_slow_s=float(os.getenv("MS_TAU_SLOW_S", "1800")),
        vol_sample_interval_s=float(os.getenv("MS_VOL_SAMPLE_INTERVAL_S", "1.0")),
        skew_window_s=float(os.getenv("MS_SKEW_WINDOW_S", "60")),
        ws_max_hz=float(os.getenv("MS_WS_MAX_HZ", "20")),
        model_path=os.getenv("MS_MODEL_PATH", "./model.json"),
        max_tick_age_hours=int(os.getenv("MS_MAX_TICK_AGE_HOURS", "0")),
    )
