import json
import os
from dataclasses import dataclass
from types import MappingProxyType
from typing import Dict, List, Mapping

_PATH = os.path.join(os.path.dirname(__file__), "instruments.json")


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str
    pip_size: float
    contract_size: float
    quote_ccy: str
    min_lot: float
    lot_step: float
    margin_rate: float

    def pip_value(self, fx_rate: float) -> float:
        """Account-currency value of one pip on one lot.

        Derived, never stored: the draft hardcoded 10.0, which is a
        USD-quoted-major assumption presented as a universal constant.
        """
        return self.pip_size * self.contract_size * fx_rate


def _load() -> Dict[str, InstrumentSpec]:
    with open(_PATH, "r") as handle:
        raw = json.load(handle)
    return {
        symbol: InstrumentSpec(symbol=symbol, **fields)
        for symbol, fields in raw.items()
    }


Symbol = str

# `InstrumentSpec` is frozen, so individual specs can't be mutated. But a
# plain dict is still reassignable at the item level (`REGISTRY["X"] = ...`),
# and REGISTRY is shared, module-level, import-time-initialized state. Wrap
# it in a read-only view so accidental mutation raises immediately instead
# of silently corrupting every caller that shares this object.
REGISTRY: Mapping[Symbol, InstrumentSpec] = MappingProxyType(_load())


def get_instrument(symbol: str) -> InstrumentSpec:
    return REGISTRY[symbol]


def all_instruments() -> List[InstrumentSpec]:
    return list(REGISTRY.values())
