from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    qty: float
    side: str
    close: float
