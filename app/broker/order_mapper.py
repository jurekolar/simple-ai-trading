from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    qty: int
    side: str
    close: float
