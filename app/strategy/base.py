from __future__ import annotations

from typing import Protocol

import pandas as pd

from app.config import Settings


class TradingStrategy(Protocol):
    name: str

    def generate_signals(self, bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        ...
