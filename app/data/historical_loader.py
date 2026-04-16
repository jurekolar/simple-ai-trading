from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.config import Settings
from app.data.alpaca_data import AlpacaDataClient


@dataclass(frozen=True)
class LoadedBars:
    bars: pd.DataFrame
    source: str


def load_bars(settings: Settings) -> pd.DataFrame:
    return load_bars_with_source(settings).bars


def load_bars_with_source(settings: Settings) -> LoadedBars:
    client = AlpacaDataClient(settings)
    result = client.get_daily_bars(settings.symbol_list, settings.lookback_days)
    bars = result.bars
    required_columns = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
    missing = required_columns.difference(bars.columns)
    if missing:
        raise ValueError(f"Missing required bar columns: {sorted(missing)}")
    return LoadedBars(
        bars=bars.sort_values(["symbol", "timestamp"]).reset_index(drop=True),
        source=result.source,
    )
