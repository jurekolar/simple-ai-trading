from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from app.config import Settings

LOGGER = logging.getLogger(__name__)

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
except ImportError:  # pragma: no cover - dependency may not be installed yet
    StockHistoricalDataClient = None
    StockBarsRequest = None
    TimeFrame = None


@dataclass(frozen=True)
class DataLoadResult:
    bars: pd.DataFrame
    source: str


class AlpacaDataClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = None
        if StockHistoricalDataClient is not None:
            self._client = StockHistoricalDataClient(settings.alpaca_api_key, settings.alpaca_secret_key)

    def get_daily_bars(self, symbols: list[str], lookback_days: int) -> DataLoadResult:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=lookback_days)

        if self._client is None or StockBarsRequest is None or TimeFrame is None:
            LOGGER.warning("alpaca-py is unavailable; generating synthetic bars")
            return DataLoadResult(bars=self._synthetic_bars(symbols, start, end), source="synthetic")

        request = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            feed=self._settings.alpaca_data_feed,
        )
        try:
            bars = self._client.get_stock_bars(request).df.reset_index()
        except Exception as exc:  # pragma: no cover - depends on external service state
            LOGGER.warning("alpaca request failed (%s); generating synthetic bars", exc)
            return DataLoadResult(bars=self._synthetic_bars(symbols, start, end), source="fallback")

        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        return DataLoadResult(bars=bars, source="alpaca")

    def _synthetic_bars(self, symbols: list[str], start: datetime, end: datetime) -> pd.DataFrame:
        index = pd.date_range(start=start, end=end, freq="B", tz="UTC")
        frames: list[pd.DataFrame] = []
        for offset, symbol in enumerate(symbols):
            base = 100 + offset * 25
            close = pd.Series(base + pd.RangeIndex(len(index)).to_series().mul(0.2).values, index=index)
            frame = pd.DataFrame(
                {
                    "timestamp": index,
                    "symbol": symbol,
                    "open": close * 0.995,
                    "high": close * 1.01,
                    "low": close * 0.99,
                    "close": close,
                    "volume": 1_000_000,
                }
            )
            frames.append(frame)
        return pd.concat(frames, ignore_index=True)
