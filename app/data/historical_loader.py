from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.config import Settings
from app.data.alpaca_data import AlpacaDataClient


@dataclass(frozen=True)
class LoadedBars:
    bars: pd.DataFrame
    source: str
    production_safe: bool


@dataclass(frozen=True)
class DataValidationReport:
    valid_bars: pd.DataFrame
    failed_symbols: dict[str, str]

    @property
    def has_partial_failure(self) -> bool:
        return bool(self.failed_symbols)


def required_market_symbols(settings: Settings, strategy_name: str | None = None) -> list[str]:
    symbols = list(settings.symbol_list)
    if strategy_name == "mean_reversion":
        benchmark_symbol = settings.mean_reversion_benchmark_symbol.strip().upper()
        if benchmark_symbol and benchmark_symbol not in symbols:
            symbols.append(benchmark_symbol)
    return symbols


def load_bars(settings: Settings, strategy_name: str | None = None) -> pd.DataFrame:
    return load_bars_with_source(settings, strategy_name).bars


def load_bars_with_source(settings: Settings, strategy_name: str | None = None) -> LoadedBars:
    client = AlpacaDataClient(settings)
    result = client.get_daily_bars(required_market_symbols(settings, strategy_name), settings.lookback_days)
    bars = result.bars
    required_columns = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
    missing = required_columns.difference(bars.columns)
    if missing:
        raise ValueError(f"Missing required bar columns: {sorted(missing)}")
    return LoadedBars(
        bars=bars.sort_values(["symbol", "timestamp"]).reset_index(drop=True),
        source=result.source,
        production_safe=result.production_safe,
    )


def validate_bars(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_name: str | None = None,
) -> DataValidationReport:
    required_symbols = required_market_symbols(settings, strategy_name)
    if bars.empty:
        return DataValidationReport(
            valid_bars=bars.copy(),
            failed_symbols={symbol: "missing_symbol" for symbol in required_symbols},
        )

    frame = bars.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    failed_symbols: dict[str, str] = {}
    valid_symbols: list[str] = []

    for symbol in required_symbols:
        symbol_frame = frame[frame["symbol"] == symbol].sort_values("timestamp")
        if symbol_frame.empty:
            failed_symbols[symbol] = "missing_symbol"
            continue
        if symbol_frame["timestamp"].isna().any():
            failed_symbols[symbol] = "invalid_timestamp"
            continue
        if symbol_frame["timestamp"].duplicated().any():
            failed_symbols[symbol] = "duplicate_timestamp"
            continue
        if symbol_frame[["open", "high", "low", "close", "volume"]].isna().any().any():
            failed_symbols[symbol] = "nan_values"
            continue
        if len(symbol_frame) < settings.min_history_days:
            failed_symbols[symbol] = "insufficient_history"
            continue
        if (symbol_frame["high"] < symbol_frame["low"]).any():
            failed_symbols[symbol] = "invalid_range"
            continue
        if ((symbol_frame["close"] <= 0) | (symbol_frame["open"] <= 0)).any():
            failed_symbols[symbol] = "non_positive_prices"
            continue
        close_change = symbol_frame["close"].pct_change().abs()
        if close_change.gt(0.8).any():
            failed_symbols[symbol] = "split_adjustment_inconsistent"
            continue
        valid_symbols.append(symbol)

    valid_bars = frame[frame["symbol"].isin(valid_symbols)].copy()
    return DataValidationReport(
        valid_bars=valid_bars.sort_values(["symbol", "timestamp"]).reset_index(drop=True),
        failed_symbols=failed_symbols,
    )
