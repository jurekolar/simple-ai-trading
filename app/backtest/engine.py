from __future__ import annotations

import pandas as pd

from app.backtest.fills import apply_slippage
from app.backtest.metrics import summarize
from app.config import Settings
from app.risk.checks import filter_trade_candidates
from app.strategy import get_strategy
from app.strategy.base import TradingStrategy
from app.strategy.signals import latest_signals


def run_backtest(
    bars: pd.DataFrame,
    settings: Settings,
    strategy: TradingStrategy | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    active_strategy = strategy or get_strategy("momentum")
    signal_frame = active_strategy.generate_signals(bars, settings)
    latest = latest_signals(signal_frame)
    trades = filter_trade_candidates(latest, settings)
    trades = apply_slippage(trades)
    return trades, summarize(trades)
