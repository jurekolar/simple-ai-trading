from __future__ import annotations

from app.strategy.base import TradingStrategy
from app.strategy.breakout import breakout_strategy
from app.strategy.mean_reversion import mean_reversion_strategy
from app.strategy.momentum import momentum_strategy
from app.strategy.politician_copy import politician_copy_strategy
from app.strategy.trend_trailing_stop import trend_trailing_stop_strategy

_STRATEGIES: dict[str, TradingStrategy] = {
    breakout_strategy.name: breakout_strategy,
    mean_reversion_strategy.name: mean_reversion_strategy,
    momentum_strategy.name: momentum_strategy,
    politician_copy_strategy.name: politician_copy_strategy,
    trend_trailing_stop_strategy.name: trend_trailing_stop_strategy,
}


def get_strategy(name: str) -> TradingStrategy:
    try:
        return _STRATEGIES[name]
    except KeyError as exc:
        available = ", ".join(sorted(_STRATEGIES))
        raise ValueError(f"unknown strategy '{name}'. Available strategies: {available}") from exc


def strategy_names() -> list[str]:
    return sorted(_STRATEGIES)


def backtest_strategy_names() -> list[str]:
    return sorted(name for name in _STRATEGIES if name != politician_copy_strategy.name)
