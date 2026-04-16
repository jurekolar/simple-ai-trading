from __future__ import annotations

from app.strategy.base import TradingStrategy
from app.strategy.mean_reversion import mean_reversion_strategy
from app.strategy.momentum import momentum_strategy

_STRATEGIES: dict[str, TradingStrategy] = {
    mean_reversion_strategy.name: mean_reversion_strategy,
    momentum_strategy.name: momentum_strategy,
}


def get_strategy(name: str) -> TradingStrategy:
    try:
        return _STRATEGIES[name]
    except KeyError as exc:
        available = ", ".join(sorted(_STRATEGIES))
        raise ValueError(f"unknown strategy '{name}'. Available strategies: {available}") from exc


def strategy_names() -> list[str]:
    return sorted(_STRATEGIES)
