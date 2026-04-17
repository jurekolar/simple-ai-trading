from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from app.backtest.compare import compare_strategies, format_strategy_comparison
from app.backtest.engine import run_backtest
from app.backtest.metrics import (
    annualize_return,
    annualize_volatility,
    compute_max_drawdown,
    risk_adjusted_score,
    sharpe_like_ratio,
)
from app.config import Settings
from app.strategy import get_strategy


def _daily_bars(symbol: str, closes: list[float]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, close in enumerate(closes):
        open_price = closes[index - 1] if index > 0 else close
        rows.append(
            {
                "timestamp": datetime(2026, 1, index + 1, 20, 0, tzinfo=UTC),
                "symbol": symbol,
                "open": float(open_price),
                "high": float(max(open_price, close) + 1.0),
                "low": float(min(open_price, close) - 1.0),
                "close": float(close),
                "volume": 1_000_000.0,
            }
        )
    return rows


def test_metric_helpers_compute_expected_values() -> None:
    equity_curve = pd.DataFrame({"equity": [100_000.0, 110_000.0, 99_000.0, 120_000.0]})
    daily_returns = equity_curve["equity"].pct_change().fillna(0.0)

    assert compute_max_drawdown(equity_curve) == pytest.approx(0.1)
    assert annualize_return(100_000.0, 120_000.0, 4) > 0.0
    assert annualize_volatility(daily_returns) > 0.0
    assert sharpe_like_ratio(daily_returns) != 0.0
    assert risk_adjusted_score(sharpe_like=1.5, max_drawdown=0.2) == 1.3


def test_run_backtest_produces_round_trip_log_and_metrics() -> None:
    settings = Settings(
        SYMBOLS="SPY",
        TREND_WINDOW=2,
        EXIT_WINDOW=2,
        ATR_WINDOW=2,
        MIN_AVERAGE_DAILY_VOLUME=100,
        MAX_ATR_RATIO=1.0,
        MAX_SYMBOLS_PER_RUN=1,
        MAX_POSITION_NOTIONAL=10_000,
        MAX_SYMBOL_EXPOSURE=10_000,
        MAX_GROSS_EXPOSURE=10_000,
    )
    bars = pd.DataFrame(_daily_bars("SPY", [100.0, 101.0, 102.0, 99.0]))

    trades, metrics = run_backtest(bars, settings, strategy=get_strategy("momentum"))

    assert not trades.empty
    assert set(trades["side"]) == {"buy", "sell"}
    assert float(trades.iloc[0]["fill_price"]) > float(trades.iloc[0]["close"])
    assert float(trades.iloc[-1]["fill_price"]) < float(trades.iloc[-1]["close"])
    assert metrics["trades"] >= 2
    assert metrics["closed_trades"] >= 1


def test_run_backtest_handles_no_signal_case() -> None:
    settings = Settings(
        SYMBOLS="SPY",
        TREND_WINDOW=2,
        EXIT_WINDOW=2,
        ATR_WINDOW=2,
        MIN_AVERAGE_DAILY_VOLUME=100,
        MAX_ATR_RATIO=1.0,
        MAX_SYMBOLS_PER_RUN=1,
    )
    bars = pd.DataFrame(_daily_bars("SPY", [100.0, 100.0, 100.0, 100.0]))

    trades, metrics = run_backtest(bars, settings, strategy=get_strategy("momentum"))

    assert trades.empty
    assert metrics["trades"] == 0.0
    assert metrics["total_return"] == 0.0


def test_compare_strategies_ranks_by_risk_adjusted_score() -> None:
    settings = Settings(MAX_SYMBOLS_PER_RUN=3)
    bars = pd.DataFrame(
        _daily_bars("SPY", [100.0, 101.0, 102.0, 99.0, 103.0, 98.0, 105.0])
        + _daily_bars("QQQ", [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0])
        + _daily_bars("IWM", [50.0, 49.0, 48.0, 47.0, 46.0, 45.0, 44.0])
        + _daily_bars("AAPL", [100.0, 103.0, 106.0, 109.0, 90.0, 92.0, 95.0])
        + _daily_bars("MSFT", [200.0, 201.0, 202.0, 203.0, 204.0, 205.0, 206.0])
    )

    summary = compare_strategies(bars, settings)

    assert summary.iloc[0]["rank"] == 1
    assert bool(summary.iloc[0]["winner"])
    assert set(summary["strategy"]) == {"momentum", "mean_reversion", "breakout", "trend_trailing_stop"}
    assert summary["risk_adjusted_score"].tolist() == sorted(summary["risk_adjusted_score"], reverse=True)


def test_format_strategy_comparison_includes_rank_and_winner() -> None:
    summary = pd.DataFrame(
        [
            {
                "rank": 1,
                "winner": True,
                "strategy": "breakout",
                "risk_adjusted_score": 1.2,
                "sharpe_like": 1.4,
                "total_return": 0.15,
                "annualized_return": 0.12,
                "max_drawdown": 0.05,
                "volatility": 0.10,
                "win_rate": 0.6,
                "trades": 12.0,
                "closed_trades": 6.0,
                "avg_holding_days": 18.0,
                "turnover": 0.8,
                "gross_exposure_usage": 0.35,
                "gross_exposure": 35_000.0,
            }
        ]
    )

    formatted = format_strategy_comparison(summary)

    assert "rank" in formatted.lower()
    assert "breakout" in formatted
    assert "*" in formatted
