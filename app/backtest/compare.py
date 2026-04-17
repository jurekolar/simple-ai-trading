from __future__ import annotations

import pandas as pd

from app.backtest.engine import run_backtest
from app.config import Settings
from app.strategy import backtest_strategy_names, get_strategy


def compare_strategies(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_names: list[str] | None = None,
) -> pd.DataFrame:
    names = strategy_names or backtest_strategy_names()
    rows: list[dict[str, object]] = []
    for strategy_name in names:
        strategy = get_strategy(strategy_name)
        _, metrics = run_backtest(bars, settings, strategy=strategy)
        rows.append({"strategy": strategy_name, **metrics})

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    summary = summary.sort_values(
        ["risk_adjusted_score", "max_drawdown", "total_return", "strategy"],
        ascending=[False, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    summary["rank"] = range(1, len(summary) + 1)
    summary["winner"] = summary["rank"] == 1
    ordered_columns = [
        "rank",
        "winner",
        "strategy",
        "risk_adjusted_score",
        "sharpe_like",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "volatility",
        "win_rate",
        "trades",
        "closed_trades",
        "avg_holding_days",
        "turnover",
        "gross_exposure_usage",
        "gross_exposure",
    ]
    return summary[ordered_columns]


def format_strategy_comparison(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "No backtest-supported strategies available for comparison."
    printable = summary.copy()
    printable["winner"] = printable["winner"].map(lambda flag: "*" if flag else "")
    for column in ("risk_adjusted_score", "sharpe_like", "avg_holding_days", "turnover"):
        printable[column] = printable[column].map(lambda value: f"{float(value):.4f}")
    for column in ("total_return", "annualized_return", "max_drawdown", "volatility", "win_rate", "gross_exposure_usage"):
        printable[column] = printable[column].map(lambda value: f"{100 * float(value):.2f}%")
    printable["gross_exposure"] = printable["gross_exposure"].map(lambda value: f"{float(value):.2f}")
    printable["trades"] = printable["trades"].map(lambda value: f"{float(value):.0f}")
    printable["closed_trades"] = printable["closed_trades"].map(lambda value: f"{float(value):.0f}")
    return printable.to_string(index=False)
