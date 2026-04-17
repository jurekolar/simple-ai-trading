from __future__ import annotations

import pandas as pd

from app.backtest.fills import apply_slippage
from app.config import Settings
from app.risk.checks import filter_trade_candidates
from app.strategy import backtest_strategy_names, get_strategy
from app.strategy.signals import latest_signals


def compare_strategies(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_names: list[str] | None = None,
) -> pd.DataFrame:
    names = strategy_names or backtest_strategy_names()
    rows: list[dict[str, object]] = []
    for strategy_name in names:
        strategy = get_strategy(strategy_name)
        signal_frame = strategy.generate_signals(bars, settings)
        latest = latest_signals(signal_frame)
        trades = filter_trade_candidates(latest, settings)
        trades = apply_slippage(trades)
        avg_atr_ratio = 0.0
        if not trades.empty and "atr_ratio" in trades.columns:
            avg_atr_ratio = float(trades["atr_ratio"].fillna(0.0).mean())
        entry_types = ""
        if strategy_name == "trend_trailing_stop" and not trades.empty and "entry_type" in trades.columns:
            entry_types = ",".join(
                f"{row.symbol}:{row.entry_type}"
                for row in trades[["symbol", "entry_type"]].itertuples(index=False)
            )
        rows.append(
            {
                "strategy": strategy_name,
                "trade_candidates": int(len(trades)),
                "gross_exposure": float((trades["qty"] * trades["fill_price"]).sum()) if not trades.empty else 0.0,
                "symbols_selected": ",".join(trades["symbol"].tolist()) if not trades.empty else "",
                "avg_score": float(trades["score"].mean()) if not trades.empty else 0.0,
                "avg_atr_ratio": avg_atr_ratio,
                "entry_types": entry_types,
            }
        )
    return pd.DataFrame(rows)


def format_strategy_comparison(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "No backtest-supported strategies available for comparison."
    printable = summary.copy()
    printable["gross_exposure"] = printable["gross_exposure"].map(lambda value: f"{value:.2f}")
    printable["avg_score"] = printable["avg_score"].map(lambda value: f"{value:.4f}")
    printable["avg_atr_ratio"] = printable["avg_atr_ratio"].map(lambda value: f"{value:.4f}")
    printable["symbols_selected"] = printable["symbols_selected"].replace("", "-")
    printable["entry_types"] = printable["entry_types"].replace("", "-")
    return printable.to_string(index=False)
