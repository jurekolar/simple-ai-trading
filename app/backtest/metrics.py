from __future__ import annotations

import math

import pandas as pd


def compute_max_drawdown(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty or "equity" not in equity_curve:
        return 0.0
    running_peak = equity_curve["equity"].cummax().replace(0.0, pd.NA)
    drawdowns = (equity_curve["equity"] / running_peak) - 1.0
    return float(abs(drawdowns.fillna(0.0).min()))


def annualize_return(initial_equity: float, final_equity: float, periods: int) -> float:
    if initial_equity <= 0 or final_equity <= 0 or periods <= 0:
        return 0.0
    return float((final_equity / initial_equity) ** (252 / periods) - 1.0)


def annualize_volatility(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return 0.0
    volatility = float(daily_returns.std(ddof=0))
    return float(volatility * math.sqrt(252))


def sharpe_like_ratio(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return 0.0
    volatility = float(daily_returns.std(ddof=0))
    if volatility <= 0:
        return 0.0
    return float((daily_returns.mean() / volatility) * math.sqrt(252))


def risk_adjusted_score(*, sharpe_like: float, max_drawdown: float) -> float:
    return float(sharpe_like - max_drawdown)


def summarize(
    trade_log: pd.DataFrame,
    equity_curve: pd.DataFrame,
    *,
    initial_equity: float,
) -> dict[str, float]:
    if equity_curve.empty:
        return {
            "trades": float(len(trade_log)),
            "closed_trades": 0.0,
            "gross_exposure": 0.0,
            "gross_exposure_usage": 0.0,
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "volatility": 0.0,
            "sharpe_like": 0.0,
            "risk_adjusted_score": 0.0,
            "win_rate": 0.0,
            "avg_holding_days": 0.0,
            "turnover": 0.0,
        }

    final_equity = float(equity_curve["equity"].iloc[-1])
    total_return = float((final_equity / initial_equity) - 1.0) if initial_equity > 0 else 0.0
    daily_returns = equity_curve["equity"].pct_change().replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
    max_drawdown = compute_max_drawdown(equity_curve)
    volatility = annualize_volatility(daily_returns)
    sharpe_like = sharpe_like_ratio(daily_returns)
    closed_trades = trade_log[trade_log["side"] == "sell"].copy() if not trade_log.empty else pd.DataFrame()
    winning_trades = closed_trades[closed_trades["realized_pnl"] > 0] if not closed_trades.empty else closed_trades
    gross_exposure = float(equity_curve["gross_exposure"].mean()) if "gross_exposure" in equity_curve else 0.0
    return {
        "trades": float(len(trade_log)),
        "closed_trades": float(len(closed_trades)),
        "gross_exposure": gross_exposure,
        "gross_exposure_usage": float(gross_exposure / initial_equity) if initial_equity > 0 else 0.0,
        "total_return": total_return,
        "annualized_return": annualize_return(initial_equity, final_equity, len(equity_curve)),
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "sharpe_like": sharpe_like,
        "risk_adjusted_score": risk_adjusted_score(sharpe_like=sharpe_like, max_drawdown=max_drawdown),
        "win_rate": float(len(winning_trades) / len(closed_trades)) if len(closed_trades) > 0 else 0.0,
        "avg_holding_days": float(closed_trades["holding_days"].mean()) if not closed_trades.empty else 0.0,
        "turnover": float(trade_log["notional"].sum() / initial_equity) if not trade_log.empty and initial_equity > 0 else 0.0,
    }
