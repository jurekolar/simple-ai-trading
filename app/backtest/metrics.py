from __future__ import annotations

import math
from collections.abc import Iterable

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
) -> dict[str, float | str]:
    if equity_curve.empty:
        return {
            "start_at": "",
            "end_at": "",
            "trading_days": 0.0,
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
    daily_returns = equity_curve["equity"].pct_change().replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
    max_drawdown = compute_max_drawdown(equity_curve)
    volatility = annualize_volatility(daily_returns)
    sharpe_like = sharpe_like_ratio(daily_returns)
    total_return = float((final_equity / initial_equity) - 1.0) if initial_equity > 0 else 0.0
    closed_trades = trade_log[trade_log["side"] == "sell"].copy() if not trade_log.empty else pd.DataFrame()
    winning_trades = closed_trades[closed_trades["realized_pnl"] > 0] if not closed_trades.empty else closed_trades
    gross_exposure = float(equity_curve["gross_exposure"].mean()) if "gross_exposure" in equity_curve else 0.0
    return {
        "start_at": pd.Timestamp(equity_curve["timestamp"].iloc[0]).isoformat(),
        "end_at": pd.Timestamp(equity_curve["timestamp"].iloc[-1]).isoformat(),
        "trading_days": float(len(equity_curve)),
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


def split_in_sample_out_of_sample_dates(
    timestamps: Iterable[pd.Timestamp],
    *,
    out_of_sample_fraction: float,
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    unique_dates = pd.Index(sorted(pd.to_datetime(list(timestamps), utc=True).unique()))
    if len(unique_dates) < 2:
        raise ValueError("need at least 2 timestamps to build in-sample and out-of-sample windows")
    oos_days = max(int(math.ceil(len(unique_dates) * out_of_sample_fraction)), 1)
    if oos_days >= len(unique_dates):
        raise ValueError("out-of-sample window consumes the full history")
    split_index = len(unique_dates) - oos_days
    return (
        pd.Timestamp(unique_dates[0]),
        pd.Timestamp(unique_dates[split_index - 1]),
        pd.Timestamp(unique_dates[split_index]),
        pd.Timestamp(unique_dates[-1]),
    )


def build_walk_forward_windows(
    timestamps: Iterable[pd.Timestamp],
    *,
    train_days: int,
    test_days: int,
) -> list[dict[str, pd.Timestamp]]:
    unique_dates = pd.Index(sorted(pd.to_datetime(list(timestamps), utc=True).unique()))
    if train_days <= 0 or test_days <= 0:
        raise ValueError("walk-forward train/test windows must be positive")
    if len(unique_dates) < train_days + test_days:
        raise ValueError("insufficient history for walk-forward windows")

    windows: list[dict[str, pd.Timestamp]] = []
    start_index = 0
    while start_index + train_days + test_days <= len(unique_dates):
        train_start = pd.Timestamp(unique_dates[start_index])
        train_end = pd.Timestamp(unique_dates[start_index + train_days - 1])
        test_start = pd.Timestamp(unique_dates[start_index + train_days])
        test_end = pd.Timestamp(unique_dates[start_index + train_days + test_days - 1])
        windows.append(
            {
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
            }
        )
        start_index += test_days
    return windows


def aggregate_fold_metrics(fold_metrics: list[dict[str, float | str]]) -> dict[str, float]:
    if not fold_metrics:
        return {
            "walk_forward_folds": 0.0,
            "walk_forward_total_return": 0.0,
            "walk_forward_annualized_return": 0.0,
            "walk_forward_max_drawdown": 0.0,
            "walk_forward_volatility": 0.0,
            "walk_forward_sharpe_like": 0.0,
            "walk_forward_risk_adjusted_score": 0.0,
            "walk_forward_win_rate": 0.0,
            "walk_forward_closed_trades": 0.0,
            "walk_forward_avg_holding_days": 0.0,
            "walk_forward_turnover": 0.0,
        }

    frame = pd.DataFrame(fold_metrics)
    return {
        "walk_forward_folds": float(len(frame)),
        "walk_forward_total_return": float(frame["total_return"].mean()),
        "walk_forward_annualized_return": float(frame["annualized_return"].mean()),
        "walk_forward_max_drawdown": float(frame["max_drawdown"].mean()),
        "walk_forward_volatility": float(frame["volatility"].mean()),
        "walk_forward_sharpe_like": float(frame["sharpe_like"].mean()),
        "walk_forward_risk_adjusted_score": float(frame["risk_adjusted_score"].mean()),
        "walk_forward_win_rate": float(frame["win_rate"].mean()),
        "walk_forward_closed_trades": float(frame["closed_trades"].sum()),
        "walk_forward_avg_holding_days": float(frame["avg_holding_days"].mean()),
        "walk_forward_turnover": float(frame["turnover"].mean()),
    }


def baseline_deltas(
    strategy_metrics: dict[str, float | str],
    baseline_metrics: dict[str, float | str],
    *,
    prefix: str,
) -> dict[str, float]:
    return {
        f"{prefix}_risk_adjusted_score_delta": float(strategy_metrics["risk_adjusted_score"]) - float(baseline_metrics["risk_adjusted_score"]),
        f"{prefix}_total_return_delta": float(strategy_metrics["total_return"]) - float(baseline_metrics["total_return"]),
        f"{prefix}_max_drawdown_delta": float(baseline_metrics["max_drawdown"]) - float(strategy_metrics["max_drawdown"]),
    }


def candidate_recommendation(
    *,
    out_of_sample_metrics: dict[str, float | str],
    spy_out_of_sample_metrics: dict[str, float | str],
    equal_weight_out_of_sample_metrics: dict[str, float | str],
    max_out_of_sample_drawdown: float,
    min_closed_trades: int,
    walk_forward_available: bool,
) -> str:
    if float(out_of_sample_metrics["closed_trades"]) < min_closed_trades:
        return "fail"
    if float(out_of_sample_metrics["max_drawdown"]) > max_out_of_sample_drawdown:
        return "fail"
    if float(out_of_sample_metrics["risk_adjusted_score"]) < float(spy_out_of_sample_metrics["risk_adjusted_score"]):
        return "fail"
    if (not walk_forward_available) or (
        float(out_of_sample_metrics["risk_adjusted_score"])
        < float(equal_weight_out_of_sample_metrics["risk_adjusted_score"])
    ):
        return "review"
    return "pass"
