from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from app.backtest.engine import BACKTEST_INITIAL_EQUITY, run_backtest
from app.backtest.metrics import (
    aggregate_fold_metrics,
    baseline_deltas,
    build_walk_forward_windows,
    candidate_recommendation,
    split_in_sample_out_of_sample_dates,
    summarize,
)
from app.config import Settings
from app.strategy import backtest_strategy_names, get_strategy


@dataclass(frozen=True)
class StrategyResearchResult:
    strategy: str
    summary: dict[str, object]
    combined_trade_log: pd.DataFrame
    in_sample_trade_log: pd.DataFrame
    out_of_sample_trade_log: pd.DataFrame
    walk_forward_trade_log: pd.DataFrame


def _buy_and_hold_trade_log(symbols: list[str], start_at: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp": start_at,
                "symbol": ",".join(symbols),
                "side": "buy",
                "qty": float(len(symbols)),
                "close": 0.0,
                "fill_price": 0.0,
                "notional": 0.0,
                "realized_pnl": 0.0,
                "holding_days": 0.0,
                "signal": "baseline",
            }
        ]
    )


def _buy_and_hold_metrics(
    bars: pd.DataFrame,
    *,
    symbols: list[str],
    start_at: pd.Timestamp,
    end_at: pd.Timestamp,
) -> dict[str, float | str]:
    frame = bars.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame[
        (frame["timestamp"] >= pd.Timestamp(start_at))
        & (frame["timestamp"] <= pd.Timestamp(end_at))
        & (frame["symbol"].astype(str).str.upper().isin(symbols))
    ].copy()
    if frame.empty:
        return summarize(pd.DataFrame(), pd.DataFrame(), initial_equity=BACKTEST_INITIAL_EQUITY)

    price_frame = (
        frame.pivot_table(index="timestamp", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
        .dropna(axis=1, how="all")
    )
    if price_frame.empty:
        return summarize(pd.DataFrame(), pd.DataFrame(), initial_equity=BACKTEST_INITIAL_EQUITY)

    initial_prices = price_frame.iloc[0]
    symbols_with_prices = [symbol for symbol in price_frame.columns if float(initial_prices[symbol]) > 0]
    if not symbols_with_prices:
        return summarize(pd.DataFrame(), pd.DataFrame(), initial_equity=BACKTEST_INITIAL_EQUITY)
    allocation = BACKTEST_INITIAL_EQUITY / len(symbols_with_prices)
    shares = {symbol: allocation / float(initial_prices[symbol]) for symbol in symbols_with_prices}
    equity_curve = pd.DataFrame(
        {
            "timestamp": price_frame.index,
            "equity": price_frame[symbols_with_prices].mul(pd.Series(shares)).sum(axis=1).astype(float),
        }
    )
    equity_curve["cash"] = 0.0
    equity_curve["gross_exposure"] = equity_curve["equity"]
    equity_curve["open_positions"] = float(len(symbols_with_prices))
    return summarize(
        _buy_and_hold_trade_log(symbols_with_prices, pd.Timestamp(equity_curve["timestamp"].iloc[0])),
        equity_curve,
        initial_equity=BACKTEST_INITIAL_EQUITY,
    )


def _window_metric_prefix(metrics: dict[str, float | str], prefix: str) -> dict[str, object]:
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


def _selection_score(summary: dict[str, object]) -> float:
    walk_forward_score = float(summary.get("walk_forward_risk_adjusted_score", 0.0))
    walk_forward_folds = float(summary.get("walk_forward_folds", 0.0))
    if walk_forward_folds > 0:
        return walk_forward_score
    return float(summary.get("out_of_sample_risk_adjusted_score", 0.0))


def _compact_float_columns(summary: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    printable = summary.copy()
    for column in columns:
        if column in printable:
            printable[column] = printable[column].map(lambda value: f"{float(value):.4f}")
    return printable


def evaluate_strategy_research(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_name: str,
) -> StrategyResearchResult:
    strategy = get_strategy(strategy_name)
    timestamps = pd.to_datetime(bars["timestamp"], utc=True)
    in_start, in_end, out_start, out_end = split_in_sample_out_of_sample_dates(
        timestamps,
        out_of_sample_fraction=settings.backtest_out_of_sample_fraction,
    )

    combined_trade_log, combined_metrics = run_backtest(bars, settings, strategy=strategy)
    in_trade_log, in_metrics = run_backtest(bars, settings, strategy=strategy, start_at=in_start, end_at=in_end)
    out_trade_log, out_metrics = run_backtest(bars, settings, strategy=strategy, start_at=out_start, end_at=out_end)

    walk_forward_windows: list[dict[str, pd.Timestamp]] = []
    try:
        walk_forward_windows = build_walk_forward_windows(
            timestamps,
            train_days=settings.backtest_walk_forward_train_days,
            test_days=settings.backtest_walk_forward_test_days,
        )
    except ValueError:
        walk_forward_windows = []

    fold_metrics: list[dict[str, float | str]] = []
    walk_forward_logs: list[pd.DataFrame] = []
    for fold_number, window in enumerate(walk_forward_windows, start=1):
        fold_trade_log, fold_summary = run_backtest(
            bars,
            settings,
            strategy=strategy,
            start_at=window["test_start"],
            end_at=window["test_end"],
        )
        fold_metrics.append(fold_summary)
        if not fold_trade_log.empty:
            fold_frame = fold_trade_log.copy()
            fold_frame["walk_forward_fold"] = fold_number
            walk_forward_logs.append(fold_frame)
    walk_forward_metrics = aggregate_fold_metrics(fold_metrics)
    walk_forward_trade_log = pd.concat(walk_forward_logs, ignore_index=True) if walk_forward_logs else pd.DataFrame()

    spy_in_metrics = _buy_and_hold_metrics(bars, symbols=["SPY"], start_at=in_start, end_at=in_end)
    spy_out_metrics = _buy_and_hold_metrics(bars, symbols=["SPY"], start_at=out_start, end_at=out_end)
    spy_combined_metrics = _buy_and_hold_metrics(bars, symbols=["SPY"], start_at=in_start, end_at=out_end)
    equal_weight_symbols = [symbol for symbol in settings.symbol_list if symbol in set(bars["symbol"].astype(str).str.upper())]
    ew_in_metrics = _buy_and_hold_metrics(bars, symbols=equal_weight_symbols, start_at=in_start, end_at=in_end)
    ew_out_metrics = _buy_and_hold_metrics(bars, symbols=equal_weight_symbols, start_at=out_start, end_at=out_end)
    ew_combined_metrics = _buy_and_hold_metrics(bars, symbols=equal_weight_symbols, start_at=in_start, end_at=out_end)

    walk_forward_available = float(walk_forward_metrics["walk_forward_folds"]) > 0
    recommendation = candidate_recommendation(
        out_of_sample_metrics=out_metrics,
        spy_out_of_sample_metrics=spy_out_metrics,
        equal_weight_out_of_sample_metrics=ew_out_metrics,
        max_out_of_sample_drawdown=settings.backtest_max_out_of_sample_drawdown,
        min_closed_trades=settings.backtest_min_closed_trades,
        walk_forward_available=walk_forward_available,
    )
    summary: dict[str, object] = {
        "strategy": strategy_name,
        "recommendation": recommendation,
        "walk_forward_available": walk_forward_available,
        "evaluation_mode": "walk_forward" if walk_forward_available else "out_of_sample",
        "selection_score": 0.0,
        "in_sample_start_at": in_start.isoformat(),
        "in_sample_end_at": in_end.isoformat(),
        "out_of_sample_start_at": out_start.isoformat(),
        "out_of_sample_end_at": out_end.isoformat(),
        **_window_metric_prefix(in_metrics, "in_sample"),
        **_window_metric_prefix(out_metrics, "out_of_sample"),
        **_window_metric_prefix(combined_metrics, "combined"),
        **walk_forward_metrics,
        **{f"spy_in_sample_{key}": value for key, value in spy_in_metrics.items()},
        **{f"spy_out_of_sample_{key}": value for key, value in spy_out_metrics.items()},
        **{f"spy_combined_{key}": value for key, value in spy_combined_metrics.items()},
        **{f"equal_weight_in_sample_{key}": value for key, value in ew_in_metrics.items()},
        **{f"equal_weight_out_of_sample_{key}": value for key, value in ew_out_metrics.items()},
        **{f"equal_weight_combined_{key}": value for key, value in ew_combined_metrics.items()},
        **baseline_deltas(out_metrics, spy_out_metrics, prefix="spy_out_of_sample"),
        **baseline_deltas(out_metrics, ew_out_metrics, prefix="equal_weight_out_of_sample"),
    }
    summary["selection_score"] = _selection_score(summary)
    return StrategyResearchResult(
        strategy=strategy_name,
        summary=summary,
        combined_trade_log=combined_trade_log,
        in_sample_trade_log=in_trade_log,
        out_of_sample_trade_log=out_trade_log,
        walk_forward_trade_log=walk_forward_trade_log,
    )


def compare_strategies(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_names: list[str] | None = None,
) -> pd.DataFrame:
    names = strategy_names or backtest_strategy_names()
    results = [evaluate_strategy_research(bars, settings, strategy_name).summary for strategy_name in names]
    summary = pd.DataFrame(results)
    if summary.empty:
        return summary
    summary = summary.sort_values(
        ["selection_score", "out_of_sample_max_drawdown", "out_of_sample_total_return", "strategy"],
        ascending=[False, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    summary["rank"] = range(1, len(summary) + 1)
    summary["winner"] = summary["rank"] == 1
    return summary


def format_strategy_comparison(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "No backtest-supported strategies available for comparison."
    printable = summary[
        [
            "rank",
            "winner",
            "strategy",
            "recommendation",
            "evaluation_mode",
            "selection_score",
            "out_of_sample_risk_adjusted_score",
            "walk_forward_risk_adjusted_score",
            "out_of_sample_total_return",
            "out_of_sample_max_drawdown",
            "out_of_sample_closed_trades",
            "spy_out_of_sample_risk_adjusted_score_delta",
            "equal_weight_out_of_sample_risk_adjusted_score_delta",
        ]
    ].copy()
    printable["winner"] = printable["winner"].map(lambda flag: "*" if flag else "")
    printable = _compact_float_columns(
        printable,
        [
            "selection_score",
            "out_of_sample_risk_adjusted_score",
            "walk_forward_risk_adjusted_score",
            "spy_out_of_sample_risk_adjusted_score_delta",
            "equal_weight_out_of_sample_risk_adjusted_score_delta",
        ],
    )
    for column in ("out_of_sample_total_return", "out_of_sample_max_drawdown"):
        printable[column] = printable[column].map(lambda value: f"{100 * float(value):.2f}%")
    printable["out_of_sample_closed_trades"] = printable["out_of_sample_closed_trades"].map(
        lambda value: f"{float(value):.0f}"
    )
    return printable.to_string(index=False)


def format_single_strategy_summary(summary: pd.Series | dict[str, object]) -> str:
    row = summary if isinstance(summary, dict) else summary.to_dict()
    return "\n".join(
        [
            "Backtest Summary",
            f"strategy={row['strategy']}",
            f"recommendation={row['recommendation']}",
            f"evaluation_mode={row['evaluation_mode']}",
            f"combined_range={row['combined_start_at']} -> {row['combined_end_at']}",
            f"combined_total_return={100 * float(row['combined_total_return']):.2f}%",
            f"combined_max_drawdown={100 * float(row['combined_max_drawdown']):.2f}%",
            f"combined_risk_adjusted_score={float(row['combined_risk_adjusted_score']):.4f}",
            f"combined_closed_trades={float(row['combined_closed_trades']):.0f}",
            f"out_of_sample_range={row['out_of_sample_start_at']} -> {row['out_of_sample_end_at']}",
            f"out_of_sample_total_return={100 * float(row['out_of_sample_total_return']):.2f}%",
            f"out_of_sample_max_drawdown={100 * float(row['out_of_sample_max_drawdown']):.2f}%",
            f"out_of_sample_risk_adjusted_score={float(row['out_of_sample_risk_adjusted_score']):.4f}",
            f"out_of_sample_closed_trades={float(row['out_of_sample_closed_trades']):.0f}",
            f"spy_out_of_sample_risk_score_delta={float(row['spy_out_of_sample_risk_adjusted_score_delta']):.4f}",
            f"walk_forward_folds={float(row['walk_forward_folds']):.0f}",
            f"walk_forward_risk_adjusted_score={float(row['walk_forward_risk_adjusted_score']):.4f}",
        ]
    )


def write_benchmark_artifacts(
    summary: pd.DataFrame,
    results: list[StrategyResearchResult],
    *,
    settings: Settings,
    source: str,
) -> Path:
    run_timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(settings.backtest_output_dir) / run_timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_dir / "strategy_comparison.csv", index=False)

    for result in results:
        result.combined_trade_log.to_csv(output_dir / f"{result.strategy}_combined_trades.csv", index=False)
        result.in_sample_trade_log.to_csv(output_dir / f"{result.strategy}_in_sample_trades.csv", index=False)
        result.out_of_sample_trade_log.to_csv(output_dir / f"{result.strategy}_out_of_sample_trades.csv", index=False)
        result.walk_forward_trade_log.to_csv(output_dir / f"{result.strategy}_walk_forward_trades.csv", index=False)

    metadata = {
        "generated_at": datetime.now(UTC).isoformat(),
        "config_profile": settings.config_profile,
        "symbols": settings.symbol_list,
        "lookback_days": settings.lookback_days,
        "source": source,
        "out_of_sample_fraction": settings.backtest_out_of_sample_fraction,
        "walk_forward_train_days": settings.backtest_walk_forward_train_days,
        "walk_forward_test_days": settings.backtest_walk_forward_test_days,
        "min_closed_trades": settings.backtest_min_closed_trades,
        "max_out_of_sample_drawdown": settings.backtest_max_out_of_sample_drawdown,
        "strategies": [result.strategy for result in results],
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return output_dir
