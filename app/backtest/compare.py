from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from app.backtest.engine import BACKTEST_INITIAL_EQUITY, BacktestResult, run_backtest, run_backtest_detailed
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


LATEST_INDEX_FILENAME = "latest.json"
LATEST_APPROVAL_SUMMARY_FILENAME = "latest_approval_summary.md"


@dataclass(frozen=True)
class StrategyResearchResult:
    strategy: str
    summary: dict[str, object]
    combined_trade_log: pd.DataFrame
    combined_equity_curve: pd.DataFrame
    in_sample_trade_log: pd.DataFrame
    in_sample_equity_curve: pd.DataFrame
    out_of_sample_trade_log: pd.DataFrame
    out_of_sample_equity_curve: pd.DataFrame
    walk_forward_trade_log: pd.DataFrame
    regime_summary: pd.DataFrame
    quarter_summary: pd.DataFrame
    sensitivity_summary: pd.DataFrame


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
    if bool(summary.get("benchmark_valid", False)) and float(summary.get("walk_forward_folds", 0.0)) > 0:
        return float(summary.get("walk_forward_risk_adjusted_score", 0.0))
    return float(summary.get("out_of_sample_risk_adjusted_score", 0.0))


def _compact_float_columns(summary: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    printable = summary.copy()
    for column in columns:
        if column in printable:
            printable[column] = printable[column].map(lambda value: f"{float(value):.4f}")
    return printable


def _symbol_concentration(trade_log: pd.DataFrame) -> float:
    if trade_log.empty or "notional" not in trade_log or float(trade_log["notional"].abs().sum()) <= 0:
        return 0.0
    by_symbol = trade_log.groupby("symbol")["notional"].sum().abs()
    return float(by_symbol.max() / by_symbol.sum()) if not by_symbol.empty else 0.0


def _regime_label(total_return: float) -> str:
    if total_return > 0.05:
        return "bull"
    if total_return < -0.05:
        return "drawdown"
    return "recovery"


def _period_summary(bars: pd.DataFrame, settings: Settings, strategy_name: str, freq: str) -> pd.DataFrame:
    frame = bars.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    period_index = frame["timestamp"].dt.tz_localize(None).dt.to_period(freq)
    rows: list[dict[str, object]] = []
    for period, period_bars in frame.groupby(period_index):
        period_result = run_backtest_detailed(period_bars, settings, strategy=get_strategy(strategy_name))
        total_return = float(period_result.metrics.get("total_return", 0.0))
        rows.append(
            {
                "strategy": strategy_name,
                "period": str(period),
                "total_return": total_return,
                "risk_adjusted_score": float(period_result.metrics.get("risk_adjusted_score", 0.0)),
                "max_drawdown": float(period_result.metrics.get("max_drawdown", 0.0)),
                "closed_trades": float(period_result.metrics.get("closed_trades", 0.0)),
                "regime": _regime_label(total_return),
            }
        )
    return pd.DataFrame(rows)


def _sensitivity_variants(settings: Settings, strategy_name: str) -> list[tuple[str, Settings]]:
    variants: list[tuple[str, Settings]] = [("base", settings)]
    if strategy_name == "breakout":
        variants.append(("entry_window_minus", settings.model_copy(update={"breakout_entry_window": max(settings.breakout_entry_window - 5, 5)})))
        variants.append(("entry_window_plus", settings.model_copy(update={"breakout_entry_window": settings.breakout_entry_window + 5})))
    elif strategy_name == "momentum":
        variants.append(("trend_window_minus", settings.model_copy(update={"trend_window": max(settings.trend_window - 10, 5)})))
        variants.append(("trend_window_plus", settings.model_copy(update={"trend_window": settings.trend_window + 10})))
    elif strategy_name == "mean_reversion":
        variants.append(("entry_zscore_looser", settings.model_copy(update={"mean_reversion_entry_zscore": settings.mean_reversion_entry_zscore + 0.2})))
        variants.append(("entry_zscore_tighter", settings.model_copy(update={"mean_reversion_entry_zscore": settings.mean_reversion_entry_zscore - 0.2})))
    elif strategy_name == "trend_trailing_stop":
        variants.append(("atr_multiplier_minus", settings.model_copy(update={"trend_trailing_atr_multiplier": max(settings.trend_trailing_atr_multiplier - 0.5, 0.5)})))
        variants.append(("atr_multiplier_plus", settings.model_copy(update={"trend_trailing_atr_multiplier": settings.trend_trailing_atr_multiplier + 0.5})))
    return variants


def _sensitivity_summary(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_name: str,
    *,
    out_start: pd.Timestamp,
    out_end: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for variant_name, variant_settings in _sensitivity_variants(settings, strategy_name):
        _, metrics = run_backtest(
            bars,
            variant_settings,
            strategy=get_strategy(strategy_name),
            start_at=out_start,
            end_at=out_end,
        )
        rows.append(
            {
                "strategy": strategy_name,
                "variant": variant_name,
                "out_of_sample_risk_adjusted_score": float(metrics.get("risk_adjusted_score", 0.0)),
                "out_of_sample_total_return": float(metrics.get("total_return", 0.0)),
                "out_of_sample_max_drawdown": float(metrics.get("max_drawdown", 0.0)),
            }
        )
    return pd.DataFrame(rows)


def _benchmark_quality_assessment(
    *,
    settings: Settings,
    source: str,
    out_metrics: dict[str, float | str],
    walk_forward_metrics: dict[str, float | str],
    spy_out_metrics: dict[str, float | str],
    equal_weight_out_metrics: dict[str, float | str],
    trade_log: pd.DataFrame,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if source != "alpaca":
        reasons.append(f"unsafe_source:{source}")
    if float(out_metrics.get("trading_days", 0.0)) < settings.backtest_min_out_of_sample_days:
        reasons.append("insufficient_out_of_sample_days")
    if float(out_metrics.get("closed_trades", 0.0)) < settings.backtest_min_closed_trades:
        reasons.append("too_few_closed_trades")
    if float(walk_forward_metrics.get("walk_forward_folds", 0.0)) < settings.backtest_min_walk_forward_folds:
        reasons.append("insufficient_walk_forward_folds")
    if _symbol_concentration(trade_log) > settings.backtest_max_symbol_concentration:
        reasons.append("excess_symbol_concentration")
    if (
        float(out_metrics.get("risk_adjusted_score", 0.0))
        - float(spy_out_metrics.get("risk_adjusted_score", 0.0))
        < settings.backtest_min_baseline_advantage
    ):
        reasons.append("insufficient_advantage_vs_spy")
    if (
        float(out_metrics.get("risk_adjusted_score", 0.0))
        - float(equal_weight_out_metrics.get("risk_adjusted_score", 0.0))
        < settings.backtest_min_baseline_advantage
    ):
        reasons.append("insufficient_advantage_vs_equal_weight")
    return (not reasons), reasons


def evaluate_strategy_research(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_name: str,
    *,
    source: str = "alpaca",
) -> StrategyResearchResult:
    strategy = get_strategy(strategy_name)
    timestamps = pd.to_datetime(bars["timestamp"], utc=True)
    in_start, in_end, out_start, out_end = split_in_sample_out_of_sample_dates(
        timestamps,
        out_of_sample_fraction=settings.backtest_out_of_sample_fraction,
    )

    combined_result = run_backtest_detailed(bars, settings, strategy=strategy)
    in_result = run_backtest_detailed(bars, settings, strategy=strategy, start_at=in_start, end_at=in_end)
    out_result = run_backtest_detailed(bars, settings, strategy=strategy, start_at=out_start, end_at=out_end)

    walk_forward_windows: list[dict[str, pd.Timestamp]]
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
        fold_result = run_backtest_detailed(
            bars,
            settings,
            strategy=strategy,
            start_at=window["test_start"],
            end_at=window["test_end"],
        )
        fold_metrics.append(fold_result.metrics)
        if not fold_result.trade_log.empty:
            fold_frame = fold_result.trade_log.copy()
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

    benchmark_valid, benchmark_invalid_reasons = _benchmark_quality_assessment(
        settings=settings,
        source=source,
        out_metrics=out_result.metrics,
        walk_forward_metrics=walk_forward_metrics,
        spy_out_metrics=spy_out_metrics,
        equal_weight_out_metrics=ew_out_metrics,
        trade_log=out_result.trade_log,
    )
    walk_forward_available = float(walk_forward_metrics["walk_forward_folds"]) > 0
    base_recommendation = candidate_recommendation(
        out_of_sample_metrics=out_result.metrics,
        spy_out_of_sample_metrics=spy_out_metrics,
        equal_weight_out_of_sample_metrics=ew_out_metrics,
        max_out_of_sample_drawdown=settings.backtest_max_out_of_sample_drawdown,
        min_closed_trades=settings.backtest_min_closed_trades,
        walk_forward_available=walk_forward_available,
    )
    recommendation = "fail" if not benchmark_valid else base_recommendation
    regime_summary = _period_summary(bars, settings, strategy_name, "2Q")
    quarter_summary = _period_summary(bars, settings, strategy_name, "Q")
    sensitivity_summary = _sensitivity_summary(
        bars,
        settings,
        strategy_name,
        out_start=out_start,
        out_end=out_end,
    )
    base_score = float(
        sensitivity_summary[sensitivity_summary["variant"] == "base"]["out_of_sample_risk_adjusted_score"].iloc[0]
    ) if not sensitivity_summary.empty else 0.0
    stability_score = float(
        (sensitivity_summary["out_of_sample_risk_adjusted_score"] - base_score).abs().max()
    ) if not sensitivity_summary.empty else 0.0

    summary: dict[str, object] = {
        "strategy": strategy_name,
        "recommendation": recommendation,
        "benchmark_valid": benchmark_valid,
        "benchmark_invalid_reasons": ",".join(benchmark_invalid_reasons) if benchmark_invalid_reasons else "",
        "walk_forward_available": walk_forward_available,
        "evaluation_mode": "walk_forward" if walk_forward_available else "out_of_sample",
        "selection_score": 0.0,
        "source": source,
        "sensitivity_stability_score": stability_score,
        "symbol_concentration": _symbol_concentration(out_result.trade_log),
        "in_sample_start_at": in_start.isoformat(),
        "in_sample_end_at": in_end.isoformat(),
        "out_of_sample_start_at": out_start.isoformat(),
        "out_of_sample_end_at": out_end.isoformat(),
        **_window_metric_prefix(in_result.metrics, "in_sample"),
        **_window_metric_prefix(out_result.metrics, "out_of_sample"),
        **_window_metric_prefix(combined_result.metrics, "combined"),
        **walk_forward_metrics,
        **{f"spy_in_sample_{key}": value for key, value in spy_in_metrics.items()},
        **{f"spy_out_of_sample_{key}": value for key, value in spy_out_metrics.items()},
        **{f"spy_combined_{key}": value for key, value in spy_combined_metrics.items()},
        **{f"equal_weight_in_sample_{key}": value for key, value in ew_in_metrics.items()},
        **{f"equal_weight_out_of_sample_{key}": value for key, value in ew_out_metrics.items()},
        **{f"equal_weight_combined_{key}": value for key, value in ew_combined_metrics.items()},
        **baseline_deltas(out_result.metrics, spy_out_metrics, prefix="spy_out_of_sample"),
        **baseline_deltas(out_result.metrics, ew_out_metrics, prefix="equal_weight_out_of_sample"),
    }
    summary["selection_score"] = _selection_score(summary) if benchmark_valid else -999.0
    return StrategyResearchResult(
        strategy=strategy_name,
        summary=summary,
        combined_trade_log=combined_result.trade_log,
        combined_equity_curve=combined_result.equity_curve,
        in_sample_trade_log=in_result.trade_log,
        in_sample_equity_curve=in_result.equity_curve,
        out_of_sample_trade_log=out_result.trade_log,
        out_of_sample_equity_curve=out_result.equity_curve,
        walk_forward_trade_log=walk_forward_trade_log,
        regime_summary=regime_summary,
        quarter_summary=quarter_summary,
        sensitivity_summary=sensitivity_summary,
    )


def compare_strategies(
    bars: pd.DataFrame,
    settings: Settings,
    strategy_names: list[str] | None = None,
    *,
    source: str = "alpaca",
) -> pd.DataFrame:
    names = strategy_names or backtest_strategy_names()
    results = [evaluate_strategy_research(bars, settings, strategy_name, source=source).summary for strategy_name in names]
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


def _approval_summary_markdown(summary: pd.DataFrame, metadata: dict[str, object]) -> str:
    winner = summary.iloc[0] if not summary.empty else None
    rejected = summary[summary["recommendation"] != "pass"] if not summary.empty else pd.DataFrame()
    lines = [
        "# Latest Benchmark Approval Summary",
        f"generated_at={metadata['generated_at']}",
        f"source={metadata['source']}",
        f"config_profile={metadata['config_profile']}",
        f"benchmark_valid={metadata['benchmark_valid']}",
        "",
    ]
    if winner is not None:
        lines += [
            "## Recommended Live Candidate",
            f"- strategy: {winner['strategy']}",
            f"- recommendation: {winner['recommendation']}",
            f"- selection_score: {float(winner['selection_score']):.4f}",
            f"- benchmark_invalid_reasons: {winner['benchmark_invalid_reasons'] or 'none'}",
            "",
        ]
    if not rejected.empty:
        lines.append("## Rejected / Review Candidates")
        for row in rejected.itertuples(index=False):
            lines.append(f"- {row.strategy}: {row.recommendation} ({row.benchmark_invalid_reasons or 'lower score / gate failure'})")
        lines.append("")
    return "\n".join(lines)


def format_strategy_comparison(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "No backtest-supported strategies available for comparison."
    printable = summary.copy()
    default_values: dict[str, object] = {
        "rank": "",
        "winner": False,
        "strategy": "",
        "recommendation": "",
        "benchmark_valid": True,
        "evaluation_mode": "",
        "selection_score": 0.0,
        "out_of_sample_risk_adjusted_score": 0.0,
        "walk_forward_risk_adjusted_score": 0.0,
        "out_of_sample_total_return": 0.0,
        "out_of_sample_max_drawdown": 0.0,
        "out_of_sample_closed_trades": 0.0,
        "benchmark_invalid_reasons": "",
    }
    for column, default in default_values.items():
        if column not in printable.columns:
            printable[column] = default
    printable = printable[list(default_values.keys())].copy()
    printable["winner"] = printable["winner"].map(lambda flag: "*" if flag else "")
    printable["benchmark_valid"] = printable["benchmark_valid"].map(lambda flag: "yes" if bool(flag) else "no")
    printable = _compact_float_columns(
        printable,
        [
            "selection_score",
            "out_of_sample_risk_adjusted_score",
            "walk_forward_risk_adjusted_score",
        ],
    )
    for column in ("out_of_sample_total_return", "out_of_sample_max_drawdown"):
        printable[column] = printable[column].map(lambda value: f"{100 * float(value):.2f}%")
    printable["out_of_sample_closed_trades"] = printable["out_of_sample_closed_trades"].map(lambda value: f"{float(value):.0f}")
    return printable.to_string(index=False)


def format_single_strategy_summary(summary: pd.Series | dict[str, object]) -> str:
    row = summary if isinstance(summary, dict) else summary.to_dict()
    warnings: list[str] = []
    if row.get("source", "alpaca") != "alpaca":
        warnings.append(f"unsafe_source={row.get('source')}")
    if not bool(row.get("benchmark_valid", True)):
        warnings.append(f"benchmark_invalid={row.get('benchmark_invalid_reasons') or 'unspecified'}")
    if row.get("recommendation") != "pass":
        warnings.append(f"candidate_status={row.get('recommendation')}")
    warning_block = "\n".join(f"warning={warning}" for warning in warnings) if warnings else "warning=none"
    return "\n".join(
        [
            "Backtest Summary",
            f"strategy={row['strategy']}",
            f"recommendation={row['recommendation']}",
            f"benchmark_valid={row.get('benchmark_valid', True)}",
            f"evaluation_mode={row.get('evaluation_mode', 'combined')}",
            warning_block,
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
            f"sensitivity_stability_score={float(row.get('sensitivity_stability_score', 0.0)):.4f}",
        ]
    )


def latest_benchmark_index_path(settings: Settings) -> Path:
    return Path(settings.backtest_output_dir) / LATEST_INDEX_FILENAME


def load_latest_benchmark_index(settings: Settings) -> dict[str, object]:
    path = latest_benchmark_index_path(settings)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_benchmark_artifacts(
    summary: pd.DataFrame,
    results: list[StrategyResearchResult],
    *,
    settings: Settings,
    source: str,
) -> Path:
    run_timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(settings.backtest_output_dir)
    output_dir = output_root / run_timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_dir / "strategy_comparison.csv", index=False)

    for result in results:
        result.combined_trade_log.to_csv(output_dir / f"{result.strategy}_combined_trades.csv", index=False)
        result.in_sample_trade_log.to_csv(output_dir / f"{result.strategy}_in_sample_trades.csv", index=False)
        result.out_of_sample_trade_log.to_csv(output_dir / f"{result.strategy}_out_of_sample_trades.csv", index=False)
        result.walk_forward_trade_log.to_csv(output_dir / f"{result.strategy}_walk_forward_trades.csv", index=False)
        result.combined_equity_curve.to_csv(output_dir / f"{result.strategy}_combined_equity_curve.csv", index=False)
        result.out_of_sample_equity_curve.to_csv(output_dir / f"{result.strategy}_out_of_sample_equity_curve.csv", index=False)
        result.regime_summary.to_csv(output_dir / f"{result.strategy}_regime_summary.csv", index=False)
        result.quarter_summary.to_csv(output_dir / f"{result.strategy}_quarter_summary.csv", index=False)
        result.sensitivity_summary.to_csv(output_dir / f"{result.strategy}_sensitivity_summary.csv", index=False)

    approved_candidates = summary[summary["recommendation"] == "pass"]
    recommended_strategy = approved_candidates.iloc[0]["strategy"] if not approved_candidates.empty else ""
    benchmark_valid = bool(summary["benchmark_valid"].all()) if not summary.empty else False
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "config_profile": settings.config_profile,
        "symbols": settings.symbol_list,
        "lookback_days": settings.lookback_days,
        "source": source,
        "out_of_sample_fraction": settings.backtest_out_of_sample_fraction,
        "walk_forward_train_days": settings.backtest_walk_forward_train_days,
        "walk_forward_test_days": settings.backtest_walk_forward_test_days,
        "min_closed_trades": settings.backtest_min_closed_trades,
        "min_out_of_sample_days": settings.backtest_min_out_of_sample_days,
        "min_walk_forward_folds": settings.backtest_min_walk_forward_folds,
        "max_out_of_sample_drawdown": settings.backtest_max_out_of_sample_drawdown,
        "max_symbol_concentration": settings.backtest_max_symbol_concentration,
        "min_baseline_advantage": settings.backtest_min_baseline_advantage,
        "strategies": [result.strategy for result in results],
        "recommended_live_candidate": recommended_strategy,
        "benchmark_valid": benchmark_valid,
        "decision_ready": benchmark_valid and bool(recommended_strategy),
        "strategy_statuses": [
            {
                "strategy": row["strategy"],
                "recommendation": row["recommendation"],
                "benchmark_valid": bool(row["benchmark_valid"]),
                "benchmark_invalid_reasons": row["benchmark_invalid_reasons"],
            }
            for _, row in summary.iterrows()
        ],
    }
    (output_dir / "metadata.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "approval_summary.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "approval_summary.md").write_text(_approval_summary_markdown(summary, manifest), encoding="utf-8")
    latest_index = {
        "generated_at": manifest["generated_at"],
        "artifact_dir": str(output_dir),
        "recommended_live_candidate": recommended_strategy,
        "benchmark_valid": benchmark_valid,
        "decision_ready": manifest["decision_ready"],
        "source": source,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    latest_benchmark_index_path(settings).write_text(json.dumps(latest_index, indent=2, sort_keys=True), encoding="utf-8")
    (output_root / LATEST_APPROVAL_SUMMARY_FILENAME).write_text(
        _approval_summary_markdown(summary, manifest),
        encoding="utf-8",
    )
    return output_dir
