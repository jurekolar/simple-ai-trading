from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd
import pytest

from app.backtest.compare import (
    compare_strategies,
    evaluate_strategy_research,
    format_single_strategy_summary,
    format_strategy_comparison,
    write_benchmark_artifacts,
)
from app.backtest.engine import run_backtest
from app.backtest.metrics import (
    aggregate_fold_metrics,
    annualize_return,
    annualize_volatility,
    baseline_deltas,
    build_walk_forward_windows,
    candidate_recommendation,
    compute_max_drawdown,
    risk_adjusted_score,
    sharpe_like_ratio,
    split_in_sample_out_of_sample_dates,
)
from app.config import Settings
from app.main import run_backtest_command
from app.strategy import get_strategy


def _daily_bars(symbol: str, closes: list[float], *, start_day: int = 1) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, close in enumerate(closes):
        open_price = closes[index - 1] if index > 0 else close
        rows.append(
            {
                "timestamp": datetime(2026, 1, start_day + index, 20, 0, tzinfo=UTC),
                "symbol": symbol,
                "open": float(open_price),
                "high": float(max(open_price, close) + 1.0),
                "low": float(min(open_price, close) - 1.0),
                "close": float(close),
                "volume": 1_000_000.0,
            }
        )
    return rows


def _research_settings(tmp_path, **overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "SYMBOLS": "SPY,QQQ,IWM,AAPL,MSFT",
        "MAX_SYMBOLS_PER_RUN": 3,
        "MAX_POSITION_NOTIONAL": 10_000,
        "MAX_SYMBOL_EXPOSURE": 10_000,
        "MAX_GROSS_EXPOSURE": 25_000,
        "MIN_AVERAGE_DAILY_VOLUME": 100,
        "MAX_ATR_RATIO": 1.0,
        "BACKTEST_OUTPUT_DIR": str(tmp_path / "artifacts"),
        "BACKTEST_OUT_OF_SAMPLE_FRACTION": 0.3,
        "BACKTEST_WALK_FORWARD_TRAIN_DAYS": 4,
        "BACKTEST_WALK_FORWARD_TEST_DAYS": 2,
        "BACKTEST_MIN_CLOSED_TRADES": 1,
        "BACKTEST_MAX_OUT_OF_SAMPLE_DRAWDOWN": 0.25,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _multi_symbol_bars() -> pd.DataFrame:
    return pd.DataFrame(
        _daily_bars("SPY", [100.0, 101.0, 102.0, 99.0, 103.0, 98.0, 105.0, 107.0, 104.0, 109.0])
        + _daily_bars("QQQ", [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 111.0, 114.0, 116.0])
        + _daily_bars("IWM", [50.0, 49.0, 48.0, 47.0, 46.0, 45.0, 44.0, 43.0, 42.0, 41.0])
        + _daily_bars("AAPL", [100.0, 103.0, 106.0, 109.0, 90.0, 92.0, 95.0, 97.0, 99.0, 101.0])
        + _daily_bars("MSFT", [200.0, 201.0, 202.0, 203.0, 204.0, 205.0, 206.0, 207.0, 208.0, 209.0])
    )


def test_metric_helpers_compute_expected_values() -> None:
    equity_curve = pd.DataFrame({"equity": [100_000.0, 110_000.0, 99_000.0, 120_000.0]})
    daily_returns = equity_curve["equity"].pct_change().fillna(0.0)

    assert compute_max_drawdown(equity_curve) == pytest.approx(0.1)
    assert annualize_return(100_000.0, 120_000.0, 4) > 0.0
    assert annualize_volatility(daily_returns) > 0.0
    assert sharpe_like_ratio(daily_returns) != 0.0
    assert risk_adjusted_score(sharpe_like=1.5, max_drawdown=0.2) == 1.3


def test_split_in_sample_out_of_sample_dates_is_deterministic() -> None:
    timestamps = [datetime(2026, 1, day, 20, 0, tzinfo=UTC) for day in range(1, 11)]

    in_start, in_end, out_start, out_end = split_in_sample_out_of_sample_dates(
        timestamps,
        out_of_sample_fraction=0.3,
    )

    assert in_start == pd.Timestamp(datetime(2026, 1, 1, 20, 0, tzinfo=UTC))
    assert in_end == pd.Timestamp(datetime(2026, 1, 7, 20, 0, tzinfo=UTC))
    assert out_start == pd.Timestamp(datetime(2026, 1, 8, 20, 0, tzinfo=UTC))
    assert out_end == pd.Timestamp(datetime(2026, 1, 10, 20, 0, tzinfo=UTC))


def test_walk_forward_windows_are_non_overlapping() -> None:
    timestamps = [datetime(2026, 1, day, 20, 0, tzinfo=UTC) for day in range(1, 11)]

    windows = build_walk_forward_windows(timestamps, train_days=4, test_days=2)

    assert len(windows) == 3
    assert windows[0]["train_end"] < windows[0]["test_start"]
    assert windows[0]["test_end"] < windows[1]["test_start"]


def test_walk_forward_windows_fail_on_insufficient_history() -> None:
    timestamps = [datetime(2026, 1, day, 20, 0, tzinfo=UTC) for day in range(1, 5)]

    with pytest.raises(ValueError, match="insufficient history"):
        build_walk_forward_windows(timestamps, train_days=4, test_days=2)


def test_aggregate_fold_metrics_is_deterministic() -> None:
    summary = aggregate_fold_metrics(
        [
            {
                "total_return": 0.10,
                "annualized_return": 0.12,
                "max_drawdown": 0.05,
                "volatility": 0.08,
                "sharpe_like": 1.5,
                "risk_adjusted_score": 1.45,
                "win_rate": 0.6,
                "closed_trades": 2.0,
                "avg_holding_days": 10.0,
                "turnover": 0.5,
            },
            {
                "total_return": 0.00,
                "annualized_return": 0.01,
                "max_drawdown": 0.10,
                "volatility": 0.12,
                "sharpe_like": 0.5,
                "risk_adjusted_score": 0.4,
                "win_rate": 0.5,
                "closed_trades": 1.0,
                "avg_holding_days": 8.0,
                "turnover": 0.4,
            },
        ]
    )

    assert summary["walk_forward_folds"] == 2.0
    assert summary["walk_forward_closed_trades"] == 3.0
    assert summary["walk_forward_risk_adjusted_score"] == pytest.approx(0.925)


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


def test_run_backtest_handles_date_window() -> None:
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
    bars = pd.DataFrame(_daily_bars("SPY", [100.0, 101.0, 102.0, 99.0, 103.0, 98.0]))

    trades, metrics = run_backtest(
        bars,
        settings,
        strategy=get_strategy("momentum"),
        start_at=pd.Timestamp(datetime(2026, 1, 3, 20, 0, tzinfo=UTC)),
        end_at=pd.Timestamp(datetime(2026, 1, 6, 20, 0, tzinfo=UTC)),
    )

    assert pd.Timestamp(metrics["start_at"]) == pd.Timestamp(datetime(2026, 1, 3, 20, 0, tzinfo=UTC))
    assert pd.Timestamp(metrics["end_at"]) == pd.Timestamp(datetime(2026, 1, 6, 20, 0, tzinfo=UTC))
    assert all(
        pd.Timestamp(trade_timestamp) >= pd.Timestamp(datetime(2026, 1, 3, 20, 0, tzinfo=UTC))
        for trade_timestamp in trades["timestamp"]
    )


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


def test_evaluate_strategy_research_includes_validation_windows_and_recommendation(tmp_path) -> None:
    settings = _research_settings(tmp_path)
    bars = _multi_symbol_bars()

    result = evaluate_strategy_research(bars, settings, "momentum")

    assert result.summary["recommendation"] in {"pass", "review", "fail"}
    assert result.summary["in_sample_start_at"]
    assert result.summary["out_of_sample_end_at"]
    assert "spy_out_of_sample_risk_adjusted_score_delta" in result.summary
    assert "equal_weight_out_of_sample_risk_adjusted_score_delta" in result.summary


def test_compare_strategies_ranks_by_selection_score(tmp_path) -> None:
    settings = _research_settings(tmp_path)
    bars = _multi_symbol_bars()

    summary = compare_strategies(bars, settings)

    assert summary.iloc[0]["rank"] == 1
    assert bool(summary.iloc[0]["winner"])
    assert set(summary["strategy"]) == {"momentum", "mean_reversion", "breakout", "trend_trailing_stop"}
    assert summary["selection_score"].tolist() == sorted(summary["selection_score"], reverse=True)


def test_baseline_deltas_compute_expected_direction() -> None:
    deltas = baseline_deltas(
        {"risk_adjusted_score": 1.2, "total_return": 0.15, "max_drawdown": 0.05},
        {"risk_adjusted_score": 1.0, "total_return": 0.10, "max_drawdown": 0.08},
        prefix="spy_out_of_sample",
    )

    assert deltas["spy_out_of_sample_risk_adjusted_score_delta"] == pytest.approx(0.2)
    assert deltas["spy_out_of_sample_total_return_delta"] == pytest.approx(0.05)
    assert deltas["spy_out_of_sample_max_drawdown_delta"] == pytest.approx(0.03)


def test_candidate_recommendation_covers_fail_review_pass() -> None:
    fail = candidate_recommendation(
        out_of_sample_metrics={"closed_trades": 0.0, "max_drawdown": 0.05, "risk_adjusted_score": 1.1},
        spy_out_of_sample_metrics={"risk_adjusted_score": 1.0},
        equal_weight_out_of_sample_metrics={"risk_adjusted_score": 0.8},
        max_out_of_sample_drawdown=0.20,
        min_closed_trades=1,
        walk_forward_available=True,
    )
    review = candidate_recommendation(
        out_of_sample_metrics={"closed_trades": 2.0, "max_drawdown": 0.05, "risk_adjusted_score": 1.1},
        spy_out_of_sample_metrics={"risk_adjusted_score": 1.0},
        equal_weight_out_of_sample_metrics={"risk_adjusted_score": 1.2},
        max_out_of_sample_drawdown=0.20,
        min_closed_trades=1,
        walk_forward_available=True,
    )
    passed = candidate_recommendation(
        out_of_sample_metrics={"closed_trades": 2.0, "max_drawdown": 0.05, "risk_adjusted_score": 1.3},
        spy_out_of_sample_metrics={"risk_adjusted_score": 1.0},
        equal_weight_out_of_sample_metrics={"risk_adjusted_score": 1.2},
        max_out_of_sample_drawdown=0.20,
        min_closed_trades=1,
        walk_forward_available=True,
    )

    assert fail == "fail"
    assert review == "review"
    assert passed == "pass"


def test_format_strategy_comparison_includes_recommendation_and_selection_score() -> None:
    summary = pd.DataFrame(
        [
            {
                "rank": 1,
                "winner": True,
                "strategy": "breakout",
                "recommendation": "pass",
                "evaluation_mode": "walk_forward",
                "selection_score": 1.2,
                "out_of_sample_risk_adjusted_score": 1.1,
                "walk_forward_risk_adjusted_score": 1.0,
                "out_of_sample_total_return": 0.15,
                "out_of_sample_max_drawdown": 0.05,
                "out_of_sample_closed_trades": 6.0,
                "spy_out_of_sample_risk_adjusted_score_delta": 0.2,
                "equal_weight_out_of_sample_risk_adjusted_score_delta": 0.1,
            }
        ]
    )

    formatted = format_strategy_comparison(summary)

    assert "recommendation" in formatted.lower()
    assert "pass" in formatted
    assert "1.2000" in formatted


def test_format_single_strategy_summary_includes_recommendation_and_ranges() -> None:
    summary = {
        "strategy": "breakout",
        "recommendation": "review",
        "evaluation_mode": "out_of_sample",
        "combined_start_at": "2026-01-01T20:00:00+00:00",
        "combined_end_at": "2026-01-10T20:00:00+00:00",
        "combined_total_return": 0.10,
        "combined_max_drawdown": 0.04,
        "combined_risk_adjusted_score": 1.1,
        "combined_closed_trades": 4.0,
        "out_of_sample_start_at": "2026-01-08T20:00:00+00:00",
        "out_of_sample_end_at": "2026-01-10T20:00:00+00:00",
        "out_of_sample_total_return": 0.02,
        "out_of_sample_max_drawdown": 0.01,
        "out_of_sample_risk_adjusted_score": 0.4,
        "out_of_sample_closed_trades": 1.0,
        "spy_out_of_sample_risk_adjusted_score_delta": -0.1,
        "walk_forward_folds": 0.0,
        "walk_forward_risk_adjusted_score": 0.0,
    }

    formatted = format_single_strategy_summary(summary)

    assert "recommendation=review" in formatted
    assert "combined_range=" in formatted
    assert "out_of_sample_range=" in formatted


def test_write_benchmark_artifacts_writes_csvs_and_metadata(tmp_path) -> None:
    settings = _research_settings(tmp_path)
    bars = _multi_symbol_bars()
    result = evaluate_strategy_research(bars, settings, "momentum")
    summary = pd.DataFrame([result.summary])

    artifact_dir = write_benchmark_artifacts(summary, [result], settings=settings, source="synthetic")

    assert (artifact_dir / "strategy_comparison.csv").exists()
    assert (artifact_dir / "momentum_combined_trades.csv").exists()
    metadata = json.loads((artifact_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["source"] == "synthetic"
    assert metadata["config_profile"] == settings.config_profile


def test_run_backtest_command_prints_human_readable_summary(tmp_path, monkeypatch, capsys) -> None:
    settings = _research_settings(
        tmp_path,
        DATABASE_URL=f"sqlite:///{tmp_path / 'journal.db'}",
    )
    bars = _multi_symbol_bars()
    loaded = type("LoadedBars", (), {"bars": bars, "source": "synthetic", "production_safe": True})()
    validation = type(
        "ValidationResult",
        (),
        {"valid_bars": bars, "failed_symbols": [], "has_partial_failure": False},
    )()

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.load_bars_with_source", lambda *args, **kwargs: loaded)
    monkeypatch.setattr("app.main.validate_bars", lambda *args, **kwargs: validation)

    run_backtest_command(get_strategy("momentum"))
    output = capsys.readouterr().out

    assert "Backtest Summary" in output
    assert "recommendation=" in output
    assert "out_of_sample_total_return=" in output
