# Backtest Benchmarking

This guide covers how to run the repo's historical backtests and how to compare the supported bar-based strategies against each other.

## What This Covers

The benchmark workflow currently compares these strategies:

- `momentum`
- `mean_reversion`
- `breakout`
- `trend_trailing_stop`

`politician_copy` is excluded because it does not have a historical backtest model in this repo.

The comparison is risk-adjusted first. Strategies are ranked by:

1. `risk_adjusted_score`
2. lower `max_drawdown`
3. higher `total_return`

## Prerequisites

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -e .[dev]
```

3. Copy `.env.example` to `.env`.
4. Set Alpaca credentials if you want live market history from Alpaca.

If Alpaca data is unavailable, the loader may fall back to synthetic data. That is useful for development, but not for choosing a real strategy winner.

## Core Commands

Run a single-strategy historical backtest:

```bash
python -m app.main --strategy momentum backtest
python -m app.main --strategy mean_reversion backtest
python -m app.main --strategy breakout backtest
python -m app.main --strategy trend_trailing_stop backtest
```

Run the full strategy benchmark:

```bash
python -m app.main compare
```

If you are using the repo venv explicitly:

```bash
.venv/bin/python -m app.main compare
```

## What `compare` Does

`compare` runs the same validated historical bar set through each backtest-supported strategy and prints one row per strategy.

Each row includes:

- `rank`
- `winner`
- `strategy`
- `risk_adjusted_score`
- `sharpe_like`
- `total_return`
- `annualized_return`
- `max_drawdown`
- `volatility`
- `win_rate`
- `trades`
- `closed_trades`
- `avg_holding_days`
- `turnover`
- `gross_exposure_usage`
- `gross_exposure`

The `winner` column uses `*` for the top-ranked strategy.

## How To Read The Output

- Start with `rank` and `winner`.
- Then inspect `risk_adjusted_score` and `sharpe_like` to see which strategy delivered the best return quality.
- Check `max_drawdown` before trusting a high-return strategy.
- Use `total_return` and `annualized_return` to compare growth.
- Use `trades`, `closed_trades`, `avg_holding_days`, and `turnover` to understand operational behavior.
- Use `gross_exposure_usage` to see how much of the simulated capital the strategy tended to deploy.

In practice:

- Higher `risk_adjusted_score` is better.
- Lower `max_drawdown` is better.
- Higher `total_return` is better, but only after checking drawdown and consistency.

## Important Notes

- Backtests use a shared starting equity of `100000`.
- The simulator is daily-bar and long-only.
- Slippage is applied on both entries and exits.
- The benchmark is a comparison of the current strategies under one shared config and one shared symbol universe.
- This is not parameter optimization, walk-forward testing, or hyperparameter search.

## Recommended Workflow

1. Confirm your `.env` symbol universe and strategy settings are what you actually want to test.
2. Run a single-strategy backtest for the candidate you care about most.
3. Run `python -m app.main compare`.
4. Review the winner, then compare the top two strategies on:
   - `risk_adjusted_score`
   - `max_drawdown`
   - `total_return`
   - `turnover`
5. Treat the compare output as a screening step.
6. Paper-trade the leading candidate before making any live decision.

## Troubleshooting

If `mean_reversion` fails:

- check `MEAN_REVERSION_BENCHMARK_SYMBOL`
- make sure benchmark history is available in the loaded bars

If the output looks unrealistic:

- verify Alpaca credentials
- confirm the loader is not using fallback or synthetic data
- check your `SYMBOLS`, `LOOKBACK_DAYS`, and `MIN_HISTORY_DAYS` settings

If the benchmark produces few or zero trades:

- inspect the active symbol universe
- check liquidity and volatility filters
- review the strategy-specific windows and thresholds in `.env`

## Useful Validation

Run tests around the benchmarking path:

```bash
.venv/bin/python -m pytest -q tests/test_backtest_benchmarking.py
.venv/bin/python -m pytest -q
```
