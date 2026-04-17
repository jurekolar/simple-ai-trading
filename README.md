# Trading Bot

Scaffold for an Alpaca-backed trading system with:

- historical data loading
- a simple momentum strategy
- a TradingView-aligned Donchian/Turtle breakout strategy
- bar-by-bar backtesting
- paper-trading orchestration
- SQLite trade journal storage
- Streamlit monitoring entrypoint

## Quick start

1. Create a virtual environment.
2. Install dependencies:

```bash
pip install -e .[dev]
```

3. Copy `.env.example` to `.env` and set Alpaca credentials.
4. Run the backtest:

```bash
python -m app.main backtest
```

To run a specific strategy explicitly:

```bash
python -m app.main --strategy momentum backtest
python -m app.main --strategy mean_reversion backtest
python -m app.main --strategy breakout backtest
python -m app.main --strategy trend_trailing_stop backtest
python -m app.main compare
```

If you want the TradingView-derived trend-following option in this repo, use:

```bash
python -m app.main --strategy breakout backtest
```

Registered strategies live under [app/strategy](/Users/jurekolar/Code/simple-ai-trading/app/strategy). To add a new one, create a module that exposes a strategy object with `name` and `generate_signals(...)`, then register it in [app/strategy/__init__.py](/Users/jurekolar/Code/simple-ai-trading/app/strategy/__init__.py).
Momentum keeps using `TREND_WINDOW` / `EXIT_WINDOW` / `ATR_WINDOW`; the example mean-reversion strategy uses its own `MEAN_REVERSION_*` settings.
The `breakout` strategy is the repo's TradingView-aligned Donchian/Turtle breakout and uses `BREAKOUT_ENTRY_WINDOW` / `BREAKOUT_EXIT_WINDOW` / `BREAKOUT_ATR_WINDOW`.
The `trend_trailing_stop` strategy uses `TREND_TRAILING_*` settings for its trend filter, breakout or pullback entries, and ATR or percent trailing stop exits.
`politician_copy` is allocation-based rather than bar-signal-based, so it supports `preview` and `paper`, but not `backtest` in v1.

5. Run the paper-trading loop:

```bash
python -m app.main paper
```

Preview the current politician-copy portfolio selection and target weights:

```bash
python -m app.main --strategy politician_copy preview
```

Run the politician-copy paper workflow:

```bash
python -m app.main --strategy politician_copy paper
```

To force a deterministic paper-trade exit test on currently held symbols, set
`FORCE_EXIT_SYMBOLS` before running `paper`, for example
`FORCE_EXIT_SYMBOLS=SPY python -m app.main paper`. Symbols without an open
position are ignored, and the normal strategy exit path still uses real `exit`
signals.

6. Reconcile broker order state into the local journal:

```bash
python -m app.main reconcile
```

7. Run the dashboard:

```bash
streamlit run app/monitoring/dashboard.py
```

## Paper Burn-In

For a structured 2-4 week paper burn-in:

1. Start from [.env.paper_burnin.example](/Users/jurekolar/Code/simple-ai-trading/.env.paper_burnin.example).
2. Review [PAPER_BURNIN_CHECKLIST.md](/Users/jurekolar/Code/simple-ai-trading/PAPER_BURNIN_CHECKLIST.md).
3. Run the burn-in summary script daily:

```bash
.venv/bin/python scripts/burnin_report.py --days 7
```

Before starting the burn-in, verify Alpaca trading auth and market-data auth separately:

```bash
.venv/bin/python scripts/check_alpaca_auth.py --symbol SPY
```

For the full daily burn-in workflow:

```bash
chmod +x scripts/run_burnin_day.sh
./scripts/run_burnin_day.sh
STRATEGY=breakout ./scripts/run_burnin_day.sh
```

For end-of-day review with archived summaries:

```bash
chmod +x scripts/review_burnin_day.sh
./scripts/review_burnin_day.sh
```

Operator procedures for startup, restart, stale data, broker outage, emergency flatten, and end-of-day signoff are in [OPERATOR_RUNBOOK.md](/Users/jurekolar/Code/simple-ai-trading/OPERATOR_RUNBOOK.md).
Use [logs/burnin/INCIDENT_TEMPLATE.md](/Users/jurekolar/Code/simple-ai-trading/logs/burnin/INCIDENT_TEMPLATE.md) to record each drill or incident consistently.
Manual close and partial-close expectations are documented in [MANUAL_POSITION_INTERVENTIONS.md](/Users/jurekolar/Code/simple-ai-trading/MANUAL_POSITION_INTERVENTIONS.md).

Or use `make` targets:

```bash
make burnin-day
make burnin-day STRATEGY=breakout
make burnin-review
```

## Current scope

Version `0.1.0` is intentionally narrow:

- US equities / ETFs
- long-only momentum
- daily bars
- SQLite persistence
- Alpaca paper trading only
