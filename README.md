# Trading Bot

Scaffold for an Alpaca-backed trading system with:

- historical data loading
- a simple momentum strategy
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

5. Run the paper-trading loop:

```bash
python -m app.main paper
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

## Current scope

Version `0.1.0` is intentionally narrow:

- US equities / ETFs
- long-only momentum
- daily bars
- SQLite persistence
- Alpaca paper trading only
