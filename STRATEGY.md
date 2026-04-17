# Strategy

This repository currently defaults to a narrow, long-only daily-bar momentum strategy for a fixed list of US equities and ETFs. Its preferred TradingView-derived trend-following option is the `breakout` strategy, which implements a Donchian/Turtle-style long-only breakout over prior channel highs and lows and is the recommended first live candidate.

## Scope

- Asset class: US equities / ETFs
- Direction: long only
- Data frequency: daily bars
- Execution style: market orders
- Default mode: Alpaca paper trading, with `DRY_RUN=true`

## Universe Selection

The strategy does not scan the market dynamically.

It trades only the symbols listed in the `SYMBOLS` configuration value. The current default is:

```text
SPY,QQQ,IWM,AAPL,MSFT
```

Those symbols are:

1. Loaded from `Settings.symbol_list` in [app/config.py](/Users/jurekolar/Code/simple-ai-trading/app/config.py:63)
2. Requested from the data loader in [app/data/historical_loader.py](/Users/jurekolar/Code/simple-ai-trading/app/data/historical_loader.py:32)
3. Validated symbol by symbol in [app/data/historical_loader.py](/Users/jurekolar/Code/simple-ai-trading/app/data/historical_loader.py:47)

Any symbol with missing, invalid, duplicated, too-short, or suspicious bar history is excluded from signal generation.

## Indicators

For each symbol, the strategy computes:

- `trend_ma`: rolling mean of close over `TREND_WINDOW` days, default `100`
- `exit_ma`: rolling mean of close over `EXIT_WINDOW` days, default `50`
- `atr`: average true range over `ATR_WINDOW` days, default `14`
- `avg_volume`: rolling average volume over `ATR_WINDOW` days
- `atr_ratio`: `atr / close`
- `score`: momentum ranking score

Implementation: [app/strategy/momentum.py](/Users/jurekolar/Code/simple-ai-trading/app/strategy/momentum.py:8)

## Signal Rules

### Long Signal

A symbol gets a `long` signal when all of the following are true:

- `close > trend_ma`
- `avg_volume >= MIN_AVERAGE_DAILY_VOLUME`
- `atr_ratio <= MAX_ATR_RATIO`

Defaults:

- `MIN_AVERAGE_DAILY_VOLUME = 500000`
- `MAX_ATR_RATIO = 0.12`

Implementation: [app/strategy/momentum.py](/Users/jurekolar/Code/simple-ai-trading/app/strategy/momentum.py:38)

### Exit Signal

A symbol gets an `exit` signal when:

- `close < exit_ma`

Implementation: [app/strategy/momentum.py](/Users/jurekolar/Code/simple-ai-trading/app/strategy/momentum.py:43)

### Flat Signal

If neither rule applies, the symbol remains `flat`.

## Candidate Ranking

After signal generation, the bot takes the most recent bar for each symbol and keeps only `long` candidates that still have:

- valid ATR
- positive ATR
- liquidity filter passing
- volatility filter passing

Each candidate is then sized and ranked by:

```text
score = ((close - trend_ma) / atr) + 0.5 * ((close - exit_ma) / atr)
```

Only the top `MAX_SYMBOLS_PER_RUN` symbols are kept. The default is `3`.

Implementation:

- [app/strategy/signals.py](/Users/jurekolar/Code/simple-ai-trading/app/strategy/signals.py:4)
- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:17)
- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:48)

## Position Sizing

Entry quantity is computed as:

```text
risk_budget = max(ATR_RISK_BUDGET, account_equity * RISK_PER_TRADE_FRACTION)
qty = min(floor(risk_budget / atr), floor(MAX_POSITION_NOTIONAL / close))
```

Defaults:

- `ATR_RISK_BUDGET = 100`
- `RISK_PER_TRADE_FRACTION = 0.01`
- `MAX_POSITION_NOTIONAL = 20000`

Implementation: [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:35)

## Buy Logic

During the paper-trading loop, the bot attempts a buy only if all of the following are true:

1. The market is open.
2. The symbol is one of the ranked `long` candidates.
3. There is no current position in that symbol.
4. There is no open broker order or unresolved broker state for that symbol.
5. `DENY_NEW_ENTRIES` is not enabled.
6. The kill switch is not blocking new entries.
7. The data source is safe for trading mode.
8. Portfolio and buying-power checks pass.

The portfolio and risk checks include:

- `MAX_POSITIONS`
- `MAX_GROSS_EXPOSURE`
- `MAX_SYMBOL_EXPOSURE`
- available buying power
- `MIN_CASH_BUFFER`

Implementation:

- [app/main.py](/Users/jurekolar/Code/simple-ai-trading/app/main.py:389)
- [app/main.py](/Users/jurekolar/Code/simple-ai-trading/app/main.py:602)
- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:118)
- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:160)

## Sell Logic

### Normal Strategy Exit

The bot attempts to sell a held symbol when the latest signal for that symbol is `exit`, which currently means:

```text
close < exit_ma
```

Exit quantity is the full current held position size.

Implementation:

- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:63)
- [app/main.py](/Users/jurekolar/Code/simple-ai-trading/app/main.py:540)

### Forced Exit Override

If `FORCE_EXIT_SYMBOLS` contains a held symbol, that symbol is treated as an exit candidate even without a normal strategy exit signal.

Implementation:

- [app/config.py](/Users/jurekolar/Code/simple-ai-trading/app/config.py:67)
- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:71)

### Protective Exit Mode

If market data is stale, or trading mode is using an unsafe data source, the bot can bypass normal signal-based exits and instead generate protective exits directly from the broker's current position state.

Implementation:

- [app/main.py](/Users/jurekolar/Code/simple-ai-trading/app/main.py:518)
- [app/risk/checks.py](/Users/jurekolar/Code/simple-ai-trading/app/risk/checks.py:84)

### Emergency Flatten

If the kill switch forces flattening, or `EMERGENCY_FLATTEN=true`, the bot attempts to close positions immediately.

Implementation:

- [app/main.py](/Users/jurekolar/Code/simple-ai-trading/app/main.py:479)
- [app/main.py](/Users/jurekolar/Code/simple-ai-trading/app/main.py:530)

## Order Submission

- Entries are submitted as market buys.
- Exits are submitted as market sells.
- Exits larger than `MAX_ORDER_QTY` are split into smaller chunks.
- Entry orders are rejected if the symbol is not whitelisted, if quantity is non-positive, if quantity exceeds `MAX_ORDER_QTY`, or if notional exceeds `MAX_POSITION_NOTIONAL`.

Implementation: [app/broker/execution.py](/Users/jurekolar/Code/simple-ai-trading/app/broker/execution.py:30)

## Flow Summary

```text
Fixed symbol list
  -> load daily bars
  -> validate symbols
  -> compute indicators
  -> assign long / exit / flat
  -> take latest bar per symbol
  -> rank long candidates
  -> size positions
  -> block or allow entries via risk checks
  -> submit market buys

Held positions
  -> normal exit if close < 50-day MA
  -> or forced / protective / emergency exit
  -> submit market sells
```

## Current Default Parameters

### Universe / Data

- `SYMBOLS = SPY,QQQ,IWM,AAPL,MSFT`
- `LOOKBACK_DAYS = 400`
- `MIN_HISTORY_DAYS = 120`
- `ALPACA_DATA_FEED = iex`
- `ALLOW_UNSAFE_DATA_FALLBACK = false`
- `ALLOW_PARTIAL_MARKET_DATA = false`

### Strategy

- `TREND_WINDOW = 100`
- `EXIT_WINDOW = 50`
- `ATR_WINDOW = 14`
- `MEAN_REVERSION_WINDOW = 20`
- `MEAN_REVERSION_VOLATILITY_WINDOW = 20`
- `MEAN_REVERSION_ENTRY_ZSCORE = -1.0`
- `MEAN_REVERSION_EXIT_ZSCORE = 0.0`
- `BREAKOUT_ENTRY_WINDOW = 55`
- `BREAKOUT_EXIT_WINDOW = 20`
- `BREAKOUT_ATR_WINDOW = 20`
- `TREND_TRAILING_TREND_WINDOW = 100`
- `TREND_TRAILING_BREAKOUT_WINDOW = 252`
- `TREND_TRAILING_PULLBACK_FAST_WINDOW = 20`
- `TREND_TRAILING_PULLBACK_SLOW_WINDOW = 50`
- `TREND_TRAILING_ATR_WINDOW = 14`
- `TREND_TRAILING_STOP_TYPE = atr`
- `TREND_TRAILING_ATR_MULTIPLIER = 3.0`
- `TREND_TRAILING_PERCENT = 0.08`
- `MIN_AVERAGE_DAILY_VOLUME = 500000`
- `MAX_ATR_RATIO = 0.12`

## Additional Strategy Differences

`breakout` is the repo's TradingView-aligned Donchian/Turtle trend-following strategy:

- entry triggers when `close >` the prior rolling high over `BREAKOUT_ENTRY_WINDOW`
- exit triggers when `close <` the prior rolling low over `BREAKOUT_EXIT_WINDOW`
- both channel levels use `shift(1)` so the current bar does not leak into its own signal
- ATR, liquidity, and ATR-ratio filters stay in place for compatibility with the existing sizing and risk pipeline
- it remains long-only, so lower-channel breaks produce `exit`, not `short`

`trend_trailing_stop` extends the current bar-based strategy set with a continuation-focused long setup:

- versus `momentum`: it requires either a breakout or a pullback into the `20`/`50` MA zone and exits on a trailing stop instead of a fixed exit MA
- versus `breakout`: it supports both breakout and pullback entries and uses a ratcheting trailing stop instead of an exit-low channel
- versus `mean_reversion`: it buys strength in established uptrends instead of buying weakness against a short-term mean

Use the comparison command to inspect candidate count, exposure, selected symbols, and average score side by side:

```bash
python -m app.main compare
```

### Throughput / Portfolio Limits

- `MAX_SYMBOLS_PER_RUN = 3`
- `MAX_POSITIONS = 3`
- `MAX_POSITION_NOTIONAL = 20000`
- `MAX_GROSS_EXPOSURE = 50000`
- `MAX_SYMBOL_EXPOSURE = 20000`
- `MIN_CASH_BUFFER = 0`

### Sizing

- `ATR_RISK_BUDGET = 100`
- `RISK_PER_TRADE_FRACTION = 0.01`

### Operational Risk Limits

- `MAX_DAILY_LOSS = 1000`
- `MAX_UNREALIZED_DRAWDOWN = 1500`
- `EMERGENCY_UNREALIZED_DRAWDOWN = 2500`
- `MAX_OPEN_ORDERS = 8`
- `MAX_STUCK_ORDER_MINUTES = 20`
- `MAX_BROKER_FAILURES = 3`

### Order Constraints

- `MAX_ORDER_QTY = 25`

### Manual Overrides

- `FORCE_EXIT_SYMBOLS = ""`
- `EMERGENCY_FLATTEN = false`
- `DENY_NEW_ENTRIES = false`

### Execution Mode

- `DRY_RUN = true`
- `PAPER_ONLY = true`
- `ALLOW_LIVE = false`
- `ALPACA_PAPER = true`

## SPY Example

Example entry:

- `SPY close = 545`
- `trend_ma = 532`
- `exit_ma = 538`
- `atr = 6`
- average volume passes
- `atr_ratio = 6 / 545 = 0.011`, which passes

That produces a `long` signal because:

- `545 > 532`
- liquidity passes
- volatility passes

If account equity is `25000`, then:

- `risk_budget = max(100, 25000 * 0.01) = 250`
- ATR sizing gives `floor(250 / 6) = 41`
- notional sizing gives `floor(20000 / 545) = 36`
- final quantity is `36`

The bot then still checks:

- no existing SPY position
- no open or unresolved SPY order
- max positions not exceeded
- gross exposure limit
- symbol exposure limit
- buying power
- cash buffer
- kill switch / data safety gates

If all pass, it submits a market buy for `36` shares.

Example exit:

- later `SPY close = 530`
- `exit_ma = 538`

Since `530 < 538`, the signal becomes `exit`. If the bot holds `36` shares and there is no unresolved broker state, it submits a market sell for the full `36`.
