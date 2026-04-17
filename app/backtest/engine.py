from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.backtest.fills import fill_price_for_side
from app.backtest.metrics import summarize
from app.config import Settings
from app.risk.checks import entry_risk_decision, filter_trade_candidates
from app.strategy import get_strategy
from app.strategy.base import TradingStrategy


BACKTEST_INITIAL_EQUITY = 100_000.0


@dataclass
class BacktestPosition:
    qty: int
    entry_price: float
    entry_timestamp: pd.Timestamp
    last_price: float


@dataclass(frozen=True)
class BacktestResult:
    trade_log: pd.DataFrame
    equity_curve: pd.DataFrame
    metrics: dict[str, float | str]


def _strategy_bars(bars: pd.DataFrame, strategy_name: str, settings: Settings) -> pd.DataFrame:
    if strategy_name == "mean_reversion":
        return bars.copy()
    return bars[bars["symbol"].astype(str).str.upper().isin(settings.symbol_list)].copy()


def run_backtest_detailed(
    bars: pd.DataFrame,
    settings: Settings,
    strategy: TradingStrategy | None = None,
    *,
    start_at: pd.Timestamp | None = None,
    end_at: pd.Timestamp | None = None,
) -> BacktestResult:
    active_strategy = strategy or get_strategy("momentum")
    strategy_bars = _strategy_bars(bars, active_strategy.name, settings)
    if "timestamp" in strategy_bars.columns:
        strategy_bars = strategy_bars.copy()
        strategy_bars["timestamp"] = pd.to_datetime(strategy_bars["timestamp"], utc=True)
    if start_at is not None:
        strategy_bars = strategy_bars[strategy_bars["timestamp"] >= pd.Timestamp(start_at)]
    if end_at is not None:
        strategy_bars = strategy_bars[strategy_bars["timestamp"] <= pd.Timestamp(end_at)]
    signal_frame = active_strategy.generate_signals(strategy_bars, settings).sort_values(["timestamp", "symbol"])
    if signal_frame.empty:
        empty_frame = pd.DataFrame()
        return BacktestResult(
            trade_log=empty_frame,
            equity_curve=empty_frame,
            metrics=summarize(pd.DataFrame(), pd.DataFrame(), initial_equity=BACKTEST_INITIAL_EQUITY),
        )

    positions: dict[str, BacktestPosition] = {}
    trade_rows: list[dict[str, object]] = []
    equity_rows: list[dict[str, object]] = []
    cash = BACKTEST_INITIAL_EQUITY

    for timestamp, day_frame in signal_frame.groupby("timestamp", sort=True):
        day_frame = day_frame.sort_values("symbol").reset_index(drop=True)
        close_by_symbol = {
            str(row.symbol): float(row.close)
            for row in day_frame[["symbol", "close"]].itertuples(index=False)
        }
        for symbol, position in positions.items():
            if symbol in close_by_symbol:
                position.last_price = close_by_symbol[symbol]

        exit_rows = day_frame[
            (day_frame["signal"] == "exit") & day_frame["symbol"].isin(positions)
        ]
        for row in exit_rows.itertuples(index=False):
            symbol = str(row.symbol)
            position = positions.pop(symbol)
            fill_price = fill_price_for_side(float(row.close), "sell")
            proceeds = float(position.qty) * fill_price
            cash += proceeds
            realized_pnl = float(position.qty) * (fill_price - position.entry_price)
            holding_days = max((pd.Timestamp(timestamp) - position.entry_timestamp).days, 0)
            trade_rows.append(
                {
                    "timestamp": pd.Timestamp(timestamp),
                    "symbol": symbol,
                    "side": "sell",
                    "qty": float(position.qty),
                    "close": float(row.close),
                    "fill_price": fill_price,
                    "notional": proceeds,
                    "realized_pnl": realized_pnl,
                    "holding_days": float(holding_days),
                    "signal": "exit",
                }
            )

        position_value = float(
            sum(position.qty * close_by_symbol.get(symbol, position.last_price) for symbol, position in positions.items())
        )
        gross_exposure = position_value
        account_equity = cash + position_value
        entry_frame = day_frame.copy()
        entry_frame["account_equity"] = account_equity
        entry_candidates = filter_trade_candidates(entry_frame, settings)

        for row in entry_candidates.itertuples(index=False):
            symbol = str(row.symbol)
            if symbol in positions:
                continue
            decision = entry_risk_decision(
                symbol=symbol,
                qty=int(row.qty),
                close=float(row.close),
                active_symbols=set(positions),
                symbol_exposure=0.0,
                gross_exposure=gross_exposure,
                reserved_gross_exposure=0.0,
                buying_power=cash,
                cash=cash,
                reserved_buying_power=0.0,
                reserved_cash=0.0,
                settings=settings,
            )
            if not decision.allowed:
                continue
            fill_price = fill_price_for_side(float(row.close), "buy")
            notional = float(row.qty) * fill_price
            if notional > cash:
                continue
            positions[symbol] = BacktestPosition(
                qty=int(row.qty),
                entry_price=fill_price,
                entry_timestamp=pd.Timestamp(timestamp),
                last_price=float(row.close),
            )
            cash -= notional
            gross_exposure += float(row.qty) * float(row.close)
            trade_rows.append(
                {
                    "timestamp": pd.Timestamp(timestamp),
                    "symbol": symbol,
                    "side": "buy",
                    "qty": float(row.qty),
                    "close": float(row.close),
                    "fill_price": fill_price,
                    "notional": notional,
                    "realized_pnl": 0.0,
                    "holding_days": 0.0,
                    "signal": "long",
                }
            )

        position_value = float(
            sum(position.qty * close_by_symbol.get(symbol, position.last_price) for symbol, position in positions.items())
        )
        equity_rows.append(
            {
                "timestamp": pd.Timestamp(timestamp),
                "cash": float(cash),
                "gross_exposure": position_value,
                "equity": float(cash + position_value),
                "open_positions": float(len(positions)),
            }
        )

    trade_log = pd.DataFrame(trade_rows)
    equity_curve = pd.DataFrame(equity_rows)
    metrics = summarize(trade_log, equity_curve, initial_equity=BACKTEST_INITIAL_EQUITY)
    return BacktestResult(trade_log=trade_log, equity_curve=equity_curve, metrics=metrics)


def run_backtest(
    bars: pd.DataFrame,
    settings: Settings,
    strategy: TradingStrategy | None = None,
    *,
    start_at: pd.Timestamp | None = None,
    end_at: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict[str, float | str]]:
    result = run_backtest_detailed(
        bars,
        settings,
        strategy=strategy,
        start_at=start_at,
        end_at=end_at,
    )
    return result.trade_log, result.metrics
