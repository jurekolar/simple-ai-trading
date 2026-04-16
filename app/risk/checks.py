from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from app.config import Settings


@dataclass(frozen=True)
class EntryRiskDecision:
    allowed: bool
    reason: str = ""


def filter_trade_candidates(signal_frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    candidates = signal_frame[signal_frame["signal"] == "long"].copy()
    candidates = candidates[candidates["atr"].notna() & (candidates["atr"] > 0)].copy()
    if "liquidity_ok" in candidates.columns:
        candidates = candidates[candidates["liquidity_ok"]]
    if "volatility_ok" in candidates.columns:
        candidates = candidates[candidates["volatility_ok"]]
    if candidates.empty:
        candidates["qty"] = pd.Series(dtype=int)
        return candidates
    candidates["qty"] = candidates.apply(
        lambda row: compute_entry_qty(row=row, settings=settings),
        axis=1,
    )
    candidates = candidates[candidates["qty"] > 0]
    return rank_trade_candidates(candidates, settings)


def compute_entry_qty(*, row: pd.Series, settings: Settings) -> int:
    account_equity = float(row.get("account_equity", 0.0))
    risk_budget = max(settings.atr_risk_budget, account_equity * settings.risk_per_trade_fraction)
    atr = float(row.get("atr", 0.0))
    close = float(row.get("close", 0.0))
    if atr <= 0 or close <= 0:
        return 0
    return min(
        math.floor(risk_budget / atr),
        math.floor(settings.max_position_notional / close),
    )


def rank_trade_candidates(candidates: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    if candidates.empty:
        candidates["qty"] = pd.Series(dtype=int)
        return candidates
    ranked = candidates.copy()
    if "score" not in ranked.columns:
        trend_ma = ranked["trend_ma"] if "trend_ma" in ranked.columns else ranked["close"]
        exit_ma = ranked["exit_ma"] if "exit_ma" in ranked.columns else ranked["close"]
        ranked["score"] = (
            ((ranked["close"] - trend_ma) / ranked["atr"]).fillna(0.0)
            + 0.5 * ((ranked["close"] - exit_ma) / ranked["atr"]).fillna(0.0)
        )
    return ranked.nlargest(settings.max_symbols_per_run, "score")


def filter_exit_candidates(
    signal_frame: pd.DataFrame,
    position_qty_by_symbol: dict[str, float],
    forced_exit_symbols: set[str] | None = None,
) -> pd.DataFrame:
    candidates = signal_frame[signal_frame["signal"] == "exit"].copy()
    forced_exit_symbols = {symbol.upper() for symbol in (forced_exit_symbols or set())}

    if forced_exit_symbols:
        forced_rows = signal_frame[signal_frame["symbol"].isin(forced_exit_symbols)].copy()
        if not forced_rows.empty:
            forced_rows["signal"] = "exit"
            candidates = pd.concat([candidates, forced_rows], ignore_index=True)
            candidates = candidates.drop_duplicates(subset="symbol", keep="last")

    candidates["qty"] = candidates["symbol"].map(position_qty_by_symbol).fillna(0.0)
    candidates = candidates[candidates["qty"] > 0]
    candidates["qty"] = candidates["qty"].astype(int)
    return candidates.reset_index(drop=True)


def protective_exit_candidates(
    position_qty_by_symbol: dict[str, float],
    position_price_by_symbol: dict[str, float],
    symbols: set[str] | None = None,
) -> pd.DataFrame:
    target_symbols = {symbol.upper() for symbol in (symbols or set(position_qty_by_symbol))}
    rows: list[dict[str, object]] = []
    for symbol in sorted(target_symbols):
        qty = float(position_qty_by_symbol.get(symbol, 0.0))
        if qty <= 0:
            continue
        rows.append(
            {
                "symbol": symbol,
                "signal": "protective_exit",
                "close": float(position_price_by_symbol.get(symbol, 0.0)),
                "qty": int(qty),
            }
        )
    return pd.DataFrame(rows)


def daily_loss_ok(realized_pnl: float, settings: Settings) -> bool:
    return realized_pnl > -settings.max_daily_loss


def total_gross_exposure(position_market_values: list[float]) -> float:
    return float(sum(abs(value) for value in position_market_values))


def total_unrealized_pnl(position_unrealized_pnl: list[float]) -> float:
    return float(sum(position_unrealized_pnl))


def enforce_portfolio_limits(
    *,
    active_symbols: set[str],
    gross_exposure: float,
    reserved_gross_exposure: float,
    order_notional: float,
    settings: Settings,
) -> EntryRiskDecision:
    if len(active_symbols) >= settings.max_positions:
        return EntryRiskDecision(False, "max_positions")
    if gross_exposure + reserved_gross_exposure + order_notional > settings.max_gross_exposure:
        return EntryRiskDecision(False, "max_gross_exposure")
    return EntryRiskDecision(True, "")


def enforce_buying_power_limit(
    *,
    order_notional: float,
    buying_power: float,
    reserved_buying_power: float,
    cash: float,
    reserved_cash: float,
    settings: Settings,
) -> EntryRiskDecision:
    if order_notional > max(buying_power - reserved_buying_power, 0.0):
        return EntryRiskDecision(False, "insufficient_buying_power")
    if cash - reserved_cash - order_notional < settings.min_cash_buffer:
        return EntryRiskDecision(False, "min_cash_buffer")
    return EntryRiskDecision(True, "")


def enforce_symbol_exposure_limit(
    *,
    symbol_exposure: float,
    order_notional: float,
    settings: Settings,
) -> EntryRiskDecision:
    if symbol_exposure + order_notional > settings.max_symbol_exposure:
        return EntryRiskDecision(False, "max_symbol_exposure")
    return EntryRiskDecision(True, "")


def entry_risk_decision(
    *,
    symbol: str,
    qty: int,
    close: float,
    active_symbols: set[str],
    symbol_exposure: float,
    gross_exposure: float,
    reserved_gross_exposure: float,
    buying_power: float,
    cash: float,
    reserved_buying_power: float,
    reserved_cash: float,
    settings: Settings,
) -> EntryRiskDecision:
    order_notional = qty * close
    for decision in (
        enforce_portfolio_limits(
            active_symbols=active_symbols,
            gross_exposure=gross_exposure,
            reserved_gross_exposure=reserved_gross_exposure,
            order_notional=order_notional,
            settings=settings,
        ),
        enforce_symbol_exposure_limit(
            symbol_exposure=symbol_exposure,
            order_notional=order_notional,
            settings=settings,
        ),
        enforce_buying_power_limit(
            order_notional=order_notional,
            buying_power=buying_power,
            reserved_buying_power=reserved_buying_power,
            cash=cash,
            reserved_cash=reserved_cash,
            settings=settings,
        ),
    ):
        if not decision.allowed:
            return decision
    return EntryRiskDecision(True, "")
