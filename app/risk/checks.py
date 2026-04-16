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
    candidates = candidates[candidates["atr"].notna() & (candidates["atr"] > 0)]
    if candidates.empty:
        candidates["qty"] = pd.Series(dtype=int)
        return candidates
    candidates["qty"] = candidates.apply(
        lambda row: min(
            math.floor(settings.atr_risk_budget / row["atr"]),
            math.floor(settings.max_position_notional / row["close"]),
        ),
        axis=1,
    )
    candidates = candidates[candidates["qty"] > 0]
    return candidates.nlargest(settings.max_positions, "close")


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


def entry_risk_decision(
    *,
    symbol: str,
    qty: int,
    close: float,
    active_symbols: set[str],
    symbol_exposure: float,
    gross_exposure: float,
    buying_power: float,
    cash: float,
    settings: Settings,
) -> EntryRiskDecision:
    order_notional = qty * close
    if len(active_symbols) >= settings.max_positions:
        return EntryRiskDecision(False, "max_positions")
    if gross_exposure + order_notional > settings.max_gross_exposure:
        return EntryRiskDecision(False, "max_gross_exposure")
    if symbol_exposure + order_notional > settings.max_symbol_exposure:
        return EntryRiskDecision(False, "max_symbol_exposure")
    if order_notional > buying_power:
        return EntryRiskDecision(False, "insufficient_buying_power")
    if cash - order_notional < settings.min_cash_buffer:
        return EntryRiskDecision(False, "min_cash_buffer")
    return EntryRiskDecision(True, "")
