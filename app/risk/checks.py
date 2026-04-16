from __future__ import annotations

import math

import pandas as pd

from app.config import Settings


def filter_trade_candidates(signal_frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    candidates = signal_frame[signal_frame["signal"] == "long"].copy()
    candidates = candidates[candidates["atr"].notna() & (candidates["atr"] > 0)]
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


def daily_loss_ok(realized_pnl: float, settings: Settings) -> bool:
    return realized_pnl > -settings.max_daily_loss
