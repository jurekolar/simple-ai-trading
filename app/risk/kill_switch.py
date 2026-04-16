from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from app.data.market_calendar import previous_trading_date

@dataclass(frozen=True)
class KillSwitchState:
    enabled: bool
    reason: str = ""


def evaluate_kill_switch(data_is_stale: bool, realized_pnl: float, max_daily_loss: float) -> KillSwitchState:
    if data_is_stale:
        return KillSwitchState(True, "stale_data")
    if realized_pnl <= -max_daily_loss:
        return KillSwitchState(True, "daily_loss_limit")
    return KillSwitchState(False, "")


def data_is_stale(
    bars: pd.DataFrame,
    *,
    source: str,
    now: datetime | None = None,
) -> bool:
    if source != "alpaca":
        return True
    if bars.empty or "timestamp" not in bars.columns:
        return True

    latest_timestamp = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce").max()
    if pd.isna(latest_timestamp):
        return True

    latest_bar_date = latest_timestamp.tz_convert("America/New_York").date()
    return latest_bar_date < previous_trading_date(now)
