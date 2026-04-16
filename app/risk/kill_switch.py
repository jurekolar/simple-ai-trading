from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pandas as pd

from app.data.market_calendar import previous_trading_date


KillSwitchSeverity = Literal["none", "reduce_only", "flatten"]


@dataclass(frozen=True)
class KillSwitchState:
    severity: KillSwitchSeverity = "none"
    reason: str = ""

    @property
    def enabled(self) -> bool:
        return self.severity != "none"

    @property
    def block_new_entries(self) -> bool:
        return self.severity in {"reduce_only", "flatten"}

    @property
    def allow_exits(self) -> bool:
        return self.enabled

    @property
    def force_flatten(self) -> bool:
        return self.severity == "flatten"


def evaluate_kill_switch(
    data_is_stale: bool,
    partial_data_failure: bool,
    realized_pnl: float,
    unrealized_pnl: float,
    broker_failure_count: int,
    open_order_count: int,
    has_stuck_orders: bool,
    max_daily_loss: float,
    max_unrealized_drawdown: float,
    emergency_unrealized_drawdown: float,
    max_broker_failures: int,
    max_open_orders: int,
) -> KillSwitchState:
    if data_is_stale:
        return KillSwitchState("reduce_only", "stale_data")
    if partial_data_failure:
        return KillSwitchState("reduce_only", "partial_data_failure")
    if realized_pnl <= -max_daily_loss:
        return KillSwitchState("reduce_only", "daily_loss_limit")
    if unrealized_pnl <= -emergency_unrealized_drawdown:
        return KillSwitchState("flatten", "emergency_unrealized_drawdown")
    if unrealized_pnl <= -max_unrealized_drawdown:
        return KillSwitchState("reduce_only", "max_unrealized_drawdown")
    if broker_failure_count >= max_broker_failures:
        return KillSwitchState("reduce_only", "broker_failures")
    if open_order_count > max_open_orders:
        return KillSwitchState("reduce_only", "too_many_open_orders")
    if has_stuck_orders:
        return KillSwitchState("reduce_only", "stuck_orders")
    return KillSwitchState("none", "")


def assess_reconciliation_health(
    local_position_qty_by_symbol: dict[str, float],
    broker_position_qty_by_symbol: dict[str, float],
) -> KillSwitchState:
    local_symbols = set(local_position_qty_by_symbol)
    broker_symbols = set(broker_position_qty_by_symbol)
    if local_symbols != broker_symbols:
        return KillSwitchState("flatten", "reconciliation_symbol_mismatch")

    max_qty_diff = 0.0
    for symbol in broker_symbols:
        qty_diff = abs(local_position_qty_by_symbol.get(symbol, 0.0) - broker_position_qty_by_symbol.get(symbol, 0.0))
        max_qty_diff = max(max_qty_diff, qty_diff)

    if max_qty_diff >= 1.0:
        return KillSwitchState("flatten", "reconciliation_qty_mismatch")
    if max_qty_diff > 0.0:
        return KillSwitchState("reduce_only", "reconciliation_qty_drift")
    return KillSwitchState("none", "")


def merge_kill_switch_states(*states: KillSwitchState) -> KillSwitchState:
    severity_rank = {"none": 0, "reduce_only": 1, "flatten": 2}
    strongest = KillSwitchState("none", "")
    for state in states:
        if severity_rank[state.severity] > severity_rank[strongest.severity]:
            strongest = state
    return strongest


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
