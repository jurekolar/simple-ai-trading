from __future__ import annotations

import argparse
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

from app.backtest.engine import run_backtest
from app.broker.alpaca_client import AlpacaTradingAdapter
from app.broker.alpaca_client import BrokerClosePositionError, BrokerExposureSnapshot, ReconciliationSnapshot
from app.broker.execution import PaperExecutor
from app.broker.order_mapper import OrderIntent
from app.config import get_settings
from app.data.historical_loader import load_bars_with_source, validate_bars
from app.data.market_calendar import market_day_window
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.logging_setup import configure_logging
from app.monitoring.alerts import send_alerts
from app.reports.daily_report import build_daily_report
from app.risk.checks import (
    entry_risk_decision,
    filter_exit_candidates,
    filter_trade_candidates,
    protective_exit_candidates,
    total_gross_exposure,
    total_unrealized_pnl,
)
from app.risk.kill_switch import data_is_stale, evaluate_kill_switch
from app.risk.kill_switch import assess_reconciliation_health, merge_kill_switch_states
from app.scheduler import should_run_trading_loop
from app.strategy.momentum import generate_signals
from app.strategy.signals import latest_signals

LOGGER = logging.getLogger(__name__)
FEE_MODEL_LABEL = "estimated_activity_allocation"


@dataclass(frozen=True)
class FillLot:
    broker_order_id: str
    client_order_id: str
    qty: float
    price: float
    fees: float
    filled_at: datetime


def compute_realized_pnl_records(
    fills: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    lots_by_symbol: dict[str, deque[FillLot]] = defaultdict(deque)
    realized_qty_by_symbol: dict[str, float] = defaultdict(float)
    realized_pnl_by_symbol: dict[str, float] = defaultdict(float)
    last_fill_at_by_symbol: dict[str, object] = {}
    lot_matches: list[dict[str, object]] = []

    sorted_fills = sorted(fills, key=lambda fill: fill["filled_at"])
    for fill in sorted_fills:
        symbol = str(fill["symbol"])
        side = str(fill["side"]).lower()
        qty = float(fill["qty"])
        price = float(fill["price"])
        fees = float(fill.get("fees", 0.0))
        filled_at = fill["filled_at"]
        if isinstance(filled_at, datetime) and filled_at.tzinfo is None:
            filled_at = filled_at.replace(tzinfo=UTC)
        last_fill_at_by_symbol[symbol] = filled_at

        if "buy" in side:
            lots_by_symbol[symbol].append(
                FillLot(
                    broker_order_id=str(fill["broker_order_id"]),
                    client_order_id=str(fill["client_order_id"]),
                    qty=qty,
                    price=price,
                    fees=fees,
                    filled_at=filled_at,
                )
            )
            continue

        remaining = qty
        sell_fee_per_share = fees / qty if qty else 0.0
        while remaining > 0 and lots_by_symbol[symbol]:
            lot = lots_by_symbol[symbol][0]
            matched_qty = min(remaining, lot.qty)
            buy_fee_per_share = lot.fees / lot.qty if lot.qty else 0.0
            matched_fees = matched_qty * (buy_fee_per_share + sell_fee_per_share)
            realized_pnl = matched_qty * (price - lot.price) - matched_fees
            realized_qty_by_symbol[symbol] += matched_qty
            realized_pnl_by_symbol[symbol] += realized_pnl
            lot_matches.append(
                {
                    "symbol": symbol,
                    "open_broker_order_id": lot.broker_order_id,
                    "close_broker_order_id": str(fill["broker_order_id"]),
                    "open_client_order_id": lot.client_order_id,
                    "close_client_order_id": str(fill["client_order_id"]),
                    "matched_qty": matched_qty,
                    "open_price": lot.price,
                    "close_price": price,
                    "fees": matched_fees,
                    "realized_pnl": realized_pnl,
                    "execution_date": filled_at.astimezone(UTC).date(),
                    "open_filled_at": lot.filled_at,
                    "close_filled_at": filled_at,
                }
            )
            remaining -= matched_qty

            if matched_qty == lot.qty:
                lots_by_symbol[symbol].popleft()
            else:
                remaining_buy_fees = max(lot.fees - (matched_qty * buy_fee_per_share), 0.0)
                lots_by_symbol[symbol][0] = FillLot(
                    broker_order_id=lot.broker_order_id,
                    client_order_id=lot.client_order_id,
                    qty=lot.qty - matched_qty,
                    price=lot.price,
                    fees=remaining_buy_fees,
                    filled_at=lot.filled_at,
                )

    realized_records = [
        {
            "symbol": symbol,
            "realized_qty": realized_qty_by_symbol[symbol],
            "realized_pnl": realized_pnl_by_symbol[symbol],
            "last_fill_at": last_fill_at_by_symbol[symbol],
        }
        for symbol in sorted(realized_pnl_by_symbol)
    ]
    return lot_matches, realized_records


def reconcile_broker_state(repo: JournalRepo, broker: AlpacaTradingAdapter) -> dict[str, int]:
    account = broker.get_account_summary()
    open_orders = broker.list_open_orders(limit=50)
    recent_orders = broker.list_recent_orders(limit=50)
    positions = broker.list_positions()
    portfolio_history = broker.get_portfolio_pnl_history(period="1M", timeframe="1D")

    for order in recent_orders:
        repo.sync_broker_order(
            symbol=order.symbol,
            side=order.side,
            qty=float(order.qty),
            status=order.status,
            client_order_id=order.client_order_id,
            broker_order_id=order.id,
            filled_avg_price=float(order.filled_avg_price or 0.0),
        )

    execution_fills = broker.list_execution_fills(limit=200)
    if not execution_fills:
        execution_fills = [
            {
                "broker_order_id": order.id,
                "client_order_id": order.client_order_id,
                "symbol": order.symbol,
                "side": order.side,
                "qty": float(order.filled_qty or 0.0),
                "price": float(order.filled_avg_price or 0.0),
                "gross_amount": float(order.filled_qty or 0.0) * float(order.filled_avg_price or 0.0),
                "fees": 0.0,
                "net_amount": float(order.filled_qty or 0.0) * float(order.filled_avg_price or 0.0),
                "execution_date": order.filled_at.astimezone(UTC).date(),
                "filled_at": order.filled_at,
            }
            for order in recent_orders
            if order.filled_at is not None and float(order.filled_qty or 0.0) > 0
        ]
    repo.replace_execution_fills(execution_fills)
    realized_lot_matches, realized_pnl_records = compute_realized_pnl_records(execution_fills)
    repo.replace_realized_lot_matches(realized_lot_matches)
    repo.replace_realized_pnl(realized_pnl_records)

    repo.replace_position_snapshots(
        [
            {
                "symbol": position.symbol,
                "qty": float(position.qty),
                "market_value": float(position.market_value),
                "avg_entry_price": float(position.avg_entry_price),
                "current_price": float(position.current_price),
                "cost_basis": float(position.cost_basis),
                "unrealized_pl": float(position.unrealized_pl),
                "unrealized_plpc": float(position.unrealized_plpc),
            }
            for position in positions
        ]
    )
    repo.add_account_snapshot(
        status=account.status,
        buying_power=float(account.buying_power),
        equity=float(account.equity),
        cash=float(account.cash),
    )
    repo.replace_portfolio_pnl_snapshots(
        [
            {
                "timestamp": snapshot.timestamp,
                "equity": snapshot.equity,
                "profit_loss": snapshot.profit_loss,
                "profit_loss_pct": snapshot.profit_loss_pct,
            }
            for snapshot in portfolio_history
        ]
    )

    return {
        "open_orders": len(open_orders),
        "recent_orders": len(recent_orders),
        "positions": len(positions),
        "pnl_points": len(portfolio_history),
        "fills": len(execution_fills),
        "lot_matches": len(realized_lot_matches),
        "realized_symbols": len(realized_pnl_records),
    }


def _has_stuck_orders(open_orders: list[object], max_stuck_order_minutes: int) -> bool:
    if max_stuck_order_minutes <= 0:
        return False
    now = datetime.now(UTC)
    max_age = timedelta(minutes=max_stuck_order_minutes)
    for order in open_orders:
        submitted_at = getattr(order, "submitted_at", None)
        if submitted_at is None:
            continue
        if submitted_at.tzinfo is None:
            submitted_at = submitted_at.replace(tzinfo=UTC)
        if now - submitted_at >= max_age:
            return True
    return False


def _fallback_split_exit_order(order: OrderIntent, max_qty: int) -> list[OrderIntent]:
    if order.qty <= max_qty or max_qty <= 0:
        return [order]
    remaining = int(order.qty)
    chunks: list[OrderIntent] = []
    while remaining > 0:
        chunk_qty = min(remaining, max_qty)
        chunks.append(OrderIntent(symbol=order.symbol, qty=chunk_qty, side=order.side, close=order.close))
        remaining -= chunk_qty
    return chunks


def _load_and_validate_data(settings) -> tuple[object, object, bool]:
    loaded = load_bars_with_source(settings)
    if settings.trading_mode_enabled and not settings.allow_unsafe_data_fallback and not loaded.production_safe:
        raise RuntimeError(f"unsafe market data source={loaded.source} blocked in trading mode")
    validation = validate_bars(loaded.bars, settings)
    if (
        settings.trading_mode_enabled
        and validation.failed_symbols
        and not settings.allow_partial_market_data
    ):
        raise RuntimeError(f"required market data validation failed: {validation.failed_symbols}")
    stale = data_is_stale(validation.valid_bars, source=loaded.source)
    return loaded, validation, stale


def _compute_reconciliation_state(
    repo: JournalRepo,
    broker: AlpacaTradingAdapter,
) -> tuple[ReconciliationSnapshot, object]:
    local_position_snapshots = repo.current_position_snapshots()
    local_position_qty_by_symbol = {
        position.symbol: float(position.qty) for position in local_position_snapshots
    }
    if hasattr(broker, "build_reconciliation_snapshot"):
        snapshot = broker.build_reconciliation_snapshot(
            local_position_qty_by_symbol=local_position_qty_by_symbol
        )
    else:
        positions = broker.list_positions()
        open_orders = broker.list_open_orders(limit=50)
        snapshot = ReconciliationSnapshot(
            local_position_qty_by_symbol=local_position_qty_by_symbol,
            broker_position_qty_by_symbol={position.symbol: float(position.qty) for position in positions},
            open_order_symbols={order.symbol for order in open_orders},
        )
    state = assess_reconciliation_health(
        snapshot.local_position_qty_by_symbol,
        snapshot.broker_position_qty_by_symbol,
    )
    return snapshot, state


def _compute_kill_switch_state(
    *,
    repo: JournalRepo,
    settings,
    stale_data: bool,
    partial_data_failure: bool,
    realized_pnl: float,
    unrealized_pnl: float,
    open_orders: list[object],
    reconciliation_state,
):
    recent_broker_failures = len(repo.recent_broker_failures(limit=50))
    stuck_orders = _has_stuck_orders(open_orders, settings.max_stuck_order_minutes)
    base_state = evaluate_kill_switch(
        stale_data,
        partial_data_failure,
        realized_pnl,
        unrealized_pnl,
        recent_broker_failures,
        len(open_orders),
        stuck_orders,
        settings.max_daily_loss,
        settings.max_unrealized_drawdown,
        settings.emergency_unrealized_drawdown,
        settings.max_broker_failures,
        settings.max_open_orders,
    )
    kill_switch = merge_kill_switch_states(base_state, reconciliation_state)
    if settings.emergency_flatten:
        kill_switch = merge_kill_switch_states(kill_switch, type(kill_switch)("flatten", "config_emergency_flatten"))
    return kill_switch


def _process_flatten_with_close_position(
    *,
    broker: AlpacaTradingAdapter,
    repo: JournalRepo,
    position_qty_by_symbol: dict[str, float],
    open_order_symbols: set[str],
    unresolved_order_symbols: set[str],
    alert_messages: list[str],
) -> int:
    closed_positions = 0
    for symbol, qty in sorted(position_qty_by_symbol.items()):
        if qty <= 0 or symbol in open_order_symbols or symbol in unresolved_order_symbols:
            continue
        try:
            broker_order = broker.close_position(symbol)
        except BrokerClosePositionError as exc:
            repo.log_broker_error_event(symbol=symbol, operation="close_position", message=str(exc))
            alert_messages.append(f"emergency flatten failed {symbol}: {exc}")
            continue
        repo.sync_broker_order(
            symbol=symbol,
            side=str(broker_order.side),
            qty=float(broker_order.qty),
            status=broker_order.status,
            client_order_id=broker_order.client_order_id,
            broker_order_id=broker_order.id,
            filled_avg_price=float(broker_order.filled_avg_price or 0.0),
        )
        open_order_symbols.add(symbol)
        unresolved_order_symbols.add(symbol)
        closed_positions += 1
    return closed_positions


def run_backtest_command() -> None:
    settings = get_settings()
    repo = JournalRepo(create_session_factory(settings.database_url))
    repo.create_run("backtest", "started")
    loaded = load_bars_with_source(settings)
    trades, metrics = run_backtest(loaded.bars, settings)
    for _, trade in trades.iterrows():
        repo.log_signal(str(trade["symbol"]), "long", float(trade["close"]))
    repo.create_run("backtest", "completed", details=f"source={loaded.source} metrics={metrics}")
    LOGGER.info("backtest metrics %s", metrics)


def run_paper_command() -> None:
    settings = get_settings()
    if not settings.alpaca_paper and settings.paper_only and not settings.allow_live:
        raise RuntimeError("live trading blocked: set ALLOW_LIVE=true to disable PAPER_ONLY safeguard")
    repo = JournalRepo(create_session_factory(settings.database_url))
    broker = AlpacaTradingAdapter(settings)
    repo.create_run("paper", "started")

    if not should_run_trading_loop(broker):
        repo.create_run("paper", "skipped", details="market_closed")
        LOGGER.info("market closed, skipping")
        return

    loaded, validation, stale_data = _load_and_validate_data(settings)
    sync_summary = reconcile_broker_state(repo, broker)
    market_day_start, market_day_end = market_day_window()
    realized_pnl = repo.realized_pnl_total_for_window(market_day_start, market_day_end)
    account = broker.get_account_summary()
    open_orders = broker.list_open_orders(limit=50)
    open_positions = broker.list_positions()
    if hasattr(broker, "get_open_exposure"):
        exposure_snapshot: BrokerExposureSnapshot = broker.get_open_exposure()
    else:
        exposure_snapshot = BrokerExposureSnapshot(
            gross_exposure=total_gross_exposure([float(position.market_value) for position in open_positions]),
            unrealized_pnl=total_unrealized_pnl([float(position.unrealized_pl) for position in open_positions]),
            position_notional_by_symbol={
                position.symbol: abs(float(position.market_value)) for position in open_positions
            },
        )
    unrealized_pnl = exposure_snapshot.unrealized_pnl
    reconciliation_snapshot, reconciliation_state = _compute_reconciliation_state(repo, broker)
    if reconciliation_state.enabled:
        local_symbols = sorted(reconciliation_snapshot.local_position_qty_by_symbol)
        broker_symbols = sorted(reconciliation_snapshot.broker_position_qty_by_symbol)
        repo.log_reconciliation_event(
            severity=reconciliation_state.severity,
            reason=reconciliation_state.reason,
            details=(
                f"local_symbols={local_symbols} "
                f"broker_symbols={broker_symbols}"
            ),
        )
    kill_switch = _compute_kill_switch_state(
        repo=repo,
        settings=settings,
        stale_data=stale_data,
        partial_data_failure=validation.has_partial_failure,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        open_orders=open_orders,
        reconciliation_state=reconciliation_state,
    )
    if kill_switch.enabled:
        LOGGER.warning(
            "paper trading risk state active severity=%s reason=%s",
            kill_switch.severity,
            kill_switch.reason,
        )
        repo.log_kill_switch_event(
            severity=kill_switch.severity,
            reason=kill_switch.reason,
            details=f"source={loaded.source} partial_data={validation.has_partial_failure}",
        )

    executor = PaperExecutor(repo, settings, broker=broker)
    open_order_symbols = {order.symbol for order in open_orders}
    unresolved_order_symbols = {order.symbol for order in repo.unresolved_orders()}
    position_qty_by_symbol = {position.symbol: float(position.qty) for position in open_positions}
    position_price_by_symbol = {
        position.symbol: float(position.current_price) for position in open_positions
    }
    position_notional_by_symbol = {
        position.symbol: abs(float(position.market_value)) for position in open_positions
    }
    active_symbols = set(position_qty_by_symbol) | open_order_symbols
    gross_exposure = exposure_snapshot.gross_exposure
    buying_power = broker.get_buying_power() if hasattr(broker, "get_buying_power") else float(account.buying_power)
    cash = broker.get_cash() if hasattr(broker, "get_cash") else float(account.cash)
    account_equity = broker.get_equity() if hasattr(broker, "get_equity") else float(account.equity)
    reserved_buy_notional_by_symbol: dict[str, float] = defaultdict(float)
    for order in open_orders:
        if str(order.side).lower() != "buy":
            continue
        remaining_qty = max(float(order.qty or 0.0) - float(order.filled_qty or 0.0), 0.0)
        order_price = position_price_by_symbol.get(order.symbol, 0.0) or 0.0
        reserved_buy_notional_by_symbol[order.symbol] += remaining_qty * order_price
    reserved_buy_notional = float(sum(reserved_buy_notional_by_symbol.values()))
    forced_exit_symbols = set(settings.force_exit_symbol_list)
    if kill_switch.force_flatten:
        forced_exit_symbols.update(position_qty_by_symbol)
        LOGGER.warning(
            "emergency flatten enabled due to %s for held symbols: %s",
            kill_switch.reason,
            sorted(forced_exit_symbols),
        )
    if forced_exit_symbols:
        LOGGER.info("forced exit override enabled for held symbols: %s", sorted(forced_exit_symbols))

    latest = None
    trades = pd.DataFrame()
    metrics: dict[str, float] = {"trades": 0.0}
    if not stale_data:
        signal_frame = generate_signals(validation.valid_bars, settings)
        latest = latest_signals(signal_frame)
        latest["account_equity"] = account_equity
        trades = filter_trade_candidates(latest, settings)
        _, metrics = run_backtest(validation.valid_bars, settings)

    for order in open_orders:
        repo.sync_broker_order(
            symbol=order.symbol,
            side=order.side,
            qty=float(order.qty),
            status=order.status,
            intent_id="",
            client_order_id=order.client_order_id,
            broker_order_id=order.id,
            filled_avg_price=float(order.filled_avg_price or 0.0),
        )
    submitted_orders = 0
    blocked_orders = 0
    skipped_existing = 0
    alert_messages: list[str] = []
    if validation.failed_symbols:
        alert_messages.append(f"data validation failed: {validation.failed_symbols}")
    if kill_switch.enabled:
        alert_messages.append(f"kill switch active: {kill_switch.reason}")
    if stale_data or (settings.trading_mode_enabled and loaded.source != "alpaca"):
        exit_candidates = protective_exit_candidates(position_qty_by_symbol, position_price_by_symbol)
        if not exit_candidates.empty:
            LOGGER.warning(
                "using protective broker-state exits because market data is stale for symbols: %s",
                sorted(exit_candidates["symbol"].tolist()),
            )
    else:
        exit_candidates = filter_exit_candidates(latest, position_qty_by_symbol, forced_exit_symbols)
    exit_orders = 0
    skipped_exit = 0

    if kill_switch.force_flatten and settings.trading_mode_enabled:
        exit_orders += _process_flatten_with_close_position(
            broker=broker,
            repo=repo,
            position_qty_by_symbol=position_qty_by_symbol,
            open_order_symbols=open_order_symbols,
            unresolved_order_symbols=unresolved_order_symbols,
            alert_messages=alert_messages,
        )

    for _, trade in exit_candidates.iterrows():
        repo.log_signal(str(trade["symbol"]), "exit", float(trade["close"]))
        base_order = OrderIntent(
            symbol=str(trade["symbol"]),
            qty=int(trade["qty"]),
            side="sell",
            close=float(trade["close"]),
        )
        if base_order.symbol in open_order_symbols or base_order.symbol in unresolved_order_symbols:
            skipped_exit += 1
            LOGGER.info("skipping exit for %s because broker already has unresolved state", base_order.symbol)
            repo.log_order(
                base_order.symbol,
                base_order.side,
                float(base_order.qty),
                "skipped_open_order",
                requested_price=base_order.close,
            )
            continue
        split_exit_orders = (
            executor.split_order_for_submit(base_order)
            if hasattr(executor, "split_order_for_submit")
            else _fallback_split_exit_order(base_order, settings.max_order_qty)
        )
        execution_results = (
            executor.submit_orders(base_order)
            if hasattr(executor, "submit_orders")
            else [executor.submit(order) for order in split_exit_orders]
        )
        for result, order in zip(execution_results, split_exit_orders, strict=False):
            if result.accepted:
                exit_orders += 1
                open_order_symbols.add(order.symbol)
                unresolved_order_symbols.add(order.symbol)
                repo.sync_broker_order(
                    symbol=order.symbol,
                    side=order.side,
                    qty=float(order.qty),
                    status=result.status,
                    status_detail=result.status_detail,
                    intent_id=result.intent_id,
                    client_order_id=result.client_order_id,
                    broker_order_id=result.broker_order_id,
                    filled_avg_price=result.filled_avg_price,
                )
            else:
                blocked_orders += 1
                LOGGER.warning(
                    "paper exit not submitted for %s: %s",
                    order.symbol,
                    result.status_detail or result.status,
                )
                alert_messages.append(f"blocked exit {order.symbol}: {result.status_detail or result.status}")
                if result.status == "error":
                    repo.log_broker_error_event(
                        symbol=order.symbol,
                        operation="submit_market_order",
                        message=result.status_detail or result.status,
                    )

    for _, trade in trades.iterrows():
        repo.log_signal(str(trade["symbol"]), "long", float(trade["close"]))
        order = OrderIntent(
            symbol=str(trade["symbol"]),
            qty=int(trade["qty"]),
            side="buy",
            close=float(trade["close"]),
        )
        if order.symbol in open_order_symbols or order.symbol in unresolved_order_symbols or position_qty_by_symbol.get(order.symbol, 0.0) > 0:
            skipped_existing += 1
            LOGGER.info("skipping %s because broker already has open exposure", order.symbol)
            repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                "skipped_existing",
                requested_price=order.close,
            )
            continue
        if settings.deny_new_entries:
            blocked_orders += 1
            repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                "blocked",
                status_detail="deny_new_entries",
                requested_price=order.close,
            )
            alert_messages.append(f"entry blocked by config for {order.symbol}")
            continue
        if kill_switch.block_new_entries or (settings.trading_mode_enabled and loaded.source != "alpaca"):
            blocked_orders += 1
            LOGGER.warning("blocking new entry for %s because kill switch is active", order.symbol)
            repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                "blocked",
                status_detail=kill_switch.reason or f"unsafe_data_source:{loaded.source}",
                requested_price=order.close,
            )
            continue
        decision = entry_risk_decision(
            symbol=order.symbol,
            qty=int(order.qty),
            close=float(order.close),
            active_symbols=active_symbols,
            symbol_exposure=position_notional_by_symbol.get(order.symbol, 0.0)
            + reserved_buy_notional_by_symbol.get(order.symbol, 0.0),
            gross_exposure=gross_exposure,
            reserved_gross_exposure=reserved_buy_notional,
            buying_power=buying_power,
            cash=cash,
            reserved_buying_power=reserved_buy_notional,
            reserved_cash=reserved_buy_notional,
            settings=settings,
        )
        if not decision.allowed:
            blocked_orders += 1
            LOGGER.warning("blocking new entry for %s because %s", order.symbol, decision.reason)
            repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                "blocked",
                status_detail=decision.reason,
                requested_price=order.close,
            )
            alert_messages.append(f"entry blocked {order.symbol}: {decision.reason}")
            continue
        result = executor.submit(order)
        if result.accepted:
            submitted_orders += 1
            open_order_symbols.add(order.symbol)
            unresolved_order_symbols.add(order.symbol)
            active_symbols.add(order.symbol)
            order_notional = float(order.qty) * float(order.close)
            gross_exposure += order_notional
            reserved_buy_notional += order_notional
            buying_power = max(buying_power - order_notional, 0.0)
            cash -= order_notional
            reserved_buy_notional_by_symbol[order.symbol] = (
                reserved_buy_notional_by_symbol.get(order.symbol, 0.0) + order_notional
            )
            position_notional_by_symbol[order.symbol] = (
                position_notional_by_symbol.get(order.symbol, 0.0) + order_notional
            )
            repo.sync_broker_order(
                symbol=order.symbol,
                side=order.side,
                qty=float(order.qty),
                status=result.status,
                status_detail=result.status_detail,
                intent_id=result.intent_id,
                client_order_id=result.client_order_id,
                broker_order_id=result.broker_order_id,
                filled_avg_price=result.filled_avg_price,
            )
        else:
            blocked_orders += 1
            LOGGER.warning(
                "paper order not submitted for %s: %s",
                order.symbol,
                result.status_detail or result.status,
            )
            alert_messages.append(f"blocked order {order.symbol}: {result.status_detail or result.status}")
            if result.status == "error":
                repo.log_broker_error_event(
                    symbol=order.symbol,
                    operation="submit_market_order",
                    message=result.status_detail or result.status,
                )

    if len(repo.recent_broker_failures(limit=50)) >= settings.max_broker_failures:
        alert_messages.append("broker/API failure threshold breached")

    sync_summary = reconcile_broker_state(repo, broker)
    report = build_daily_report(repo)
    send_alerts(dict.fromkeys(alert_messages), settings)
    repo.create_run(
        "paper",
        "completed",
        details=(
            f"source={loaded.source} fee_model={FEE_MODEL_LABEL} dry_run={settings.dry_run} "
            f"paper_only={settings.paper_only} allow_live={settings.allow_live} "
            f"kill_switch_severity={kill_switch.severity} "
            f"kill_switch={kill_switch.reason or 'none'} "
            f"reconciliation_state={reconciliation_state.reason or 'none'} "
            f"partial_data={validation.has_partial_failure} "
            f"realized_pnl={realized_pnl:.2f} unrealized_pnl={unrealized_pnl:.2f} "
            f"reserved_buy_notional={reserved_buy_notional:.2f} "
            f"submitted={submitted_orders} exits={exit_orders} blocked={blocked_orders} "
            f"skipped_existing={skipped_existing} skipped_exit={skipped_exit} "
            f"synced_orders={sync_summary['recent_orders']} "
            f"open_orders={sync_summary['open_orders']} positions={sync_summary['positions']} "
            f"pnl_points={sync_summary['pnl_points']} fills={sync_summary['fills']} "
            f"lot_matches={sync_summary['lot_matches']} "
            f"realized_symbols={sync_summary['realized_symbols']} "
            f"metrics={metrics} {report}"
        ),
    )
    LOGGER.info("paper trading complete %s", metrics)


def run_reconcile_command() -> None:
    settings = get_settings()
    repo = JournalRepo(create_session_factory(settings.database_url))
    broker = AlpacaTradingAdapter(settings)
    repo.create_run("reconcile", "started")
    summary = reconcile_broker_state(repo, broker)
    repo.create_run(
        "reconcile",
        "completed",
        details=(
            f"fee_model={FEE_MODEL_LABEL} "
            f"synced_orders={summary['recent_orders']} "
            f"open_orders={summary['open_orders']} positions={summary['positions']} "
            f"pnl_points={summary['pnl_points']} fills={summary['fills']} "
            f"lot_matches={summary['lot_matches']} "
            f"realized_symbols={summary['realized_symbols']}"
        ),
    )
    LOGGER.info("reconcile complete %s", summary)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trading bot entrypoint")
    parser.add_argument("command", choices=["backtest", "paper", "reconcile", "report"])
    return parser


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    args = build_parser().parse_args()

    if args.command == "backtest":
        run_backtest_command()
    elif args.command == "paper":
        run_paper_command()
    elif args.command == "reconcile":
        run_reconcile_command()
    else:
        repo = JournalRepo(create_session_factory(settings.database_url))
        print(build_daily_report(repo))


if __name__ == "__main__":
    main()
