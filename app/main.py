from __future__ import annotations

import argparse
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime

from app.backtest.engine import run_backtest
from app.broker.alpaca_client import AlpacaTradingAdapter
from app.broker.execution import PaperExecutor
from app.broker.order_mapper import OrderIntent
from app.config import get_settings
from app.data.historical_loader import load_bars_with_source
from app.data.market_calendar import market_day_window
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.logging_setup import configure_logging
from app.reports.daily_report import build_daily_report
from app.risk.checks import daily_loss_ok, filter_exit_candidates, filter_trade_candidates
from app.risk.kill_switch import data_is_stale, evaluate_kill_switch
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
    repo = JournalRepo(create_session_factory(settings.database_url))
    broker = AlpacaTradingAdapter(settings)
    repo.create_run("paper", "started")

    if not should_run_trading_loop(broker):
        repo.create_run("paper", "skipped", details="market_closed")
        LOGGER.info("market closed, skipping")
        return

    loaded = load_bars_with_source(settings)
    stale_data = data_is_stale(loaded.bars, source=loaded.source)
    sync_summary = reconcile_broker_state(repo, broker)
    market_day_start, market_day_end = market_day_window()
    realized_pnl = repo.realized_pnl_total_for_window(market_day_start, market_day_end)
    kill_switch = evaluate_kill_switch(stale_data, realized_pnl, settings.max_daily_loss)
    if kill_switch.enabled or not daily_loss_ok(realized_pnl, settings):
        repo.create_run(
            "paper",
            "blocked",
            details=(
                f"{kill_switch.reason or 'daily_loss_limit'} "
                f"source={loaded.source} "
                f"fee_model={FEE_MODEL_LABEL} "
                f"realized_pnl={realized_pnl:.2f} "
                f"synced_orders={sync_summary['recent_orders']} "
                f"open_orders={sync_summary['open_orders']} positions={sync_summary['positions']} "
                f"pnl_points={sync_summary['pnl_points']} fills={sync_summary['fills']} "
                f"lot_matches={sync_summary['lot_matches']} "
                f"realized_symbols={sync_summary['realized_symbols']}"
            ),
        )
        LOGGER.warning("paper trading blocked: %s", kill_switch.reason)
        return

    signal_frame = generate_signals(loaded.bars, settings)
    latest = latest_signals(signal_frame)
    trades = filter_trade_candidates(latest, settings)
    _, metrics = run_backtest(loaded.bars, settings)
    executor = PaperExecutor(repo, settings, broker=broker)
    open_orders = broker.list_open_orders(limit=50)
    open_positions = broker.list_positions()
    open_order_symbols = {order.symbol for order in open_orders}
    position_qty_by_symbol = {position.symbol: float(position.qty) for position in open_positions}
    forced_exit_symbols = set(settings.force_exit_symbol_list)
    if forced_exit_symbols:
        LOGGER.info("forced exit override enabled for held symbols: %s", sorted(forced_exit_symbols))
    for order in open_orders:
        repo.sync_broker_order(
            symbol=order.symbol,
            side=order.side,
            qty=float(order.qty),
            status=order.status,
            client_order_id=order.client_order_id,
            broker_order_id=order.id,
            filled_avg_price=float(order.filled_avg_price or 0.0),
        )
    submitted_orders = 0
    blocked_orders = 0
    skipped_existing = 0
    exit_candidates = filter_exit_candidates(latest, position_qty_by_symbol, forced_exit_symbols)
    exit_orders = 0
    skipped_exit = 0

    for _, trade in exit_candidates.iterrows():
        repo.log_signal(str(trade["symbol"]), "exit", float(trade["close"]))
        order = OrderIntent(
            symbol=str(trade["symbol"]),
            qty=int(trade["qty"]),
            side="sell",
            close=float(trade["close"]),
        )
        if order.symbol in open_order_symbols:
            skipped_exit += 1
            LOGGER.info("skipping exit for %s because broker already has an open order", order.symbol)
            repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                "skipped_open_order",
                requested_price=order.close,
            )
            continue
        try:
            result = executor.submit(order)
            if result.accepted:
                exit_orders += 1
                open_order_symbols.add(order.symbol)
                repo.sync_broker_order(
                    symbol=order.symbol,
                    side=order.side,
                    qty=float(order.qty),
                    status=result.status,
                    status_detail=result.status_detail,
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
        except Exception:
            raise

    for _, trade in trades.iterrows():
        repo.log_signal(str(trade["symbol"]), "long", float(trade["close"]))
        order = OrderIntent(
            symbol=str(trade["symbol"]),
            qty=int(trade["qty"]),
            side="buy",
            close=float(trade["close"]),
        )
        if order.symbol in open_order_symbols or position_qty_by_symbol.get(order.symbol, 0.0) > 0:
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
        try:
            result = executor.submit(order)
            if result.accepted:
                submitted_orders += 1
                open_order_symbols.add(order.symbol)
                repo.sync_broker_order(
                    symbol=order.symbol,
                    side=order.side,
                    qty=float(order.qty),
                    status=result.status,
                    status_detail=result.status_detail,
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
        except Exception:
            raise

    sync_summary = reconcile_broker_state(repo, broker)
    report = build_daily_report(repo)
    repo.create_run(
        "paper",
        "completed",
        details=(
            f"source={loaded.source} fee_model={FEE_MODEL_LABEL} dry_run={settings.dry_run} "
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
