from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, func

from app.config import Settings
from app.db.models import (
    BrokerErrorEventRecord,
    KillSwitchEventRecord,
    OrderRecord,
    ReconciliationEventRecord,
    StrategyRun,
    create_session_factory,
)
from app.db.repo import JournalRepo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize paper-trading burn-in health metrics.")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in calendar days.")
    parser.add_argument("--database-url", default=None, help="Override DATABASE_URL.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(DATABASE_URL=args.database_url) if args.database_url else Settings()
    repo = JournalRepo(create_session_factory(settings.database_url))
    session_factory = create_session_factory(settings.database_url)

    now = datetime.now(UTC)
    start_at = now - timedelta(days=args.days)

    with session_factory() as session:
        runs = list(
            session.query(StrategyRun)
            .filter(and_(StrategyRun.run_type == "paper", StrategyRun.created_at >= start_at))
            .order_by(StrategyRun.created_at.asc())
            .all()
        )
        orders = list(
            session.query(OrderRecord)
            .filter(OrderRecord.submitted_at >= start_at)
            .order_by(OrderRecord.submitted_at.asc())
            .all()
        )
        reconciliation_events = list(
            session.query(ReconciliationEventRecord)
            .filter(ReconciliationEventRecord.created_at >= start_at)
            .order_by(ReconciliationEventRecord.created_at.asc())
            .all()
        )
        kill_switch_events = list(
            session.query(KillSwitchEventRecord)
            .filter(KillSwitchEventRecord.created_at >= start_at)
            .order_by(KillSwitchEventRecord.created_at.asc())
            .all()
        )
        broker_error_events = list(
            session.query(BrokerErrorEventRecord)
            .filter(BrokerErrorEventRecord.created_at >= start_at)
            .order_by(BrokerErrorEventRecord.created_at.asc())
            .all()
        )
        duplicate_client_ids = session.query(func.count()).select_from(
            session.query(OrderRecord.client_order_id)
            .filter(OrderRecord.client_order_id != "", OrderRecord.submitted_at >= start_at)
            .group_by(OrderRecord.client_order_id)
            .having(func.count(OrderRecord.id) > 1)
            .subquery()
        ).scalar()
        duplicate_broker_ids = session.query(func.count()).select_from(
            session.query(OrderRecord.broker_order_id)
            .filter(OrderRecord.broker_order_id != "", OrderRecord.submitted_at >= start_at)
            .group_by(OrderRecord.broker_order_id)
            .having(func.count(OrderRecord.id) > 1)
            .subquery()
        ).scalar()

    unresolved_orders = repo.unresolved_orders()
    latest_account = repo.latest_account_snapshot()
    latest_positions = repo.current_position_snapshots()
    status_counts = Counter(order.status for order in orders)
    kill_switch_counts = Counter(event.reason for event in kill_switch_events)
    reconciliation_counts = Counter(event.reason for event in reconciliation_events)
    broker_error_counts = Counter(event.operation for event in broker_error_events)
    gross_exposure = float(sum(abs(position.market_value) for position in latest_positions))
    unrealized_pnl = float(sum(position.unrealized_pl for position in latest_positions))

    print(f"Burn-In Summary ({args.days}d)")
    print(f"Window start: {start_at.isoformat()}")
    print(f"Paper runs: {len(runs)}")
    print(f"Orders: {len(orders)}")
    print(f"Blocked orders: {status_counts.get('blocked', 0)}")
    print(f"Error orders: {status_counts.get('error', 0)}")
    print(f"Skipped existing/open-order events: {status_counts.get('skipped_existing', 0) + status_counts.get('skipped_open_order', 0)}")
    print(f"Unresolved orders now: {len(unresolved_orders)}")
    print(f"Duplicate client_order_ids: {int(duplicate_client_ids or 0)}")
    print(f"Duplicate broker_order_ids: {int(duplicate_broker_ids or 0)}")
    print(f"Reconciliation events: {len(reconciliation_events)}")
    print(f"Kill-switch events: {len(kill_switch_events)}")
    print(f"Broker error events: {len(broker_error_events)}")
    print(f"Latest equity: {getattr(latest_account, 'equity', 0.0)}")
    print(f"Latest buying power: {getattr(latest_account, 'buying_power', 0.0)}")
    print(f"Latest cash: {getattr(latest_account, 'cash', 0.0)}")
    print(f"Gross exposure now: {gross_exposure:.2f}")
    print(f"Unrealized PnL now: {unrealized_pnl:.2f}")

    if kill_switch_counts:
        print("\nKill-Switch Reasons")
        for reason, count in sorted(kill_switch_counts.items()):
            print(f"- {reason}: {count}")

    if reconciliation_counts:
        print("\nReconciliation Reasons")
        for reason, count in sorted(reconciliation_counts.items()):
            print(f"- {reason}: {count}")

    if broker_error_counts:
        print("\nBroker Error Operations")
        for operation, count in sorted(broker_error_counts.items()):
            print(f"- {operation}: {count}")

    if unresolved_orders:
        print("\nCurrent Unresolved Orders")
        for order in unresolved_orders[:10]:
            print(
                f"- {order.symbol} side={order.side} status={order.status} "
                f"lifecycle={order.lifecycle_state} client_order_id={order.client_order_id}"
            )

    print("\nAssessment")
    issues: list[str] = []
    if duplicate_client_ids or duplicate_broker_ids:
        issues.append("duplicate order IDs detected")
    if reconciliation_events:
        issues.append("reconciliation drift events present")
    if broker_error_events:
        issues.append("broker errors present")
    if status_counts.get("error", 0) > 0:
        issues.append("order submit errors present")
    if issues:
        for issue in issues:
            print(f"- review required: {issue}")
    else:
        print("- no immediate red flags in the selected window")


if __name__ == "__main__":
    main()
