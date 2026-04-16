from __future__ import annotations

from app.db.repo import JournalRepo


def build_daily_report(repo: JournalRepo) -> str:
    runs = repo.recent_runs(limit=5)
    orders = repo.recent_orders(limit=5)
    positions = repo.current_position_snapshots()
    account = repo.latest_account_snapshot()
    pnl_snapshots = repo.portfolio_pnl_snapshots()
    latest_pnl = pnl_snapshots[-1] if pnl_snapshots else None
    unresolved_orders = repo.unresolved_orders()
    fills = repo.execution_fills()
    lot_matches = repo.realized_lot_matches()
    realized = repo.realized_pnl()
    gross_exposure = float(sum(abs(position.market_value) for position in positions))
    unrealized_pnl = float(sum(position.unrealized_pl for position in positions))
    return (
        f"recent_runs={len(runs)}\n"
        f"recent_orders={len(orders)}\n"
        f"positions={len(positions)}\n"
        f"unresolved_orders={len(unresolved_orders)}\n"
        f"latest_equity={account.equity if account else 0}\n"
        f"gross_exposure={gross_exposure}\n"
        f"unrealized_pnl={unrealized_pnl}\n"
        f"buying_power={account.buying_power if account else 0}\n"
        f"cash={account.cash if account else 0}\n"
        f"portfolio_profit_loss={latest_pnl.profit_loss if latest_pnl else 0}\n"
        "fee_model=estimated_activity_allocation\n"
        f"execution_fills={len(fills)}\n"
        f"lot_matches={len(lot_matches)}\n"
        f"realized_symbols={len(realized)}\n"
    )
