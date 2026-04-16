from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import func, or_
from sqlalchemy.orm import sessionmaker

from app.db.models import (
    AccountSnapshotRecord,
    ExecutionFillRecord,
    OrderRecord,
    PortfolioPnlSnapshotRecord,
    PositionSnapshotRecord,
    ReconciliationEventRecord,
    RealizedLotMatchRecord,
    RealizedPnlRecord,
    SignalRecord,
    StrategyRun,
)


class JournalRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    def create_run(self, run_type: str, status: str, details: str = "") -> StrategyRun:
        with self._session_factory() as session:
            record = StrategyRun(run_type=run_type, status=status, details=details)
            session.add(record)
            session.commit()
            session.refresh(record)
            return record

    def log_signal(self, symbol: str, signal: str, price: float) -> None:
        with self._session_factory() as session:
            session.add(SignalRecord(symbol=symbol, signal=signal, price=price))
            session.commit()

    def log_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        status: str,
        status_detail: str = "",
        client_order_id: str = "",
        broker_order_id: str = "",
        requested_price: float = 0.0,
        filled_avg_price: float = 0.0,
    ) -> None:
        with self._session_factory() as session:
            session.add(
                OrderRecord(
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    status=status,
                    status_detail=status_detail,
                    client_order_id=client_order_id,
                    broker_order_id=broker_order_id,
                    requested_price=requested_price,
                    filled_avg_price=filled_avg_price,
                )
            )
            session.commit()

    def recent_runs(self, limit: int = 20) -> list[StrategyRun]:
        with self._session_factory() as session:
            return list(
                session.query(StrategyRun).order_by(StrategyRun.created_at.desc()).limit(limit).all()
            )

    def recent_signals(self, limit: int = 50) -> list[SignalRecord]:
        with self._session_factory() as session:
            return list(
                session.query(SignalRecord)
                .order_by(SignalRecord.created_at.desc())
                .limit(limit)
                .all()
            )

    def recent_orders(self, limit: int = 50) -> list[OrderRecord]:
        with self._session_factory() as session:
            return list(
                session.query(OrderRecord).order_by(OrderRecord.submitted_at.desc()).limit(limit).all()
            )

    def log_reconciliation_event(self, *, severity: str, reason: str, details: str = "") -> None:
        with self._session_factory() as session:
            session.add(
                ReconciliationEventRecord(
                    severity=severity,
                    reason=reason,
                    details=details,
                )
            )
            session.commit()

    def recent_reconciliation_events(self, limit: int = 50) -> list[ReconciliationEventRecord]:
        with self._session_factory() as session:
            return list(
                session.query(ReconciliationEventRecord)
                .order_by(ReconciliationEventRecord.created_at.desc())
                .limit(limit)
                .all()
            )

    def sync_broker_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        status: str,
        client_order_id: str,
        broker_order_id: str,
        filled_avg_price: float = 0.0,
        status_detail: str = "",
    ) -> None:
        with self._session_factory() as session:
            record = (
                session.query(OrderRecord)
                .filter(
                    or_(
                        OrderRecord.client_order_id == client_order_id,
                        OrderRecord.broker_order_id == broker_order_id,
                    )
                )
                .order_by(OrderRecord.submitted_at.desc())
                .first()
            )
            if record is None:
                record = OrderRecord(
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    status=status,
                    status_detail=status_detail,
                    client_order_id=client_order_id,
                    broker_order_id=broker_order_id,
                    filled_avg_price=filled_avg_price,
                )
                session.add(record)
            else:
                record.status = status
                record.status_detail = status_detail
                record.filled_avg_price = filled_avg_price
                if broker_order_id:
                    record.broker_order_id = broker_order_id
                if client_order_id:
                    record.client_order_id = client_order_id
            session.commit()

    def replace_position_snapshots(self, positions: list[dict[str, float | str]]) -> None:
        with self._session_factory() as session:
            session.query(PositionSnapshotRecord).delete()
            session.add_all(
                [
                    PositionSnapshotRecord(
                        symbol=str(position["symbol"]),
                        qty=float(position["qty"]),
                        market_value=float(position["market_value"]),
                        avg_entry_price=float(position["avg_entry_price"]),
                        current_price=float(position["current_price"]),
                        cost_basis=float(position["cost_basis"]),
                        unrealized_pl=float(position["unrealized_pl"]),
                        unrealized_plpc=float(position["unrealized_plpc"]),
                    )
                    for position in positions
                ]
            )
            session.commit()

    def add_account_snapshot(self, *, status: str, buying_power: float, equity: float, cash: float) -> None:
        with self._session_factory() as session:
            session.add(
                AccountSnapshotRecord(
                    status=status,
                    buying_power=buying_power,
                    equity=equity,
                    cash=cash,
                )
            )
            session.commit()

    def current_position_snapshots(self) -> list[PositionSnapshotRecord]:
        with self._session_factory() as session:
            return list(
                session.query(PositionSnapshotRecord)
                .order_by(PositionSnapshotRecord.symbol.asc())
                .all()
            )

    def latest_account_snapshot(self) -> AccountSnapshotRecord | None:
        with self._session_factory() as session:
            return (
                session.query(AccountSnapshotRecord)
                .order_by(AccountSnapshotRecord.captured_at.desc())
                .first()
            )

    def replace_portfolio_pnl_snapshots(self, snapshots: list[dict[str, float | str]]) -> None:
        with self._session_factory() as session:
            session.query(PortfolioPnlSnapshotRecord).delete()
            session.add_all(
                [
                    PortfolioPnlSnapshotRecord(
                        timestamp=snapshot["timestamp"],
                        equity=float(snapshot["equity"]),
                        profit_loss=float(snapshot["profit_loss"]),
                        profit_loss_pct=float(snapshot["profit_loss_pct"]),
                    )
                    for snapshot in snapshots
                ]
            )
            session.commit()

    def portfolio_pnl_snapshots(self, limit: int = 90) -> list[PortfolioPnlSnapshotRecord]:
        with self._session_factory() as session:
            return list(
                session.query(PortfolioPnlSnapshotRecord)
                .order_by(PortfolioPnlSnapshotRecord.timestamp.asc())
                .limit(limit)
                .all()
            )

    def replace_execution_fills(self, fills: list[dict[str, object]]) -> None:
        with self._session_factory() as session:
            session.query(ExecutionFillRecord).delete()
            session.add_all(
                [
                    ExecutionFillRecord(
                        broker_order_id=str(fill["broker_order_id"]),
                        client_order_id=str(fill["client_order_id"]),
                        symbol=str(fill["symbol"]),
                        side=str(fill["side"]),
                        qty=float(fill["qty"]),
                        price=float(fill["price"]),
                        gross_amount=float(fill["gross_amount"]),
                        fees=float(fill["fees"]),
                        net_amount=float(fill["net_amount"]),
                        execution_date=fill["execution_date"],
                        filled_at=fill["filled_at"],
                    )
                    for fill in fills
                ]
            )
            session.commit()

    def execution_fills(self) -> list[ExecutionFillRecord]:
        with self._session_factory() as session:
            return list(
                session.query(ExecutionFillRecord)
                .order_by(ExecutionFillRecord.filled_at.asc(), ExecutionFillRecord.id.asc())
                .all()
            )

    def replace_realized_lot_matches(self, records: list[dict[str, object]]) -> None:
        with self._session_factory() as session:
            session.query(RealizedLotMatchRecord).delete()
            session.add_all(
                [
                    RealizedLotMatchRecord(
                        symbol=str(record["symbol"]),
                        open_broker_order_id=str(record["open_broker_order_id"]),
                        close_broker_order_id=str(record["close_broker_order_id"]),
                        open_client_order_id=str(record["open_client_order_id"]),
                        close_client_order_id=str(record["close_client_order_id"]),
                        matched_qty=float(record["matched_qty"]),
                        open_price=float(record["open_price"]),
                        close_price=float(record["close_price"]),
                        fees=float(record["fees"]),
                        realized_pnl=float(record["realized_pnl"]),
                        execution_date=record["execution_date"],
                        open_filled_at=record["open_filled_at"],
                        close_filled_at=record["close_filled_at"],
                    )
                    for record in records
                ]
            )
            session.commit()

    def realized_lot_matches(self, limit: int = 200) -> list[RealizedLotMatchRecord]:
        with self._session_factory() as session:
            return list(
                session.query(RealizedLotMatchRecord)
                .order_by(RealizedLotMatchRecord.close_filled_at.desc(), RealizedLotMatchRecord.id.desc())
                .limit(limit)
                .all()
            )

    def replace_realized_pnl(self, records: list[dict[str, object]]) -> None:
        with self._session_factory() as session:
            session.query(RealizedPnlRecord).delete()
            session.add_all(
                [
                    RealizedPnlRecord(
                        symbol=str(record["symbol"]),
                        realized_qty=float(record["realized_qty"]),
                        realized_pnl=float(record["realized_pnl"]),
                        last_fill_at=record["last_fill_at"],
                    )
                    for record in records
                ]
            )
            session.commit()

    def realized_pnl(self) -> list[RealizedPnlRecord]:
        with self._session_factory() as session:
            return list(
                session.query(RealizedPnlRecord)
                .order_by(RealizedPnlRecord.symbol.asc())
                .all()
            )

    def realized_pnl_total_for_date(self, as_of: date) -> float:
        with self._session_factory() as session:
            total = (
                session.query(func.sum(RealizedPnlRecord.realized_pnl))
                .filter(func.date(RealizedPnlRecord.last_fill_at) == as_of.isoformat())
                .scalar()
            )
            return float(total or 0.0)

    def realized_pnl_total_for_window(
        self,
        start_at: datetime,
        end_at: datetime,
    ) -> float:
        with self._session_factory() as session:
            records = list(
                session.query(RealizedLotMatchRecord)
                .order_by(RealizedLotMatchRecord.close_filled_at.asc(), RealizedLotMatchRecord.id.asc())
                .all()
            )
        total = 0.0
        for record in records:
            fill_at = record.close_filled_at
            if fill_at.tzinfo is None:
                fill_at = fill_at.replace(tzinfo=start_at.tzinfo)
            if start_at <= fill_at < end_at:
                total += float(record.realized_pnl)
        return total
