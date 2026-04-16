from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import Date, DateTime, Float, Integer, String, create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


class StrategyRun(Base):
    __tablename__ = "strategy_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_type: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    details: Mapped[str] = mapped_column(String(1024), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SignalRecord(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    signal: Mapped[str] = mapped_column(String(16), index=True)
    price: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class OrderRecord(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    side: Mapped[str] = mapped_column(String(8))
    qty: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32), index=True)
    status_detail: Mapped[str] = mapped_column(String(256), default="")
    client_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    broker_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    requested_price: Mapped[float] = mapped_column(Float, default=0.0)
    filled_avg_price: Mapped[float] = mapped_column(Float, default=0.0)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class PositionSnapshotRecord(Base):
    __tablename__ = "position_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    qty: Mapped[float] = mapped_column(Float)
    market_value: Mapped[float] = mapped_column(Float, default=0.0)
    avg_entry_price: Mapped[float] = mapped_column(Float, default=0.0)
    current_price: Mapped[float] = mapped_column(Float, default=0.0)
    cost_basis: Mapped[float] = mapped_column(Float, default=0.0)
    unrealized_pl: Mapped[float] = mapped_column(Float, default=0.0)
    unrealized_plpc: Mapped[float] = mapped_column(Float, default=0.0)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)


class AccountSnapshotRecord(Base):
    __tablename__ = "account_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    buying_power: Mapped[float] = mapped_column(Float, default=0.0)
    equity: Mapped[float] = mapped_column(Float, default=0.0)
    cash: Mapped[float] = mapped_column(Float, default=0.0)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)


class PortfolioPnlSnapshotRecord(Base):
    __tablename__ = "portfolio_pnl_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    equity: Mapped[float] = mapped_column(Float, default=0.0)
    profit_loss: Mapped[float] = mapped_column(Float, default=0.0)
    profit_loss_pct: Mapped[float] = mapped_column(Float, default=0.0)


class ExecutionFillRecord(Base):
    __tablename__ = "execution_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_order_id: Mapped[str] = mapped_column(String(64), index=True)
    client_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    side: Mapped[str] = mapped_column(String(8), index=True)
    qty: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float, default=0.0)
    gross_amount: Mapped[float] = mapped_column(Float, default=0.0)
    fees: Mapped[float] = mapped_column(Float, default=0.0)
    net_amount: Mapped[float] = mapped_column(Float, default=0.0)
    execution_date: Mapped[date] = mapped_column(Date, index=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime, index=True)


class RealizedLotMatchRecord(Base):
    __tablename__ = "realized_lot_matches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    open_broker_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    close_broker_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    open_client_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    close_client_order_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    matched_qty: Mapped[float] = mapped_column(Float, default=0.0)
    open_price: Mapped[float] = mapped_column(Float, default=0.0)
    close_price: Mapped[float] = mapped_column(Float, default=0.0)
    fees: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    execution_date: Mapped[date] = mapped_column(Date, index=True)
    open_filled_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    close_filled_at: Mapped[datetime] = mapped_column(DateTime, index=True)


class RealizedPnlRecord(Base):
    __tablename__ = "realized_pnl"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    realized_qty: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    last_fill_at: Mapped[datetime] = mapped_column(DateTime, index=True)


def _upgrade_orders_table(engine) -> None:
    inspector = inspect(engine)
    if "orders" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("orders")}
    expected_columns = {
        "client_order_id": "ALTER TABLE orders ADD COLUMN client_order_id VARCHAR(64) DEFAULT ''",
        "broker_order_id": "ALTER TABLE orders ADD COLUMN broker_order_id VARCHAR(64) DEFAULT ''",
        "requested_price": "ALTER TABLE orders ADD COLUMN requested_price FLOAT DEFAULT 0",
        "filled_avg_price": "ALTER TABLE orders ADD COLUMN filled_avg_price FLOAT DEFAULT 0",
        "status_detail": "ALTER TABLE orders ADD COLUMN status_detail VARCHAR(256) DEFAULT ''",
    }
    with engine.begin() as connection:
        for column_name, ddl in expected_columns.items():
            if column_name not in existing_columns:
                connection.execute(text(ddl))


def _upgrade_position_snapshots_table(engine) -> None:
    inspector = inspect(engine)
    if "position_snapshots" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("position_snapshots")}
    expected_columns = {
        "current_price": "ALTER TABLE position_snapshots ADD COLUMN current_price FLOAT DEFAULT 0",
        "cost_basis": "ALTER TABLE position_snapshots ADD COLUMN cost_basis FLOAT DEFAULT 0",
        "unrealized_pl": "ALTER TABLE position_snapshots ADD COLUMN unrealized_pl FLOAT DEFAULT 0",
        "unrealized_plpc": "ALTER TABLE position_snapshots ADD COLUMN unrealized_plpc FLOAT DEFAULT 0",
    }
    with engine.begin() as connection:
        for column_name, ddl in expected_columns.items():
            if column_name not in existing_columns:
                connection.execute(text(ddl))


def _upgrade_execution_fills_table(engine) -> None:
    inspector = inspect(engine)
    if "execution_fills" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("execution_fills")}
    expected_columns = {
        "gross_amount": "ALTER TABLE execution_fills ADD COLUMN gross_amount FLOAT DEFAULT 0",
        "fees": "ALTER TABLE execution_fills ADD COLUMN fees FLOAT DEFAULT 0",
        "net_amount": "ALTER TABLE execution_fills ADD COLUMN net_amount FLOAT DEFAULT 0",
        "execution_date": "ALTER TABLE execution_fills ADD COLUMN execution_date DATE",
    }
    with engine.begin() as connection:
        for column_name, ddl in expected_columns.items():
            if column_name not in existing_columns:
                connection.execute(text(ddl))
        if "execution_date" not in existing_columns:
            connection.execute(text("UPDATE execution_fills SET execution_date = date(filled_at)"))


def create_session_factory(database_url: str) -> sessionmaker:
    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)
    _upgrade_orders_table(engine)
    _upgrade_position_snapshots_table(engine)
    _upgrade_execution_fills_table(engine)
    return sessionmaker(bind=engine, future=True)
