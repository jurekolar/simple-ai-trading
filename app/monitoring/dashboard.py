from __future__ import annotations

import pandas as pd
import streamlit as st

from app.broker.alpaca_client import AlpacaTradingAdapter
from app.config import get_settings
from app.db.models import create_session_factory
from app.db.repo import JournalRepo


def _records_to_frame(records: list[object], columns: list[str]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([{column: getattr(record, column) for column in columns} for record in records])


def _extract_data_source(runs: pd.DataFrame) -> str:
    if runs.empty:
        return "unknown"
    for details in runs["details"]:
        if isinstance(details, str) and "source=" in details:
            return details.split("source=", maxsplit=1)[1].split()[0]
    return "unknown"


def _extract_flag(runs: pd.DataFrame, flag_name: str, default: str = "unknown") -> str:
    if runs.empty:
        return default
    for details in runs["details"]:
        if isinstance(details, str) and f"{flag_name}=" in details:
            return details.split(f"{flag_name}=", maxsplit=1)[1].split()[0]
    return default


def main() -> None:
    settings = get_settings()
    repo = JournalRepo(create_session_factory(settings.database_url))
    broker = AlpacaTradingAdapter(settings)

    st.set_page_config(page_title="Trading Bot Dashboard", layout="wide")
    st.title("Trading Bot Dashboard")
    st.caption("Paper-trading state and recent journal activity.")

    signals = _records_to_frame(repo.recent_signals(), ["symbol", "signal", "price", "created_at"])
    orders = _records_to_frame(repo.recent_orders(), ["symbol", "side", "qty", "status", "submitted_at"])
    runs = _records_to_frame(repo.recent_runs(), ["run_type", "status", "details", "created_at"])
    data_source = _extract_data_source(runs)
    dry_run = _extract_flag(runs, "dry_run", default=str(settings.dry_run).lower())
    account = broker.get_account_summary()
    broker_orders = pd.DataFrame([snapshot.__dict__ for snapshot in broker.list_recent_orders(limit=10)])
    broker_positions = pd.DataFrame([snapshot.__dict__ for snapshot in broker.list_positions()])
    local_positions = _records_to_frame(
        repo.current_position_snapshots(),
        [
            "symbol",
            "qty",
            "market_value",
            "avg_entry_price",
            "current_price",
            "cost_basis",
            "unrealized_pl",
            "unrealized_plpc",
            "captured_at",
        ],
    )
    local_account_snapshot = repo.latest_account_snapshot()
    local_pnl_history = _records_to_frame(
        repo.portfolio_pnl_snapshots(),
        ["timestamp", "equity", "profit_loss", "profit_loss_pct"],
    )
    latest_local_pnl = local_pnl_history.iloc[-1] if not local_pnl_history.empty else None
    execution_fills = _records_to_frame(
        repo.execution_fills(),
        [
            "symbol",
            "side",
            "qty",
            "price",
            "gross_amount",
            "fees",
            "net_amount",
            "execution_date",
            "filled_at",
            "broker_order_id",
        ],
    )
    if not execution_fills.empty:
        execution_fills = execution_fills.rename(columns={"fees": "estimated_fees"})
    realized_matches = _records_to_frame(
        repo.realized_lot_matches(),
        [
            "symbol",
            "matched_qty",
            "open_price",
            "close_price",
            "fees",
            "realized_pnl",
            "execution_date",
            "open_filled_at",
            "close_filled_at",
        ],
    )
    if not realized_matches.empty:
        realized_matches = realized_matches.rename(columns={"fees": "estimated_fees"})
    realized_pnl = _records_to_frame(
        repo.realized_pnl(),
        ["symbol", "realized_qty", "realized_pnl", "last_fill_at"],
    )

    top_left, top_mid, top_right = st.columns(3)
    with top_left:
        st.metric("Data Source", data_source)
    with top_mid:
        st.metric("Dry Run", dry_run)
    with top_right:
        st.metric("Recent Runs", len(runs))

    broker_left, broker_mid, broker_right = st.columns(3)
    with broker_left:
        st.metric("Account Status", account.status)
    with broker_mid:
        st.metric("Buying Power", account.buying_power)
    with broker_right:
        st.metric("Equity", account.equity)

    snapshot_left, snapshot_mid, snapshot_right = st.columns(3)
    with snapshot_left:
        st.metric("Local Snapshot Equity", getattr(local_account_snapshot, "equity", 0))
    with snapshot_mid:
        st.metric("Portfolio PnL", getattr(latest_local_pnl, "profit_loss", 0))
    with snapshot_right:
        st.metric("Tracked Positions", len(local_positions))

    pnl_left, pnl_mid, pnl_right = st.columns(3)
    with pnl_left:
        st.metric("Local Snapshot Cash", getattr(local_account_snapshot, "cash", 0))
    with pnl_mid:
        st.metric("PnL %", getattr(latest_local_pnl, "profit_loss_pct", 0))
    with pnl_right:
        st.metric(
            "Position Unrealized PnL",
            float(local_positions["unrealized_pl"].sum()) if not local_positions.empty else 0,
        )

    realized_left, realized_mid, realized_right = st.columns(3)
    with realized_left:
        st.metric("Execution Fills", len(execution_fills))
    with realized_mid:
        st.metric("Realized Matches", len(realized_matches))
    with realized_right:
        st.metric(
            "Total Realized PnL",
            float(realized_matches["realized_pnl"].sum()) if not realized_matches.empty else 0,
        )

    left, right = st.columns(2)
    with left:
        st.subheader("Recent Signals")
        st.dataframe(signals, width="stretch")
    with right:
        st.subheader("Local Orders")
        st.dataframe(orders, width="stretch")

    broker_table_left, broker_table_right = st.columns(2)
    with broker_table_left:
        st.subheader("Alpaca Orders")
        st.dataframe(broker_orders, width="stretch")
    with broker_table_right:
        st.subheader("Alpaca Positions")
        st.dataframe(broker_positions, width="stretch")

    st.subheader("Local Position Snapshots")
    st.dataframe(local_positions, width="stretch")

    st.subheader("Portfolio PnL History")
    if local_pnl_history.empty:
        st.info("Run reconcile to populate the portfolio PnL timeline.")
    else:
        st.line_chart(local_pnl_history.set_index("timestamp")[["equity", "profit_loss"]], width="stretch")
        st.dataframe(local_pnl_history, width="stretch")

    ledger_left, ledger_right = st.columns(2)
    with ledger_left:
        st.subheader("Execution Fills")
        st.caption("`estimated_fees` are allocated from Alpaca fee activity cashflows, not matched one-to-one per fill.")
        st.dataframe(execution_fills, width="stretch")
    with ledger_right:
        st.subheader("Realized Lot Matches")
        st.caption("`estimated_fees` reflect allocated broker fee cashflows across matched fills.")
        st.dataframe(realized_matches, width="stretch")

    st.subheader("Realized PnL")
    st.dataframe(realized_pnl, width="stretch")

    st.subheader("Recent Runs")
    st.dataframe(runs, width="stretch")


if __name__ == "__main__":
    main()
