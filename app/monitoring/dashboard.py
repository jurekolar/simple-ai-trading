from __future__ import annotations

from collections import Counter

import pandas as pd
import streamlit as st

from app.backtest.compare import load_latest_benchmark_index
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


def _drawdown_frame(pnl_history: pd.DataFrame) -> pd.DataFrame:
    if pnl_history.empty:
        return pd.DataFrame(columns=["drawdown_pct"])
    frame = pnl_history.copy()
    frame["equity_peak"] = frame["equity"].cummax()
    frame["drawdown_pct"] = ((frame["equity"] - frame["equity_peak"]) / frame["equity_peak"]).fillna(0.0)
    return frame.set_index("timestamp")[["drawdown_pct"]]


def main() -> None:
    settings = get_settings()
    repo = JournalRepo(create_session_factory(settings.database_url))
    broker = AlpacaTradingAdapter(settings)

    st.set_page_config(page_title="Trading Bot Dashboard", layout="wide")
    st.title("Trading Bot Dashboard")
    st.caption("Paper and live-readiness state, operator health, and recent journal activity.")

    signals = _records_to_frame(repo.recent_signals(), ["symbol", "signal", "price", "created_at"])
    orders = _records_to_frame(
        repo.recent_orders(),
        ["symbol", "side", "qty", "status", "status_detail", "submitted_at"],
    )
    runs = _records_to_frame(repo.recent_runs(), ["run_type", "status", "details", "created_at"])
    config_snapshots = _records_to_frame(
        repo.recent_config_snapshots(),
        ["run_type", "strategy_name", "config_profile", "broker_mode", "created_at", "details"],
    )
    kill_switch_events = _records_to_frame(
        repo.recent_kill_switch_events(),
        ["severity", "reason", "details", "created_at"],
    )
    reconciliation_events = _records_to_frame(
        repo.recent_reconciliation_events(),
        ["severity", "reason", "details", "created_at"],
    )
    alert_events = _records_to_frame(
        repo.recent_alert_events(),
        ["channel", "delivery_status", "message", "error_message", "created_at"],
    )
    operator_actions = _records_to_frame(
        repo.recent_operator_action_events(),
        ["action", "old_value", "new_value", "details", "created_at"],
    )
    latest_benchmark = load_latest_benchmark_index(settings)
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
    unresolved_orders = _records_to_frame(
        repo.unresolved_orders(),
        ["symbol", "side", "qty", "status", "status_detail", "submitted_at", "client_order_id"],
    )
    unresolved_summary = repo.unresolved_order_age_summary()

    blocked_reason_frame = pd.DataFrame(
        [
            {"reason": reason, "count": count}
            for reason, count in Counter(
                order.status_detail or order.status
                for order in repo.recent_orders(limit=200)
                if order.status == "blocked"
            ).most_common()
        ]
    )
    exposure_frame = (
        local_positions[["symbol", "market_value", "unrealized_pl"]]
        .sort_values("market_value", key=lambda series: series.abs(), ascending=False)
        if not local_positions.empty
        else pd.DataFrame(columns=["symbol", "market_value", "unrealized_pl"])
    )

    top_left, top_mid, top_right, top_fourth = st.columns(4)
    with top_left:
        st.metric("Config Profile", config_snapshots.iloc[0]["config_profile"] if not config_snapshots.empty else "unknown")
    with top_mid:
        st.metric("Broker Mode", config_snapshots.iloc[0]["broker_mode"] if not config_snapshots.empty else settings.broker_mode)
    with top_right:
        st.metric("Data Source", data_source)
    with top_fourth:
        st.metric("Dry Run", dry_run)

    live_left, live_mid, live_right, live_fourth = st.columns(4)
    with live_left:
        st.metric("Strategy", config_snapshots.iloc[0]["strategy_name"] if not config_snapshots.empty else "unknown")
    with live_mid:
        latest_kill_switch = kill_switch_events.iloc[0]["reason"] if not kill_switch_events.empty else "none"
        st.metric("Kill Switch", latest_kill_switch)
    with live_right:
        latest_reconciliation = reconciliation_events.iloc[0]["reason"] if not reconciliation_events.empty else "none"
        st.metric("Reconciliation", latest_reconciliation)
    with live_fourth:
        st.metric("Benchmark Candidate", latest_benchmark.get("recommended_live_candidate", "none"))

    benchmark_left, benchmark_mid, benchmark_right = st.columns(3)
    with benchmark_left:
        st.metric("Benchmark Ready", "yes" if latest_benchmark.get("decision_ready") else "no")
    with benchmark_mid:
        st.metric("Benchmark Valid", "yes" if latest_benchmark.get("benchmark_valid") else "no")
    with benchmark_right:
        st.metric("Benchmark Generated", latest_benchmark.get("generated_at", "missing"))

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

    unresolved_left, unresolved_mid, unresolved_right = st.columns(3)
    with unresolved_left:
        st.metric("Unresolved Orders", int(unresolved_summary["count"]))
    with unresolved_mid:
        st.metric("Oldest Unresolved (m)", f"{unresolved_summary['oldest_minutes']:.1f}")
    with unresolved_right:
        st.metric("Alert Failures", int((alert_events["delivery_status"] == "failed").sum()) if not alert_events.empty else 0)

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

    risk_left, risk_right = st.columns(2)
    with risk_left:
        st.subheader("Blocked Order Reasons")
        if blocked_reason_frame.empty:
            st.info("No blocked orders recorded in the recent window.")
        else:
            st.dataframe(blocked_reason_frame, width="stretch")
    with risk_right:
        st.subheader("Unresolved Order Aging")
        if unresolved_orders.empty:
            st.info("No unresolved orders.")
        else:
            st.dataframe(unresolved_orders, width="stretch")

    if latest_benchmark:
        st.subheader("Latest Benchmark Artifact")
        st.write(f"path={latest_benchmark.get('artifact_dir', 'unknown')}")

    broker_table_left, broker_table_right = st.columns(2)
    with broker_table_left:
        st.subheader("Alpaca Orders")
        st.dataframe(broker_orders, width="stretch")
    with broker_table_right:
        st.subheader("Alpaca Positions")
        st.dataframe(broker_positions, width="stretch")

    exposure_left, exposure_right = st.columns(2)
    with exposure_left:
        st.subheader("Exposure By Symbol")
        st.dataframe(exposure_frame, width="stretch")
    with exposure_right:
        st.subheader("Current Kill Switch / Reconciliation")
        st.dataframe(kill_switch_events, width="stretch")
        st.dataframe(reconciliation_events, width="stretch")

    st.subheader("Local Position Snapshots")
    st.dataframe(local_positions, width="stretch")

    st.subheader("Portfolio PnL History")
    if local_pnl_history.empty:
        st.info("Run reconcile to populate the portfolio PnL timeline.")
    else:
        st.line_chart(local_pnl_history.set_index("timestamp")[["equity", "profit_loss"]], width="stretch")
        st.line_chart(_drawdown_frame(local_pnl_history), width="stretch")
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

    bottom_left, bottom_right = st.columns(2)
    with bottom_left:
        st.subheader("Alert Delivery")
        st.dataframe(alert_events, width="stretch")
    with bottom_right:
        st.subheader("Config Snapshots")
        st.dataframe(config_snapshots, width="stretch")

    st.subheader("Operator Action Audit")
    if operator_actions.empty:
        st.info("No operator action changes recorded yet.")
    else:
        st.dataframe(operator_actions, width="stretch")

    st.subheader("Recent Runs")
    st.dataframe(runs, width="stretch")


if __name__ == "__main__":
    main()
