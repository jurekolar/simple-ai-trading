from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
import re

from app.backtest.compare import load_latest_benchmark_index
from app.config import get_settings
from app.db.repo import JournalRepo


def _format_float(value: float) -> str:
    return f"{float(value):.2f}"


def _scorecard_line(repo: JournalRepo, days: int) -> str:
    start_at = datetime.now(UTC) - timedelta(days=days)
    orders = repo.orders_since(start_at)
    reconciliation_events = repo.reconciliation_events_since(start_at)
    broker_errors = repo.broker_error_events_since(start_at)
    alert_events = repo.alert_events_since(start_at)
    duplicate_client_order_ids = [
        client_order_id
        for client_order_id, count in Counter(
            order.client_order_id for order in orders if order.client_order_id
        ).items()
        if count > 1
    ]
    duplicate_broker_order_ids = [
        broker_order_id
        for broker_order_id, count in Counter(
            order.broker_order_id for order in orders if order.broker_order_id
        ).items()
        if count > 1
    ]
    failures: list[str] = []
    if duplicate_client_order_ids or duplicate_broker_order_ids:
        failures.append("duplicate_order_ids")
    if reconciliation_events:
        failures.append("reconciliation_events")
    if broker_errors:
        failures.append("broker_errors")
    if any(event.delivery_status == "failed" for event in alert_events):
        failures.append("alert_delivery_failures")
    if repo.unresolved_orders():
        failures.append("unresolved_orders_present")
    status = "pass" if not failures else "review"
    reasons = ",".join(failures) if failures else "clean"
    return f"{days}d={status} ({reasons})"


def build_daily_report(repo: JournalRepo) -> str:
    now = datetime.now(UTC)
    settings = get_settings()
    start_at = now - timedelta(days=1)
    runs = repo.recent_runs(limit=10)
    paper_runs = repo.runs_since(start_at, run_type="paper")
    orders = repo.orders_since(start_at)
    positions = repo.current_position_snapshots()
    account = repo.latest_account_snapshot()
    pnl_snapshots = repo.portfolio_pnl_snapshots()
    latest_pnl = pnl_snapshots[-1] if pnl_snapshots else None
    unresolved_orders = repo.unresolved_orders()
    unresolved_age = repo.unresolved_order_age_summary()
    fills = repo.execution_fills()
    lot_matches = repo.realized_lot_matches(limit=500)
    realized = repo.realized_pnl()
    kill_switch_events = repo.kill_switch_events_since(start_at)
    reconciliation_events = repo.reconciliation_events_since(start_at)
    broker_error_events = repo.broker_error_events_since(start_at)
    alert_events = repo.alert_events_since(start_at)
    operator_action_events = repo.operator_action_events_since(start_at)
    config_snapshot = repo.latest_config_snapshot(run_type="paper") or repo.latest_config_snapshot()
    config_details = repo.parse_config_snapshot(config_snapshot)
    latest_benchmark = load_latest_benchmark_index(settings)

    gross_exposure = float(sum(abs(position.market_value) for position in positions))
    unrealized_pnl = float(sum(position.unrealized_pl for position in positions))
    realized_total = float(sum(record.realized_pnl for record in realized))

    status_counts = Counter(order.status for order in orders)
    blocked_reason_counts = Counter(order.status_detail or order.status for order in orders if order.status == "blocked")
    kill_switch_reason_counts = Counter(event.reason for event in kill_switch_events)
    broker_error_counts = Counter(event.operation for event in broker_error_events)
    alert_status_counts = Counter(event.delivery_status for event in alert_events)

    winners = [match.realized_pnl for match in lot_matches if float(match.realized_pnl) > 0]
    losers = [match.realized_pnl for match in lot_matches if float(match.realized_pnl) < 0]
    matched_trades = len(lot_matches)
    win_rate = (len(winners) / matched_trades) if matched_trades else 0.0
    average_win = (sum(winners) / len(winners)) if winners else 0.0
    average_loss = (sum(losers) / len(losers)) if losers else 0.0
    expectancy = ((win_rate * average_win) + ((1 - win_rate) * average_loss)) if matched_trades else 0.0
    average_holding_days = (
        sum(
            max((match.close_filled_at - match.open_filled_at).total_seconds(), 0.0) / 86400
            for match in lot_matches
        )
        / matched_trades
        if matched_trades
        else 0.0
    )
    total_traded_notional = float(sum(abs(fill.gross_amount) for fill in fills))
    turnover = (total_traded_notional / float(account.equity)) if account and float(account.equity) else 0.0

    equity_values = [float(snapshot.equity) for snapshot in pnl_snapshots]
    running_peak = 0.0
    max_drawdown = 0.0
    for equity in equity_values:
        running_peak = max(running_peak, equity)
        if running_peak > 0:
            max_drawdown = min(max_drawdown, (equity - running_peak) / running_peak)

    top_positions = ", ".join(
        f"{position.symbol}:{_format_float(position.market_value)}"
        for position in sorted(positions, key=lambda position: abs(position.market_value), reverse=True)[:5]
    ) or "none"
    realized_by_symbol = ", ".join(
        f"{record.symbol}:{_format_float(record.realized_pnl)}"
        for record in sorted(realized, key=lambda record: float(record.realized_pnl), reverse=True)[:5]
    ) or "none"
    blocked_reasons = ", ".join(f"{reason}:{count}" for reason, count in blocked_reason_counts.most_common(5)) or "none"
    kill_switch_reasons = ", ".join(
        f"{reason}:{count}" for reason, count in kill_switch_reason_counts.most_common(5)
    ) or "none"
    broker_error_operations = ", ".join(
        f"{operation}:{count}" for operation, count in broker_error_counts.most_common(5)
    ) or "none"
    alert_delivery = ", ".join(
        f"{status}:{count}" for status, count in sorted(alert_status_counts.items())
    ) or "none"
    operator_actions = ", ".join(
        f"{event.action}:{event.old_value}->{event.new_value}"
        for event in operator_action_events[:5]
    ) or "none"
    benchmark_candidate = latest_benchmark.get("recommended_live_candidate", "none")
    benchmark_generated_at = latest_benchmark.get("generated_at", "missing")
    benchmark_valid = latest_benchmark.get("benchmark_valid", False)
    benchmark_ready = latest_benchmark.get("decision_ready", False)
    benchmark_matches_active = benchmark_candidate == (
        config_details.get("strategy", getattr(config_snapshot, "strategy_name", "unknown"))
    )
    latest_paper_run = paper_runs[-1] if paper_runs else None
    latest_paper_details = latest_paper_run.details if latest_paper_run is not None else ""
    politician_rejected = _extract_detail_value(latest_paper_details, "politician_copy_rejected_disclosures")
    politician_selected = _extract_detail_value(latest_paper_details, "politician_copy_selected")
    politician_targets = _extract_detail_value(latest_paper_details, "politician_copy_targets")

    return "\n".join(
        [
            "Daily Operator Report",
            f"generated_at={now.isoformat()}",
            f"profile={config_details.get('config_profile', getattr(config_snapshot, 'config_profile', 'unknown'))}",
            f"broker_mode={config_details.get('broker_mode', getattr(config_snapshot, 'broker_mode', 'unknown'))}",
            f"strategy={config_details.get('strategy', getattr(config_snapshot, 'strategy_name', 'unknown'))}",
            f"primary_live_strategy={config_details.get('primary_live_strategy', 'unknown')}",
            f"symbols={','.join(config_details.get('symbols', [])) if config_details.get('symbols') else 'unknown'}",
            f"latest_benchmark_candidate={benchmark_candidate}",
            f"latest_benchmark_generated_at={benchmark_generated_at}",
            f"latest_benchmark_valid={benchmark_valid}",
            f"latest_benchmark_ready={benchmark_ready}",
            f"active_strategy_matches_benchmark={benchmark_matches_active}",
            "",
            "Operator Summary",
            f"paper_runs_1d={len(paper_runs)}",
            f"recent_runs={len(runs)}",
            f"orders_1d={len(orders)}",
            f"submitted_orders_1d={status_counts.get('filled', 0) + status_counts.get('dry_run', 0) + status_counts.get('accepted', 0) + status_counts.get('new', 0)}",
            f"blocked_orders_1d={status_counts.get('blocked', 0)}",
            f"error_orders_1d={status_counts.get('error', 0)}",
            f"reconciliation_events_1d={len(reconciliation_events)}",
            f"kill_switch_events_1d={len(kill_switch_events)}",
            f"broker_errors_1d={len(broker_error_events)}",
            f"blocked_order_reasons={blocked_reasons}",
            f"kill_switch_reasons={kill_switch_reasons}",
            f"politician_copy_rejected_disclosures={politician_rejected or '0'}",
            f"politician_copy_selected={politician_selected or '0'}",
            f"politician_copy_targets={politician_targets or '0'}",
            "",
            "Portfolio Snapshot",
            f"latest_equity={account.equity if account else 0}",
            f"buying_power={account.buying_power if account else 0}",
            f"cash={account.cash if account else 0}",
            f"gross_exposure={_format_float(gross_exposure)}",
            f"unrealized_pnl={_format_float(unrealized_pnl)}",
            f"realized_pnl={_format_float(realized_total)}",
            f"portfolio_profit_loss={latest_pnl.profit_loss if latest_pnl else 0}",
            f"portfolio_profit_loss_pct={latest_pnl.profit_loss_pct if latest_pnl else 0}",
            f"tracked_positions={len(positions)}",
            f"top_positions={top_positions}",
            "",
            "Strategy Performance",
            f"matched_trades={matched_trades}",
            f"win_rate={win_rate:.2%}",
            f"average_win={_format_float(average_win)}",
            f"average_loss={_format_float(average_loss)}",
            f"expectancy={_format_float(expectancy)}",
            f"turnover={turnover:.2f}",
            f"average_holding_days={average_holding_days:.2f}",
            f"max_drawdown_pct={max_drawdown:.2%}",
            f"realized_by_symbol={realized_by_symbol}",
            "",
            "Operational Reliability",
            f"unresolved_orders={len(unresolved_orders)}",
            f"unresolved_order_oldest_minutes={unresolved_age['oldest_minutes']:.2f}",
            f"alert_delivery={alert_delivery}",
            f"alert_failures_1d={alert_status_counts.get('failed', 0)}",
            f"stale_data_incidents_1d={kill_switch_reason_counts.get('stale_data', 0)}",
            f"broker_error_operations={broker_error_operations}",
            f"operator_actions_1d={len(operator_action_events)}",
            f"operator_action_changes={operator_actions}",
            "",
            "Burn-In Scorecard",
            f"scorecard={_scorecard_line(repo, 7)}; {_scorecard_line(repo, 14)}; {_scorecard_line(repo, 30)}",
        ]
    )


def _extract_detail_value(details: str, key: str) -> str:
    match = re.search(rf"{re.escape(key)}=([^ ]+)", details)
    return match.group(1) if match else ""
