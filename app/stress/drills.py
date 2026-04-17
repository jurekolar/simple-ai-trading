from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.config import Settings
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.risk.kill_switch import KillSwitchState, assess_reconciliation_health, evaluate_kill_switch


SCENARIO_NAMES = (
    "restart_with_open_orders",
    "stale_data_block",
    "broker_submit_failures",
    "reconciliation_drift",
    "emergency_flatten",
)


@dataclass(frozen=True)
class StressDrillResult:
    scenario: str
    severity: str
    reason: str
    expected_behavior: str
    observed_behavior: str
    follow_up_commands: tuple[str, ...]
    log_path: str


def run_named_drill(
    scenario: str,
    *,
    settings: Settings | None = None,
    log_dir: str | Path = "logs/burnin",
) -> StressDrillResult:
    active_settings = settings or Settings()
    _ensure_paper_stress_profile(active_settings)
    if scenario not in SCENARIO_NAMES:
        raise ValueError(f"unsupported scenario={scenario}")
    repo = JournalRepo(create_session_factory(active_settings.database_url))
    handlers = {
        "restart_with_open_orders": _run_restart_with_open_orders,
        "stale_data_block": _run_stale_data_block,
        "broker_submit_failures": _run_broker_submit_failures,
        "reconciliation_drift": _run_reconciliation_drift,
        "emergency_flatten": _run_emergency_flatten,
    }
    result = handlers[scenario](repo, active_settings, Path(log_dir))
    _append_log_entry(Path(result.log_path), result)
    return result


def _ensure_paper_stress_profile(settings: Settings) -> None:
    profile_errors: list[str] = []
    if not settings.alpaca_paper:
        profile_errors.append("ALPACA_PAPER must remain true")
    if not settings.paper_only:
        profile_errors.append("PAPER_ONLY must remain true")
    if settings.allow_live:
        profile_errors.append("ALLOW_LIVE must remain false")
    if settings.config_profile.lower() != "paper":
        profile_errors.append("CONFIG_PROFILE must be paper")
    if "burnin" not in settings.database_url.lower():
        profile_errors.append("DATABASE_URL must point to the burn-in journal")
    if profile_errors:
        joined = "; ".join(profile_errors)
        raise RuntimeError(f"stress drills require the paper burn-in profile: {joined}")


def _run_restart_with_open_orders(
    repo: JournalRepo,
    settings: Settings,
    log_dir: Path,
) -> StressDrillResult:
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    repo.log_order(
        "SPY",
        "buy",
        5.0,
        "partially_filled",
        status_detail="stress_drill_restart_with_open_orders",
        client_order_id=f"stress-restart-{stamp}",
        broker_order_id=f"stress-restart-{stamp}",
        requested_price=100.0,
        filled_avg_price=100.0,
    )
    unresolved = repo.unresolved_order_age_summary()
    observed_behavior = (
        f"unresolved_orders={int(unresolved['count'])} oldest_minutes={unresolved['oldest_minutes']:.2f} "
        "restart should skip duplicate risk until reconciliation clears the symbol"
    )
    repo.log_alert_event(
        channel="stress_drill",
        delivery_status="simulated",
        message="restart_with_open_orders created unresolved order state",
    )
    return StressDrillResult(
        scenario="restart_with_open_orders",
        severity="info",
        reason="unresolved_order_restart_guard",
        expected_behavior="A restart must detect unresolved order state and avoid duplicate entries.",
        observed_behavior=observed_behavior,
        follow_up_commands=(
            ".venv/bin/python -m app.main reconcile",
            ".venv/bin/python -m app.main paper",
        ),
        log_path=str(_daily_log_path(log_dir)),
    )


def _run_stale_data_block(
    repo: JournalRepo,
    settings: Settings,
    log_dir: Path,
) -> StressDrillResult:
    state = evaluate_kill_switch(
        True,
        False,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        broker_failure_count=0,
        open_order_count=0,
        has_stuck_orders=False,
        max_daily_loss=settings.max_daily_loss,
        max_unrealized_drawdown=settings.max_unrealized_drawdown,
        emergency_unrealized_drawdown=settings.emergency_unrealized_drawdown,
        max_broker_failures=settings.max_broker_failures,
        max_open_orders=settings.max_open_orders,
    )
    repo.log_kill_switch_event(
        severity=state.severity,
        reason=state.reason,
        details="scenario=stale_data_block source=fallback expected=reduce_only",
    )
    repo.log_alert_event(
        channel="stress_drill",
        delivery_status="simulated",
        message="kill switch active: stale_data",
    )
    return StressDrillResult(
        scenario="stale_data_block",
        severity=state.severity,
        reason=state.reason,
        expected_behavior="New entries should block while exits and reconcile remain allowed.",
        observed_behavior=f"kill_switch={state.reason} severity={state.severity}",
        follow_up_commands=(
            "make burnin-auth",
            ".venv/bin/python -m app.main reconcile",
        ),
        log_path=str(_daily_log_path(log_dir)),
    )


def _run_broker_submit_failures(
    repo: JournalRepo,
    settings: Settings,
    log_dir: Path,
) -> StressDrillResult:
    failure_count = max(settings.max_broker_failures, 1)
    for _ in range(failure_count):
        repo.log_broker_error_event(
            symbol="SPY",
            operation="submit_market_order",
            message="stress drill simulated broker submit failure",
        )
    state = evaluate_kill_switch(
        False,
        False,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        broker_failure_count=failure_count,
        open_order_count=0,
        has_stuck_orders=False,
        max_daily_loss=settings.max_daily_loss,
        max_unrealized_drawdown=settings.max_unrealized_drawdown,
        emergency_unrealized_drawdown=settings.emergency_unrealized_drawdown,
        max_broker_failures=settings.max_broker_failures,
        max_open_orders=settings.max_open_orders,
    )
    repo.log_kill_switch_event(
        severity=state.severity,
        reason=state.reason,
        details=f"scenario=broker_submit_failures failures={failure_count}",
    )
    repo.log_alert_event(
        channel="stress_drill",
        delivery_status="simulated",
        message=f"broker/API failure threshold breached count={failure_count}",
    )
    return StressDrillResult(
        scenario="broker_submit_failures",
        severity=state.severity,
        reason=state.reason,
        expected_behavior="Repeated broker failures should degrade the session to reduce-only mode.",
        observed_behavior=f"broker_failures={failure_count} kill_switch={state.reason}",
        follow_up_commands=(
            ".venv/bin/python -m app.main reconcile",
            ".venv/bin/python scripts/burnin_report.py --days 1",
        ),
        log_path=str(_daily_log_path(log_dir)),
    )


def _run_reconciliation_drift(
    repo: JournalRepo,
    settings: Settings,
    log_dir: Path,
) -> StressDrillResult:
    state = assess_reconciliation_health({"SPY": 10.0}, {"SPY": 10.25})
    repo.log_reconciliation_event(
        severity=state.severity,
        reason=state.reason,
        details="scenario=reconciliation_drift local_symbols=['SPY'] broker_symbols=['SPY'] qty_diff=0.25",
    )
    repo.log_kill_switch_event(
        severity=state.severity,
        reason=state.reason,
        details="scenario=reconciliation_drift expected=reduce_only",
    )
    repo.log_alert_event(
        channel="stress_drill",
        delivery_status="simulated",
        message="reconciliation drift detected for SPY",
    )
    return StressDrillResult(
        scenario="reconciliation_drift",
        severity=state.severity,
        reason=state.reason,
        expected_behavior="Reconciliation drift should degrade to reduce-only and require operator review.",
        observed_behavior=f"reconciliation_state={state.reason} severity={state.severity}",
        follow_up_commands=(
            ".venv/bin/python -m app.main reconcile",
            "make burnin-review",
        ),
        log_path=str(_daily_log_path(log_dir)),
    )


def _run_emergency_flatten(
    repo: JournalRepo,
    settings: Settings,
    log_dir: Path,
) -> StressDrillResult:
    state = KillSwitchState("flatten", "config_emergency_flatten")
    repo.log_kill_switch_event(
        severity=state.severity,
        reason=state.reason,
        details="scenario=emergency_flatten expected=flatten",
    )
    repo.log_alert_event(
        channel="stress_drill",
        delivery_status="simulated",
        message="emergency flatten requested; new entries must stay blocked",
    )
    return StressDrillResult(
        scenario="emergency_flatten",
        severity=state.severity,
        reason=state.reason,
        expected_behavior="All held symbols should be forced toward exit and no new entries should be allowed.",
        observed_behavior=f"kill_switch={state.reason} severity={state.severity}",
        follow_up_commands=(
            "EMERGENCY_FLATTEN=true .venv/bin/python -m app.main paper",
            ".venv/bin/python -m app.main reconcile",
        ),
        log_path=str(_daily_log_path(log_dir)),
    )


def _append_log_entry(path: Path, result: StressDrillResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).astimezone().isoformat(timespec="seconds")
    commands = " | ".join(result.follow_up_commands)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"[{timestamp}] stress_drill scenario={result.scenario} severity={result.severity} "
            f"reason={result.reason}\n"
        )
        handle.write(f"expected={result.expected_behavior}\n")
        handle.write(f"observed={result.observed_behavior}\n")
        handle.write(f"follow_up={commands}\n")


def _daily_log_path(log_dir: Path) -> Path:
    return log_dir / f"{datetime.now(UTC).astimezone().date().isoformat()}.log"
