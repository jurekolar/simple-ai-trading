from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.backtest.compare import load_latest_benchmark_index
from app.config import Settings
from app.db.repo import JournalRepo


@dataclass(frozen=True)
class PreflightCheck:
    name: str
    status: str
    detail: str


def evaluate_preflight(
    *,
    settings: Settings,
    repo: JournalRepo,
    broker,
    strategy_name: str,
) -> list[PreflightCheck]:
    checks: list[PreflightCheck] = []
    config_ok = settings.config_profile.strip().lower() == ("live" if settings.live_trading_enabled else settings.config_profile.strip().lower())
    checks.append(
        PreflightCheck(
            "config_profile",
            "pass" if config_ok else "fail",
            f"profile={settings.config_profile}",
        )
    )
    db_separated = "burnin" not in settings.database_url.lower() and "trading.db" not in settings.database_url.lower()
    checks.append(
        PreflightCheck(
            "database_separation",
            "pass" if (not settings.live_trading_enabled or db_separated) else "fail",
            f"database_url={settings.database_url}",
        )
    )
    alert_ok = bool(settings.alert_webhook_url)
    checks.append(
        PreflightCheck(
            "alert_routing",
            "pass" if (not settings.live_trading_enabled or alert_ok) else "fail",
            "configured" if alert_ok else "alert_webhook_url_missing",
        )
    )

    try:
        account = broker.get_account_summary()
        auth_ok = bool(getattr(account, "status", ""))
        auth_detail = f"account_status={getattr(account, 'status', 'unknown')}"
    except Exception as exc:  # pragma: no cover - external dependency path
        auth_ok = False
        auth_detail = str(exc)
    checks.append(PreflightCheck("broker_auth", "pass" if auth_ok else "fail", auth_detail))

    latest_index = load_latest_benchmark_index(settings)
    benchmark_generated_at = latest_index.get("generated_at", "")
    benchmark_age_ok = False
    if benchmark_generated_at:
        try:
            benchmark_generated_dt = datetime.fromisoformat(str(benchmark_generated_at))
            if benchmark_generated_dt.tzinfo is None:
                benchmark_generated_dt = benchmark_generated_dt.replace(tzinfo=UTC)
            benchmark_age_ok = benchmark_generated_dt >= datetime.now(UTC) - timedelta(
                days=settings.live_preflight_max_benchmark_age_days
            )
        except ValueError:
            benchmark_age_ok = False
    benchmark_decision_ready = bool(latest_index.get("decision_ready"))
    benchmark_candidate = str(latest_index.get("recommended_live_candidate", "") or "")
    benchmark_ok = benchmark_decision_ready and benchmark_age_ok
    checks.append(
        PreflightCheck(
            "benchmark_artifact",
            "pass" if benchmark_ok else "fail",
            f"candidate={benchmark_candidate or 'none'} generated_at={benchmark_generated_at or 'missing'}",
        )
    )
    strategy_match_ok = (not settings.live_trading_enabled) or (benchmark_candidate == strategy_name == settings.primary_live_strategy)
    checks.append(
        PreflightCheck(
            "approved_strategy_match",
            "pass" if strategy_match_ok else "fail",
            f"primary_live_strategy={settings.primary_live_strategy} benchmark_candidate={benchmark_candidate or 'none'} requested={strategy_name}",
        )
    )

    latest_account_snapshot = repo.latest_account_snapshot()
    if latest_account_snapshot is None:
        snapshot_status = "review"
        snapshot_detail = "no_account_snapshot"
    else:
        captured_at = latest_account_snapshot.captured_at
        if captured_at.tzinfo is None:
            captured_at = captured_at.replace(tzinfo=UTC)
        snapshot_age = datetime.now(UTC) - captured_at
        snapshot_status = (
            "pass"
            if snapshot_age <= timedelta(minutes=settings.live_preflight_max_snapshot_age_minutes)
            else "review"
        )
        snapshot_detail = f"snapshot_age_minutes={snapshot_age.total_seconds() / 60:.1f}"
    checks.append(PreflightCheck("snapshot_freshness", snapshot_status, snapshot_detail))

    reconcile_runs = repo.runs_since(datetime.now(UTC) - timedelta(days=1), run_type="reconcile")
    checks.append(
        PreflightCheck(
            "recent_reconcile",
            "pass" if reconcile_runs else "review",
            f"runs_1d={len(reconcile_runs)}",
        )
    )
    unresolved_orders = repo.unresolved_order_age_summary()
    checks.append(
        PreflightCheck(
            "unresolved_orders",
            "pass" if unresolved_orders["count"] == 0 else "review",
            f"count={int(unresolved_orders['count'])} oldest_minutes={unresolved_orders['oldest_minutes']:.1f}",
        )
    )
    burnin_scorecard = repo.benchmark_free_scorecard() if hasattr(repo, "benchmark_free_scorecard") else ""
    if not burnin_scorecard:
        burnin_scorecard = "not_available"
    checks.append(
        PreflightCheck(
            "burnin_scorecard",
            "pass" if "pass" in burnin_scorecard or not settings.live_trading_enabled else "review",
            burnin_scorecard,
        )
    )
    return checks


def format_preflight_checks(checks: list[PreflightCheck]) -> str:
    return "\n".join(
        ["Preflight Validation"] + [f"{check.status} {check.name}: {check.detail}" for check in checks]
    )


def preflight_passed(checks: list[PreflightCheck], *, live_mode: bool) -> bool:
    failing_statuses = {"fail"} | ({"review"} if live_mode else set())
    return all(check.status not in failing_statuses for check in checks)
