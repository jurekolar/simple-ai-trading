import json
from datetime import UTC, datetime

from app.config import Settings
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.main import _log_runtime_config_snapshot
from app.ops.preflight import evaluate_preflight, format_preflight_checks, preflight_passed


class _FakeBroker:
    def get_account_summary(self) -> object:
        return type("AccountSnapshotStub", (), {"status": "ACTIVE"})()


def test_evaluate_preflight_live_requires_recent_benchmark_artifact(tmp_path) -> None:
    settings = Settings(
        DRY_RUN=False,
        ALPACA_PAPER=False,
        PAPER_ONLY=False,
        ALLOW_LIVE=True,
        CONFIG_PROFILE="live",
        LIVE_CONFIG_PROFILE="live",
        LIVE_DEPLOYMENT_ACK="I_ACKNOWLEDGE_LIVE_TRADING",
        ALERT_WEBHOOK_URL="https://example.test/webhook",
        PRIMARY_LIVE_STRATEGY="breakout",
        DATABASE_URL=f"sqlite:///{tmp_path / 'live.db'}",
        BACKTEST_OUTPUT_DIR=str(tmp_path / "artifacts"),
    )
    repo = JournalRepo(create_session_factory(settings.database_url))
    repo.add_account_snapshot(status="ACTIVE", buying_power=10000.0, equity=10000.0, cash=10000.0)
    repo.create_run("reconcile", "completed", details="ok")

    checks = evaluate_preflight(settings=settings, repo=repo, broker=_FakeBroker(), strategy_name="breakout")

    assert not preflight_passed(checks, live_mode=True)
    statuses = {check.name: check.status for check in checks}
    assert statuses["benchmark_artifact"] == "fail"
    assert "benchmark_artifact" in format_preflight_checks(checks)


def test_evaluate_preflight_live_passes_with_recent_approved_candidate(tmp_path) -> None:
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "latest.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "decision_ready": True,
                "benchmark_valid": True,
                "recommended_live_candidate": "breakout",
                "artifact_dir": str(artifact_dir / "run-001"),
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        DRY_RUN=False,
        ALPACA_PAPER=False,
        PAPER_ONLY=False,
        ALLOW_LIVE=True,
        CONFIG_PROFILE="live",
        LIVE_CONFIG_PROFILE="live",
        LIVE_DEPLOYMENT_ACK="I_ACKNOWLEDGE_LIVE_TRADING",
        ALERT_WEBHOOK_URL="https://example.test/webhook",
        PRIMARY_LIVE_STRATEGY="breakout",
        DATABASE_URL=f"sqlite:///{tmp_path / 'live.db'}",
        BACKTEST_OUTPUT_DIR=str(artifact_dir),
    )
    repo = JournalRepo(create_session_factory(settings.database_url))
    repo.add_account_snapshot(status="ACTIVE", buying_power=10000.0, equity=10000.0, cash=10000.0)
    repo.create_run("reconcile", "completed", details="ok")

    checks = evaluate_preflight(settings=settings, repo=repo, broker=_FakeBroker(), strategy_name="breakout")

    assert preflight_passed(checks, live_mode=True)
    statuses = {check.name: check.status for check in checks}
    assert statuses["benchmark_artifact"] == "pass"
    assert statuses["approved_strategy_match"] == "pass"


def test_log_runtime_config_snapshot_records_operator_action_changes(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'journal.db'}"
    repo = JournalRepo(create_session_factory(database_url))
    baseline = Settings(DATABASE_URL=database_url, DENY_NEW_ENTRIES=False, EMERGENCY_FLATTEN=False, SAFE_OPEN_ENABLED=True)
    updated = Settings(DATABASE_URL=database_url, DENY_NEW_ENTRIES=True, EMERGENCY_FLATTEN=True, SAFE_OPEN_ENABLED=False)

    _log_runtime_config_snapshot(repo, settings=baseline, run_type="paper", strategy_name="breakout")
    _log_runtime_config_snapshot(repo, settings=updated, run_type="paper", strategy_name="breakout")

    events = repo.recent_operator_action_events(limit=10)
    actions = {event.action: (event.old_value, event.new_value) for event in events}
    assert actions["deny_new_entries"] == ("false", "true")
    assert actions["emergency_flatten"] == ("false", "true")
    assert actions["safe_open_enabled"] == ("true", "false")
