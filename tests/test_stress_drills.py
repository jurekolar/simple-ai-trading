from __future__ import annotations

from pathlib import Path

import pytest

from app.broker.alpaca_client import BrokerClosePositionError, BrokerOrderSnapshot
from app.broker.execution import PaperExecutor
from app.broker.order_mapper import OrderIntent
from app.config import Settings
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.main import _process_flatten_with_close_position
from app.risk.checks import entry_risk_decision
from app.risk.kill_switch import assess_reconciliation_health, evaluate_kill_switch
from app.stress.drills import run_named_drill


def _journal_repo(tmp_path: Path) -> JournalRepo:
    return JournalRepo(create_session_factory(f"sqlite:///{tmp_path / 'journal.db'}"))


def _paper_burnin_settings(tmp_path: Path, **overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "DATABASE_URL": f"sqlite:///{tmp_path / 'trading_burnin.db'}",
        "CONFIG_PROFILE": "paper",
        "ALPACA_PAPER": True,
        "PAPER_ONLY": True,
        "ALLOW_LIVE": False,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def test_kill_switch_blocks_on_partial_data_failure() -> None:
    state = evaluate_kill_switch(
        False,
        True,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        broker_failure_count=0,
        open_order_count=0,
        has_stuck_orders=False,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
        max_broker_failures=3,
        max_open_orders=8,
    )

    assert state.severity == "reduce_only"
    assert state.reason == "partial_data_failure"


def test_kill_switch_blocks_on_daily_loss_limit() -> None:
    state = evaluate_kill_switch(
        False,
        False,
        realized_pnl=-1_000.0,
        unrealized_pnl=0.0,
        broker_failure_count=0,
        open_order_count=0,
        has_stuck_orders=False,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
        max_broker_failures=3,
        max_open_orders=8,
    )

    assert state.severity == "reduce_only"
    assert state.reason == "daily_loss_limit"


def test_kill_switch_blocks_on_broker_failures() -> None:
    state = evaluate_kill_switch(
        False,
        False,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        broker_failure_count=3,
        open_order_count=0,
        has_stuck_orders=False,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
        max_broker_failures=3,
        max_open_orders=8,
    )

    assert state.severity == "reduce_only"
    assert state.reason == "broker_failures"


def test_kill_switch_blocks_on_excess_open_orders() -> None:
    state = evaluate_kill_switch(
        False,
        False,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        broker_failure_count=0,
        open_order_count=9,
        has_stuck_orders=False,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
        max_broker_failures=3,
        max_open_orders=8,
    )

    assert state.severity == "reduce_only"
    assert state.reason == "too_many_open_orders"


def test_kill_switch_blocks_on_stuck_orders() -> None:
    state = evaluate_kill_switch(
        False,
        False,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        broker_failure_count=0,
        open_order_count=1,
        has_stuck_orders=True,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
        max_broker_failures=3,
        max_open_orders=8,
    )

    assert state.severity == "reduce_only"
    assert state.reason == "stuck_orders"


def test_reconciliation_health_flattens_on_large_qty_mismatch() -> None:
    state = assess_reconciliation_health({"SPY": 10.0}, {"SPY": 11.0})

    assert state.severity == "flatten"
    assert state.reason == "reconciliation_qty_mismatch"


def test_entry_risk_decision_blocks_on_max_positions() -> None:
    settings = Settings(MAX_POSITIONS=1, MAX_GROSS_EXPOSURE=10_000, MAX_SYMBOL_EXPOSURE=10_000)

    decision = entry_risk_decision(
        symbol="QQQ",
        qty=1,
        close=100.0,
        active_symbols={"SPY"},
        symbol_exposure=0.0,
        gross_exposure=100.0,
        reserved_gross_exposure=0.0,
        buying_power=10_000.0,
        cash=10_000.0,
        reserved_buying_power=0.0,
        reserved_cash=0.0,
        settings=settings,
    )

    assert not decision.allowed
    assert decision.reason == "max_positions"


def test_entry_risk_decision_blocks_on_symbol_exposure() -> None:
    settings = Settings(MAX_GROSS_EXPOSURE=10_000, MAX_SYMBOL_EXPOSURE=1_000, MIN_CASH_BUFFER=0.0)

    decision = entry_risk_decision(
        symbol="SPY",
        qty=3,
        close=100.0,
        active_symbols=set(),
        symbol_exposure=800.0,
        gross_exposure=800.0,
        reserved_gross_exposure=0.0,
        buying_power=10_000.0,
        cash=10_000.0,
        reserved_buying_power=0.0,
        reserved_cash=0.0,
        settings=settings,
    )

    assert not decision.allowed
    assert decision.reason == "max_symbol_exposure"


def test_entry_risk_decision_blocks_on_cash_buffer() -> None:
    settings = Settings(MAX_GROSS_EXPOSURE=10_000, MAX_SYMBOL_EXPOSURE=10_000, MIN_CASH_BUFFER=500.0)

    decision = entry_risk_decision(
        symbol="SPY",
        qty=2,
        close=400.0,
        active_symbols=set(),
        symbol_exposure=0.0,
        gross_exposure=0.0,
        reserved_gross_exposure=0.0,
        buying_power=10_000.0,
        cash=1_000.0,
        reserved_buying_power=0.0,
        reserved_cash=0.0,
        settings=settings,
    )

    assert not decision.allowed
    assert decision.reason == "min_cash_buffer"


def test_executor_chunks_large_exit_order_for_submit(tmp_path: Path) -> None:
    settings = Settings(MAX_ORDER_QTY=25, DRY_RUN=True)
    repo = _journal_repo(tmp_path)
    executor = PaperExecutor(repo, settings)

    orders = executor.split_order_for_submit(OrderIntent(symbol="SPY", qty=60, side="sell", close=100.0))

    assert [order.qty for order in orders] == [25, 25, 10]


def test_process_flatten_logs_close_position_failure(tmp_path: Path) -> None:
    repo = _journal_repo(tmp_path)
    alerts: list[str] = []

    class FailingBroker:
        def close_position(self, symbol: str) -> BrokerOrderSnapshot:
            raise BrokerClosePositionError(f"close failed for {symbol}")

    closed_positions = _process_flatten_with_close_position(
        broker=FailingBroker(),
        repo=repo,
        position_qty_by_symbol={"SPY": 5.0},
        open_order_symbols=set(),
        unresolved_order_symbols=set(),
        alert_messages=alerts,
    )

    assert closed_positions == 0
    broker_errors = repo.recent_broker_error_events(limit=5)
    assert broker_errors[0].operation == "close_position"
    assert "close failed for SPY" in broker_errors[0].message
    assert alerts == ["emergency flatten failed SPY: close failed for SPY"]


def test_stress_drill_rejects_non_burnin_profile(tmp_path: Path) -> None:
    settings = Settings(
        DATABASE_URL=f"sqlite:///{tmp_path / 'live.db'}",
        CONFIG_PROFILE="live",
        ALPACA_PAPER=False,
        PAPER_ONLY=False,
        ALLOW_LIVE=True,
    )

    with pytest.raises(RuntimeError, match="paper burn-in profile"):
        run_named_drill("stale_data_block", settings=settings, log_dir=tmp_path / "logs")


def test_stress_drill_stale_data_logs_kill_switch_and_alert(tmp_path: Path) -> None:
    settings = _paper_burnin_settings(tmp_path)

    result = run_named_drill("stale_data_block", settings=settings, log_dir=tmp_path / "logs")

    repo = JournalRepo(create_session_factory(settings.database_url))
    kill_switch = repo.recent_kill_switch_events(limit=5)[0]
    alert = repo.recent_alert_events(limit=5)[0]
    assert result.reason == "stale_data"
    assert kill_switch.reason == "stale_data"
    assert alert.message == "kill switch active: stale_data"
    assert Path(result.log_path).exists()


def test_stress_drill_broker_submit_failures_logs_threshold_and_alert(tmp_path: Path) -> None:
    settings = _paper_burnin_settings(tmp_path, MAX_BROKER_FAILURES=2)

    result = run_named_drill("broker_submit_failures", settings=settings, log_dir=tmp_path / "logs")

    repo = JournalRepo(create_session_factory(settings.database_url))
    broker_errors = repo.recent_broker_error_events(limit=10)
    kill_switch = repo.recent_kill_switch_events(limit=5)[0]
    assert result.reason == "broker_failures"
    assert len(broker_errors) == 2
    assert kill_switch.reason == "broker_failures"


def test_stress_drill_reconciliation_drift_logs_reconciliation_event(tmp_path: Path) -> None:
    settings = _paper_burnin_settings(tmp_path)

    result = run_named_drill("reconciliation_drift", settings=settings, log_dir=tmp_path / "logs")

    repo = JournalRepo(create_session_factory(settings.database_url))
    event = repo.recent_reconciliation_events(limit=5)[0]
    assert result.reason == "reconciliation_qty_drift"
    assert event.reason == "reconciliation_qty_drift"
    assert "qty_diff=0.25" in event.details


def test_stress_drill_restart_with_open_orders_creates_unresolved_order(tmp_path: Path) -> None:
    settings = _paper_burnin_settings(tmp_path)

    result = run_named_drill("restart_with_open_orders", settings=settings, log_dir=tmp_path / "logs")

    repo = JournalRepo(create_session_factory(settings.database_url))
    assert result.reason == "unresolved_order_restart_guard"
    assert repo.unresolved_order_symbols() == {"SPY"}


def test_stress_drill_emergency_flatten_logs_flatten_state(tmp_path: Path) -> None:
    settings = _paper_burnin_settings(tmp_path)

    result = run_named_drill("emergency_flatten", settings=settings, log_dir=tmp_path / "logs")

    repo = JournalRepo(create_session_factory(settings.database_url))
    kill_switch = repo.recent_kill_switch_events(limit=5)[0]
    assert result.severity == "flatten"
    assert kill_switch.reason == "config_emergency_flatten"
