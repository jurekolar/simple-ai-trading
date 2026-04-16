from datetime import UTC, datetime, timedelta

import pandas as pd

from app.broker.execution import PaperExecutor
from app.backtest.engine import run_backtest
from app.broker.alpaca_client import AlpacaTradingAdapter, BrokerOrderSnapshot, BrokerSubmitError
from app.broker.order_mapper import OrderIntent
from app.config import Settings
from app.data.historical_loader import load_bars
from app.data.market_calendar import market_day_window, market_is_open
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.main import compute_realized_pnl_records, run_paper_command
from app.risk.checks import entry_risk_decision, filter_exit_candidates, protective_exit_candidates
from app.risk.kill_switch import (
    assess_reconciliation_health,
    data_is_stale,
    evaluate_kill_switch,
    merge_kill_switch_states,
)


def test_backtest_produces_trade_candidates() -> None:
    settings = Settings()
    bars = load_bars(settings)
    trades, metrics = run_backtest(bars, settings)

    assert not trades.empty
    assert metrics["trades"] >= 1


def test_filter_exit_candidates_uses_existing_positions() -> None:
    signal_frame = pd.DataFrame(
        [
            {"symbol": "SPY", "signal": "exit", "close": 100.0},
            {"symbol": "QQQ", "signal": "exit", "close": 200.0},
            {"symbol": "IWM", "signal": "long", "close": 50.0},
        ]
    )

    exits = filter_exit_candidates(signal_frame, {"SPY": 12.0, "QQQ": 0.0})

    assert list(exits["symbol"]) == ["SPY"]
    assert int(exits.iloc[0]["qty"]) == 12


def test_filter_exit_candidates_supports_forced_exit_override() -> None:
    signal_frame = pd.DataFrame(
        [
            {"symbol": "SPY", "signal": "long", "close": 100.0},
            {"symbol": "QQQ", "signal": "flat", "close": 200.0},
            {"symbol": "IWM", "signal": "exit", "close": 50.0},
        ]
    )

    exits = filter_exit_candidates(
        signal_frame,
        {"SPY": 12.0, "QQQ": 0.0, "IWM": 5.0},
        {"spy", "qqq"},
    )

    assert list(exits["symbol"]) == ["IWM", "SPY"]
    assert list(exits["qty"]) == [5, 12]


def test_protective_exit_candidates_use_broker_position_state() -> None:
    exits = protective_exit_candidates(
        {"SPY": 12.0, "QQQ": 0.0, "IWM": 5.0},
        {"SPY": 501.25, "IWM": 201.0},
    )

    assert list(exits["symbol"]) == ["IWM", "SPY"]
    assert list(exits["qty"]) == [5, 12]
    assert list(exits["close"]) == [201.0, 501.25]


def test_realized_pnl_total_for_date_only_counts_matching_fill_day(tmp_path) -> None:
    repo = JournalRepo(create_session_factory(f"sqlite:///{tmp_path / 'journal.db'}"))
    now = datetime.now(UTC).replace(microsecond=0)
    yesterday = now - timedelta(days=1)

    repo.replace_realized_lot_matches(
        [
            {
                "symbol": "SPY",
                "open_broker_order_id": "buy-spy",
                "close_broker_order_id": "sell-spy",
                "open_client_order_id": "buy-spy",
                "close_client_order_id": "sell-spy",
                "matched_qty": 10.0,
                "open_price": 100.0,
                "close_price": 87.45,
                "fees": 0.0,
                "realized_pnl": -125.5,
                "execution_date": now.date(),
                "open_filled_at": now - timedelta(days=2),
                "close_filled_at": now,
            },
            {
                "symbol": "QQQ",
                "open_broker_order_id": "buy-qqq",
                "close_broker_order_id": "sell-qqq",
                "open_client_order_id": "buy-qqq",
                "close_client_order_id": "sell-qqq",
                "matched_qty": 5.0,
                "open_price": 100.0,
                "close_price": 105.0,
                "fees": 0.0,
                "realized_pnl": 25.0,
                "execution_date": now.date(),
                "open_filled_at": now - timedelta(days=2),
                "close_filled_at": now,
            },
            {
                "symbol": "IWM",
                "open_broker_order_id": "buy-iwm",
                "close_broker_order_id": "sell-iwm",
                "open_client_order_id": "buy-iwm",
                "close_client_order_id": "sell-iwm",
                "matched_qty": 3.0,
                "open_price": 100.0,
                "close_price": 83.3333333333,
                "fees": 0.0,
                "realized_pnl": -50.0,
                "execution_date": yesterday.date(),
                "open_filled_at": yesterday - timedelta(days=2),
                "close_filled_at": yesterday,
            },
        ]
    )

    start_at, end_at = market_day_window(now)
    assert repo.realized_pnl_total_for_window(start_at, end_at) == -100.5


def test_compute_realized_pnl_records_creates_fifo_lot_matches_with_fees() -> None:
    fills = [
        {
            "broker_order_id": "buy-1",
            "client_order_id": "buy-1",
            "symbol": "SPY",
            "side": "buy",
            "qty": 10.0,
            "price": 100.0,
            "gross_amount": 1000.0,
            "fees": 1.0,
            "net_amount": 1001.0,
            "execution_date": datetime(2026, 4, 14, 20, 0, tzinfo=UTC).date(),
            "filled_at": datetime(2026, 4, 14, 20, 0, tzinfo=UTC),
        },
        {
            "broker_order_id": "buy-2",
            "client_order_id": "buy-2",
            "symbol": "SPY",
            "side": "buy",
            "qty": 5.0,
            "price": 102.0,
            "gross_amount": 510.0,
            "fees": 0.5,
            "net_amount": 510.5,
            "execution_date": datetime(2026, 4, 15, 20, 0, tzinfo=UTC).date(),
            "filled_at": datetime(2026, 4, 15, 20, 0, tzinfo=UTC),
        },
        {
            "broker_order_id": "sell-1",
            "client_order_id": "sell-1",
            "symbol": "SPY",
            "side": "sell",
            "qty": 12.0,
            "price": 105.0,
            "gross_amount": 1260.0,
            "fees": 1.2,
            "net_amount": 1258.8,
            "execution_date": datetime(2026, 4, 16, 20, 0, tzinfo=UTC).date(),
            "filled_at": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
        },
    ]

    lot_matches, realized_records = compute_realized_pnl_records(fills)

    assert len(lot_matches) == 2
    assert [match["matched_qty"] for match in lot_matches] == [10.0, 2.0]
    assert round(sum(match["fees"] for match in lot_matches), 2) == 2.4
    assert round(sum(match["realized_pnl"] for match in lot_matches), 2) == 53.6
    assert realized_records == [
        {
            "symbol": "SPY",
            "realized_qty": 12.0,
            "realized_pnl": 53.6,
            "last_fill_at": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
        }
    ]


def test_market_is_open_uses_us_equity_hours() -> None:
    assert market_is_open(datetime(2026, 4, 16, 14, 0, tzinfo=UTC))
    assert not market_is_open(datetime(2026, 4, 16, 12, 0, tzinfo=UTC))
    assert not market_is_open(datetime(2026, 4, 18, 15, 0, tzinfo=UTC))


def test_data_is_stale_when_source_is_not_alpaca() -> None:
    bars = pd.DataFrame(
        [{"timestamp": datetime(2026, 4, 15, 20, 0, tzinfo=UTC), "symbol": "SPY"}]
    )

    assert data_is_stale(bars, source="synthetic", now=datetime(2026, 4, 16, 15, 0, tzinfo=UTC))


def test_data_is_stale_when_latest_bar_is_older_than_previous_trading_day() -> None:
    bars = pd.DataFrame(
        [{"timestamp": datetime(2026, 4, 14, 20, 0, tzinfo=UTC), "symbol": "SPY"}]
    )

    assert data_is_stale(bars, source="alpaca", now=datetime(2026, 4, 16, 15, 0, tzinfo=UTC))


def test_kill_switch_blocks_on_stale_data() -> None:
    state = evaluate_kill_switch(
        True,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
    )

    assert state.enabled
    assert state.severity == "reduce_only"
    assert state.block_new_entries
    assert state.allow_exits
    assert not state.force_flatten
    assert state.reason == "stale_data"


def test_kill_switch_blocks_entries_on_soft_unrealized_drawdown() -> None:
    state = evaluate_kill_switch(
        False,
        realized_pnl=0.0,
        unrealized_pnl=-1_600.0,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
    )

    assert state.enabled
    assert state.severity == "reduce_only"
    assert state.block_new_entries
    assert state.allow_exits
    assert not state.force_flatten
    assert state.reason == "max_unrealized_drawdown"


def test_kill_switch_forces_flatten_on_emergency_unrealized_drawdown() -> None:
    state = evaluate_kill_switch(
        False,
        realized_pnl=0.0,
        unrealized_pnl=-2_600.0,
        max_daily_loss=1_000.0,
        max_unrealized_drawdown=1_500.0,
        emergency_unrealized_drawdown=2_500.0,
    )

    assert state.enabled
    assert state.severity == "flatten"
    assert state.block_new_entries
    assert state.allow_exits
    assert state.force_flatten
    assert state.reason == "emergency_unrealized_drawdown"


def test_reconciliation_health_reduce_only_on_small_qty_drift() -> None:
    state = assess_reconciliation_health({"SPY": 10.0}, {"SPY": 10.25})

    assert state.enabled
    assert state.severity == "reduce_only"
    assert state.reason == "reconciliation_qty_drift"


def test_reconciliation_health_flattens_on_symbol_mismatch() -> None:
    state = assess_reconciliation_health({"SPY": 10.0}, {"QQQ": 10.0})

    assert state.enabled
    assert state.severity == "flatten"
    assert state.reason == "reconciliation_symbol_mismatch"


def test_merge_kill_switch_states_prefers_more_severe_state() -> None:
    state = merge_kill_switch_states(
        evaluate_kill_switch(
            False,
            realized_pnl=0.0,
            unrealized_pnl=-1_600.0,
            max_daily_loss=1_000.0,
            max_unrealized_drawdown=1_500.0,
            emergency_unrealized_drawdown=2_500.0,
        ),
        assess_reconciliation_health({"SPY": 10.0}, {"QQQ": 10.0}),
    )

    assert state.severity == "flatten"
    assert state.reason == "reconciliation_symbol_mismatch"


def test_repo_persists_reconciliation_events(tmp_path) -> None:
    repo = JournalRepo(create_session_factory(f"sqlite:///{tmp_path / 'journal.db'}"))

    repo.log_reconciliation_event(
        severity="reduce_only",
        reason="reconciliation_qty_drift",
        details="local_symbols=['SPY'] broker_symbols=['SPY']",
    )

    events = repo.recent_reconciliation_events(limit=5)

    assert len(events) == 1
    assert events[0].severity == "reduce_only"
    assert events[0].reason == "reconciliation_qty_drift"
    assert "local_symbols=['SPY']" in events[0].details


def test_entry_risk_decision_blocks_on_buying_power() -> None:
    settings = Settings(MAX_GROSS_EXPOSURE=50_000, MAX_SYMBOL_EXPOSURE=20_000, MIN_CASH_BUFFER=0.0)

    decision = entry_risk_decision(
        symbol="SPY",
        qty=5,
        close=100.0,
        active_symbols=set(),
        symbol_exposure=0.0,
        gross_exposure=0.0,
        buying_power=400.0,
        cash=10_000.0,
        settings=settings,
    )

    assert not decision.allowed
    assert decision.reason == "insufficient_buying_power"


def test_entry_risk_decision_blocks_on_gross_exposure() -> None:
    settings = Settings(MAX_GROSS_EXPOSURE=1_000, MAX_SYMBOL_EXPOSURE=20_000, MIN_CASH_BUFFER=0.0)

    decision = entry_risk_decision(
        symbol="SPY",
        qty=3,
        close=200.0,
        active_symbols=set(),
        symbol_exposure=0.0,
        gross_exposure=500.0,
        buying_power=10_000.0,
        cash=10_000.0,
        settings=settings,
    )

    assert not decision.allowed
    assert decision.reason == "max_gross_exposure"


def test_list_execution_fills_uses_trade_activities_and_allocates_fees() -> None:
    adapter = object.__new__(AlpacaTradingAdapter)
    adapter._settings = Settings()
    adapter._client = object()

    adapter.list_recent_orders = lambda limit=200: [
        BrokerOrderSnapshot(
            id="order-1",
            client_order_id="client-1",
            symbol="SPY",
            side="buy",
            qty="10",
            status="filled",
            filled_avg_price="100",
            filled_qty="10",
            submitted_at=datetime(2026, 4, 16, 14, 0, tzinfo=UTC),
            filled_at=datetime(2026, 4, 16, 14, 1, tzinfo=UTC),
        ),
        BrokerOrderSnapshot(
            id="order-2",
            client_order_id="client-2",
            symbol="SPY",
            side="sell",
            qty="10",
            status="filled",
            filled_avg_price="105",
            filled_qty="10",
            submitted_at=datetime(2026, 4, 16, 15, 0, tzinfo=UTC),
            filled_at=datetime(2026, 4, 16, 15, 1, tzinfo=UTC),
        ),
    ]
    adapter.list_trade_activities = lambda days=30: [
        type(
            "TradeActivitySnapshotStub",
            (),
            {
                "activity_id": "activity-1",
                "order_id": "order-1",
                "symbol": "SPY",
                "side": "buy",
                "qty": 10.0,
                "price": 100.0,
                "transaction_time": datetime(2026, 4, 16, 14, 1, tzinfo=UTC),
            },
        )(),
        type(
            "TradeActivitySnapshotStub",
            (),
            {
                "activity_id": "activity-2",
                "order_id": "order-2",
                "symbol": "SPY",
                "side": "sell",
                "qty": 10.0,
                "price": 105.0,
                "transaction_time": datetime(2026, 4, 16, 15, 1, tzinfo=UTC),
            },
        )(),
    ]
    adapter.list_fee_activities = lambda days=30: [
        type(
            "FeeActivitySnapshotStub",
            (),
            {
                "activity_id": "fee-1",
                "activity_type": "FEE",
                "net_amount": -3.0,
                "activity_date": datetime(2026, 4, 16, 0, 0, tzinfo=UTC).date(),
                "symbol": "SPY",
            },
        )()
    ]

    fills = adapter.list_execution_fills(limit=200)

    assert len(fills) == 2
    assert [fill["client_order_id"] for fill in fills] == ["client-1", "client-2"]
    assert round(sum(float(fill["fees"]) for fill in fills), 2) == 3.0
    assert fills[0]["fees"] < fills[1]["fees"]


def test_executor_allows_large_exit_orders_above_entry_qty_limit(tmp_path) -> None:
    settings = Settings(MAX_ORDER_QTY=25, DRY_RUN=True)
    repo = JournalRepo(create_session_factory(f"sqlite:///{tmp_path / 'journal.db'}"))
    executor = PaperExecutor(repo, settings)

    result = executor.submit(OrderIntent(symbol="SPY", qty=40, side="sell", close=100.0))

    assert result.status == "dry_run"
    orders = repo.recent_orders(limit=1)
    assert len(orders) == 1
    assert orders[0].status == "dry_run"
    assert orders[0].qty == 40.0


def test_executor_returns_error_result_when_broker_submit_fails(tmp_path) -> None:
    settings = Settings(DRY_RUN=False)
    repo = JournalRepo(create_session_factory(f"sqlite:///{tmp_path / 'journal.db'}"))
    broker = object.__new__(AlpacaTradingAdapter)
    broker._settings = settings
    broker._client = object()

    def raise_submit_error(**_: object) -> BrokerOrderSnapshot:
        raise BrokerSubmitError("broker rejected order")

    broker.submit_market_order = raise_submit_error
    executor = PaperExecutor(repo, settings, broker=broker)

    result = executor.submit(OrderIntent(symbol="SPY", qty=5, side="buy", close=100.0))

    assert result.status == "error"
    assert result.status_detail == "broker rejected order"
    orders = repo.recent_orders(limit=1)
    assert len(orders) == 1
    assert orders[0].status == "error"
    assert orders[0].status_detail == "broker rejected order"


def test_run_paper_command_skips_entry_when_symbol_has_open_order(tmp_path, monkeypatch) -> None:
    settings = Settings(DRY_RUN=True, DATABASE_URL=f"sqlite:///{tmp_path / 'journal.db'}")
    bars = pd.DataFrame(
        [
            {
                "timestamp": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
                "symbol": "SPY",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1_000_000,
            }
        ]
    )
    latest = pd.DataFrame(
        [
            {
                "timestamp": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
                "symbol": "SPY",
                "signal": "long",
                "close": 100.0,
                "atr": 1.0,
            }
        ]
    )

    class FakeBroker:
        def __init__(self, _: Settings) -> None:
            pass

        def get_account_summary(self) -> object:
            return type(
                "AccountSnapshotStub",
                (),
                {"status": "ACTIVE", "buying_power": "10000", "equity": "10000", "cash": "10000"},
            )()

        def list_open_orders(self, limit: int = 50) -> list[BrokerOrderSnapshot]:
            return [
                BrokerOrderSnapshot(
                    id="order-1",
                    client_order_id="client-1",
                    symbol="SPY",
                    side="buy",
                    qty="5",
                    status="new",
                    filled_avg_price="",
                    filled_qty="0",
                    submitted_at=datetime(2026, 4, 16, 19, 55, tzinfo=UTC),
                    filled_at=None,
                )
            ]

        def list_positions(self) -> list[object]:
            return []

    class FailIfCalledExecutor:
        def __init__(self, repo: JournalRepo, settings: Settings, broker: FakeBroker | None = None) -> None:
            self.repo = repo
            self.settings = settings
            self.broker = broker

        def submit(self, order: OrderIntent) -> object:
            raise AssertionError(f"submit should not be called for duplicate symbol {order.symbol}")

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr(
        "app.main.load_bars_with_source",
        lambda _: type("LoadedBarsStub", (), {"bars": bars, "source": "alpaca"})(),
    )
    monkeypatch.setattr("app.main.reconcile_broker_state", lambda repo, broker: {"recent_orders": 0, "open_orders": 0, "positions": 0, "pnl_points": 0, "fills": 0, "lot_matches": 0, "realized_symbols": 0})
    monkeypatch.setattr(
        "app.main.market_day_window",
        lambda: (
            datetime(2026, 4, 16, 0, 0, tzinfo=UTC),
            datetime(2026, 4, 17, 0, 0, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr("app.main.generate_signals", lambda bars, settings: latest)
    monkeypatch.setattr("app.main.latest_signals", lambda frame: frame)
    monkeypatch.setattr("app.main.run_backtest", lambda bars, settings: (pd.DataFrame(), {"trades": 0.0}))
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report=ok")
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", FakeBroker)
    monkeypatch.setattr("app.main.PaperExecutor", FailIfCalledExecutor)

    run_paper_command()

    repo = JournalRepo(create_session_factory(settings.database_url))
    orders = repo.recent_orders(limit=5)
    assert any(order.status == "skipped_existing" and order.symbol == "SPY" for order in orders)


def test_run_paper_command_emergency_drawdown_forces_exit(tmp_path, monkeypatch) -> None:
    settings = Settings(
        DRY_RUN=True,
        DATABASE_URL=f"sqlite:///{tmp_path / 'journal.db'}",
        MAX_UNREALIZED_DRAWDOWN=1_500.0,
        EMERGENCY_UNREALIZED_DRAWDOWN=2_500.0,
    )
    bars = pd.DataFrame(
        [
            {
                "timestamp": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
                "symbol": "SPY",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1_000_000,
            }
        ]
    )
    latest = pd.DataFrame(
        [
            {
                "timestamp": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
                "symbol": "SPY",
                "signal": "flat",
                "close": 100.0,
                "atr": 1.0,
            }
        ]
    )

    position = type(
        "PositionSnapshotStub",
        (),
        {
            "symbol": "SPY",
            "qty": "10",
            "market_value": "1000",
            "avg_entry_price": "120",
            "current_price": "100",
            "cost_basis": "1200",
            "unrealized_pl": "-2600",
            "unrealized_plpc": "-0.2",
        },
    )()
    account = type(
        "AccountSnapshotStub",
        (),
        {"status": "ACTIVE", "buying_power": "10000", "equity": "10000", "cash": "8000"},
    )()

    class FakeBroker:
        def __init__(self, _: Settings) -> None:
            pass

        def get_account_summary(self) -> object:
            return account

        def list_open_orders(self, limit: int = 50) -> list[BrokerOrderSnapshot]:
            return []

        def list_positions(self) -> list[object]:
            return [position]

    submitted_orders: list[OrderIntent] = []

    class RecordingExecutor:
        def __init__(self, repo: JournalRepo, settings: Settings, broker: FakeBroker | None = None) -> None:
            self.repo = repo

        def submit(self, order: OrderIntent) -> object:
            submitted_orders.append(order)
            return type(
                "ExecutionResultStub",
                (),
                {
                    "accepted": True,
                    "status": "dry_run",
                    "status_detail": "",
                    "client_order_id": "client-1",
                    "broker_order_id": "",
                    "filled_avg_price": 0.0,
                },
            )()

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr(
        "app.main.load_bars_with_source",
        lambda _: type("LoadedBarsStub", (), {"bars": bars, "source": "alpaca"})(),
    )
    monkeypatch.setattr(
        "app.main.reconcile_broker_state",
        lambda repo, broker: {
            "recent_orders": 0,
            "open_orders": 0,
            "positions": 1,
            "pnl_points": 0,
            "fills": 0,
            "lot_matches": 0,
            "realized_symbols": 0,
        },
    )
    monkeypatch.setattr(
        "app.main.market_day_window",
        lambda: (
            datetime(2026, 4, 16, 0, 0, tzinfo=UTC),
            datetime(2026, 4, 17, 0, 0, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr("app.main.generate_signals", lambda bars, settings: latest)
    monkeypatch.setattr("app.main.latest_signals", lambda frame: frame)
    monkeypatch.setattr("app.main.run_backtest", lambda bars, settings: (pd.DataFrame(), {"trades": 0.0}))
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report=ok")
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", FakeBroker)
    monkeypatch.setattr("app.main.PaperExecutor", RecordingExecutor)

    run_paper_command()

    assert len(submitted_orders) == 1
    assert submitted_orders[0].symbol == "SPY"
    assert submitted_orders[0].side == "sell"


def test_run_paper_command_stale_data_still_allows_exit(tmp_path, monkeypatch) -> None:
    settings = Settings(DRY_RUN=True, DATABASE_URL=f"sqlite:///{tmp_path / 'journal.db'}")
    bars = pd.DataFrame(
        [
            {
                "timestamp": datetime(2026, 4, 16, 20, 0, tzinfo=UTC),
                "symbol": "SPY",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1_000_000,
            }
        ]
    )
    position = type(
        "PositionSnapshotStub",
        (),
        {
            "symbol": "SPY",
            "qty": "10",
            "market_value": "1000",
            "avg_entry_price": "120",
            "current_price": "98",
            "cost_basis": "1200",
            "unrealized_pl": "-200",
            "unrealized_plpc": "-0.02",
        },
    )()
    account = type(
        "AccountSnapshotStub",
        (),
        {"status": "ACTIVE", "buying_power": "10000", "equity": "10000", "cash": "9000"},
    )()

    class FakeBroker:
        def __init__(self, _: Settings) -> None:
            pass

        def get_account_summary(self) -> object:
            return account

        def list_open_orders(self, limit: int = 50) -> list[BrokerOrderSnapshot]:
            return []

        def list_positions(self) -> list[object]:
            return [position]

    submitted_orders: list[OrderIntent] = []

    class RecordingExecutor:
        def __init__(self, repo: JournalRepo, settings: Settings, broker: FakeBroker | None = None) -> None:
            self.repo = repo

        def submit(self, order: OrderIntent) -> object:
            submitted_orders.append(order)
            return type(
                "ExecutionResultStub",
                (),
                {
                    "accepted": True,
                    "status": "dry_run",
                    "status_detail": "",
                    "client_order_id": "client-1",
                    "broker_order_id": "",
                    "filled_avg_price": 0.0,
                },
            )()

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr(
        "app.main.load_bars_with_source",
        lambda _: type("LoadedBarsStub", (), {"bars": bars, "source": "synthetic"})(),
    )
    monkeypatch.setattr(
        "app.main.reconcile_broker_state",
        lambda repo, broker: {
            "recent_orders": 0,
            "open_orders": 0,
            "positions": 1,
            "pnl_points": 0,
            "fills": 0,
            "lot_matches": 0,
            "realized_symbols": 0,
        },
    )
    monkeypatch.setattr(
        "app.main.market_day_window",
        lambda: (
            datetime(2026, 4, 16, 0, 0, tzinfo=UTC),
            datetime(2026, 4, 17, 0, 0, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr(
        "app.main.generate_signals",
        lambda bars, settings: (_ for _ in ()).throw(AssertionError("stale data should not use strategy exits")),
    )
    monkeypatch.setattr(
        "app.main.latest_signals",
        lambda frame: (_ for _ in ()).throw(AssertionError("stale data should not use latest strategy signals")),
    )
    monkeypatch.setattr("app.main.run_backtest", lambda bars, settings: (pd.DataFrame(), {"trades": 0.0}))
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report=ok")
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", FakeBroker)
    monkeypatch.setattr("app.main.PaperExecutor", RecordingExecutor)

    run_paper_command()

    assert len(submitted_orders) == 1
    assert submitted_orders[0].symbol == "SPY"
    assert submitted_orders[0].side == "sell"
    assert submitted_orders[0].close == 98.0


def test_run_paper_command_daily_loss_limit_still_allows_exit(tmp_path, monkeypatch) -> None:
    settings = Settings(
        DRY_RUN=True,
        DATABASE_URL=f"sqlite:///{tmp_path / 'journal.db'}",
        MAX_DAILY_LOSS=1_000.0,
    )
    repo = JournalRepo(create_session_factory(settings.database_url))
    now = datetime(2026, 4, 16, 20, 0, tzinfo=UTC)
    repo.replace_realized_lot_matches(
        [
            {
                "symbol": "SPY",
                "open_broker_order_id": "buy-spy",
                "close_broker_order_id": "sell-spy",
                "open_client_order_id": "buy-spy",
                "close_client_order_id": "sell-spy",
                "matched_qty": 10.0,
                "open_price": 100.0,
                "close_price": 90.0,
                "fees": 0.0,
                "realized_pnl": -1_100.0,
                "execution_date": now.date(),
                "open_filled_at": now - timedelta(days=1),
                "close_filled_at": now,
            }
        ]
    )
    bars = pd.DataFrame(
        [
            {
                "timestamp": now,
                "symbol": "SPY",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1_000_000,
            }
        ]
    )
    latest = pd.DataFrame(
        [
            {
                "timestamp": now,
                "symbol": "SPY",
                "signal": "exit",
                "close": 100.0,
                "atr": 1.0,
            }
        ]
    )
    position = type(
        "PositionSnapshotStub",
        (),
        {
            "symbol": "SPY",
            "qty": "10",
            "market_value": "1000",
            "avg_entry_price": "120",
            "current_price": "100",
            "cost_basis": "1200",
            "unrealized_pl": "-200",
            "unrealized_plpc": "-0.02",
        },
    )()
    account = type(
        "AccountSnapshotStub",
        (),
        {"status": "ACTIVE", "buying_power": "10000", "equity": "9000", "cash": "9000"},
    )()

    class FakeBroker:
        def __init__(self, _: Settings) -> None:
            pass

        def get_account_summary(self) -> object:
            return account

        def list_open_orders(self, limit: int = 50) -> list[BrokerOrderSnapshot]:
            return []

        def list_positions(self) -> list[object]:
            return [position]

    submitted_orders: list[OrderIntent] = []

    class RecordingExecutor:
        def __init__(self, repo: JournalRepo, settings: Settings, broker: FakeBroker | None = None) -> None:
            self.repo = repo

        def submit(self, order: OrderIntent) -> object:
            submitted_orders.append(order)
            return type(
                "ExecutionResultStub",
                (),
                {
                    "accepted": True,
                    "status": "dry_run",
                    "status_detail": "",
                    "client_order_id": "client-1",
                    "broker_order_id": "",
                    "filled_avg_price": 0.0,
                },
            )()

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr(
        "app.main.load_bars_with_source",
        lambda _: type("LoadedBarsStub", (), {"bars": bars, "source": "alpaca"})(),
    )
    monkeypatch.setattr(
        "app.main.reconcile_broker_state",
        lambda repo, broker: {
            "recent_orders": 0,
            "open_orders": 0,
            "positions": 1,
            "pnl_points": 0,
            "fills": 0,
            "lot_matches": 1,
            "realized_symbols": 1,
        },
    )
    monkeypatch.setattr("app.main.market_day_window", lambda: (now.replace(hour=0, minute=0), now.replace(day=17, hour=0, minute=0)))
    monkeypatch.setattr("app.main.generate_signals", lambda bars, settings: latest)
    monkeypatch.setattr("app.main.latest_signals", lambda frame: frame)
    monkeypatch.setattr("app.main.run_backtest", lambda bars, settings: (pd.DataFrame(), {"trades": 0.0}))
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report=ok")
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", FakeBroker)
    monkeypatch.setattr("app.main.PaperExecutor", RecordingExecutor)

    run_paper_command()

    assert len(submitted_orders) == 1
    assert submitted_orders[0].symbol == "SPY"
    assert submitted_orders[0].side == "sell"
