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
from app.risk.checks import filter_exit_candidates
from app.risk.kill_switch import data_is_stale, evaluate_kill_switch


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
    state = evaluate_kill_switch(True, realized_pnl=0.0, max_daily_loss=1_000.0)

    assert state.enabled
    assert state.reason == "stale_data"


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
