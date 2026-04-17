from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd

from app.config import Settings
from app.data.alpaca_data import DataLoadResult
from app.data.capitol_trades import CapitolTradeDisclosure, CapitolTradesClient, PoliticianCandidate
from app.db.models import create_session_factory
from app.db.repo import JournalRepo
from app.main import build_parser, run_paper_command, run_preview_command, run_replay_command
from app.reports.daily_report import build_daily_report
from app.strategy import get_strategy, strategy_names
from app.strategy.politician_copy import (
    AllocationOrder,
    AllocationPlan,
    PoliticianCopyStrategy,
    PoliticianScore,
    TargetAllocation,
)
from app.strategy.politician_copy_replay import (
    build_point_in_time_allocation_plan,
    format_politician_copy_replay_summary,
    run_politician_copy_replay,
)


def test_strategy_registry_lists_politician_copy() -> None:
    assert "politician_copy" in strategy_names()
    assert get_strategy("politician_copy").name == "politician_copy"


def test_parser_accepts_preview_command_for_politician_copy() -> None:
    args = build_parser().parse_args(["--strategy", "politician_copy", "preview"])

    assert args.strategy == "politician_copy"
    assert args.command == "preview"


def test_parser_accepts_replay_command_for_politician_copy() -> None:
    args = build_parser().parse_args(["--strategy", "politician_copy", "replay"])

    assert args.strategy == "politician_copy"
    assert args.command == "replay"


def test_capitol_trades_parser_extracts_candidates_and_disclosures() -> None:
    settings = Settings()
    client = CapitolTradesClient(settings)
    list_html = """
    <html><body>
      <a href="/politicians/M001190">Markwayne Mullin</a>
      <a href="/politicians/G000583">Josh Gottheimer</a>
    </body></html>
    """
    profile_html = """
    <html><body>
      <h3>Traded Issuer</h3>
      <div>Unitedhealth Group Inc</div>
      <div>UNH:US</div>
      <div>10 Mar</div>
      <div>2026</div>
      <div>25 Feb</div>
      <div>2026</div>
      <div>days</div>
      <div>13</div>
      <div>buy</div>
      <div>50K–100K</div>
      <a href="/trades/1">Goto trade detail page.</a>
      <div>VSE CORP</div>
      <div>N/A</div>
      <div>2 Mar</div>
      <div>2026</div>
      <div>4 Feb</div>
      <div>2026</div>
      <div>days</div>
      <div>26</div>
      <div>buy</div>
      <div>15K–50K</div>
      <a href="/trades/2">Goto trade detail page.</a>
    </body></html>
    """

    candidates = client._parse_politician_candidates(list_html)
    disclosures = client._parse_profile_page(
        profile_html,
        politician_id="M001190",
        politician_name="Markwayne Mullin",
        page_url="https://www.capitoltrades.com/politicians/M001190",
    )

    assert [candidate.politician_name for candidate in candidates] == [
        "Markwayne Mullin",
        "Josh Gottheimer",
    ]
    assert len(disclosures) == 1
    assert disclosures[0].symbol == "UNH"
    assert disclosures[0].amount_midpoint == 75_000
    assert disclosures[0].source_url.endswith("/trades/1")


class _FakeTradesClient:
    def __init__(self, disclosures_by_politician: dict[str, list[CapitolTradeDisclosure]]) -> None:
        self._disclosures_by_politician = disclosures_by_politician

    def fetch_politician_candidates(self) -> list[PoliticianCandidate]:
        return [
            PoliticianCandidate(
                politician_id=politician_id,
                politician_name=disclosures[0].politician_name,
                profile_url=f"https://www.capitoltrades.com/politicians/{politician_id}",
            )
            for politician_id, disclosures in self._disclosures_by_politician.items()
        ]

    def fetch_recent_disclosures(self, candidate: PoliticianCandidate) -> list[CapitolTradeDisclosure]:
        return self._disclosures_by_politician[candidate.politician_id]


class _FakeDataClient:
    def __init__(self, bars: pd.DataFrame, source: str = "alpaca", production_safe: bool = True) -> None:
        self._bars = bars
        self._source = source
        self._production_safe = production_safe

    def get_daily_bars(self, symbols: list[str], lookback_days: int) -> DataLoadResult:
        return DataLoadResult(
            bars=self._bars[self._bars["symbol"].isin(symbols)].copy(),
            source=self._source,
            production_safe=self._production_safe,
        )


def test_politician_copy_builds_ranked_targets() -> None:
    strategy = PoliticianCopyStrategy()
    settings = Settings(
        POLITICIAN_COPY_NUM_POLITICIANS=1,
        POLITICIAN_COPY_MIN_DISCLOSURES_PER_POLITICIAN=1,
        POLITICIAN_COPY_MIN_TARGET_WEIGHT=0.0,
        POLITICIAN_COPY_MAX_SYMBOL_WEIGHT=1.0,
        MIN_AVERAGE_DAILY_VOLUME=100,
    )
    disclosures_by_politician = {
        "M001190": [
            CapitolTradeDisclosure(
                politician_id="M001190",
                politician_name="Markwayne Mullin",
                trade_date=datetime(2026, 2, 25, tzinfo=UTC),
                published_at=datetime(2026, 3, 10, tzinfo=UTC),
                symbol="UNH",
                asset_type="us_equity",
                side="buy",
                amount_bucket="50K–100K",
                amount_midpoint=75_000,
                source_url="https://www.capitoltrades.com/trades/1",
                filing_delay_days=13,
            ),
            CapitolTradeDisclosure(
                politician_id="M001190",
                politician_name="Markwayne Mullin",
                trade_date=datetime(2026, 2, 4, tzinfo=UTC),
                published_at=datetime(2026, 3, 2, tzinfo=UTC),
                symbol="MPWR",
                asset_type="us_equity",
                side="buy",
                amount_bucket="15K–50K",
                amount_midpoint=32_500,
                source_url="https://www.capitoltrades.com/trades/2",
                filing_delay_days=26,
            ),
        ],
        "G000583": [
            CapitolTradeDisclosure(
                politician_id="G000583",
                politician_name="Josh Gottheimer",
                trade_date=datetime(2026, 2, 25, tzinfo=UTC),
                published_at=datetime(2026, 3, 10, tzinfo=UTC),
                symbol="T",
                asset_type="us_equity",
                side="buy",
                amount_bucket="50K–100K",
                amount_midpoint=75_000,
                source_url="https://www.capitoltrades.com/trades/3",
                filing_delay_days=13,
            )
        ],
    }
    bars = pd.DataFrame(
        [
            {"timestamp": datetime(2026, 3, 10, tzinfo=UTC), "symbol": "UNH", "close": 100.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 4, 17, tzinfo=UTC), "symbol": "UNH", "close": 120.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 3, 2, tzinfo=UTC), "symbol": "MPWR", "close": 200.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 4, 17, tzinfo=UTC), "symbol": "MPWR", "close": 230.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 3, 10, tzinfo=UTC), "symbol": "T", "close": 30.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 4, 17, tzinfo=UTC), "symbol": "T", "close": 27.0, "volume": 1_000_000},
        ]
    )

    plan = strategy.build_allocation_plan(
        settings=settings,
        account_equity=100_000,
        position_qty_by_symbol={},
        now=datetime(2026, 4, 17, tzinfo=UTC),
        trades_client=_FakeTradesClient(disclosures_by_politician),
        data_client=_FakeDataClient(bars),
    )

    assert plan.selected_politicians[0].politician_name == "Markwayne Mullin"
    assert {target.symbol for target in plan.target_allocations} == {"UNH", "MPWR"}
    assert round(sum(target.target_weight for target in plan.target_allocations), 6) == 1.0
    assert any(order.side == "buy" and order.symbol == "UNH" for order in plan.planned_orders)


def test_politician_copy_filter_reasons_are_deterministic() -> None:
    strategy = PoliticianCopyStrategy()
    settings = Settings(
        POLITICIAN_COPY_SYMBOL_ALLOWLIST="NVDA",
        POLITICIAN_COPY_SYMBOL_BLOCKLIST="AAPL",
        POLITICIAN_COPY_MAX_DISCLOSURE_LAG_DAYS=30,
    )
    disclosures = [
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="NVDA",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/0",
            filing_delay_days=10,
        ),
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="GLD",
            asset_type="etf",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/1",
            filing_delay_days=1,
        ),
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2025, 1, 1, tzinfo=UTC),
            published_at=datetime(2025, 1, 2, tzinfo=UTC),
            symbol="MSFT",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/2",
            filing_delay_days=1,
        ),
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="MSFT",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/3",
            filing_delay_days=45,
        ),
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="AAPL",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/4",
            filing_delay_days=10,
        ),
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="MSFT",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/5",
            filing_delay_days=10,
        ),
    ]

    accepted, rejected = strategy._filter_disclosures(disclosures, settings=settings, as_of=datetime(2026, 4, 17, tzinfo=UTC))

    assert [disclosure.symbol for disclosure in accepted] == ["NVDA"]
    assert Counter(rejection.reason for rejection in rejected) == Counter(
        {
            "unsupported_asset_type": 1,
            "outside_lookback": 1,
            "excessive_filing_delay": 1,
            "symbol_not_allowlisted": 2,
        }
    )


def test_politician_copy_point_in_time_plan_uses_published_at_without_lookahead() -> None:
    settings = Settings(
        POLITICIAN_COPY_NUM_POLITICIANS=1,
        POLITICIAN_COPY_MIN_DISCLOSURES_PER_POLITICIAN=1,
        POLITICIAN_COPY_MIN_TARGET_WEIGHT=0.0,
        POLITICIAN_COPY_MAX_SYMBOL_WEIGHT=1.0,
        MIN_AVERAGE_DAILY_VOLUME=100,
    )
    disclosures = [
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 2, tzinfo=UTC),
            published_at=datetime(2026, 1, 20, tzinfo=UTC),
            symbol="NVDA",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/1",
            filing_delay_days=18,
        )
    ]
    bars = pd.DataFrame(
        [
            {"timestamp": datetime(2026, 1, 10, tzinfo=UTC), "symbol": "NVDA", "close": 100.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 21, tzinfo=UTC), "symbol": "NVDA", "close": 105.0, "volume": 1_000_000},
        ]
    )

    before_publication = build_point_in_time_allocation_plan(
        disclosures=disclosures,
        price_frame=bars,
        settings=settings,
        account_equity=100_000.0,
        position_qty_by_symbol={},
        as_of=datetime(2026, 1, 15, tzinfo=UTC),
    )
    after_publication = build_point_in_time_allocation_plan(
        disclosures=disclosures,
        price_frame=bars,
        settings=settings,
        account_equity=100_000.0,
        position_qty_by_symbol={},
        as_of=datetime(2026, 1, 21, tzinfo=UTC),
    )

    assert before_publication.target_allocations == ()
    assert {target.symbol for target in after_publication.target_allocations} == {"NVDA"}


def test_politician_copy_plan_generates_sell_when_holding_drops_out_of_window() -> None:
    settings = Settings(
        POLITICIAN_COPY_NUM_POLITICIANS=1,
        POLITICIAN_COPY_MIN_DISCLOSURES_PER_POLITICIAN=1,
        POLITICIAN_COPY_MIN_TARGET_WEIGHT=0.0,
        POLITICIAN_COPY_MAX_SYMBOL_WEIGHT=1.0,
        POLITICIAN_COPY_HOLDING_WINDOW_DAYS=10,
        MIN_AVERAGE_DAILY_VOLUME=100,
    )
    disclosures = [
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="NVDA",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/1",
            filing_delay_days=1,
        )
    ]
    bars = pd.DataFrame(
        [
            {"timestamp": datetime(2026, 1, 2, tzinfo=UTC), "symbol": "NVDA", "close": 100.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 20, tzinfo=UTC), "symbol": "NVDA", "close": 110.0, "volume": 1_000_000},
        ]
    )

    plan = build_point_in_time_allocation_plan(
        disclosures=disclosures,
        price_frame=bars,
        settings=settings,
        account_equity=100_000.0,
        position_qty_by_symbol={"NVDA": 5.0},
        as_of=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert any(order.side == "sell" and order.symbol == "NVDA" for order in plan.planned_orders)


def test_politician_copy_plan_no_order_when_delta_below_one_share() -> None:
    settings = Settings(
        POLITICIAN_COPY_NUM_POLITICIANS=1,
        POLITICIAN_COPY_MIN_DISCLOSURES_PER_POLITICIAN=1,
        POLITICIAN_COPY_MIN_TARGET_WEIGHT=0.0,
        POLITICIAN_COPY_MAX_SYMBOL_WEIGHT=0.001,
        MIN_AVERAGE_DAILY_VOLUME=100,
    )
    disclosures = [
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 2, tzinfo=UTC),
            symbol="NVDA",
            asset_type="us_equity",
            side="buy",
            amount_bucket="15K–50K",
            amount_midpoint=32_500,
            source_url="https://x/1",
            filing_delay_days=1,
        )
    ]
    bars = pd.DataFrame(
        [
            {"timestamp": datetime(2026, 1, 2, tzinfo=UTC), "symbol": "NVDA", "close": 500.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 10, tzinfo=UTC), "symbol": "NVDA", "close": 500.0, "volume": 1_000_000},
        ]
    )

    plan = build_point_in_time_allocation_plan(
        disclosures=disclosures,
        price_frame=bars,
        settings=settings,
        account_equity=100_000.0,
        position_qty_by_symbol={},
        as_of=datetime(2026, 1, 10, tzinfo=UTC),
    )

    assert plan.target_allocations
    assert plan.planned_orders == ()


def test_politician_copy_replay_uses_next_available_price_and_no_lookahead() -> None:
    settings = Settings(
        POLITICIAN_COPY_NUM_POLITICIANS=1,
        POLITICIAN_COPY_MIN_DISCLOSURES_PER_POLITICIAN=1,
        POLITICIAN_COPY_MIN_TARGET_WEIGHT=0.0,
        POLITICIAN_COPY_MAX_SYMBOL_WEIGHT=1.0,
        POLITICIAN_COPY_REBALANCE_FREQUENCY="W-FRI",
        POLITICIAN_COPY_REPLAY_INITIAL_EQUITY=100_000.0,
        POLITICIAN_COPY_REPLAY_SLIPPAGE_BPS=0.0,
        MIN_AVERAGE_DAILY_VOLUME=100,
    )
    disclosures = [
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 1, tzinfo=UTC),
            published_at=datetime(2026, 1, 7, tzinfo=UTC),
            symbol="NVDA",
            asset_type="us_equity",
            side="buy",
            amount_bucket="50K–100K",
            amount_midpoint=75_000,
            source_url="https://x/1",
            filing_delay_days=6,
        ),
        CapitolTradeDisclosure(
            politician_id="P1",
            politician_name="Pol One",
            trade_date=datetime(2026, 1, 15, tzinfo=UTC),
            published_at=datetime(2026, 1, 21, tzinfo=UTC),
            symbol="MSFT",
            asset_type="us_equity",
            side="buy",
            amount_bucket="50K–100K",
            amount_midpoint=75_000,
            source_url="https://x/2",
            filing_delay_days=6,
        ),
    ]
    bars = pd.DataFrame(
        [
            {"timestamp": datetime(2026, 1, 8, tzinfo=UTC), "symbol": "NVDA", "close": 100.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 9, tzinfo=UTC), "symbol": "NVDA", "close": 101.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 22, tzinfo=UTC), "symbol": "NVDA", "close": 110.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 23, tzinfo=UTC), "symbol": "NVDA", "close": 112.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 22, tzinfo=UTC), "symbol": "MSFT", "close": 200.0, "volume": 1_000_000},
            {"timestamp": datetime(2026, 1, 23, tzinfo=UTC), "symbol": "MSFT", "close": 210.0, "volume": 1_000_000},
        ]
    )

    result = run_politician_copy_replay(
        disclosures=disclosures,
        price_frame=bars,
        settings=settings,
        rebalance_dates=[datetime(2026, 1, 9, tzinfo=UTC), datetime(2026, 1, 22, tzinfo=UTC)],
        source="alpaca",
        production_safe=True,
    )

    assert result.summary["replay_valid"] is True
    assert not result.trade_log.empty
    assert pd.Timestamp(result.trade_log.iloc[0]["timestamp"]) == pd.Timestamp(datetime(2026, 1, 22, tzinfo=UTC))
    assert "MSFT" not in set(result.trade_log[result.trade_log["rebalance_at"] == pd.Timestamp(datetime(2026, 1, 9, tzinfo=UTC))]["symbol"])


def test_politician_copy_replay_flags_missing_price_history() -> None:
    settings = Settings()
    result = run_politician_copy_replay(
        disclosures=[],
        price_frame=pd.DataFrame(),
        settings=settings,
        source="synthetic",
        production_safe=False,
    )

    assert result.summary["replay_valid"] is False
    assert "missing_price_history" in str(result.summary["replay_invalid_reasons"])


def test_format_politician_copy_replay_summary_includes_warnings() -> None:
    summary = format_politician_copy_replay_summary(
        {
            "strategy": "politician_copy",
            "source": "synthetic",
            "replay_valid": False,
            "replay_invalid_reasons": "unsafe_replay_source",
            "start_at": "2026-01-01T00:00:00+00:00",
            "end_at": "2026-01-31T00:00:00+00:00",
            "rebalances": 2.0,
            "trades": 3.0,
            "closed_trades": 1.0,
            "total_return": 0.1,
            "annualized_return": 0.2,
            "max_drawdown": 0.05,
            "risk_adjusted_score": 0.8,
            "turnover": 0.4,
            "avg_filing_lag_days": 10.0,
            "symbol_concentration": 0.6,
            "rejected_pct_symbol_blocklisted": 1.0,
        }
    )

    assert "warning=unsafe_source=synthetic" in summary
    assert "warning=replay_invalid=unsafe_replay_source" in summary


def test_run_preview_command_prints_plan(monkeypatch, capsys) -> None:
    settings = Settings()
    preview_plan = AllocationPlan(
        selected_politicians=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        politician_scores=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        target_allocations=(TargetAllocation("UNH", 0.25, 25_000, 500.0, ("https://x",)),),
        planned_orders=(AllocationOrder("UNH", "buy", 10, 500.0, 0.25, "target_rebalance"),),
        rejected_disclosures=(),
        source="alpaca",
        production_safe=True,
        account_equity=100_000,
    )

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main._build_politician_copy_plan", lambda settings: preview_plan)

    run_preview_command(get_strategy("politician_copy"))
    captured = capsys.readouterr()

    assert "Markwayne Mullin" in captured.out
    assert "UNH=25.0%" in captured.out


@dataclass(frozen=True)
class _FakeOrder:
    id: str
    client_order_id: str
    symbol: str
    side: str
    qty: str
    status: str
    filled_avg_price: str = "0"
    filled_qty: str = "0"
    submitted_at: datetime | None = None
    filled_at: datetime | None = None


@dataclass(frozen=True)
class _FakePosition:
    symbol: str
    qty: str
    market_value: str
    avg_entry_price: str
    current_price: str
    cost_basis: str
    unrealized_pl: str
    unrealized_plpc: str


class _FakeBroker:
    def get_account_summary(self):
        return SimpleNamespace(status="ACTIVE", buying_power="100000", equity="100000", cash="100000")

    def list_positions(self) -> list[_FakePosition]:
        return []

    def list_open_orders(self, limit: int = 50) -> list[_FakeOrder]:
        return []

    def list_recent_orders(self, limit: int = 50) -> list[_FakeOrder]:
        return []

    def get_portfolio_pnl_history(self, period: str = "1M", timeframe: str = "1D") -> list[object]:
        return []

    def list_execution_fills(self, limit: int = 200) -> list[dict[str, object]]:
        return []


def test_run_paper_command_allows_politician_symbols_outside_static_symbols(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{tmp_path / 'journal.db'}"
    settings = Settings(
        DATABASE_URL=database_url,
        DRY_RUN=True,
        SYMBOLS="SPY",
    )
    create_session_factory(database_url)
    fake_plan = AllocationPlan(
        selected_politicians=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        politician_scores=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        target_allocations=(TargetAllocation("NVDA", 0.2, 20_000, 100.0, ("https://x",)),),
        planned_orders=(AllocationOrder("NVDA", "buy", 5, 100.0, 0.2, "target_rebalance"),),
        rejected_disclosures=(),
        source="alpaca",
        production_safe=True,
        account_equity=100_000,
    )
    kill_switch = SimpleNamespace(
        enabled=False,
        severity="ok",
        reason="",
        force_flatten=False,
        block_new_entries=False,
    )
    reconciliation_state = SimpleNamespace(enabled=False, severity="ok", reason="")
    reconciliation_snapshot = SimpleNamespace(
        local_position_qty_by_symbol={},
        broker_position_qty_by_symbol={},
        unresolved_order_symbols=set(),
    )

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", lambda settings: _FakeBroker())
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr("app.main.politician_copy_strategy.build_allocation_plan", lambda **kwargs: fake_plan)
    monkeypatch.setattr("app.main._compute_reconciliation_state", lambda repo, broker: (reconciliation_snapshot, reconciliation_state))
    monkeypatch.setattr("app.main._compute_kill_switch_state", lambda **kwargs: kill_switch)
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report")
    monkeypatch.setattr("app.main.send_alerts", lambda messages, settings, repo=None: None)

    run_paper_command(get_strategy("politician_copy"))
    repo = JournalRepo(create_session_factory(database_url))
    orders = repo.recent_orders(limit=10)

    assert any(order.symbol == "NVDA" and order.status == "dry_run" for order in orders)


def test_run_paper_command_politician_copy_blocks_buys_but_allows_rebalance_sells(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{tmp_path / 'journal.db'}"
    settings = Settings(
        DATABASE_URL=database_url,
        DRY_RUN=True,
        DENY_NEW_ENTRIES=True,
        SYMBOLS="SPY",
    )
    create_session_factory(database_url)
    fake_plan = AllocationPlan(
        selected_politicians=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        politician_scores=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        target_allocations=(TargetAllocation("NVDA", 0.2, 20_000, 100.0, ("https://x",)),),
        planned_orders=(
            AllocationOrder("AAPL", "sell", 3, 150.0, 0.0, "target_rebalance"),
            AllocationOrder("NVDA", "buy", 5, 100.0, 0.2, "target_rebalance"),
        ),
        rejected_disclosures=(),
        source="alpaca",
        production_safe=True,
        account_equity=100_000,
    )
    kill_switch = SimpleNamespace(enabled=False, severity="ok", reason="", force_flatten=False, block_new_entries=False)
    reconciliation_state = SimpleNamespace(enabled=False, severity="ok", reason="")
    reconciliation_snapshot = SimpleNamespace(local_position_qty_by_symbol={"AAPL": 3.0}, broker_position_qty_by_symbol={"AAPL": 3.0}, unresolved_order_symbols=set())

    class _SellBroker(_FakeBroker):
        def list_positions(self) -> list[_FakePosition]:
            return [
                _FakePosition(
                    symbol="AAPL",
                    qty="3",
                    market_value="450",
                    avg_entry_price="100",
                    current_price="150",
                    cost_basis="300",
                    unrealized_pl="150",
                    unrealized_plpc="0.5",
                )
            ]

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", lambda settings: _SellBroker())
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr("app.main.politician_copy_strategy.build_allocation_plan", lambda **kwargs: fake_plan)
    monkeypatch.setattr("app.main._compute_reconciliation_state", lambda repo, broker: (reconciliation_snapshot, reconciliation_state))
    monkeypatch.setattr("app.main._compute_kill_switch_state", lambda **kwargs: kill_switch)
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report")
    monkeypatch.setattr("app.main.send_alerts", lambda messages, settings, repo=None: None)

    run_paper_command(get_strategy("politician_copy"))
    repo = JournalRepo(create_session_factory(database_url))
    orders = repo.recent_orders(limit=10)

    assert any(order.symbol == "AAPL" and order.side == "sell" and order.status == "dry_run" for order in orders)
    assert any(order.symbol == "NVDA" and order.side == "buy" and order.status == "blocked" and order.status_detail == "deny_new_entries" for order in orders)


def test_run_paper_command_politician_copy_blocks_unsafe_buys_but_keeps_exits(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{tmp_path / 'journal.db'}"
    settings = Settings(
        DATABASE_URL=database_url,
        DRY_RUN=False,
        SYMBOLS="SPY",
    )
    create_session_factory(database_url)
    fake_plan = AllocationPlan(
        selected_politicians=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        politician_scores=(PoliticianScore("M001190", "Markwayne Mullin", 0.12, 2, 0.18),),
        target_allocations=(TargetAllocation("NVDA", 0.2, 20_000, 100.0, ("https://x",)),),
        planned_orders=(
            AllocationOrder("AAPL", "sell", 3, 150.0, 0.0, "target_rebalance"),
            AllocationOrder("NVDA", "buy", 5, 100.0, 0.2, "target_rebalance"),
        ),
        rejected_disclosures=(SimpleNamespace(reason="symbol_blocklisted"),),
        source="synthetic",
        production_safe=False,
        account_equity=100_000,
    )
    kill_switch = SimpleNamespace(enabled=False, severity="ok", reason="", force_flatten=False, block_new_entries=False)
    reconciliation_state = SimpleNamespace(enabled=False, severity="ok", reason="")
    reconciliation_snapshot = SimpleNamespace(local_position_qty_by_symbol={"AAPL": 3.0}, broker_position_qty_by_symbol={"AAPL": 3.0}, unresolved_order_symbols=set())

    class _SellBroker(_FakeBroker):
        def list_positions(self) -> list[_FakePosition]:
            return [
                _FakePosition(
                    symbol="AAPL",
                    qty="3",
                    market_value="450",
                    avg_entry_price="100",
                    current_price="150",
                    cost_basis="300",
                    unrealized_pl="150",
                    unrealized_plpc="0.5",
                )
            ]

    class _AcceptedExecutor:
        def __init__(self, repo: JournalRepo, settings: Settings, broker=None) -> None:
            self.repo = repo

        def submit(self, order, allowed_symbols=None):
            return SimpleNamespace(
                accepted=True,
                status="accepted",
                status_detail="accepted",
                intent_id=f"intent-{order.symbol}-{order.side}",
                client_order_id=f"client-{order.symbol}-{order.side}",
                broker_order_id=f"broker-{order.symbol}-{order.side}",
                filled_avg_price=float(order.close),
            )

    captured_alerts: list[str] = []
    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.AlpacaTradingAdapter", lambda settings: _SellBroker())
    monkeypatch.setattr("app.main.PaperExecutor", _AcceptedExecutor)
    monkeypatch.setattr("app.main.should_run_trading_loop", lambda broker: True)
    monkeypatch.setattr("app.main.politician_copy_strategy.build_allocation_plan", lambda **kwargs: fake_plan)
    monkeypatch.setattr("app.main._compute_reconciliation_state", lambda repo, broker: (reconciliation_snapshot, reconciliation_state))
    monkeypatch.setattr("app.main._compute_kill_switch_state", lambda **kwargs: kill_switch)
    monkeypatch.setattr("app.main.build_daily_report", lambda repo: "report")
    monkeypatch.setattr("app.main.send_alerts", lambda messages, settings, repo=None: captured_alerts.extend(messages))

    run_paper_command(get_strategy("politician_copy"))
    repo = JournalRepo(create_session_factory(database_url))
    orders = repo.recent_orders(limit=10)

    assert any(order.symbol == "AAPL" and order.side == "sell" and order.status == "accepted" for order in orders)
    assert any(order.symbol == "NVDA" and order.side == "buy" and order.status == "blocked" and order.status_detail == "unsafe_data_source:synthetic" for order in orders)
    assert any("politician_copy rejected_disclosures=1" in message for message in captured_alerts)
    daily_report = build_daily_report(repo)
    assert "politician_copy_rejected_disclosures=1" in daily_report


def test_run_replay_command_prints_summary(monkeypatch, capsys, tmp_path) -> None:
    settings = Settings(DATABASE_URL=f"sqlite:///{tmp_path / 'journal.db'}", BACKTEST_OUTPUT_DIR=str(tmp_path / "artifacts"))
    replay_result = SimpleNamespace(
        summary={
            "strategy": "politician_copy",
            "source": "alpaca",
            "replay_valid": True,
            "replay_invalid_reasons": "",
            "start_at": "2026-01-01T00:00:00+00:00",
            "end_at": "2026-01-31T00:00:00+00:00",
            "rebalances": 2.0,
            "trades": 3.0,
            "closed_trades": 1.0,
            "total_return": 0.1,
            "annualized_return": 0.2,
            "max_drawdown": 0.05,
            "risk_adjusted_score": 0.8,
            "turnover": 0.4,
            "avg_filing_lag_days": 10.0,
            "symbol_concentration": 0.6,
        },
        trade_log=pd.DataFrame(),
        equity_curve=pd.DataFrame(),
        decision_log=pd.DataFrame(),
    )

    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.load_politician_copy_replay_inputs", lambda settings: ([], pd.DataFrame(), "alpaca", True))
    monkeypatch.setattr("app.main.run_politician_copy_replay", lambda **kwargs: replay_result)
    monkeypatch.setattr("app.main.write_politician_copy_replay_artifacts", lambda result, settings, disclosures: tmp_path / "artifacts" / "run-001")

    run_replay_command(get_strategy("politician_copy"))
    captured = capsys.readouterr()

    assert "Politician Copy Replay" in captured.out
    assert "artifacts=" in captured.out
