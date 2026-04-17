from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from app.config import Settings

LOGGER = logging.getLogger(__name__)

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.enums import ActivityType, QueryOrderStatus
    from alpaca.trading.models import NonTradeActivity, TradeActivity
    from alpaca.trading.requests import GetOrdersRequest, GetPortfolioHistoryRequest
except ImportError:  # pragma: no cover - dependency may not be installed yet
    TradingClient = None
    ActivityType = None
    TradeActivity = None
    NonTradeActivity = None
    QueryOrderStatus = None
    GetOrdersRequest = None
    GetPortfolioHistoryRequest = None


@dataclass(frozen=True)
class AccountSnapshot:
    status: str
    buying_power: str
    equity: str
    cash: str


@dataclass(frozen=True)
class BrokerOrderSnapshot:
    id: str
    client_order_id: str
    symbol: str
    side: str
    qty: str
    status: str
    filled_avg_price: str
    filled_qty: str
    submitted_at: datetime | None
    filled_at: datetime | None


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    qty: str
    market_value: str
    avg_entry_price: str
    current_price: str
    cost_basis: str
    unrealized_pl: str
    unrealized_plpc: str


@dataclass(frozen=True)
class PortfolioPnlSnapshot:
    timestamp: datetime
    equity: float
    profit_loss: float
    profit_loss_pct: float


@dataclass(frozen=True)
class MarketClockSnapshot:
    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime


@dataclass(frozen=True)
class TradeActivitySnapshot:
    activity_id: str
    order_id: str
    symbol: str
    side: str
    qty: float
    price: float
    transaction_time: datetime


@dataclass(frozen=True)
class FeeActivitySnapshot:
    activity_id: str
    activity_type: str
    net_amount: float
    activity_date: date
    symbol: str | None = None


class BrokerSubmitError(RuntimeError):
    pass


class BrokerCancelError(RuntimeError):
    pass


class BrokerClosePositionError(RuntimeError):
    pass


@dataclass(frozen=True)
class BrokerExposureSnapshot:
    gross_exposure: float
    unrealized_pnl: float
    position_notional_by_symbol: dict[str, float]


@dataclass(frozen=True)
class ReconciliationSnapshot:
    local_position_qty_by_symbol: dict[str, float]
    broker_position_qty_by_symbol: dict[str, float]
    open_order_symbols: set[str]
    unresolved_order_symbols: set[str]


class AlpacaTradingAdapter:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = None
        if TradingClient is not None:
            self._client = TradingClient(
                api_key=settings.alpaca_api_key,
                secret_key=settings.alpaca_secret_key,
                paper=settings.alpaca_paper,
                url_override=settings.alpaca_base_url,
            )

    def get_account_summary(self) -> AccountSnapshot:
        if self._client is None:
            return AccountSnapshot(status="stub", buying_power="0", equity="0", cash="0")
        account = self._client.get_account()
        return AccountSnapshot(
            status=str(account.status),
            buying_power=str(account.buying_power),
            equity=str(account.equity),
            cash=str(account.cash),
        )

    def get_account_identifiers(self) -> dict[str, str]:
        if self._client is None:
            return {"account_id": "stub", "account_number": "stub"}
        account = self._client.get_account()
        return {
            "account_id": str(getattr(account, "id", "") or ""),
            "account_number": str(getattr(account, "account_number", "") or ""),
        }

    def list_recent_orders(self, limit: int = 20) -> list[BrokerOrderSnapshot]:
        if self._client is None or GetOrdersRequest is None or QueryOrderStatus is None:
            return []
        orders = self._client.get_orders(GetOrdersRequest(status=QueryOrderStatus.ALL, limit=limit))
        return [self._to_order_snapshot(order) for order in orders]

    def list_open_orders(self, limit: int = 50) -> list[BrokerOrderSnapshot]:
        if self._client is None or GetOrdersRequest is None or QueryOrderStatus is None:
            return []
        orders = self._client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=limit))
        return [self._to_order_snapshot(order) for order in orders]

    def _to_order_snapshot(self, order: object) -> BrokerOrderSnapshot:
        return BrokerOrderSnapshot(
            id=str(order.id),
            client_order_id=str(order.client_order_id),
            symbol=str(order.symbol),
            side=str(order.side),
            qty=str(order.qty),
            status=str(order.status),
            filled_avg_price=str(order.filled_avg_price or ""),
            filled_qty=str(order.filled_qty or ""),
            submitted_at=getattr(order, "submitted_at", None),
            filled_at=getattr(order, "filled_at", None),
        )

    @staticmethod
    def normalize_order_status(status: str) -> str:
        raw = (status or "").lower()
        status_map = {
            "new": "new",
            "accepted": "accepted",
            "pending_new": "pending_new",
            "accepted_for_bidding": "accepted_for_bidding",
            "partially_filled": "partially_filled",
            "filled": "filled",
            "done_for_day": "done_for_day",
            "canceled": "canceled",
            "cancelled": "canceled",
            "pending_cancel": "pending_cancel",
            "pending_replace": "pending_replace",
            "replaced": "replaced",
            "expired": "expired",
            "rejected": "rejected",
            "stopped": "stopped",
            "suspended": "suspended",
            "calculated": "calculated",
        }
        return status_map.get(raw, raw or "unknown")

    @classmethod
    def is_unresolved_order_status(cls, status: str) -> bool:
        return cls.normalize_order_status(status) in {
            "new",
            "accepted",
            "pending_new",
            "accepted_for_bidding",
            "partially_filled",
            "pending_cancel",
            "pending_replace",
        }

    def submit_market_order(
        self,
        *,
        symbol: str,
        qty: float,
        side: str,
        client_order_id: str,
        time_in_force: str = "day",
    ) -> BrokerOrderSnapshot:
        if self._client is None:
            raise BrokerSubmitError("trading client unavailable")

        try:
            from alpaca.trading.enums import OrderSide, TimeInForce
            from alpaca.trading.requests import MarketOrderRequest
        except ImportError as exc:  # pragma: no cover - depends on optional dependency state
            raise BrokerSubmitError("alpaca trading dependencies unavailable") from exc

        tif_value = time_in_force.lower()
        if tif_value != "day":
            raise BrokerSubmitError(f"unsupported time_in_force={time_in_force}")

        try:
            request = MarketOrderRequest(
                symbol=symbol,
                qty=float(qty),
                side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                client_order_id=client_order_id,
            )
            order = self._client.submit_order(request)
        except Exception as exc:  # pragma: no cover - depends on external service state
            raise BrokerSubmitError(str(exc)) from exc

        return self._to_order_snapshot(order)

    def cancel_order(self, broker_order_id: str) -> None:
        if self._client is None:
            raise BrokerCancelError("trading client unavailable")
        try:
            self._client.cancel_order_by_id(broker_order_id)
        except Exception as exc:  # pragma: no cover - depends on external service state
            raise BrokerCancelError(str(exc)) from exc

    def close_position(self, symbol: str) -> BrokerOrderSnapshot:
        if self._client is None:
            raise BrokerClosePositionError("trading client unavailable")
        try:
            order = self._client.close_position(symbol)
        except Exception as exc:  # pragma: no cover - depends on external service state
            raise BrokerClosePositionError(str(exc)) from exc
        return self._to_order_snapshot(order)

    def list_positions(self) -> list[PositionSnapshot]:
        if self._client is None:
            return []
        return [
            PositionSnapshot(
                symbol=str(position.symbol),
                qty=str(position.qty),
                market_value=str(position.market_value),
                avg_entry_price=str(position.avg_entry_price),
                current_price=str(position.current_price),
                cost_basis=str(position.cost_basis),
                unrealized_pl=str(position.unrealized_pl),
                unrealized_plpc=str(position.unrealized_plpc),
            )
            for position in self._client.get_all_positions()
        ]

    def get_buying_power(self) -> float:
        return float(self.get_account_summary().buying_power)

    def get_cash(self) -> float:
        return float(self.get_account_summary().cash)

    def get_equity(self) -> float:
        return float(self.get_account_summary().equity)

    def get_open_exposure(self) -> BrokerExposureSnapshot:
        positions = self.list_positions()
        position_notional_by_symbol = {
            position.symbol: abs(float(position.market_value)) for position in positions
        }
        return BrokerExposureSnapshot(
            gross_exposure=float(sum(position_notional_by_symbol.values())),
            unrealized_pnl=float(sum(float(position.unrealized_pl) for position in positions)),
            position_notional_by_symbol=position_notional_by_symbol,
        )

    def build_reconciliation_snapshot(
        self,
        *,
        local_position_qty_by_symbol: dict[str, float],
    ) -> ReconciliationSnapshot:
        positions = self.list_positions()
        open_orders = self.list_open_orders(limit=50)
        return ReconciliationSnapshot(
            local_position_qty_by_symbol=local_position_qty_by_symbol,
            broker_position_qty_by_symbol={position.symbol: float(position.qty) for position in positions},
            open_order_symbols={order.symbol for order in open_orders},
            unresolved_order_symbols={
                order.symbol
                for order in self.list_recent_orders(limit=50)
                if self.is_unresolved_order_status(order.status)
            },
        )

    def get_portfolio_pnl_history(
        self,
        *,
        period: str = "1M",
        timeframe: str = "1D",
    ) -> list[PortfolioPnlSnapshot]:
        if self._client is None or GetPortfolioHistoryRequest is None:
            return []
        history = self._client.get_portfolio_history(
            GetPortfolioHistoryRequest(period=period, timeframe=timeframe)
        )
        if not history.timestamp:
            return []
        return [
            PortfolioPnlSnapshot(
                timestamp=datetime.fromtimestamp(timestamp, tz=timezone.utc),
                equity=float(equity),
                profit_loss=float(profit_loss),
                profit_loss_pct=float(profit_loss_pct),
            )
            for timestamp, equity, profit_loss, profit_loss_pct in zip(
                history.timestamp,
                history.equity or [],
                history.profit_loss or [],
                history.profit_loss_pct or [],
                strict=False,
            )
        ]

    def get_market_clock(self) -> MarketClockSnapshot | None:
        if self._client is None:
            return None
        clock = self._client.get_clock()
        return MarketClockSnapshot(
            timestamp=clock.timestamp,
            is_open=bool(clock.is_open),
            next_open=clock.next_open,
            next_close=clock.next_close,
        )

    @staticmethod
    def _activity_value(activity: object, key: str) -> object:
        if isinstance(activity, dict):
            return activity.get(key)
        return getattr(activity, key, None)

    @staticmethod
    def _coerce_datetime(value: object) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        return None

    @staticmethod
    def _coerce_date(value: object) -> date | None:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        parsed = AlpacaTradingAdapter._coerce_datetime(value)
        return parsed.date() if parsed is not None else None

    def list_trade_activities(self, days: int = 30) -> list[TradeActivitySnapshot]:
        if self._client is None or ActivityType is None or TradeActivity is None:
            return []
        raw_activities = self._client.get(
            "/account/activities",
            {
                "activity_types": ActivityType.FILL.value,
                "after": (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(),
            },
        )
        snapshots: list[TradeActivitySnapshot] = []
        for raw_activity in raw_activities:
            transaction_time = self._coerce_datetime(
                self._activity_value(raw_activity, "transaction_time")
                or self._activity_value(raw_activity, "date")
            )
            order_id = self._activity_value(raw_activity, "order_id")
            symbol = self._activity_value(raw_activity, "symbol")
            side = self._activity_value(raw_activity, "side")
            qty = self._activity_value(raw_activity, "qty")
            price = self._activity_value(raw_activity, "price")
            activity_id = self._activity_value(raw_activity, "id")
            if (
                transaction_time is None
                or order_id in (None, "")
                or symbol in (None, "")
                or side in (None, "")
                or qty in (None, "")
                or price in (None, "")
            ):
                LOGGER.warning("skipping malformed trade activity payload: %s", raw_activity)
                continue
            snapshots.append(
                TradeActivitySnapshot(
                    activity_id=str(activity_id or ""),
                    order_id=str(order_id),
                    symbol=str(symbol),
                    side=str(side),
                    qty=float(qty),
                    price=float(price),
                    transaction_time=transaction_time,
                )
            )
        return snapshots

    def list_fee_activities(self, days: int = 30) -> list[FeeActivitySnapshot]:
        if self._client is None or ActivityType is None or NonTradeActivity is None:
            return []
        raw_activities = self._client.get(
            "/account/activities",
            {
                "activity_types": ",".join([ActivityType.FEE.value, ActivityType.CFEE.value]),
                "after": (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(),
            },
        )
        snapshots: list[FeeActivitySnapshot] = []
        for raw_activity in raw_activities:
            activity_date = self._coerce_date(
                self._activity_value(raw_activity, "date")
                or self._activity_value(raw_activity, "transaction_time")
            )
            activity_type = self._activity_value(raw_activity, "activity_type")
            net_amount = self._activity_value(raw_activity, "net_amount")
            if activity_date is None or activity_type in (None, "") or net_amount in (None, ""):
                LOGGER.warning("skipping malformed fee activity payload: %s", raw_activity)
                continue
            symbol = self._activity_value(raw_activity, "symbol")
            activity_id = self._activity_value(raw_activity, "id")
            snapshots.append(
                FeeActivitySnapshot(
                    activity_id=str(activity_id or ""),
                    activity_type=str(activity_type),
                    net_amount=float(net_amount),
                    activity_date=activity_date,
                    symbol=str(symbol) if symbol else None,
                )
            )
        return snapshots

    def list_execution_fills(self, limit: int = 200) -> list[dict[str, object]]:
        recent_orders = self.list_recent_orders(limit=limit)
        order_by_id = {order.id: order for order in recent_orders}
        trade_activities = self.list_trade_activities(days=30)
        fee_activities = self.list_fee_activities(days=30)

        fees_by_key: dict[tuple[date, str | None], float] = defaultdict(float)
        for fee in fee_activities:
            fees_by_key[(fee.activity_date, fee.symbol)] += abs(fee.net_amount)

        gross_by_key: dict[tuple[date, str | None], float] = defaultdict(float)
        for activity in trade_activities:
            activity_date = activity.transaction_time.astimezone(timezone.utc).date()
            gross_by_key[(activity_date, activity.symbol)] += activity.qty * activity.price

        fills: list[dict[str, object]] = []
        for activity in trade_activities:
            activity_date = activity.transaction_time.astimezone(timezone.utc).date()
            gross_amount = activity.qty * activity.price
            symbol_key = (activity_date, activity.symbol)
            day_symbol_fee = fees_by_key.get(symbol_key, 0.0)
            day_symbol_gross = gross_by_key.get(symbol_key, 0.0)
            allocated_fee = day_symbol_fee * (gross_amount / day_symbol_gross) if day_symbol_gross else 0.0
            order = order_by_id.get(activity.order_id)
            fills.append(
                {
                    "broker_order_id": activity.order_id,
                    "client_order_id": order.client_order_id if order is not None else "",
                    "symbol": activity.symbol,
                    "side": activity.side,
                    "qty": activity.qty,
                    "price": activity.price,
                    "gross_amount": gross_amount,
                    "fees": allocated_fee,
                    "net_amount": gross_amount - allocated_fee if activity.side.lower() == "sell" else gross_amount + allocated_fee,
                    "execution_date": activity_date,
                    "filled_at": activity.transaction_time,
                }
            )
        return fills
