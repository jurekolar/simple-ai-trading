from __future__ import annotations

import uuid
from dataclasses import dataclass

import logging

from app.broker.alpaca_client import AlpacaTradingAdapter, BrokerSubmitError
from app.broker.order_mapper import OrderIntent
from app.config import Settings
from app.db.repo import JournalRepo

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    intent_id: str
    client_order_id: str
    broker_order_id: str = ""
    filled_avg_price: float = 0.0
    status_detail: str = ""

    @property
    def accepted(self) -> bool:
        return self.status not in {"blocked", "error"}


class PaperExecutor:
    def __init__(
        self,
        repo: JournalRepo,
        settings: Settings,
        broker: AlpacaTradingAdapter | None = None,
    ) -> None:
        self._repo = repo
        self._settings = settings
        self._broker = broker or AlpacaTradingAdapter(settings)

    def _validate_entry_order(self, order: OrderIntent) -> None:
        if order.symbol not in self._settings.symbol_list:
            raise ValueError(f"symbol {order.symbol} is not whitelisted")
        if order.qty <= 0:
            raise ValueError("order qty must be positive")
        if order.qty > self._settings.max_order_qty:
            raise ValueError(
                f"order qty {order.qty} exceeds MAX_ORDER_QTY={self._settings.max_order_qty}"
            )
        if order.qty * order.close > self._settings.max_position_notional:
            raise ValueError("order exceeds max position notional")

    def _validate_exit_order(self, order: OrderIntent) -> None:
        if order.symbol not in self._settings.symbol_list:
            raise ValueError(f"symbol {order.symbol} is not whitelisted")
        if order.qty <= 0:
            raise ValueError("order qty must be positive")

    def submit_orders(self, order: OrderIntent) -> list[ExecutionResult]:
        orders = self.split_order_for_submit(order)
        return [self.submit(chunk) for chunk in orders]

    def split_order_for_submit(self, order: OrderIntent) -> list[OrderIntent]:
        return self._chunk_exit_order(order) if order.side.lower() == "sell" else [order]

    def _chunk_exit_order(self, order: OrderIntent) -> list[OrderIntent]:
        if order.qty <= self._settings.max_order_qty or self._settings.max_order_qty <= 0:
            return [order]
        remaining = int(order.qty)
        chunks: list[OrderIntent] = []
        while remaining > 0:
            chunk_qty = min(remaining, self._settings.max_order_qty)
            chunks.append(
                OrderIntent(
                    symbol=order.symbol,
                    qty=chunk_qty,
                    side=order.side,
                    close=order.close,
                )
            )
            remaining -= chunk_qty
        return chunks

    def submit(self, order: OrderIntent) -> ExecutionResult:
        intent_id = f"intent-{uuid.uuid4().hex[:20]}"
        client_order_id = f"codex-{uuid.uuid4().hex[:20]}"
        self._repo.log_order(
            order.symbol,
            order.side,
            float(order.qty),
            "intent",
            intent_id=intent_id,
            lifecycle_state="intent",
            requested_price=order.close,
        )
        try:
            if order.side.lower() == "buy":
                self._validate_entry_order(order)
            else:
                self._validate_exit_order(order)
        except ValueError as exc:
            result = ExecutionResult(
                status="blocked",
                intent_id=intent_id,
                client_order_id=client_order_id,
                status_detail=str(exc),
            )
            self._repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                result.status,
                status_detail=result.status_detail,
                intent_id=result.intent_id,
                client_order_id=result.client_order_id,
                requested_price=order.close,
            )
            return result

        if self._settings.dry_run:
            LOGGER.info("dry-run paper order %s %s %s", order.side, order.qty, order.symbol)
            result = ExecutionResult(status="dry_run", intent_id=intent_id, client_order_id=client_order_id)
            self._repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                result.status,
                status_detail=result.status_detail,
                intent_id=result.intent_id,
                client_order_id=result.client_order_id,
                requested_price=order.close,
            )
            return result

        try:
            broker_order = self._broker.submit_market_order(
                symbol=order.symbol,
                qty=float(order.qty),
                side=order.side,
                client_order_id=client_order_id,
            )
        except BrokerSubmitError as exc:
            LOGGER.warning("paper order submit failed for %s: %s", order.symbol, exc)
            self._repo.log_broker_error_event(
                symbol=order.symbol,
                operation="submit_market_order",
                message=str(exc),
            )
            result = ExecutionResult(
                status="error",
                intent_id=intent_id,
                client_order_id=client_order_id,
                status_detail=str(exc),
            )
            self._repo.log_order(
                order.symbol,
                order.side,
                float(order.qty),
                result.status,
                status_detail=result.status_detail,
                intent_id=result.intent_id,
                client_order_id=result.client_order_id,
                requested_price=order.close,
            )
            return result

        result = ExecutionResult(
            status=broker_order.status,
            intent_id=intent_id,
            client_order_id=broker_order.client_order_id,
            broker_order_id=broker_order.id,
            filled_avg_price=float(broker_order.filled_avg_price or 0.0),
        )
        LOGGER.info("paper order submitted %s", result)
        self._repo.log_order(
            order.symbol,
            order.side,
            float(order.qty),
            result.status,
            status_detail=result.status_detail,
            intent_id=result.intent_id,
            client_order_id=result.client_order_id,
            broker_order_id=result.broker_order_id,
            requested_price=order.close,
            filled_avg_price=result.filled_avg_price,
        )
        return result
