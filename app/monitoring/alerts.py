from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.config import Settings

if TYPE_CHECKING:
    from app.db.repo import JournalRepo

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AlertDeliveryResult:
    channel: str
    delivery_status: str
    message: str
    error_message: str = ""


def send_alert(message: str) -> None:
    LOGGER.warning("alert %s", message)


def _send_webhook_alert(message: str, settings: Settings) -> AlertDeliveryResult:
    if not settings.alert_webhook_url:
        return AlertDeliveryResult(
            channel="webhook",
            delivery_status="skipped",
            message=message,
            error_message="alert_webhook_url_not_configured",
        )
    payload = json.dumps({"text": message}).encode("utf-8")
    request = Request(
        settings.alert_webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.alert_webhook_timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            if status_code >= 400:
                raise RuntimeError(f"alert webhook responded with status={status_code}")
    except (HTTPError, URLError, RuntimeError) as exc:
        LOGGER.warning("alert webhook delivery failed: %s", exc)
        return AlertDeliveryResult(
            channel="webhook",
            delivery_status="failed",
            message=message,
            error_message=str(exc),
        )
    return AlertDeliveryResult(channel="webhook", delivery_status="sent", message=message)


def should_send_alert(message: str, settings: Settings) -> bool:
    normalized = message.lower()
    if "stale" in normalized and not settings.alert_on_stale_data:
        return False
    if "reconciliation" in normalized and not settings.alert_on_reconciliation_drift:
        return False
    if ("drawdown" in normalized or "daily_loss" in normalized) and not settings.alert_on_drawdown_breach:
        return False
    if "blocked" in normalized and not settings.alert_on_blocked_orders:
        return False
    return True


def send_alerts(
    messages: Iterable[str],
    settings: Settings | None = None,
    repo: JournalRepo | None = None,
) -> list[AlertDeliveryResult]:
    results: list[AlertDeliveryResult] = []
    for message in messages:
        if settings is not None and not should_send_alert(message, settings):
            results.append(
                AlertDeliveryResult(
                    channel="policy",
                    delivery_status="suppressed",
                    message=message,
                    error_message="notification_suppressed_by_settings",
                )
            )
            continue
        send_alert(message)
        results.append(AlertDeliveryResult(channel="log", delivery_status="sent", message=message))
        if settings is not None:
            results.append(_send_webhook_alert(message, settings))
    if repo is not None:
        for result in results:
            repo.log_alert_event(
                channel=result.channel,
                delivery_status=result.delivery_status,
                message=result.message,
                error_message=result.error_message,
            )
    return results
