from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.config import Settings

LOGGER = logging.getLogger(__name__)


def send_alert(message: str) -> None:
    LOGGER.warning("alert %s", message)


def _send_webhook_alert(message: str, settings: Settings) -> None:
    if not settings.alert_webhook_url:
        return
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


def send_alerts(messages: Iterable[str], settings: Settings | None = None) -> None:
    for message in messages:
        if settings is None or should_send_alert(message, settings):
            send_alert(message)
            if settings is not None:
                _send_webhook_alert(message, settings)
