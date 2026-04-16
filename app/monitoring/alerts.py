from __future__ import annotations

import logging
from collections.abc import Iterable

from app.config import Settings

LOGGER = logging.getLogger(__name__)


def send_alert(message: str) -> None:
    LOGGER.warning("alert %s", message)


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
