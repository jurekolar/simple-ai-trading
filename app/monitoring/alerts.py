from __future__ import annotations

import logging
from collections.abc import Iterable

LOGGER = logging.getLogger(__name__)


def send_alert(message: str) -> None:
    LOGGER.warning("alert %s", message)


def send_alerts(messages: Iterable[str]) -> None:
    for message in messages:
        send_alert(message)
