from __future__ import annotations

import logging

LOGGER = logging.getLogger(__name__)


def send_alert(message: str) -> None:
    LOGGER.warning("alert %s", message)
