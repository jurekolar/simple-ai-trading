from __future__ import annotations

from datetime import datetime

from app.broker.alpaca_client import AlpacaTradingAdapter
from app.data.market_calendar import market_is_open


def should_run_trading_loop(
    broker: AlpacaTradingAdapter | None = None,
    now: datetime | None = None,
) -> bool:
    if broker is not None:
        try:
            clock = broker.get_market_clock()
        except Exception:
            clock = None
        if clock is not None:
            return clock.is_open
    return market_is_open(now)
