from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

US_EASTERN = ZoneInfo("America/New_York")
MARKET_OPEN = time(hour=9, minute=30)
MARKET_CLOSE = time(hour=16, minute=0)


def market_now(now: datetime | None = None) -> datetime:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(US_EASTERN)


def market_day_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    current = market_now(now)
    market_date = current.date()
    start = datetime.combine(market_date, time.min, tzinfo=US_EASTERN)
    end = start + timedelta(days=1)
    return start.astimezone(UTC), end.astimezone(UTC)


def current_market_date(now: datetime | None = None) -> date:
    return market_now(now).date()


def previous_trading_date(now: datetime | None = None) -> date:
    market_date = current_market_date(now)
    previous_date = market_date - timedelta(days=1)
    while previous_date.weekday() >= 5:
        previous_date -= timedelta(days=1)
    return previous_date


def market_is_open(now: datetime | None = None) -> bool:
    current = market_now(now)
    if current.weekday() >= 5:
        return False
    return MARKET_OPEN <= current.time() < MARKET_CLOSE
