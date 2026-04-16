from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from app.config import Settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Alpaca trading and data authentication.")
    parser.add_argument("--symbol", default="SPY", help="Symbol to use for the market-data auth check.")
    return parser.parse_args()


def check_trading_auth(settings: Settings) -> tuple[bool, str]:
    try:
        from alpaca.trading.client import TradingClient
    except ImportError:
        return False, "alpaca trading client not installed"

    try:
        client = TradingClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_secret_key,
            paper=settings.alpaca_paper,
            url_override=settings.alpaca_base_url,
        )
        account = client.get_account()
        return True, f"ok status={account.status} cash={account.cash} buying_power={account.buying_power}"
    except Exception as exc:  # pragma: no cover - depends on external service state
        return False, str(exc)


def check_data_auth(settings: Settings, symbol: str) -> tuple[bool, str]:
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
    except ImportError:
        return False, "alpaca data client not installed"

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=5)

    try:
        client = StockHistoricalDataClient(settings.alpaca_api_key, settings.alpaca_secret_key)
        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            feed=settings.alpaca_data_feed,
        )
        bars = client.get_stock_bars(request).df.reset_index()
        if bars.empty:
            return False, f"authenticated but returned no bars for {symbol}"
        return True, f"ok bars={len(bars)} latest_symbol={bars.iloc[-1]['symbol']}"
    except Exception as exc:  # pragma: no cover - depends on external service state
        return False, str(exc)


def main() -> None:
    args = parse_args()
    settings = Settings()

    trading_ok, trading_message = check_trading_auth(settings)
    data_ok, data_message = check_data_auth(settings, args.symbol)

    print("Alpaca Auth Check")
    print(f"Trading auth: {'PASS' if trading_ok else 'FAIL'} - {trading_message}")
    print(f"Data auth: {'PASS' if data_ok else 'FAIL'} - {data_message}")

    if not trading_ok or not data_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
