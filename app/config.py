from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    alpaca_api_key: str = Field(default="demo-key", alias="ALPACA_API_KEY")
    alpaca_secret_key: str = Field(default="demo-secret", alias="ALPACA_SECRET_KEY")
    alpaca_paper: bool = Field(default=True, alias="ALPACA_PAPER")
    alpaca_base_url: str = Field(
        default="https://paper-api.alpaca.markets",
        alias="ALPACA_BASE_URL",
    )
    alpaca_data_feed: str = Field(default="iex", alias="ALPACA_DATA_FEED")
    dry_run: bool = Field(default=True, alias="DRY_RUN")
    database_url: str = Field(default="sqlite:///trading.db", alias="DATABASE_URL")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    symbols: str = Field(default="SPY,QQQ,IWM,AAPL,MSFT", alias="SYMBOLS")
    max_positions: int = Field(default=3, alias="MAX_POSITIONS")
    max_position_notional: float = Field(default=20_000, alias="MAX_POSITION_NOTIONAL")
    max_daily_loss: float = Field(default=1_000, alias="MAX_DAILY_LOSS")
    max_order_qty: int = Field(default=25, alias="MAX_ORDER_QTY")
    trend_window: int = Field(default=100, alias="TREND_WINDOW")
    exit_window: int = Field(default=50, alias="EXIT_WINDOW")
    atr_window: int = Field(default=14, alias="ATR_WINDOW")
    atr_risk_budget: float = Field(default=100, alias="ATR_RISK_BUDGET")
    lookback_days: int = Field(default=400, alias="LOOKBACK_DAYS")
    force_exit_symbols: str = Field(default="", alias="FORCE_EXIT_SYMBOLS")

    @property
    def symbol_list(self) -> list[str]:
        return [symbol.strip().upper() for symbol in self.symbols.split(",") if symbol.strip()]

    @property
    def force_exit_symbol_list(self) -> list[str]:
        return [
            symbol.strip().upper()
            for symbol in self.force_exit_symbols.split(",")
            if symbol.strip()
        ]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
