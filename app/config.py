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
    paper_only: bool = Field(default=True, alias="PAPER_ONLY")
    allow_live: bool = Field(default=False, alias="ALLOW_LIVE")
    live_config_profile: str = Field(default="", alias="LIVE_CONFIG_PROFILE")
    live_deployment_ack: str = Field(default="", alias="LIVE_DEPLOYMENT_ACK")
    database_url: str = Field(default="sqlite:///trading.db", alias="DATABASE_URL")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    symbols: str = Field(default="SPY,QQQ,IWM,AAPL,MSFT", alias="SYMBOLS")
    max_positions: int = Field(default=3, alias="MAX_POSITIONS")
    max_symbols_per_run: int = Field(default=3, alias="MAX_SYMBOLS_PER_RUN")
    max_position_notional: float = Field(default=20_000, alias="MAX_POSITION_NOTIONAL")
    max_gross_exposure: float = Field(default=50_000, alias="MAX_GROSS_EXPOSURE")
    max_symbol_exposure: float = Field(default=20_000, alias="MAX_SYMBOL_EXPOSURE")
    max_daily_loss: float = Field(default=1_000, alias="MAX_DAILY_LOSS")
    max_unrealized_drawdown: float = Field(default=1_500, alias="MAX_UNREALIZED_DRAWDOWN")
    emergency_unrealized_drawdown: float = Field(
        default=2_500,
        alias="EMERGENCY_UNREALIZED_DRAWDOWN",
    )
    min_cash_buffer: float = Field(default=0.0, alias="MIN_CASH_BUFFER")
    max_order_qty: int = Field(default=25, alias="MAX_ORDER_QTY")
    max_open_orders: int = Field(default=8, alias="MAX_OPEN_ORDERS")
    max_stuck_order_minutes: int = Field(default=20, alias="MAX_STUCK_ORDER_MINUTES")
    max_broker_failures: int = Field(default=3, alias="MAX_BROKER_FAILURES")
    trend_window: int = Field(default=100, alias="TREND_WINDOW")
    exit_window: int = Field(default=50, alias="EXIT_WINDOW")
    atr_window: int = Field(default=14, alias="ATR_WINDOW")
    mean_reversion_window: int = Field(default=20, alias="MEAN_REVERSION_WINDOW")
    mean_reversion_volatility_window: int = Field(default=20, alias="MEAN_REVERSION_VOLATILITY_WINDOW")
    mean_reversion_entry_zscore: float = Field(default=-1.0, alias="MEAN_REVERSION_ENTRY_ZSCORE")
    mean_reversion_exit_zscore: float = Field(default=0.0, alias="MEAN_REVERSION_EXIT_ZSCORE")
    breakout_entry_window: int = Field(default=55, alias="BREAKOUT_ENTRY_WINDOW")
    breakout_exit_window: int = Field(default=20, alias="BREAKOUT_EXIT_WINDOW")
    breakout_atr_window: int = Field(default=20, alias="BREAKOUT_ATR_WINDOW")
    atr_risk_budget: float = Field(default=100, alias="ATR_RISK_BUDGET")
    risk_per_trade_fraction: float = Field(default=0.01, alias="RISK_PER_TRADE_FRACTION")
    min_average_daily_volume: float = Field(default=500_000, alias="MIN_AVERAGE_DAILY_VOLUME")
    max_atr_ratio: float = Field(default=0.12, alias="MAX_ATR_RATIO")
    lookback_days: int = Field(default=400, alias="LOOKBACK_DAYS")
    min_history_days: int = Field(default=120, alias="MIN_HISTORY_DAYS")
    allow_unsafe_data_fallback: bool = Field(default=False, alias="ALLOW_UNSAFE_DATA_FALLBACK")
    allow_partial_market_data: bool = Field(default=False, alias="ALLOW_PARTIAL_MARKET_DATA")
    force_exit_symbols: str = Field(default="", alias="FORCE_EXIT_SYMBOLS")
    emergency_flatten: bool = Field(default=False, alias="EMERGENCY_FLATTEN")
    deny_new_entries: bool = Field(default=False, alias="DENY_NEW_ENTRIES")
    alert_on_blocked_orders: bool = Field(default=True, alias="ALERT_ON_BLOCKED_ORDERS")
    alert_on_reconciliation_drift: bool = Field(default=True, alias="ALERT_ON_RECONCILIATION_DRIFT")
    alert_on_drawdown_breach: bool = Field(default=True, alias="ALERT_ON_DRAWDOWN_BREACH")
    alert_on_stale_data: bool = Field(default=True, alias="ALERT_ON_STALE_DATA")
    alert_webhook_url: str = Field(default="", alias="ALERT_WEBHOOK_URL")
    alert_webhook_timeout_seconds: float = Field(default=5.0, alias="ALERT_WEBHOOK_TIMEOUT_SECONDS")
    politician_copy_base_url: str = Field(
        default="https://www.capitoltrades.com",
        alias="POLITICIAN_COPY_BASE_URL",
    )
    politician_copy_scrape_timeout_seconds: float = Field(
        default=10.0,
        alias="POLITICIAN_COPY_SCRAPE_TIMEOUT_SECONDS",
    )
    politician_copy_user_agent: str = Field(
        default="simple-ai-trading/0.1 politician-copy",
        alias="POLITICIAN_COPY_USER_AGENT",
    )
    politician_copy_candidate_pages: int = Field(
        default=3,
        alias="POLITICIAN_COPY_CANDIDATE_PAGES",
    )
    politician_copy_max_profile_pages: int = Field(
        default=4,
        alias="POLITICIAN_COPY_MAX_PROFILE_PAGES",
    )
    politician_copy_ranking_lookback_days: int = Field(
        default=180,
        alias="POLITICIAN_COPY_RANKING_LOOKBACK_DAYS",
    )
    politician_copy_min_disclosures_per_politician: int = Field(
        default=2,
        alias="POLITICIAN_COPY_MIN_DISCLOSURES_PER_POLITICIAN",
    )
    politician_copy_num_politicians: int = Field(
        default=3,
        alias="POLITICIAN_COPY_NUM_POLITICIANS",
    )
    politician_copy_holding_window_days: int = Field(
        default=90,
        alias="POLITICIAN_COPY_HOLDING_WINDOW_DAYS",
    )
    politician_copy_max_disclosure_lag_days: int = Field(
        default=45,
        alias="POLITICIAN_COPY_MAX_DISCLOSURE_LAG_DAYS",
    )
    politician_copy_recency_half_life_days: float = Field(
        default=30.0,
        alias="POLITICIAN_COPY_RECENCY_HALF_LIFE_DAYS",
    )
    politician_copy_max_symbol_weight: float = Field(
        default=0.25,
        alias="POLITICIAN_COPY_MAX_SYMBOL_WEIGHT",
    )
    politician_copy_min_target_weight: float = Field(
        default=0.02,
        alias="POLITICIAN_COPY_MIN_TARGET_WEIGHT",
    )
    politician_copy_symbol_allowlist: str = Field(
        default="",
        alias="POLITICIAN_COPY_SYMBOL_ALLOWLIST",
    )
    politician_copy_symbol_blocklist: str = Field(
        default="",
        alias="POLITICIAN_COPY_SYMBOL_BLOCKLIST",
    )
    politician_copy_preview_limit: int = Field(
        default=10,
        alias="POLITICIAN_COPY_PREVIEW_LIMIT",
    )

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

    @property
    def trading_mode_enabled(self) -> bool:
        return not self.dry_run

    @property
    def live_trading_enabled(self) -> bool:
        return not self.alpaca_paper

    @property
    def politician_copy_symbol_allowlist_set(self) -> set[str]:
        return {
            symbol.strip().upper()
            for symbol in self.politician_copy_symbol_allowlist.split(",")
            if symbol.strip()
        }

    @property
    def politician_copy_symbol_blocklist_set(self) -> set[str]:
        return {
            symbol.strip().upper()
            for symbol in self.politician_copy_symbol_blocklist.split(",")
            if symbol.strip()
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
