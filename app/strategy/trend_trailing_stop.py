from __future__ import annotations

import pandas as pd

from app.config import Settings


class TrendTrailingStopStrategy:
    name = "trend_trailing_stop"

    def generate_signals(self, bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        frame = bars.copy()
        frame["prev_close"] = frame.groupby("symbol")["close"].shift(1)
        frame["trend_ma"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.trend_trailing_trend_window,
                min_periods=settings.trend_trailing_trend_window,
            ).mean()
        )
        frame["pullback_fast_ma"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.trend_trailing_pullback_fast_window,
                min_periods=settings.trend_trailing_pullback_fast_window,
            ).mean()
        )
        frame["pullback_slow_ma"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.trend_trailing_pullback_slow_window,
                min_periods=settings.trend_trailing_pullback_slow_window,
            ).mean()
        )
        frame["breakout_high"] = frame.groupby("symbol")["high"].transform(
            lambda series: series.shift(1).rolling(
                settings.trend_trailing_breakout_window,
                min_periods=settings.trend_trailing_breakout_window,
            ).max()
        )
        true_range = pd.concat(
            [
                frame["high"] - frame["low"],
                (frame["high"] - frame["prev_close"]).abs(),
                (frame["low"] - frame["prev_close"]).abs(),
            ],
            axis=1,
        ).max(axis=1)
        frame["atr"] = true_range.groupby(frame["symbol"]).transform(
            lambda series: series.rolling(
                settings.trend_trailing_atr_window,
                min_periods=settings.trend_trailing_atr_window,
            ).mean()
        )
        frame["avg_volume"] = frame.groupby("symbol")["volume"].transform(
            lambda series: series.rolling(
                settings.trend_trailing_atr_window,
                min_periods=settings.trend_trailing_atr_window,
            ).mean()
        )
        frame["atr_ratio"] = frame["atr"] / frame["close"]
        frame["liquidity_ok"] = frame["avg_volume"] >= settings.min_average_daily_volume
        frame["volatility_ok"] = frame["atr_ratio"] <= settings.max_atr_ratio
        frame["rolling_peak_high"] = frame.groupby("symbol")["high"].cummax()
        frame["atr_trailing_stop"] = (
            frame["rolling_peak_high"] - settings.trend_trailing_atr_multiplier * frame["atr"]
        )
        frame["percent_trailing_stop"] = (
            frame["rolling_peak_high"] * (1.0 - settings.trend_trailing_percent)
        )

        stop_type = settings.trend_trailing_stop_type.strip().lower()
        if stop_type not in {"atr", "percent"}:
            raise ValueError(
                "TREND_TRAILING_STOP_TYPE must be 'atr' or 'percent'"
            )
        stop_column = "atr_trailing_stop" if stop_type == "atr" else "percent_trailing_stop"
        frame["active_trailing_stop"] = frame[stop_column]

        trend_ok = frame["close"] > frame["trend_ma"]
        frame["breakout_entry"] = trend_ok & (frame["close"] > frame["breakout_high"])
        frame["pullback_entry"] = (
            trend_ok
            & (frame["close"] <= frame["pullback_fast_ma"])
            & (frame["close"] >= frame["pullback_slow_ma"])
            & (frame["pullback_fast_ma"] >= frame["pullback_slow_ma"])
        )
        frame["entry_type"] = "none"
        frame.loc[frame["breakout_entry"], "entry_type"] = "breakout"
        frame.loc[frame["pullback_entry"] & ~frame["breakout_entry"], "entry_type"] = "pullback"

        breakout_score = ((frame["close"] - frame["breakout_high"]) / frame["atr"]).fillna(0.0)
        pullback_score = ((frame["pullback_fast_ma"] - frame["close"]) / frame["atr"]).fillna(0.0)
        trend_score = ((frame["close"] - frame["trend_ma"]) / frame["atr"]).fillna(0.0)
        frame["score"] = trend_score + breakout_score + 0.5 * pullback_score

        frame["signal"] = "flat"
        frame.loc[
            (frame["breakout_entry"] | frame["pullback_entry"])
            & frame["liquidity_ok"]
            & frame["volatility_ok"],
            "signal",
        ] = "long"
        frame.loc[frame["close"] < frame["active_trailing_stop"], "signal"] = "exit"
        return frame


trend_trailing_stop_strategy = TrendTrailingStopStrategy()
