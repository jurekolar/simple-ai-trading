from __future__ import annotations

import pandas as pd

from app.config import Settings


class BreakoutStrategy:
    name = "breakout"

    def generate_signals(self, bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        frame = bars.copy()
        frame["prev_close"] = frame.groupby("symbol")["close"].shift(1)
        # Use prior-bar channel levels so the Donchian breakout stays free of lookahead bias.
        frame["entry_high"] = frame.groupby("symbol")["high"].transform(
            lambda series: series.shift(1).rolling(
                settings.breakout_entry_window,
                min_periods=settings.breakout_entry_window,
            ).max()
        )
        frame["exit_low"] = frame.groupby("symbol")["low"].transform(
            lambda series: series.shift(1).rolling(
                settings.breakout_exit_window,
                min_periods=settings.breakout_exit_window,
            ).min()
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
                settings.breakout_atr_window,
                min_periods=settings.breakout_atr_window,
            ).mean()
        )
        frame["avg_volume"] = frame.groupby("symbol")["volume"].transform(
            lambda series: series.rolling(
                settings.breakout_atr_window,
                min_periods=settings.breakout_atr_window,
            ).mean()
        )
        frame["atr_ratio"] = frame["atr"] / frame["close"]
        frame["liquidity_ok"] = frame["avg_volume"] >= settings.min_average_daily_volume
        frame["volatility_ok"] = frame["atr_ratio"] <= settings.max_atr_ratio
        frame["score"] = ((frame["close"] - frame["entry_high"]) / frame["atr"]).fillna(0.0)
        frame["signal"] = "flat"
        frame.loc[
            (frame["close"] > frame["entry_high"]) & frame["liquidity_ok"] & frame["volatility_ok"],
            "signal",
        ] = "long"
        # Breakout stays long-only; a lower-channel breach closes an existing long instead of going short.
        frame.loc[frame["close"] < frame["exit_low"], "signal"] = "exit"
        return frame


breakout_strategy = BreakoutStrategy()
