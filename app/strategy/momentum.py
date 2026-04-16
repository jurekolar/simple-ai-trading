from __future__ import annotations

import pandas as pd

from app.config import Settings


def generate_signals(bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    frame = bars.copy()
    frame["prev_close"] = frame.groupby("symbol")["close"].shift(1)
    frame["trend_ma"] = frame.groupby("symbol")["close"].transform(
        lambda series: series.rolling(settings.trend_window, min_periods=settings.trend_window).mean()
    )
    frame["exit_ma"] = frame.groupby("symbol")["close"].transform(
        lambda series: series.rolling(settings.exit_window, min_periods=settings.exit_window).mean()
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
        lambda series: series.rolling(settings.atr_window, min_periods=settings.atr_window).mean()
    )
    frame["avg_volume"] = frame.groupby("symbol")["volume"].transform(
        lambda series: series.rolling(settings.atr_window, min_periods=settings.atr_window).mean()
    )
    frame["atr_ratio"] = frame["atr"] / frame["close"]
    frame["liquidity_ok"] = frame["avg_volume"] >= settings.min_average_daily_volume
    frame["volatility_ok"] = frame["atr_ratio"] <= settings.max_atr_ratio
    frame["score"] = (
        ((frame["close"] - frame["trend_ma"]) / frame["atr"]).fillna(0.0)
        + 0.5 * ((frame["close"] - frame["exit_ma"]) / frame["atr"]).fillna(0.0)
    )
    frame["signal"] = "flat"
    frame.loc[
        (frame["close"] > frame["trend_ma"]) & frame["liquidity_ok"] & frame["volatility_ok"],
        "signal",
    ] = "long"
    frame.loc[frame["close"] < frame["exit_ma"], "signal"] = "exit"
    return frame
