from __future__ import annotations

import pandas as pd

from app.config import Settings


def generate_signals(bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    frame = bars.copy()
    frame["trend_ma"] = frame.groupby("symbol")["close"].transform(
        lambda series: series.rolling(settings.trend_window, min_periods=settings.trend_window).mean()
    )
    frame["exit_ma"] = frame.groupby("symbol")["close"].transform(
        lambda series: series.rolling(settings.exit_window, min_periods=settings.exit_window).mean()
    )
    true_range = (
        frame[["high", "close"]].max(axis=1) - frame[["low", "close"]].min(axis=1)
    )
    frame["atr"] = true_range.groupby(frame["symbol"]).transform(
        lambda series: series.rolling(settings.atr_window, min_periods=settings.atr_window).mean()
    )
    frame["signal"] = "flat"
    frame.loc[frame["close"] > frame["trend_ma"], "signal"] = "long"
    frame.loc[frame["close"] < frame["exit_ma"], "signal"] = "exit"
    return frame
