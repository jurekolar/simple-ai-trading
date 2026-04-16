from __future__ import annotations

import pandas as pd

from app.config import Settings


class MeanReversionStrategy:
    name = "mean_reversion"

    def generate_signals(self, bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        frame = bars.copy()
        frame["mean_ma"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.mean_reversion_window,
                min_periods=settings.mean_reversion_window,
            ).mean()
        )
        frame["stddev"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.mean_reversion_window,
                min_periods=settings.mean_reversion_window,
            ).std()
        )
        frame["z_score"] = ((frame["close"] - frame["mean_ma"]) / frame["stddev"]).replace(
            [float("inf"), float("-inf")],
            0.0,
        )
        frame["avg_volume"] = frame.groupby("symbol")["volume"].transform(
            lambda series: series.rolling(
                settings.mean_reversion_volatility_window,
                min_periods=settings.mean_reversion_volatility_window,
            ).mean()
        )
        frame["liquidity_ok"] = frame["avg_volume"] >= settings.min_average_daily_volume
        frame["signal"] = "flat"
        frame.loc[
            (frame["z_score"] <= settings.mean_reversion_entry_zscore) & frame["liquidity_ok"],
            "signal",
        ] = "long"
        frame.loc[frame["z_score"] >= settings.mean_reversion_exit_zscore, "signal"] = "exit"
        frame["score"] = (-frame["z_score"]).fillna(0.0)
        frame["atr"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.mean_reversion_volatility_window,
                min_periods=settings.mean_reversion_volatility_window,
            ).std()
        ).fillna(0.0)
        return frame


mean_reversion_strategy = MeanReversionStrategy()
