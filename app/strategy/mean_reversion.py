from __future__ import annotations

import pandas as pd

from app.config import Settings


class MeanReversionStrategy:
    name = "mean_reversion"

    def generate_signals(self, bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        frame = bars.copy()
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        benchmark_symbol = settings.mean_reversion_benchmark_symbol.strip().upper()
        if not benchmark_symbol:
            raise ValueError("MEAN_REVERSION_BENCHMARK_SYMBOL must be configured")
        if benchmark_symbol not in set(frame["symbol"].unique()):
            raise ValueError(f"missing benchmark bars for mean_reversion benchmark={benchmark_symbol}")

        frame["prev_close"] = frame.groupby("symbol")["close"].shift(1)
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
        frame["trend_ma"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.rolling(
                settings.mean_reversion_trend_window,
                min_periods=settings.mean_reversion_trend_window,
            ).mean()
        )
        frame["stock_return_rs"] = frame.groupby("symbol")["close"].transform(
            lambda series: series.pct_change(periods=settings.mean_reversion_rs_window)
        )
        benchmark_returns = (
            frame.loc[frame["symbol"] == benchmark_symbol, ["timestamp", "stock_return_rs"]]
            .rename(columns={"stock_return_rs": "benchmark_return_rs"})
            .drop_duplicates(subset="timestamp", keep="last")
        )
        if benchmark_returns.empty:
            raise ValueError(f"missing benchmark return series for mean_reversion benchmark={benchmark_symbol}")
        frame = frame.merge(benchmark_returns, on="timestamp", how="left")
        frame["rs_delta"] = frame["stock_return_rs"] - frame["benchmark_return_rs"]

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
                settings.mean_reversion_atr_window,
                min_periods=settings.mean_reversion_atr_window,
            ).mean()
        )
        frame["avg_volume"] = frame.groupby("symbol")["volume"].transform(
            lambda series: series.rolling(
                settings.mean_reversion_volume_window,
                min_periods=settings.mean_reversion_volume_window,
            ).mean()
        )
        frame["liquidity_ok"] = frame["avg_volume"] >= settings.min_average_daily_volume
        frame["tradeable_symbol"] = frame["symbol"].isin(settings.symbol_list)
        frame["signal"] = "flat"
        frame.loc[
            (frame["z_score"] <= settings.mean_reversion_entry_zscore)
            & (
                frame["rs_delta"]
                <= settings.mean_reversion_relative_weakness_threshold
            )
            & (frame["close"] > frame["trend_ma"])
            & frame["liquidity_ok"]
            & frame["tradeable_symbol"],
            "signal",
        ] = "long"
        frame.loc[
            (frame["z_score"] >= settings.mean_reversion_exit_zscore) & frame["tradeable_symbol"],
            "signal",
        ] = "exit"
        threshold = abs(settings.mean_reversion_relative_weakness_threshold)
        if threshold <= 0:
            raise ValueError("MEAN_REVERSION_RELATIVE_WEAKNESS_THRESHOLD must be non-zero")
        frame["score"] = (
            (-frame["z_score"]).fillna(0.0)
            + (-(frame["rs_delta"]) / threshold).fillna(0.0)
        )
        frame["atr"] = frame["atr"].fillna(0.0)
        return frame


mean_reversion_strategy = MeanReversionStrategy()
