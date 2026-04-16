from __future__ import annotations

import pandas as pd


def apply_slippage(trades: pd.DataFrame, bps: float = 5.0) -> pd.DataFrame:
    frame = trades.copy()
    if frame.empty:
        return frame
    frame["fill_price"] = frame["close"] * (1 + bps / 10_000)
    return frame
