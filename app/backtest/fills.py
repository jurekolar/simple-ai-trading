from __future__ import annotations

import pandas as pd


def apply_slippage(trades: pd.DataFrame, bps: float = 5.0) -> pd.DataFrame:
    frame = trades.copy()
    if frame.empty:
        return frame
    frame["fill_price"] = frame["close"] * (1 + bps / 10_000)
    return frame


def fill_price_for_side(price: float, side: str, bps: float = 5.0) -> float:
    direction = 1.0 if side.lower() == "buy" else -1.0
    return float(price * (1 + direction * bps / 10_000))
