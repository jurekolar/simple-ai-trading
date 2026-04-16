from __future__ import annotations

import pandas as pd


def equity_curve(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["timestamp", "equity"])
    frame = trades.copy()
    frame["equity"] = (frame["qty"] * frame["close"]).cumsum()
    return frame[["timestamp", "equity"]]
