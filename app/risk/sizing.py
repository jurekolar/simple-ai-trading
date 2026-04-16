from __future__ import annotations

import pandas as pd


def notional_exposure(trades: pd.DataFrame) -> float:
    if trades.empty:
        return 0.0
    return float((trades["qty"] * trades["close"]).sum())
