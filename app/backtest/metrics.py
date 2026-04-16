from __future__ import annotations

import pandas as pd


def summarize(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {"trades": 0.0, "gross_exposure": 0.0}
    return {
        "trades": float(len(trades)),
        "gross_exposure": float((trades["qty"] * trades["fill_price"]).sum()),
    }
