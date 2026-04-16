from __future__ import annotations

import pandas as pd


def latest_signals(signal_frame: pd.DataFrame) -> pd.DataFrame:
    return (
        signal_frame.sort_values("timestamp")
        .groupby("symbol", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
