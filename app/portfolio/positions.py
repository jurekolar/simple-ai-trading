from __future__ import annotations

import pandas as pd


def current_positions(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=["symbol", "qty"])
    signed = orders.assign(
        signed_qty=orders.apply(lambda row: row["qty"] if row["side"] == "buy" else -row["qty"], axis=1)
    )
    return signed.groupby("symbol", as_index=False)["signed_qty"].sum().rename(columns={"signed_qty": "qty"})
