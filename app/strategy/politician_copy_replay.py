from __future__ import annotations

import json
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from app.backtest.metrics import summarize
from app.config import Settings
from app.data.alpaca_data import AlpacaDataClient
from app.data.capitol_trades import CapitolTradeDisclosure, CapitolTradesClient
from app.strategy.politician_copy import (
    AllocationPlan,
    AllocationOrder,
    PoliticianCopyStrategy,
    politician_copy_strategy,
)

LATEST_POLITICIAN_COPY_REPLAY_FILENAME = "latest_politician_copy_replay.json"


@dataclass(frozen=True)
class PoliticianCopyReplayResult:
    trade_log: pd.DataFrame
    equity_curve: pd.DataFrame
    decision_log: pd.DataFrame
    summary: dict[str, float | str | bool]


def load_politician_copy_replay_inputs(
    settings: Settings,
    *,
    trades_client: CapitolTradesClient | None = None,
    data_client: AlpacaDataClient | None = None,
) -> tuple[list[CapitolTradeDisclosure], pd.DataFrame, str, bool]:
    trades_client = trades_client or CapitolTradesClient(settings)
    data_client = data_client or AlpacaDataClient(settings)
    candidates = trades_client.fetch_politician_candidates()
    disclosures: list[CapitolTradeDisclosure] = []
    for candidate in candidates:
        disclosures.extend(trades_client.fetch_recent_disclosures(candidate))
    symbols = sorted({disclosure.symbol.upper() for disclosure in disclosures if disclosure.symbol})
    if not symbols:
        return disclosures, pd.DataFrame(columns=["timestamp", "symbol", "close", "volume"]), "alpaca", False
    earliest_published_at = min(disclosure.published_at for disclosure in disclosures)
    lookback_days = max((datetime.now(UTC) - earliest_published_at).days + 30, 30)
    loaded = data_client.get_daily_bars(symbols, lookback_days)
    bars = loaded.bars.copy()
    if not bars.empty:
        bars["symbol"] = bars["symbol"].astype(str).str.upper()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
        bars = bars.dropna(subset=["timestamp", "close"]).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    return disclosures, bars, loaded.source, loaded.production_safe


def build_point_in_time_allocation_plan(
    *,
    disclosures: list[CapitolTradeDisclosure],
    price_frame: pd.DataFrame,
    settings: Settings,
    account_equity: float,
    position_qty_by_symbol: dict[str, float],
    as_of: datetime,
    strategy: PoliticianCopyStrategy = politician_copy_strategy,
    source: str = "historical_replay",
    production_safe: bool = True,
) -> AllocationPlan:
    plan, _ = _build_point_in_time_plan_state(
        disclosures=disclosures,
        price_frame=price_frame,
        settings=settings,
        account_equity=account_equity,
        position_qty_by_symbol=position_qty_by_symbol,
        as_of=as_of,
        strategy=strategy,
        source=source,
        production_safe=production_safe,
    )
    return plan


def run_politician_copy_replay(
    *,
    disclosures: list[CapitolTradeDisclosure],
    price_frame: pd.DataFrame,
    settings: Settings,
    strategy: PoliticianCopyStrategy = politician_copy_strategy,
    rebalance_dates: list[datetime] | None = None,
    rebalance_frequency: str | None = None,
    initial_equity: float | None = None,
    slippage_bps: float | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    source: str = "historical_replay",
    production_safe: bool = True,
) -> PoliticianCopyReplayResult:
    frame = price_frame.copy()
    if frame.empty:
        summary = {
            "strategy": strategy.name,
            "source": source,
            "replay_valid": False,
            "replay_invalid_reasons": "missing_price_history",
        }
        return PoliticianCopyReplayResult(
            trade_log=pd.DataFrame(),
            equity_curve=pd.DataFrame(),
            decision_log=pd.DataFrame(),
            summary=summary,
        )

    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"]).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    trading_dates = pd.Index(sorted(frame["timestamp"].unique()))
    effective_start = pd.Timestamp(start_at, tz=UTC) if start_at is not None else pd.Timestamp(trading_dates[0])
    effective_end = pd.Timestamp(end_at, tz=UTC) if end_at is not None else pd.Timestamp(trading_dates[-1])
    trading_dates = trading_dates[(trading_dates >= effective_start) & (trading_dates <= effective_end)]
    if trading_dates.empty:
        summary = {
            "strategy": strategy.name,
            "source": source,
            "replay_valid": False,
            "replay_invalid_reasons": "no_trading_dates_in_range",
        }
        return PoliticianCopyReplayResult(
            trade_log=pd.DataFrame(),
            equity_curve=pd.DataFrame(),
            decision_log=pd.DataFrame(),
            summary=summary,
        )

    rebalance_frequency = rebalance_frequency or settings.politician_copy_rebalance_frequency
    initial_equity = float(initial_equity or settings.politician_copy_replay_initial_equity)
    slippage_bps = float(slippage_bps if slippage_bps is not None else settings.politician_copy_replay_slippage_bps)
    aligned_rebalance_dates = _resolve_rebalance_dates(
        trading_dates=trading_dates,
        requested_dates=rebalance_dates,
        frequency=rebalance_frequency,
    )
    price_groups = {symbol: group.reset_index(drop=True) for symbol, group in frame.groupby("symbol")}
    positions: dict[str, deque[dict[str, object]]] = defaultdict(deque)
    cash = initial_equity
    trade_rows: list[dict[str, object]] = []
    decision_rows: list[dict[str, object]] = []
    invalid_reasons: list[str] = []

    for rebalance_at in aligned_rebalance_dates:
        account_equity = _mark_to_market_equity(cash=cash, positions=positions, price_groups=price_groups, as_of=rebalance_at)
        position_qty_by_symbol = {
            symbol: float(sum(float(lot["qty"]) for lot in lots))
            for symbol, lots in positions.items()
            if sum(float(lot["qty"]) for lot in lots) > 0
        }
        plan, plan_state = _build_point_in_time_plan_state(
            disclosures=disclosures,
            price_frame=frame,
            settings=settings,
            account_equity=account_equity,
            position_qty_by_symbol=position_qty_by_symbol,
            as_of=rebalance_at.to_pydatetime(),
            strategy=strategy,
            source=source,
            production_safe=production_safe,
        )
        if not plan.production_safe and "unsafe_replay_source" not in invalid_reasons:
            invalid_reasons.append("unsafe_replay_source")
        decision_rows.append(
            {
                "rebalance_at": rebalance_at,
                "visible_disclosures": float(plan_state["visible_disclosures"]),
                "accepted_disclosures": float(plan_state["accepted_disclosures"]),
                "rejected_disclosures": float(plan_state["rejected_disclosures"]),
                "selected_politicians": float(len(plan.selected_politicians)),
                "targets": float(len(plan.target_allocations)),
                "orders": float(len(plan.planned_orders)),
                "avg_filing_lag_days": float(plan_state["avg_filing_lag_days"]),
                "source": plan.source,
                "production_safe": plan.production_safe,
                "rejected_reason_counts": json.dumps(plan_state["rejected_reason_counts"], sort_keys=True),
            }
        )

        for order in plan.planned_orders:
            execution_price, execution_at = _next_execution_price(
                price_groups=price_groups,
                symbol=order.symbol,
                after=rebalance_at,
            )
            if execution_price <= 0 or execution_at is None:
                invalid_reasons.append(f"missing_execution_price:{order.symbol}")
                continue
            fill_price = execution_price * (1 + (slippage_bps / 10_000)) if order.side == "buy" else execution_price * (1 - (slippage_bps / 10_000))
            if fill_price <= 0:
                invalid_reasons.append(f"invalid_execution_price:{order.symbol}")
                continue
            qty = int(order.qty)
            if order.side == "buy":
                qty = min(qty, int(cash // fill_price))
                if qty <= 0:
                    continue
                notional = float(qty * fill_price)
                cash -= notional
                positions[order.symbol].append(
                    {
                        "qty": float(qty),
                        "price": float(fill_price),
                        "filled_at": execution_at.to_pydatetime(),
                    }
                )
                trade_rows.append(
                    {
                        "timestamp": execution_at,
                        "rebalance_at": rebalance_at,
                        "symbol": order.symbol,
                        "side": "buy",
                        "signal": "long",
                        "qty": float(qty),
                        "close": float(execution_price),
                        "fill_price": float(fill_price),
                        "notional": notional,
                        "realized_pnl": 0.0,
                        "holding_days": 0.0,
                        "target_weight": float(order.target_weight),
                        "reason": order.reason,
                    }
                )
                continue

            available_qty = int(sum(float(lot["qty"]) for lot in positions.get(order.symbol, deque())))
            qty = min(qty, available_qty)
            if qty <= 0:
                continue
            remaining = float(qty)
            realized_pnl = 0.0
            weighted_holding_days = 0.0
            while remaining > 0 and positions[order.symbol]:
                lot = positions[order.symbol][0]
                matched_qty = min(remaining, float(lot["qty"]))
                realized_pnl += matched_qty * (fill_price - float(lot["price"]))
                holding_days = max((execution_at.to_pydatetime() - lot["filled_at"]).total_seconds(), 0.0) / 86400
                weighted_holding_days += matched_qty * holding_days
                remaining -= matched_qty
                if matched_qty == float(lot["qty"]):
                    positions[order.symbol].popleft()
                else:
                    lot["qty"] = float(lot["qty"]) - matched_qty
            if not positions[order.symbol]:
                positions.pop(order.symbol, None)
            notional = float(qty * fill_price)
            cash += notional
            trade_rows.append(
                {
                    "timestamp": execution_at,
                    "rebalance_at": rebalance_at,
                    "symbol": order.symbol,
                    "side": "sell",
                    "signal": "exit",
                    "qty": float(qty),
                    "close": float(execution_price),
                    "fill_price": float(fill_price),
                    "notional": notional,
                    "realized_pnl": float(realized_pnl),
                    "holding_days": float(weighted_holding_days / qty) if qty > 0 else 0.0,
                    "target_weight": float(order.target_weight),
                    "reason": order.reason,
                }
            )

    equity_curve = _build_equity_curve(
        trading_dates=trading_dates,
        positions=positions,
        price_groups=price_groups,
        cash=cash,
        trade_rows=trade_rows,
    )
    trade_log = pd.DataFrame(trade_rows).sort_values(["timestamp", "symbol", "side"]).reset_index(drop=True) if trade_rows else pd.DataFrame(
        columns=["timestamp", "rebalance_at", "symbol", "side", "signal", "qty", "close", "fill_price", "notional", "realized_pnl", "holding_days", "target_weight", "reason"]
    )
    decision_log = pd.DataFrame(decision_rows).sort_values("rebalance_at").reset_index(drop=True) if decision_rows else pd.DataFrame()

    metrics = summarize(trade_log, equity_curve, initial_equity=initial_equity)
    rejected_counts = _aggregate_rejected_reason_counts(decision_log)
    total_rejected = float(sum(rejected_counts.values()))
    summary: dict[str, float | str | bool] = {
        "strategy": strategy.name,
        "source": source,
        "replay_valid": source == "alpaca" and not invalid_reasons,
        "replay_invalid_reasons": ",".join(dict.fromkeys(invalid_reasons)) if invalid_reasons else "",
        "rebalance_frequency": rebalance_frequency,
        "rebalances": float(len(decision_log)),
        "avg_filing_lag_days": float(decision_log["avg_filing_lag_days"].mean()) if not decision_log.empty else 0.0,
        "symbol_concentration": _symbol_concentration(trade_log),
        "accepted_disclosures_avg": float(decision_log["accepted_disclosures"].mean()) if not decision_log.empty else 0.0,
        "rejected_disclosures_total": total_rejected,
    }
    summary.update(metrics)
    for reason, count in sorted(rejected_counts.items()):
        summary[f"rejected_pct_{reason}"] = float(count / total_rejected) if total_rejected > 0 else 0.0
    return PoliticianCopyReplayResult(
        trade_log=trade_log,
        equity_curve=equity_curve,
        decision_log=decision_log,
        summary=summary,
    )


def format_politician_copy_replay_summary(summary: dict[str, float | str | bool]) -> str:
    warnings: list[str] = []
    if summary.get("source") != "alpaca":
        warnings.append(f"unsafe_source={summary.get('source')}")
    if not bool(summary.get("replay_valid", False)):
        warnings.append(f"replay_invalid={summary.get('replay_invalid_reasons') or 'unspecified'}")
    warning_block = "\n".join(f"warning={warning}" for warning in warnings) if warnings else "warning=none"
    rejected_cols = sorted(key for key in summary if str(key).startswith("rejected_pct_"))
    rejected_summary = ", ".join(
        f"{column.removeprefix('rejected_pct_')}={100 * float(summary[column]):.1f}%"
        for column in rejected_cols
    ) or "none"
    return "\n".join(
        [
            "Politician Copy Replay",
            f"strategy={summary.get('strategy', 'politician_copy')}",
            f"source={summary.get('source', 'unknown')}",
            f"replay_valid={summary.get('replay_valid', False)}",
            warning_block,
            f"range={summary.get('start_at', '')} -> {summary.get('end_at', '')}",
            f"rebalances={float(summary.get('rebalances', 0.0)):.0f}",
            f"trades={float(summary.get('trades', 0.0)):.0f}",
            f"closed_trades={float(summary.get('closed_trades', 0.0)):.0f}",
            f"total_return={100 * float(summary.get('total_return', 0.0)):.2f}%",
            f"annualized_return={100 * float(summary.get('annualized_return', 0.0)):.2f}%",
            f"max_drawdown={100 * float(summary.get('max_drawdown', 0.0)):.2f}%",
            f"risk_adjusted_score={float(summary.get('risk_adjusted_score', 0.0)):.4f}",
            f"turnover={float(summary.get('turnover', 0.0)):.4f}",
            f"avg_filing_lag_days={float(summary.get('avg_filing_lag_days', 0.0)):.2f}",
            f"symbol_concentration={float(summary.get('symbol_concentration', 0.0)):.4f}",
            f"rejected_reason_mix={rejected_summary}",
        ]
    )


def write_politician_copy_replay_artifacts(
    result: PoliticianCopyReplayResult,
    *,
    settings: Settings,
    disclosures: list[CapitolTradeDisclosure],
) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    artifact_dir = Path(settings.backtest_output_dir) / "politician_copy_replays" / timestamp
    artifact_dir.mkdir(parents=True, exist_ok=True)
    summary_frame = pd.DataFrame([result.summary])
    summary_frame.to_csv(artifact_dir / "summary.csv", index=False)
    (artifact_dir / "summary.json").write_text(json.dumps(result.summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    result.trade_log.to_csv(artifact_dir / "trade_log.csv", index=False)
    result.equity_curve.to_csv(artifact_dir / "equity_curve.csv", index=False)
    result.decision_log.to_csv(artifact_dir / "decision_log.csv", index=False)
    disclosure_frame = pd.DataFrame(
        [
            {
                "politician_id": disclosure.politician_id,
                "politician_name": disclosure.politician_name,
                "trade_date": disclosure.trade_date.isoformat(),
                "published_at": disclosure.published_at.isoformat(),
                "symbol": disclosure.symbol,
                "asset_type": disclosure.asset_type,
                "side": disclosure.side,
                "amount_bucket": disclosure.amount_bucket,
                "amount_midpoint": disclosure.amount_midpoint,
                "source_url": disclosure.source_url,
                "filing_delay_days": disclosure.filing_delay_days,
            }
            for disclosure in disclosures
        ]
    )
    disclosure_frame.to_csv(artifact_dir / "disclosures.csv", index=False)
    metadata = {
        "strategy": "politician_copy",
        "generated_at": datetime.now(UTC).isoformat(),
        "source": result.summary.get("source"),
        "replay_valid": result.summary.get("replay_valid"),
        "replay_invalid_reasons": result.summary.get("replay_invalid_reasons"),
        "rebalance_frequency": settings.politician_copy_rebalance_frequency,
        "initial_equity": settings.politician_copy_replay_initial_equity,
        "slippage_bps": settings.politician_copy_replay_slippage_bps,
        "disclosures": len(disclosures),
        "artifact_dir": str(artifact_dir),
    }
    (artifact_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    latest_index = {
        "generated_at": metadata["generated_at"],
        "artifact_dir": str(artifact_dir),
        "strategy": "politician_copy",
        "source": result.summary.get("source"),
        "replay_valid": result.summary.get("replay_valid"),
        "replay_invalid_reasons": result.summary.get("replay_invalid_reasons"),
    }
    latest_path = Path(settings.backtest_output_dir) / LATEST_POLITICIAN_COPY_REPLAY_FILENAME
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest_index, indent=2, sort_keys=True), encoding="utf-8")
    return artifact_dir


def _build_point_in_time_plan_state(
    *,
    disclosures: list[CapitolTradeDisclosure],
    price_frame: pd.DataFrame,
    settings: Settings,
    account_equity: float,
    position_qty_by_symbol: dict[str, float],
    as_of: datetime,
    strategy: PoliticianCopyStrategy,
    source: str,
    production_safe: bool,
) -> tuple[AllocationPlan, dict[str, object]]:
    as_of = as_of.astimezone(UTC)
    visible_disclosures = [disclosure for disclosure in disclosures if disclosure.published_at <= as_of]
    filtered_disclosures, rejected = strategy._filter_disclosures(visible_disclosures, settings=settings, as_of=as_of)
    frame = price_frame.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"])
    frame = frame[frame["timestamp"] <= pd.Timestamp(as_of)].sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    symbols_for_prices = sorted(
        {disclosure.symbol for disclosure in filtered_disclosures} | {symbol.upper() for symbol in position_qty_by_symbol}
    )
    available_symbols = set(frame["symbol"].astype(str).str.upper()) if not frame.empty else set()
    plan_production_safe = production_safe and all(symbol in available_symbols for symbol in symbols_for_prices)
    scores = strategy._score_politicians(filtered_disclosures, price_frame=frame, settings=settings, as_of=as_of)
    selected = tuple(scores[: max(settings.politician_copy_num_politicians, 1)])
    targets = strategy._build_targets(
        filtered_disclosures,
        selected=selected,
        price_frame=frame,
        account_equity=account_equity,
        settings=settings,
        as_of=as_of,
    )
    planned_orders = strategy._plan_orders(targets, position_qty_by_symbol=position_qty_by_symbol, price_frame=frame)
    plan = AllocationPlan(
        selected_politicians=selected,
        politician_scores=tuple(scores),
        target_allocations=targets,
        planned_orders=planned_orders,
        rejected_disclosures=tuple(rejected),
        source=source,
        production_safe=plan_production_safe,
        account_equity=account_equity,
    )
    return plan, {
        "visible_disclosures": len(visible_disclosures),
        "accepted_disclosures": len(filtered_disclosures),
        "rejected_disclosures": len(rejected),
        "avg_filing_lag_days": (
            sum(disclosure.filing_delay_days for disclosure in filtered_disclosures) / len(filtered_disclosures)
            if filtered_disclosures
            else 0.0
        ),
        "rejected_reason_counts": dict(Counter(rejection.reason for rejection in rejected)),
    }


def _resolve_rebalance_dates(
    *,
    trading_dates: pd.Index,
    requested_dates: list[datetime] | None,
    frequency: str,
) -> list[pd.Timestamp]:
    if requested_dates:
        resolved: list[pd.Timestamp] = []
        for requested in requested_dates:
            requested_ts = pd.Timestamp(requested)
            if requested_ts.tzinfo is None:
                requested_ts = requested_ts.tz_localize(UTC)
            else:
                requested_ts = requested_ts.tz_convert(UTC)
            match = next((pd.Timestamp(date) for date in trading_dates if pd.Timestamp(date) >= requested_ts), None)
            if match is not None:
                resolved.append(match)
        return list(dict.fromkeys(sorted(resolved)))
    frame = pd.DataFrame({"timestamp": pd.to_datetime(trading_dates, utc=True)})
    periods = frame["timestamp"].dt.tz_localize(None).dt.to_period(frequency)
    return list(frame.groupby(periods)["timestamp"].max().tolist())


def _next_execution_price(
    *,
    price_groups: dict[str, pd.DataFrame],
    symbol: str,
    after: pd.Timestamp,
) -> tuple[float, pd.Timestamp | None]:
    price_series = price_groups.get(symbol)
    if price_series is None or price_series.empty:
        return 0.0, None
    matches = price_series[price_series["timestamp"] > after]
    if matches.empty:
        return 0.0, None
    row = matches.iloc[0]
    return float(row["close"]), pd.Timestamp(row["timestamp"])


def _mark_to_market_equity(
    *,
    cash: float,
    positions: dict[str, deque[dict[str, object]]],
    price_groups: dict[str, pd.DataFrame],
    as_of: pd.Timestamp,
) -> float:
    equity = float(cash)
    for symbol, lots in positions.items():
        price = _latest_price_on_or_before(price_groups.get(symbol), as_of)
        qty = float(sum(float(lot["qty"]) for lot in lots))
        equity += qty * price
    return equity


def _build_equity_curve(
    *,
    trading_dates: pd.Index,
    positions: dict[str, deque[dict[str, object]]],
    price_groups: dict[str, pd.DataFrame],
    cash: float,
    trade_rows: list[dict[str, object]],
) -> pd.DataFrame:
    trades_by_date: dict[pd.Timestamp, list[dict[str, object]]] = defaultdict(list)
    for row in trade_rows:
        trades_by_date[pd.Timestamp(row["timestamp"])].append(row)

    replay_positions: dict[str, deque[dict[str, object]]] = defaultdict(deque)
    running_cash = 0.0
    equity_rows: list[dict[str, object]] = []
    if trade_rows:
        initial_cash = float(cash)
        for row in trade_rows:
            if row["side"] == "buy":
                initial_cash += float(row["notional"])
            else:
                initial_cash -= float(row["notional"])
        running_cash = initial_cash
    else:
        running_cash = float(cash)

    for trading_date in trading_dates:
        for trade in trades_by_date.get(pd.Timestamp(trading_date), []):
            symbol = str(trade["symbol"])
            qty = float(trade["qty"])
            fill_price = float(trade["fill_price"])
            if trade["side"] == "buy":
                running_cash -= qty * fill_price
                replay_positions[symbol].append({"qty": qty, "price": fill_price, "filled_at": pd.Timestamp(trade["timestamp"]).to_pydatetime()})
                continue
            remaining = qty
            while remaining > 0 and replay_positions[symbol]:
                lot = replay_positions[symbol][0]
                matched_qty = min(remaining, float(lot["qty"]))
                remaining -= matched_qty
                if matched_qty == float(lot["qty"]):
                    replay_positions[symbol].popleft()
                else:
                    lot["qty"] = float(lot["qty"]) - matched_qty
            if not replay_positions[symbol]:
                replay_positions.pop(symbol, None)
            running_cash += qty * fill_price

        gross_exposure = 0.0
        equity = float(running_cash)
        for symbol, lots in replay_positions.items():
            price = _latest_price_on_or_before(price_groups.get(symbol), pd.Timestamp(trading_date))
            qty = float(sum(float(lot["qty"]) for lot in lots))
            gross_exposure += abs(qty * price)
            equity += qty * price
        equity_rows.append(
            {
                "timestamp": pd.Timestamp(trading_date),
                "cash": float(running_cash),
                "gross_exposure": float(gross_exposure),
                "equity": float(equity),
            }
        )
    return pd.DataFrame(equity_rows)


def _latest_price_on_or_before(price_frame: pd.DataFrame | None, timestamp: pd.Timestamp) -> float:
    if price_frame is None or price_frame.empty:
        return 0.0
    matches = price_frame[price_frame["timestamp"] <= timestamp]
    if matches.empty:
        return 0.0
    return float(matches.iloc[-1]["close"])


def _aggregate_rejected_reason_counts(decision_log: pd.DataFrame) -> dict[str, int]:
    counts: Counter[str] = Counter()
    if decision_log.empty or "rejected_reason_counts" not in decision_log:
        return {}
    for raw in decision_log["rejected_reason_counts"]:
        if not raw:
            continue
        parsed = json.loads(str(raw))
        for reason, count in parsed.items():
            counts[str(reason)] += int(count)
    return dict(counts)


def _symbol_concentration(trade_log: pd.DataFrame) -> float:
    if trade_log.empty or "notional" not in trade_log:
        return 0.0
    totals = trade_log.groupby("symbol")["notional"].sum().abs()
    if totals.empty or float(totals.sum()) <= 0:
        return 0.0
    return float(totals.max() / totals.sum())
