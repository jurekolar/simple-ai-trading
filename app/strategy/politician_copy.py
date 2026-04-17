from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

from app.config import Settings
from app.data.alpaca_data import AlpacaDataClient
from app.data.capitol_trades import CapitolTradeDisclosure, CapitolTradesClient, recency_weight


@dataclass(frozen=True)
class RejectedDisclosure:
    politician_id: str
    politician_name: str
    symbol: str
    side: str
    reason: str
    source_url: str


@dataclass(frozen=True)
class PoliticianScore:
    politician_id: str
    politician_name: str
    score: float
    disclosures: int
    average_return: float


@dataclass(frozen=True)
class TargetAllocation:
    symbol: str
    target_weight: float
    target_notional: float
    price: float
    source_urls: tuple[str, ...]


@dataclass(frozen=True)
class AllocationOrder:
    symbol: str
    side: str
    qty: int
    price: float
    target_weight: float
    reason: str


@dataclass(frozen=True)
class AllocationPlan:
    selected_politicians: tuple[PoliticianScore, ...]
    politician_scores: tuple[PoliticianScore, ...]
    target_allocations: tuple[TargetAllocation, ...]
    planned_orders: tuple[AllocationOrder, ...]
    rejected_disclosures: tuple[RejectedDisclosure, ...]
    source: str
    production_safe: bool
    account_equity: float

    def preview(self, limit: int = 10) -> str:
        selected = ", ".join(
            f"{item.politician_name}={item.score:.3f}" for item in self.selected_politicians[:limit]
        ) or "none"
        targets = ", ".join(
            f"{item.symbol}={item.target_weight:.1%}" for item in self.target_allocations[:limit]
        ) or "none"
        orders = ", ".join(
            f"{item.side} {item.qty} {item.symbol}" for item in self.planned_orders[:limit]
        ) or "none"
        rejected = len(self.rejected_disclosures)
        return (
            f"selected=[{selected}] targets=[{targets}] orders=[{orders}] "
            f"rejected={rejected} source={self.source} production_safe={self.production_safe}"
        )


class PoliticianCopyStrategy:
    name = "politician_copy"

    def generate_signals(self, bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
        raise NotImplementedError("politician_copy uses allocation planning, not bar signals")

    def build_allocation_plan(
        self,
        *,
        settings: Settings,
        account_equity: float,
        position_qty_by_symbol: dict[str, float],
        now: datetime | None = None,
        trades_client: CapitolTradesClient | None = None,
        data_client: AlpacaDataClient | None = None,
    ) -> AllocationPlan:
        as_of = now.astimezone(UTC) if now is not None else datetime.now(UTC)
        trades_client = trades_client or CapitolTradesClient(settings)
        data_client = data_client or AlpacaDataClient(settings)

        candidates = trades_client.fetch_politician_candidates()
        disclosures_by_politician: dict[str, list[CapitolTradeDisclosure]] = {}
        for candidate in candidates:
            disclosures_by_politician[candidate.politician_id] = trades_client.fetch_recent_disclosures(candidate)

        all_disclosures = [
            disclosure
            for disclosures in disclosures_by_politician.values()
            for disclosure in disclosures
        ]
        filtered_disclosures, rejected = self._filter_disclosures(all_disclosures, settings=settings, as_of=as_of)
        symbols_for_prices = sorted(
            {
                disclosure.symbol
                for disclosure in filtered_disclosures
            }
            | {symbol.upper() for symbol in position_qty_by_symbol}
        )
        price_frame, price_source, production_safe = self._load_price_frame(
            symbols=symbols_for_prices,
            settings=settings,
            data_client=data_client,
        )
        scores = self._score_politicians(
            filtered_disclosures,
            price_frame=price_frame,
            settings=settings,
            as_of=as_of,
        )
        selected = tuple(scores[: max(settings.politician_copy_num_politicians, 1)])
        targets = self._build_targets(
            filtered_disclosures,
            selected=selected,
            price_frame=price_frame,
            account_equity=account_equity,
            settings=settings,
            as_of=as_of,
        )
        planned_orders = self._plan_orders(
            targets,
            position_qty_by_symbol=position_qty_by_symbol,
            price_frame=price_frame,
        )
        return AllocationPlan(
            selected_politicians=selected,
            politician_scores=tuple(scores),
            target_allocations=targets,
            planned_orders=planned_orders,
            rejected_disclosures=tuple(rejected),
            source=price_source,
            production_safe=production_safe,
            account_equity=account_equity,
        )

    def _filter_disclosures(
        self,
        disclosures: list[CapitolTradeDisclosure],
        *,
        settings: Settings,
        as_of: datetime,
    ) -> tuple[list[CapitolTradeDisclosure], list[RejectedDisclosure]]:
        allowed_symbols = settings.politician_copy_symbol_allowlist_set
        blocked_symbols = settings.politician_copy_symbol_blocklist_set
        max_age_days = max(
            settings.politician_copy_ranking_lookback_days,
            settings.politician_copy_holding_window_days,
        )
        accepted: list[CapitolTradeDisclosure] = []
        rejected: list[RejectedDisclosure] = []
        for disclosure in disclosures:
            age_days = (as_of - disclosure.published_at).days
            if disclosure.asset_type != "us_equity":
                reason = "unsupported_asset_type"
            elif age_days < 0:
                reason = "future_disclosure"
            elif age_days > max_age_days:
                reason = "outside_lookback"
            elif disclosure.filing_delay_days > settings.politician_copy_max_disclosure_lag_days:
                reason = "excessive_filing_delay"
            elif allowed_symbols and disclosure.symbol not in allowed_symbols:
                reason = "symbol_not_allowlisted"
            elif disclosure.symbol in blocked_symbols:
                reason = "symbol_blocklisted"
            else:
                accepted.append(disclosure)
                continue
            rejected.append(
                RejectedDisclosure(
                    politician_id=disclosure.politician_id,
                    politician_name=disclosure.politician_name,
                    symbol=disclosure.symbol,
                    side=disclosure.side,
                    reason=reason,
                    source_url=disclosure.source_url,
                )
            )
        return accepted, rejected

    def _load_price_frame(
        self,
        *,
        symbols: list[str],
        settings: Settings,
        data_client: AlpacaDataClient,
    ) -> tuple[pd.DataFrame, str, bool]:
        if not symbols:
            return pd.DataFrame(columns=["timestamp", "symbol", "close", "volume"]), "alpaca", True
        lookback_days = max(
            settings.politician_copy_ranking_lookback_days,
            settings.politician_copy_holding_window_days,
            30,
        )
        loaded = data_client.get_daily_bars(symbols, lookback_days)
        frame = loaded.bars.copy()
        if frame.empty:
            return frame, loaded.source, False
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp", "close"]).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        prices_by_symbol = {symbol: group for symbol, group in frame.groupby("symbol")}
        production_safe = loaded.production_safe and all(symbol in prices_by_symbol for symbol in symbols)
        return frame, loaded.source, production_safe

    def _score_politicians(
        self,
        disclosures: list[CapitolTradeDisclosure],
        *,
        price_frame: pd.DataFrame,
        settings: Settings,
        as_of: datetime,
    ) -> list[PoliticianScore]:
        if price_frame.empty:
            return []
        price_groups = {symbol: group.reset_index(drop=True) for symbol, group in price_frame.groupby("symbol")}
        by_politician: dict[str, list[float]] = {}
        metadata: dict[str, tuple[str, list[float]]] = {}
        lookback_cutoff = as_of - timedelta(days=settings.politician_copy_ranking_lookback_days)
        for disclosure in disclosures:
            if disclosure.side != "buy" or disclosure.published_at < lookback_cutoff:
                continue
            price_series = price_groups.get(disclosure.symbol)
            if price_series is None:
                continue
            entry_price = _price_on_or_after(price_series, disclosure.published_at)
            latest_price = _latest_price(price_series)
            avg_volume = float(price_series["volume"].tail(5).mean()) if "volume" in price_series else 0.0
            if entry_price <= 0 or latest_price <= 0:
                continue
            if avg_volume < settings.min_average_daily_volume:
                continue
            gross_return = (latest_price / entry_price) - 1.0
            lag_penalty = 1.0 / (1.0 + (disclosure.filing_delay_days / max(settings.politician_copy_max_disclosure_lag_days, 1)))
            weighted_return = gross_return * recency_weight(
                (as_of - disclosure.published_at).days,
                settings.politician_copy_recency_half_life_days,
            ) * lag_penalty
            by_politician.setdefault(disclosure.politician_id, []).append(weighted_return)
            metadata.setdefault(disclosure.politician_id, (disclosure.politician_name, []) )[1].append(gross_return)
        scores: list[PoliticianScore] = []
        for politician_id, values in by_politician.items():
            if len(values) < settings.politician_copy_min_disclosures_per_politician:
                continue
            politician_name, gross_returns = metadata[politician_id]
            scores.append(
                PoliticianScore(
                    politician_id=politician_id,
                    politician_name=politician_name,
                    score=sum(values) / len(values),
                    disclosures=len(values),
                    average_return=sum(gross_returns) / len(gross_returns),
                )
            )
        return sorted(scores, key=lambda item: (item.score, item.average_return, item.politician_name), reverse=True)

    def _build_targets(
        self,
        disclosures: list[CapitolTradeDisclosure],
        *,
        selected: tuple[PoliticianScore, ...],
        price_frame: pd.DataFrame,
        account_equity: float,
        settings: Settings,
        as_of: datetime,
    ) -> tuple[TargetAllocation, ...]:
        if not selected or price_frame.empty:
            return ()
        selected_ids = {item.politician_id for item in selected}
        price_groups = {symbol: group.reset_index(drop=True) for symbol, group in price_frame.groupby("symbol")}
        holding_cutoff = as_of - timedelta(days=settings.politician_copy_holding_window_days)
        latest_by_politician_symbol: dict[tuple[str, str], CapitolTradeDisclosure] = {}
        for disclosure in disclosures:
            if disclosure.politician_id not in selected_ids or disclosure.published_at < holding_cutoff:
                continue
            key = (disclosure.politician_id, disclosure.symbol)
            current = latest_by_politician_symbol.get(key)
            if current is None or disclosure.published_at > current.published_at:
                latest_by_politician_symbol[key] = disclosure
        per_symbol_weight: dict[str, float] = {}
        source_urls_by_symbol: dict[str, set[str]] = {}
        per_politician_budget = 1.0 / len(selected)
        for politician in selected:
            active = [
                disclosure
                for (politician_id, _), disclosure in latest_by_politician_symbol.items()
                if politician_id == politician.politician_id and disclosure.side == "buy"
            ]
            active_weights: dict[str, float] = {}
            for disclosure in active:
                price_series = price_groups.get(disclosure.symbol)
                if price_series is None:
                    continue
                latest_price = _latest_price(price_series)
                avg_volume = float(price_series["volume"].tail(5).mean()) if "volume" in price_series else 0.0
                if latest_price <= 0 or avg_volume < settings.min_average_daily_volume:
                    continue
                active_weights[disclosure.symbol] = (
                    disclosure.amount_midpoint
                    * recency_weight(
                        (as_of - disclosure.published_at).days,
                        settings.politician_copy_recency_half_life_days,
                    )
                )
                source_urls_by_symbol.setdefault(disclosure.symbol, set()).add(disclosure.source_url)
            total = float(sum(active_weights.values()))
            if total <= 0:
                continue
            for symbol, raw_weight in active_weights.items():
                per_symbol_weight[symbol] = per_symbol_weight.get(symbol, 0.0) + (raw_weight / total) * per_politician_budget
        normalized = _cap_and_normalize_weights(
            per_symbol_weight,
            max_weight=settings.politician_copy_max_symbol_weight,
            min_weight=settings.politician_copy_min_target_weight,
        )
        deployable_equity = account_equity if account_equity > 0 else settings.max_gross_exposure
        targets = [
            TargetAllocation(
                symbol=symbol,
                target_weight=weight,
                target_notional=weight * deployable_equity,
                price=_latest_price(price_groups[symbol]),
                source_urls=tuple(sorted(source_urls_by_symbol.get(symbol, set()))),
            )
            for symbol, weight in sorted(normalized.items(), key=lambda item: item[1], reverse=True)
            if symbol in price_groups and _latest_price(price_groups[symbol]) > 0
        ]
        return tuple(targets)

    def _plan_orders(
        self,
        targets: tuple[TargetAllocation, ...],
        *,
        position_qty_by_symbol: dict[str, float],
        price_frame: pd.DataFrame,
    ) -> tuple[AllocationOrder, ...]:
        price_groups = {symbol: group.reset_index(drop=True) for symbol, group in price_frame.groupby("symbol")}
        target_by_symbol = {target.symbol: target for target in targets}
        symbols = sorted(set(position_qty_by_symbol) | set(target_by_symbol))
        orders: list[AllocationOrder] = []
        for symbol in symbols:
            price_series = price_groups.get(symbol)
            if price_series is None:
                continue
            price = _latest_price(price_series)
            if price <= 0:
                continue
            current_qty = float(position_qty_by_symbol.get(symbol, 0.0))
            current_notional = current_qty * price
            target = target_by_symbol.get(symbol)
            target_notional = target.target_notional if target is not None else 0.0
            delta = target_notional - current_notional
            if delta > price:
                qty = int(math.floor(delta / price))
                if qty > 0:
                    orders.append(
                        AllocationOrder(
                            symbol=symbol,
                            side="buy",
                            qty=qty,
                            price=price,
                            target_weight=target.target_weight if target is not None else 0.0,
                            reason="target_rebalance",
                        )
                    )
            elif delta < -price and current_qty > 0:
                qty = min(int(math.ceil(abs(delta) / price)), int(current_qty))
                if qty > 0:
                    orders.append(
                        AllocationOrder(
                            symbol=symbol,
                            side="sell",
                            qty=qty,
                            price=price,
                            target_weight=target.target_weight if target is not None else 0.0,
                            reason="target_rebalance",
                        )
                    )
        sells = [order for order in orders if order.side == "sell"]
        buys = [order for order in orders if order.side == "buy"]
        return tuple(sells + buys)


def _price_on_or_after(price_frame: pd.DataFrame, timestamp: datetime) -> float:
    matches = price_frame[price_frame["timestamp"] >= timestamp]
    if matches.empty:
        return 0.0
    return float(matches.iloc[0]["close"])


def _latest_price(price_frame: pd.DataFrame) -> float:
    if price_frame.empty:
        return 0.0
    return float(price_frame.iloc[-1]["close"])


def _cap_and_normalize_weights(
    weights: dict[str, float],
    *,
    max_weight: float,
    min_weight: float,
) -> dict[str, float]:
    filtered = {symbol: weight for symbol, weight in weights.items() if weight >= min_weight}
    total = float(sum(filtered.values()))
    if total <= 0:
        return {}
    normalized = {symbol: weight / total for symbol, weight in filtered.items()}
    if max_weight <= 0:
        return normalized
    capped: dict[str, float] = {}
    remaining = normalized.copy()
    remaining_budget = 1.0
    while remaining:
        progressed = False
        total_remaining = float(sum(remaining.values()))
        for symbol, weight in list(remaining.items()):
            scaled = (weight / total_remaining) * remaining_budget if total_remaining > 0 else 0.0
            if scaled >= max_weight:
                capped[symbol] = max_weight
                remaining_budget -= max_weight
                remaining.pop(symbol)
                progressed = True
        if not progressed:
            for symbol, weight in remaining.items():
                capped[symbol] = (weight / total_remaining) * remaining_budget if total_remaining > 0 else 0.0
            break
    return {symbol: weight for symbol, weight in capped.items() if weight >= min_weight}


politician_copy_strategy = PoliticianCopyStrategy()
