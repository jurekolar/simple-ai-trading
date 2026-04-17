from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from app.config import Settings

MONTH_DAY_PATTERN = re.compile(r"^[0-3]?\d [A-Z][a-z]{2}$|^[A-Z][a-z]{2} [0-3]?\d$")
YEAR_PATTERN = re.compile(r"^\d{4}$")
SYMBOL_PATTERN = re.compile(r"^(?P<symbol>[A-Z][A-Z0-9.\-]{0,9}):US$")


@dataclass(frozen=True)
class PoliticianCandidate:
    politician_id: str
    politician_name: str
    profile_url: str


@dataclass(frozen=True)
class CapitolTradeDisclosure:
    politician_id: str
    politician_name: str
    trade_date: datetime
    published_at: datetime
    symbol: str
    asset_type: str
    side: str
    amount_bucket: str
    amount_midpoint: float
    source_url: str
    filing_delay_days: int
    issuer_name: str = ""


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._href_stack: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self._href_stack.append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href_stack:
            self._href_stack.pop()

    def handle_data(self, data: str) -> None:
        if not self._href_stack:
            return
        text = " ".join(data.split())
        if text:
            self.links.append((self._href_stack[-1], text))


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def handle_data(self, data: str) -> None:
        text = " ".join(unescape(data).split())
        if text:
            self.lines.append(text)


def amount_bucket_midpoint(bucket: str) -> float:
    cleaned = bucket.replace(",", "").replace("$", "").replace(" ", "").upper()
    normalized = cleaned.replace("–", "-").replace("—", "-")
    match = re.match(r"^(?P<low>\d+(?:\.\d+)?[KM]?)-(?P<high>\d+(?:\.\d+)?[KM]?)$", normalized)
    if not match:
        raise ValueError(f"unsupported amount bucket: {bucket}")
    return (_parse_amount(match.group("low")) + _parse_amount(match.group("high"))) / 2.0


def _parse_amount(raw: str) -> float:
    multiplier = 1.0
    value = raw
    if raw.endswith("K"):
        multiplier = 1_000.0
        value = raw[:-1]
    elif raw.endswith("M"):
        multiplier = 1_000_000.0
        value = raw[:-1]
    return float(value) * multiplier


def _parse_month_day_year(month_day: str, year: str) -> datetime:
    value = f"{month_day} {year}"
    for fmt in ("%d %b %Y", "%b %d %Y"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"unsupported date fragment: {value}")


class CapitolTradesClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def fetch_politician_candidates(self) -> list[PoliticianCandidate]:
        candidates: dict[str, PoliticianCandidate] = {}
        for page in range(1, max(self._settings.politician_copy_candidate_pages, 1) + 1):
            url = f"{self._settings.politician_copy_base_url}/politicians?page={page}"
            html = self._fetch_text(url)
            for candidate in self._parse_politician_candidates(html):
                candidates[candidate.politician_id] = candidate
        return list(candidates.values())

    def fetch_recent_disclosures(self, candidate: PoliticianCandidate) -> list[CapitolTradeDisclosure]:
        max_pages = max(self._settings.politician_copy_max_profile_pages, 1)
        disclosures: list[CapitolTradeDisclosure] = []
        cutoff = datetime.now(UTC).timestamp() - (
            max(
                self._settings.politician_copy_ranking_lookback_days,
                self._settings.politician_copy_holding_window_days,
            )
            * 24
            * 60
            * 60
        )
        for page in range(1, max_pages + 1):
            page_url = candidate.profile_url if page == 1 else f"{candidate.profile_url}?page={page}"
            html = self._fetch_text(page_url)
            page_disclosures = self._parse_profile_page(
                html,
                politician_id=candidate.politician_id,
                politician_name=candidate.politician_name,
                page_url=page_url,
            )
            if not page_disclosures:
                break
            disclosures.extend(page_disclosures)
            oldest_trade = min(disclosure.trade_date.timestamp() for disclosure in page_disclosures)
            if oldest_trade < cutoff:
                break
        return disclosures

    def _fetch_text(self, url: str) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": self._settings.politician_copy_user_agent,
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urlopen(request, timeout=self._settings.politician_copy_scrape_timeout_seconds) as response:
            return response.read().decode("utf-8", errors="replace")

    def _parse_politician_candidates(self, html: str) -> list[PoliticianCandidate]:
        parser = _AnchorParser()
        parser.feed(html)
        candidates: list[PoliticianCandidate] = []
        seen_ids: set[str] = set()
        for href, text in parser.links:
            if not href.startswith("/politicians/"):
                continue
            politician_id = href.rsplit("/", 1)[-1]
            if not politician_id or politician_id in seen_ids:
                continue
            name = text.strip()
            if len(name.split()) < 2 or "Trades" in name:
                continue
            candidates.append(
                PoliticianCandidate(
                    politician_id=politician_id,
                    politician_name=name,
                    profile_url=urljoin(self._settings.politician_copy_base_url, href),
                )
            )
            seen_ids.add(politician_id)
        return candidates

    def _parse_profile_page(
        self,
        html: str,
        *,
        politician_id: str,
        politician_name: str,
        page_url: str,
    ) -> list[CapitolTradeDisclosure]:
        text_parser = _TextParser()
        text_parser.feed(html)
        lines = text_parser.lines

        anchor_parser = _AnchorParser()
        anchor_parser.feed(html)
        detail_urls = [
            urljoin(self._settings.politician_copy_base_url, href)
            for href, text in anchor_parser.links
            if "Goto trade detail page" in text
        ]

        start_index = next((idx for idx, line in enumerate(lines) if line == "Traded Issuer"), -1)
        if start_index < 0:
            return []

        trades: list[CapitolTradeDisclosure] = []
        idx = start_index + 1
        trade_index = 0
        while idx + 7 < len(lines):
            issuer_name = lines[idx]
            if issuer_name.startswith("Page ") or issuer_name in {"Show", "EXPLORE IN TRADES"}:
                break
            symbol_line = lines[idx + 1]
            published_month_day = lines[idx + 2]
            published_year = lines[idx + 3]
            traded_month_day = lines[idx + 4]
            traded_year = lines[idx + 5]
            maybe_days = lines[idx + 6]
            filing_delay = lines[idx + 7]
            side = lines[idx + 8] if idx + 8 < len(lines) else ""
            amount_bucket = lines[idx + 9] if idx + 9 < len(lines) else ""
            if not (
                (SYMBOL_PATTERN.match(symbol_line) or symbol_line == "N/A")
                and MONTH_DAY_PATTERN.match(published_month_day)
                and YEAR_PATTERN.match(published_year)
                and MONTH_DAY_PATTERN.match(traded_month_day)
                and YEAR_PATTERN.match(traded_year)
                and maybe_days == "days"
                and filing_delay.isdigit()
                and side.lower() in {"buy", "sell"}
            ):
                idx += 1
                continue
            source_url = detail_urls[trade_index] if trade_index < len(detail_urls) else page_url
            trade_index += 1
            idx += 10
            if not SYMBOL_PATTERN.match(symbol_line):
                continue
            try:
                amount_midpoint = amount_bucket_midpoint(amount_bucket)
                published_at = _parse_month_day_year(published_month_day, published_year)
                trade_date = _parse_month_day_year(traded_month_day, traded_year)
            except ValueError:
                continue
            symbol = SYMBOL_PATTERN.match(symbol_line).group("symbol")  # type: ignore[union-attr]
            trades.append(
                CapitolTradeDisclosure(
                    politician_id=politician_id,
                    politician_name=politician_name,
                    trade_date=trade_date,
                    published_at=published_at,
                    symbol=symbol,
                    asset_type="us_equity",
                    side=side.lower(),
                    amount_bucket=amount_bucket,
                    amount_midpoint=amount_midpoint,
                    source_url=source_url,
                    filing_delay_days=int(filing_delay),
                    issuer_name=issuer_name,
                )
            )
        return trades


def recency_weight(age_days: float, half_life_days: float) -> float:
    if half_life_days <= 0:
        return 1.0
    return math.exp(-math.log(2) * max(age_days, 0.0) / half_life_days)
