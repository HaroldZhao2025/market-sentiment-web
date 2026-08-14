from __future__ import annotations

from dataclasses import asdict, dataclass
from io import StringIO

import pandas as pd
import requests

from .universe import fetch_sp500

SP400_SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
SP600_SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"


@dataclass(frozen=True)
class CompanyRecord:
    ticker: str
    name: str
    sector: str
    industry: str
    universe: str


def normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper().replace(".", "-")


def parse_constituent_html(html: str, universe: str, minimum_rows: int) -> list[CompanyRecord]:
    for table in pd.read_html(StringIO(html)):
        normalized = {str(c).strip().lower(): c for c in table.columns}
        symbol_col = next((normalized[k] for k in ("symbol", "ticker", "ticker symbol") if k in normalized), None)
        name_col = next((normalized[k] for k in ("security", "company", "company name") if k in normalized), None)
        sector_col = next((normalized[k] for k in ("gics sector", "sector") if k in normalized), None)
        industry_col = next((normalized[k] for k in ("gics sub-industry", "gics sub industry", "industry") if k in normalized), None)
        if symbol_col is None or name_col is None:
            continue

        out: list[CompanyRecord] = []
        for _, row in table.iterrows():
            ticker = normalize_ticker(row.get(symbol_col))
            if not ticker or ticker == "NAN":
                continue
            name = str(row.get(name_col) or "").strip()
            if not name or name.lower() == "nan":
                name = ticker
            sector = str(row.get(sector_col) or "Unknown").strip() if sector_col is not None else "Unknown"
            industry = str(row.get(industry_col) or "Unknown").strip() if industry_col is not None else "Unknown"
            out.append(
                CompanyRecord(
                    ticker=ticker,
                    name=name,
                    sector=sector if sector and sector.lower() != "nan" else "Unknown",
                    industry=industry if industry and industry.lower() != "nan" else "Unknown",
                    universe=universe,
                )
            )
        if len(out) >= minimum_rows:
            return out
    raise RuntimeError(f"Could not locate constituent table for {universe}")


def fetch_constituents(source: str, universe: str, minimum_rows: int) -> list[CompanyRecord]:
    response = requests.get(source, headers={"User-Agent": "market-sentiment-web/7.0"}, timeout=30)
    response.raise_for_status()
    return parse_constituent_html(response.text, universe=universe, minimum_rows=minimum_rows)


def fetch_sp400() -> list[CompanyRecord]:
    return fetch_constituents(SP400_SOURCE, "S&P MidCap 400", 300)


def fetch_sp600() -> list[CompanyRecord]:
    return fetch_constituents(SP600_SOURCE, "S&P SmallCap 600", 450)


def build_extended_universe() -> list[dict[str, str]]:
    """Build S&P Composite 1500-style coverage without changing SPX core semantics."""
    merged: dict[str, CompanyRecord] = {}
    sp500 = fetch_sp500()
    for _, row in sp500.iterrows():
        ticker = normalize_ticker(row.get("ticker"))
        if ticker:
            merged[ticker] = CompanyRecord(
                ticker=ticker,
                name=str(row.get("name") or "").strip() or ticker,
                sector=str(row.get("sector") or "Unknown").strip() or "Unknown",
                industry="Unknown",
                universe="S&P 500",
            )

    # Larger-cap membership wins if a source briefly overlaps during index transitions.
    for record in fetch_sp400():
        merged.setdefault(record.ticker, record)
    for record in fetch_sp600():
        merged.setdefault(record.ticker, record)

    rows = [asdict(merged[ticker]) for ticker in sorted(merged)]
    if len(rows) < 1300:
        raise RuntimeError(f"Composite universe unexpectedly small after deduplication: {len(rows)}")
    return rows
