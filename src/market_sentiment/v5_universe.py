from __future__ import annotations

from dataclasses import asdict, dataclass
from io import StringIO

import pandas as pd
import requests

from .universe import fetch_sp500

SP400_SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"


@dataclass(frozen=True)
class CompanyRecord:
    ticker: str
    name: str
    sector: str
    industry: str
    universe: str


def normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper().replace(".", "-")


def parse_sp400_html(html: str) -> list[CompanyRecord]:
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
            out.append(
                CompanyRecord(
                    ticker=ticker,
                    name=str(row.get(name_col) or "").strip(),
                    sector=str(row.get(sector_col) or "Unknown").strip() if sector_col is not None else "Unknown",
                    industry=str(row.get(industry_col) or "Unknown").strip() if industry_col is not None else "Unknown",
                    universe="S&P MidCap 400",
                )
            )
        if len(out) >= 100:
            return out
    raise RuntimeError("Could not locate S&P MidCap 400 constituent table")


def fetch_sp400() -> list[CompanyRecord]:
    response = requests.get(SP400_SOURCE, headers={"User-Agent": "market-sentiment-web/5.0"}, timeout=30)
    response.raise_for_status()
    return parse_sp400_html(response.text)


def build_extended_universe() -> list[dict[str, str]]:
    merged: dict[str, CompanyRecord] = {}
    sp500 = fetch_sp500()
    for _, row in sp500.iterrows():
        ticker = normalize_ticker(row.get("ticker"))
        if ticker:
            merged[ticker] = CompanyRecord(
                ticker=ticker,
                name=str(row.get("name") or "").strip(),
                sector=str(row.get("sector") or "Unknown").strip(),
                industry="Unknown",
                universe="S&P 500",
            )
    for record in fetch_sp400():
        merged.setdefault(record.ticker, record)
    return [asdict(merged[ticker]) for ticker in sorted(merged)]
