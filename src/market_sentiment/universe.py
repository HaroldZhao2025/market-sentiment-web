from __future__ import annotations
import pandas as pd
import requests
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
# Fallback dataset (community-maintained; good enough if Wikipedia blocks CI)
FALLBACK_CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

def _session_with_retries(total: int = 4, backoff: float = 0.5) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    s.headers.update({"User-Agent": UA})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def _parse_wiki_html(html: str) -> pd.DataFrame:
    """
    Parse the S&P 500 constituents table from Wikipedia HTML.
    We read from the HTML string (not URL) to avoid 403s in CI.
    """
    # Let pandas pick the best parser (lxml/html5lib). We already list both in requirements.
    tables = pd.read_html(StringIO(html))
    if not tables:
        raise RuntimeError("No tables found on Wikipedia page.")

    # Find a table that looks like constituents (has Symbol + Security or GICS Sector)
    df = None
    for t in tables:
        cols = {str(c).strip().lower() for c in t.columns}
        if ("symbol" in cols or "ticker" in cols) and (
            "security" in cols or "company" in cols or "gics sector" in cols
        ):
            df = t
            break

    if df is None:
        # fallback to first table
        df = tables[0]

    # Normalize columns
    rename = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if "symbol" in lc or "ticker" in lc:
            rename[c] = "ticker"
        elif "security" in lc or "company" in lc:
            rename[c] = "name"
        elif "gics" in lc and "sector" in lc:
            rename[c] = "sector"
    df = df.rename(columns=rename)

    if "ticker" not in df.columns:
        # Try common variants
        for c in df.columns:
            if str(c).strip().lower() in {"symbol", "symbols"}:
                df = df.rename(columns={c: "ticker"})
                break

    # Keep only the needed columns if present
    keep = ["ticker", "name", "sector"]
    avail = [c for c in keep if c in df.columns]
    if "ticker" not in avail:
        raise RuntimeError("Ticker column not found after parsing Wikipedia.")

    out = df[avail].copy()
    out["ticker"] = (
        out["ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(".", "-", regex=False)  # BRK.B -> BRK-B (Yahoo Finance style)
    )
    out = out.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    # Ensure all three columns exist
    if "name" not in out.columns:
        out["name"] = ""
    if "sector" not in out.columns:
        out["sector"] = ""

    return out[["ticker", "name", "sector"]]

def fetch_sp500() -> pd.DataFrame:
    """
    Robust S&P 500 constituents fetcher:
    1) Wikipedia (with UA + retries, parse from HTML string)
    2) Fallback CSV from GitHub dataset
    """
    sess = _session_with_retries()
    try:
        r = sess.get(WIKI_URL, timeout=30)
        if r.ok and r.text:
            return _parse_wiki_html(r.text)
        else:
            # Wikipedia refused or empty -> fallback
            raise RuntimeError(f"Wikipedia HTTP {r.status_code}")
    except Exception:
        # Fallback to the CSV dataset
        df = pd.read_csv(FALLBACK_CSV)
        # Normalize columns to ticker/name/sector
        rename = {}
        for c in df.columns:
            lc = str(c).strip().lower()
            if lc in {"symbol", "ticker"}:
                rename[c] = "ticker"
            elif lc in {"security", "name", "company"}:
                rename[c] = "name"
            elif "sector" in lc:
                rename[c] = "sector"
        df = df.rename(columns=rename)
        if "ticker" not in df.columns:
            # Try very conservative fallback
            if "Symbol" in df.columns:
                df = df.rename(columns={"Symbol": "ticker"})
            else:
                raise RuntimeError("Fallback CSV missing ticker column.")
        df["ticker"] = (
            df["ticker"].astype(str).str.strip().str.upper().str.replace(".", "-", regex=False)
        )
        if "name" not in df.columns:
            df["name"] = ""
        if "sector" not in df.columns:
            df["sector"] = ""
        df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
        return df[["ticker", "name", "sector"]]
