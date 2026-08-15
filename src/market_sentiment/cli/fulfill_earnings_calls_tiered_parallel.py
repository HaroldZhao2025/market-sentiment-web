from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from market_sentiment import broad_transcripts as broad
from market_sentiment.cli import fulfill_earnings_calls_tiered as tiered


_ORIGINAL_GET = broad._get


def _fast_get(url: str, timeout: int = 7):
    """Bound broad-source latency so missing tickers cannot stall a full sweep."""
    return _ORIGINAL_GET(url, timeout=min(timeout, 7))


def _marketbeat_candidates_parallel(symbol: str, limit: int = 2):
    ticker = symbol.upper()
    variants = list(dict.fromkeys((ticker, ticker.replace("-", "."))))
    exchanges = ("NASDAQ", "NYSE", "AMEX")
    urls = [
        f"https://www.marketbeat.com/stocks/{exchange}/{variant}/earnings/"
        for exchange in exchanges
        for variant in variants
    ]

    def fetch(url: str):
        return url, _fast_get(url)

    valid: list[tuple[str, object]] = []
    with ThreadPoolExecutor(max_workers=min(6, len(urls))) as pool:
        futures = [pool.submit(fetch, url) for url in urls]
        for future in as_completed(futures):
            try:
                url, response = future.result()
            except Exception:
                continue
            if response is not None:
                valid.append((url, response))

    for index_url, response in valid:
        soup = BeautifulSoup(response.content, "lxml")
        h1 = soup.find("h1")
        heading = h1.get_text(" ", strip=True) if h1 else ""
        if ticker.replace("-", ".") not in heading.upper() and f"({ticker})" not in heading.upper():
            continue
        out = []
        seen: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            text = " ".join(anchor.get_text(" ", strip=True).split())
            href = str(anchor.get("href") or "")
            lower = text.lower()
            if not ("conference call transcript" in lower or "read transcript" in lower):
                continue
            if "/earnings/reports/" not in href:
                continue
            url = urljoin(index_url, href)
            if url in seen:
                continue
            seen.add(url)
            date_match = re.search(r"/earnings/reports/(20\d{2})-(\d{1,2})-(\d{1,2})-", url)
            published = (
                f"{date_match.group(1)}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}T00:00:00+00:00"
                if date_match
                else ""
            )
            out.append(
                broad.BroadCandidate(
                    url=url,
                    title=f"{ticker} Earnings Call Transcript",
                    source="MarketBeat",
                    published=published,
                )
            )
            if len(out) >= limit:
                return out
        if out:
            return out
    return []


def main() -> None:
    broad._get = _fast_get
    broad.marketbeat_candidates = _marketbeat_candidates_parallel
    tiered.main()


if __name__ == "__main__":
    main()
