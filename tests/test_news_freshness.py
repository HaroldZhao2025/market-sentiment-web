"""Offline recency and real fulfillment-entrypoint regressions; no model/network use."""
from __future__ import annotations

import argparse
import ast
import copy
import importlib.util
import io
import json
import math
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("news_freshness", ROOT / "src/market_sentiment/news_freshness.py")
assert SPEC is not None and SPEC.loader is not None
freshness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(freshness)
NOW = datetime(2026, 10, 2, 20, tzinfo=timezone.utc)
OLD = {"title": "Retained August headline", "ts": "2026-08-17T21:16:00Z", "url": "https://x.example/old", "s": .2}
NEW = {"title": "Current October headline", "ts": "2026-10-02T12:00:00Z", "url": "https://x.example/new", "s": -.4}
PREVIOUS = {"updated_at_utc": "2026-08-18T00:00:00Z", "articles": [OLD]}


class FreshnessTests(unittest.TestCase):
    def metadata(self, fetched, retained=None, **kwargs):
        return freshness.news_refresh_metadata(copy.deepcopy(PREVIOUS), fetched, retained or [OLD], NOW, **kwargs)

    def test_empty_fetch_preserves_update_time_and_old_news_is_not_fresh(self):
        result = self.metadata([])
        self.assertEqual(result["updated_at_utc"], PREVIOUS["updated_at_utc"])
        self.assertEqual(result["last_attempt_utc"], NOW.isoformat())
        self.assertEqual(result["latest_article_at_utc"], "2026-08-17T21:16:00+00:00")
        self.assertEqual(result["refresh_status"], "empty_or_failed")
        self.assertEqual(result["new_article_count"], 0)
        self.assertIsNone(result["last_nonempty_fetch_utc"])

    def test_fetch_exception_is_not_a_success(self):
        result = self.metadata([], fetch_error_type="TimeoutError")
        self.assertEqual(result["refresh_status"], "error")
        self.assertEqual(result["fetch_error_type"], "TimeoutError")
        self.assertEqual(result["updated_at_utc"], PREVIOUS["updated_at_utc"])

    def test_new_article_advances_real_article_date_and_counts(self):
        result = self.metadata([NEW], [NEW, OLD])
        self.assertEqual(result["latest_article_at_utc"], "2026-10-02T12:00:00+00:00")
        self.assertEqual(result["new_article_count"], 1)
        self.assertEqual(result["fetched_article_count"], 1)
        self.assertEqual(result["refresh_status"], "updated")
        self.assertEqual(result["updated_at_utc"], NOW.isoformat())

    def test_repeated_fetch_is_not_new(self):
        result = self.metadata([OLD, OLD])
        self.assertEqual(result["fetched_article_count"], 1)
        self.assertEqual(result["new_article_count"], 0)
        self.assertEqual(result["refresh_status"], "no_new_articles")

    def test_scoring_changes_are_not_new_articles(self):
        result = self.metadata([OLD], [{**OLD, "s": .3}])
        self.assertEqual(result["new_article_count"], 0)
        self.assertEqual(result["latest_article_at_utc"], "2026-08-17T21:16:00+00:00")

    def test_new_count_does_not_include_fetched_articles_dropped_by_retention(self):
        self.assertEqual(self.metadata([NEW])["new_article_count"], 0)

    def test_same_url_or_normalized_title_does_not_count_as_new(self):
        self.assertEqual(self.metadata([{**OLD, "title": "Revised title"}], [{**OLD, "title": "Revised title"}])["new_article_count"], 0)
        self.assertEqual(self.metadata([{**OLD, "url": "https://other.example/article", "title": OLD["title"].upper()}],
                                       [{**OLD, "url": "https://other.example/article", "title": OLD["title"].upper()}])["new_article_count"], 0)

    def test_identity_bearing_url_query_parameters_are_preserved(self):
        first = {**OLD, "url": "https://finnhub.io/api/news?id=1"}
        second = {**NEW, "url": "https://finnhub.io/api/news?id=2"}
        result = freshness.news_refresh_metadata({"articles": [first]}, [second], [first, second], NOW)
        self.assertEqual(result["new_article_count"], 1)

    def test_invalid_and_far_future_publication_dates_do_not_advance_recency(self):
        self.assertEqual(freshness.latest_article_timestamp([OLD, {**NEW, "ts": "broken"}, {**NEW, "ts": "2099-01-01"}], NOW),
                         "2026-08-17T21:16:00+00:00")
        self.assertIsNone(freshness.latest_article_timestamp([], NOW))

    def test_latest_article_compares_actual_timezone_not_input_string(self):
        result = freshness.latest_article_timestamp([{**NEW, "ts": "2026-10-02T12:00:00+02:00"},
                                                      {**OLD, "ts": "2026-10-02T11:00:00Z"}], NOW)
        self.assertEqual(result, "2026-10-02T11:00:00+00:00")

    def test_successful_fallback_does_not_hide_partial_collector_error(self):
        result = self.metadata([NEW], [NEW, OLD], fetch_error_type="TimeoutError")
        self.assertEqual(result["refresh_status"], "partial_error")
        self.assertEqual(result["new_article_count"], 1)

    def test_input_rows_and_existing_metadata_are_not_mutated(self):
        before = copy.deepcopy(PREVIOUS)
        self.metadata([NEW], [NEW, OLD])
        self.assertEqual(PREVIOUS, before)


class FulfillmentEntrypointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.public = Path(self.temp.name)
        self.root = self.public / "data/v5"
        self.root.mkdir(parents=True)
        self.write(self.root / "news/MSFT.json", PREVIOUS)
        self.write(self.root / "universe.json", {"companies": [{"ticker": "MSFT"}]})

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    @staticmethod
    def read(path, default):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return default

    def run_fulfillment(self, response):
        # Execute the actual production main/coverage functions. Only network/price/
        # model boundaries are stubbed, so artifact wiring and timestamps are tested.
        filename = ROOT / "src/market_sentiment/cli/fulfill_company_data.py"
        tree = ast.parse(filename.read_text(encoding="utf-8"))
        body = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return NOW
        class StubScorer:
            def __init__(self, *args, **kwargs):
                pass
            def score(self, rows):
                return copy.deepcopy(rows)
        def collect(*args):
            if isinstance(response, Exception):
                raise response
            return copy.deepcopy(response)
        namespace = dict(argparse=argparse, Path=Path, datetime=FixedDatetime, timedelta=timedelta, timezone=timezone,
                         ThreadPoolExecutor=ThreadPoolExecutor, as_completed=as_completed,
                         load_json=self.read, atomic_json=self.write,
                         latest_article_timestamp=freshness.latest_article_timestamp,
                         news_refresh_metadata=freshness.news_refresh_metadata,
                         ReusableNewsScorer=StubScorer,
                         finite=lambda value: value if isinstance(value, (float, int)) and math.isfinite(value) else None)
        exec(compile(ast.Module(body=body, type_ignores=[]), str(filename), "exec",
                     flags=__import__("__future__").annotations.compiler_flag), namespace)
        companies = [{"ticker": "MSFT"}] + [{"ticker": f"T{i:04d}"} for i in range(1299)]
        namespace.update(load_companies=lambda _: companies, target_rows=lambda *args: companies[:1],
                         download_price_history=lambda *args, **kwargs: {}, collect_finnhub_history=lambda *args: {},
                         collect_historical_news=collect,
                         deduplicate_news=lambda values: list({(row["title"], row["ts"]): row for row in values}.values()))
        output = io.StringIO()
        with patch.object(sys, "argv", ["fulfill", "--public-root", str(self.public)]), redirect_stdout(output):
            namespace["main"]()
        return (self.read(self.root / "news/MSFT.json", {}),
                self.read(self.root / "company_data_fulfillment_attempts.json", {})["symbols"]["MSFT"],
                self.read(self.root / "company_data_coverage.json", {}), output.getvalue())

    def test_real_main_empty_fetch_preserves_articles_and_records_failed_or_empty_attempt(self):
        news, attempt, coverage, log = self.run_fulfillment([])
        self.assertEqual(news["articles"], [OLD])
        self.assertEqual(news["updated_at_utc"], PREVIOUS["updated_at_utc"])
        self.assertEqual(attempt["new_article_count"], 0)
        self.assertEqual(attempt["refresh_status"], "empty_or_failed")
        self.assertEqual(coverage["news_ready_count"], 1)
        self.assertEqual(coverage["news_recent_count"], 0)
        self.assertEqual(coverage["companies"][0]["latest_article_at_utc"], "2026-08-17T21:16:00+00:00")
        self.assertIn("::warning::", log)
        self.assertIn("new_articles=0", log)

    def test_real_main_retains_new_articles_and_exposes_recency_in_all_artifacts(self):
        news, attempt, coverage, log = self.run_fulfillment([NEW])
        self.assertEqual(news["articles"], [NEW, OLD])
        self.assertEqual(attempt["new_article_count"], 1)
        self.assertEqual(coverage["news_recent_count"], 1)
        self.assertEqual(coverage["news_recency_window_days"], 7)
        universe = self.read(self.root / "universe.json", {})
        self.assertEqual(universe["companies"][0]["latest_article_at_utc"], news["latest_article_at_utc"])
        self.assertIn("new_articles=1", log)

    def test_real_main_exception_is_explicit_and_does_not_expose_error_message(self):
        news, attempt, coverage, log = self.run_fulfillment(TimeoutError("secret-token-must-not-appear"))
        self.assertEqual(news["articles"], [OLD])
        self.assertEqual(attempt["refresh_status"], "error")
        self.assertEqual(attempt["fetch_error_type"], "TimeoutError")
        self.assertNotIn("secret-token-must-not-appear", json.dumps(attempt) + log)


if __name__ == "__main__":
    unittest.main()
