"""Offline regressions for the production company-news refresh queue."""
from __future__ import annotations

import ast
import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("news_schedule", ROOT / "src/market_sentiment/news_schedule.py")
assert SPEC is not None and SPEC.loader is not None
scheduler = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scheduler)


class NewsScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.news = self.root / "news"
        self.history = self.root / "history"
        self.news.mkdir()
        self.history.mkdir()
        self.now = datetime(2026, 10, 2, 18, tzinfo=timezone.utc)
        self.attempts = {}

    def artifact(self, symbol, *, count=360, days=1095, price_days=40, updated=None):
        payload = {"articles": [{"title": str(i)} for i in range(count)], "history_days_requested": days}
        if updated is not None:
            payload["updated_at_utc"] = updated
        (self.news / f"{symbol}.json").write_text(json.dumps(payload), encoding="utf-8")
        (self.history / f"{symbol}.json").write_text(json.dumps({"date": ["2026-01-01"] * price_days, "price": [1] * price_days}), encoding="utf-8")

    def attempted(self, symbol, hours=0):
        self.attempts[symbol] = {"last_attempt_utc": (self.now - timedelta(hours=hours)).isoformat()}

    def select(self, symbols, size=250, **kwargs):
        return [row["ticker"] for row in scheduler.select_company_targets(
            [{"ticker": symbol} for symbol in symbols], self.news, self.history,
            self.attempts, size, now=self.now, **kwargs)]

    def test_stale_healthy_company_precedes_recent_missing_company(self):
        self.artifact("AAPL")
        self.attempted("AAPL", 264)
        self.attempted("AAA", 8)
        self.assertEqual(self.select(["AAA", "AAPL"], 1), ["AAPL"])

    def test_oldest_attempt_beats_alphabetical_order(self):
        self.attempted("AAA", 7)
        self.attempted("ZZZ", 24)
        self.assertEqual(self.select(["AAA", "ZZZ"], 1), ["ZZZ"])

    def test_missing_and_thin_archives_observe_cooldown(self):
        self.artifact("THIN", count=2)
        self.attempted("MISSING", 1)
        self.attempted("THIN", 1)
        self.assertEqual(self.select(["MISSING", "THIN"]), [])

    def test_missing_history_does_not_bypass_cooldown(self):
        self.artifact("A", price_days=0)
        self.attempted("A", 2)
        self.assertEqual(self.select(["A"]), [])

    def test_migration_does_not_bypass_cooldown(self):
        self.artifact("A", days=0)
        self.attempted("A", 2)
        self.assertEqual(self.select(["A"]), [])

    def test_multiple_batches_visit_every_company_even_when_all_fetches_empty(self):
        symbols = [f"T{i:03d}" for i in range(17)]
        visited = []
        for _ in range(6):
            batch = self.select(symbols, 3)
            visited.extend(batch)
            for symbol in batch:
                self.attempted(symbol)
        self.assertEqual(sorted(visited), symbols)
        self.assertEqual(len(visited), len(set(visited)))
        self.assertEqual(self.select(symbols, 3), [])

    def test_equal_age_uses_missing_coverage_then_symbol(self):
        self.artifact("AAA")
        self.assertEqual(self.select(["ZZZ", "AAA", "BBB"]), ["BBB", "ZZZ", "AAA"])

    def test_six_hour_boundary_is_eligible(self):
        self.attempted("A", 6)
        self.assertEqual(self.select(["A"]), ["A"])

    def test_recent_artifact_is_fallback_when_attempt_is_absent(self):
        self.artifact("A", updated=self.now.isoformat())
        self.assertEqual(self.select(["A"]), [])

    def test_attempt_takes_precedence_over_old_artifact(self):
        self.artifact("A", updated=(self.now - timedelta(days=20)).isoformat())
        self.attempted("A", 1)
        self.assertEqual(self.select(["A"]), [])

    def test_naive_and_z_timestamps(self):
        self.attempts = {"A": {"last_attempt_utc": "2026-10-02T17:00:00"}, "B": {"last_attempt_utc": "2026-10-02T17:00:00Z"}}
        self.assertEqual(self.select(["A", "B"]), [])

    def test_invalid_and_future_timestamps_cannot_starve_company(self):
        self.attempts = {"A": {"last_attempt_utc": "broken"}, "B": {"last_attempt_utc": "2099-01-01T00:00:00Z"}}
        self.assertEqual(self.select(["B", "A"]), ["A", "B"])

    def test_corrupt_artifact_is_due_without_mutation(self):
        path = self.news / "A.json"
        path.write_text("{broken", encoding="utf-8")
        self.assertEqual(self.select(["A"]), ["A"])
        self.assertEqual(path.read_text(), "{broken")

    def test_bad_metadata_does_not_crash(self):
        self.artifact("A", days="not-an-integer")
        self.attempts["A"] = []
        self.assertEqual(self.select(["A"]), ["A"])

    def test_duplicates_and_unsafe_symbols_are_ignored(self):
        self.assertEqual(self.select(["", "..", "../A", "A", "A"]), ["A"])

    def test_non_positive_limits_are_rejected(self):
        with self.assertRaises(ValueError):
            self.select(["A"], 0)
        with self.assertRaises(ValueError):
            self.select(["A"], refresh_interval=timedelta(0))

    def test_empty_universe(self):
        self.assertEqual(self.select([]), [])

    def test_production_priority_entrypoint_calls_fair_scheduler(self):
        path = ROOT / "src/market_sentiment/cli/fulfill_company_data_priority.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        target = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "target_rows")
        imports = [node for node in tree.body if isinstance(node, ast.ImportFrom) and node.module == "market_sentiment.news_schedule"]
        self.assertTrue(any(alias.name == "select_company_targets" for node in imports for alias in node.names))
        namespace = {"Path": Path, "Any": object, "select_company_targets": scheduler.select_company_targets,
                     "NEWS_DEPTH_TARGET": 360, "NEWS_HISTORY_DAYS_TARGET": 1095}
        # Exercise the actual production wrapper without importing network/model packages.
        exec(compile(ast.Module(body=[target], type_ignores=[]), str(path), "exec", flags=__import__('__future__').annotations.compiler_flag), namespace)
        self.assertEqual(namespace["target_rows"]([{"ticker": "A"}], self.news, self.history, {}, 1), [{"ticker": "A"}])


if __name__ == "__main__":
    unittest.main()
