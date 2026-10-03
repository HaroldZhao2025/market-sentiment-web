"""Verify rendered HTML, not merely embedded JSON, and catch stale deployments."""
import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timezone
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("publication", ROOT / "scripts/verify_news_publication.py")
v = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v)
NOW = datetime(2026, 10, 3, tzinfo=timezone.utc)
BASE = "https://haroldzhao2025.github.io/market-sentiment-web/"


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)
        for symbol in v.SYMBOLS:
            for relative, payload in [(f"data/v5/news/{symbol}.json", {"articles": [{"ts": "2026-08-17T12:00:00Z", "title": "Old headline"}]}),
                                      (f"data/ticker/{symbol}.json", {"news": [{"ts": "2026-10-02T15:00:00Z", "title": f"{symbol} new & real headline"}]})]:
                path = self.out / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(payload))
            path = self.out / f"ticker/{symbol}/index.html"
            path.parent.mkdir(parents=True)
            path.write_text(f"<span>Latest article (UTC): 2026-10-02</span><h3>{escape(symbol + ' new & real headline')}</h3>")

    def prepare(self):
        return v.prepare(self.out, "123-1", "abcdef", now=NOW)

    def fetch(self, url):
        return (self.out / url.removeprefix(BASE).split("?")[0]).read_bytes()

    def test_current_core_beats_old_archive_and_live_html_is_checked(self):
        manifest = self.prepare()
        self.assertEqual(len(manifest["samples"]), 4)
        self.assertEqual(manifest["samples"][0]["latest_article_at_utc"][:10], "2026-10-02")
        self.assertEqual(v.verify(BASE, "123-1", attempts=1, fetch=self.fetch), manifest)

    def test_old_archive_page_fails_before_publication(self):
        (self.out / "ticker/MSFT/index.html").write_text("<h3>Old headline</h3>")
        with self.assertRaisesRegex(RuntimeError, "hides its latest"):
            self.prepare()

    def test_hydration_json_alone_is_not_visible_evidence(self):
        (self.out / "ticker/AAPL/index.html").write_text('<script>Latest article (UTC): 2026-10-02 AAPL new & real headline</script><h3>Old headline</h3>')
        with self.assertRaisesRegex(RuntimeError, "hides its latest"):
            self.prepare()

    def test_old_live_manifest_is_rejected(self):
        self.prepare()
        with self.assertRaisesRegex(RuntimeError, "another build"):
            v.verify(BASE, "different", attempts=1, fetch=self.fetch)

    def test_new_manifest_with_old_html_is_rejected(self):
        self.prepare()
        (self.out / "ticker/AAPL/index.html").write_text("old deployment")
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            v.verify(BASE, "123-1", attempts=1, fetch=self.fetch)

    def test_network_failures_do_not_count_as_success(self):
        def failing(url):
            raise OSError("network unavailable")
        with self.assertRaisesRegex(RuntimeError, "verification failed"):
            v.verify(BASE, "123-1", attempts=1, fetch=failing)

    def test_stale_inputs_are_rejected_even_if_export_was_just_built(self):
        for path in (self.out / "data/ticker").glob("*.json"):
            path.unlink()
        with self.assertRaisesRegex(RuntimeError, "stale"):
            self.prepare()

    def test_timezone_ordering_and_invalid_dates(self):
        actual = v.latest_article([{"news": [{"ts": "2026-10-02T18:00:00+08:00", "title": "Earlier"},
                                            {"ts": "2026-10-02T12:00:00Z", "title": "Later"},
                                            {"ts": "2026-02-30", "title": "Invalid"}]}], NOW)
        self.assertEqual(actual[1], "Later")

    def test_future_dates_and_invalid_destinations_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Future"):
            v.latest_article([{"news": [{"ts": "2099-01-01", "title": "Bad"}]}], NOW)
        with self.assertRaises(ValueError):
            v.verify("https://example.com", "123", attempts=1, fetch=self.fetch)

    def test_missing_live_samples_is_failure(self):
        manifest = self.prepare()
        manifest["samples"] = manifest["samples"][:1]
        (self.out / v.MANIFEST).write_text(json.dumps(manifest))
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            v.verify(BASE, "123-1", attempts=1, fetch=self.fetch)


if __name__ == "__main__":
    unittest.main()
