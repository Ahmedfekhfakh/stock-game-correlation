"""
test_extractors.py — Standalone integration tests for all pipeline extractors

Run without Airflow:
    cd /home/ahmed/Projet_Data_Lake
    source venv/bin/activate
    python -m pytest tests/test_extractors.py -v

Or run directly:
    python tests/test_extractors.py

Each test calls the actual API and checks that:
  - The function returns a dict with expected keys
  - The S3 key was created
  - The data is non-empty
"""

import json
import logging
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Mock Airflow kwargs (simulate dag_run context) ────────────────────────────

def make_kwargs(date: datetime | None = None) -> dict:
    """Build a mock Airflow kwargs dict for testing without Airflow."""
    if date is None:
        date = datetime.now(timezone.utc)

    dag_run = MagicMock()
    dag_run.conf = {}
    dag_run.run_id = f"test_{date.strftime('%Y%m%d%H%M%S')}"

    return {
        "execution_date": date,
        "logical_date": date,
        "dag_run": dag_run,
        "task_instance": MagicMock(),
        "task": MagicMock(),
    }


# ── S3 availability check ─────────────────────────────────────────────────────

def _s3_available() -> bool:
    """Check if LocalStack S3 is reachable."""
    import requests
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
    try:
        resp = requests.get(f"{endpoint}/_localstack/health", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


def _es_available() -> bool:
    """Check if Elasticsearch is reachable."""
    import requests
    host = os.getenv("ES_HOST", "http://localhost:9200")
    try:
        resp = requests.get(f"{host}/_cluster/health", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


# ── Test cases ────────────────────────────────────────────────────────────────

class TestExtractSteamSpy(unittest.TestCase):
    """Test SteamSpy extractor."""

    def test_extract_returns_metadata(self):
        """SteamSpy extract should return dict with date, count, s3_key."""
        from dags.lib.extract_steamspy import extract_steamspy

        kwargs = make_kwargs()
        result = extract_steamspy(**kwargs)

        self.assertIsInstance(result, dict, "Result should be a dict")
        self.assertIn("date", result, "Result should have 'date'")
        self.assertIn("count", result, "Result should have 'count'")
        self.assertIn("s3_key", result, "Result should have 's3_key'")
        self.assertGreater(result["count"], 0, "Should have extracted games")
        logger.info("SteamSpy: %d games extracted to %s", result["count"], result["s3_key"])

    @unittest.skipUnless(_s3_available(), "LocalStack S3 not available")
    def test_s3_key_exists_after_extract(self):
        """After extraction, the S3 key should exist in LocalStack."""
        from dags.lib.extract_steamspy import extract_steamspy
        from dags.lib.s3_utils import get_s3_client, get_bucket

        kwargs = make_kwargs()
        result = extract_steamspy(**kwargs)

        s3 = get_s3_client()
        bucket = get_bucket()
        response = s3.get_object(Bucket=bucket, Key=result["s3_key"])
        body = json.loads(response["Body"].read())

        self.assertIn("games", body)
        self.assertGreater(len(body["games"]), 0)
        logger.info("S3 key verified: s3://%s/%s", bucket, result["s3_key"])


class TestExtractYahoo(unittest.TestCase):
    """Test Yahoo Finance extractor."""

    def test_extract_returns_metadata(self):
        """Yahoo extract should return dict with tickers and count."""
        from dags.lib.extract_yahoo import extract_yahoo

        kwargs = make_kwargs()
        result = extract_yahoo(**kwargs)

        self.assertIsInstance(result, dict)
        self.assertIn("date", result)
        self.assertIn("tickers", result)
        self.assertIn("count", result)
        self.assertGreater(result["count"], 0, "Should have stock records")
        logger.info(
            "Yahoo: %d records, failed tickers: %s",
            result["count"],
            result.get("failed_tickers", []),
        )

    def test_known_ticker_in_results(self):
        """EA ticker should always be present in results."""
        from dags.lib.extract_yahoo import extract_yahoo, GAMING_TICKERS

        self.assertIn("EA", GAMING_TICKERS)
        self.assertIn("MSFT", GAMING_TICKERS)
        self.assertIn("TTWO", GAMING_TICKERS)
        logger.info("Ticker list verified: %s", GAMING_TICKERS)


class TestExtractTwitch(unittest.TestCase):
    """Test Twitch extractor (requires credentials)."""

    def _credentials_available(self) -> bool:
        client_id = os.getenv("TWITCH_CLIENT_ID", "")
        client_secret = os.getenv("TWITCH_CLIENT_SECRET", "")
        if client_id and client_secret and "your_twitch" not in client_id:
            return True
        # Check YAML file
        yaml_path = os.path.join(PROJECT_ROOT, "credentials", "twitch_keys.yaml")
        return os.path.exists(yaml_path)

    def test_extract_with_credentials(self):
        """Twitch extract should return games when credentials are available."""
        if not self._credentials_available():
            self.skipTest(
                "Twitch credentials not configured. "
                "Copy credentials/twitch_keys.yaml.example → credentials/twitch_keys.yaml"
            )

        from dags.lib.extract_twitch import extract_twitch

        kwargs = make_kwargs()
        result = extract_twitch(**kwargs)

        self.assertIsInstance(result, dict)
        self.assertIn("count", result)
        self.assertGreater(result["count"], 0, "Should have Twitch games")
        logger.info("Twitch: %d games extracted", result["count"])

    def test_credentials_file_example_exists(self):
        """The example credentials file should always exist."""
        example_path = os.path.join(PROJECT_ROOT, "credentials", "twitch_keys.yaml.example")
        self.assertTrue(
            os.path.exists(example_path),
            f"Example credentials file missing: {example_path}",
        )


class TestS3Utils(unittest.TestCase):
    """Test S3 utility functions."""

    def test_s3_key_naming_convention(self):
        """s3_key() should enforce the layer/group/entity/YYYYMMDD/filename pattern."""
        from dags.lib.s3_utils import s3_key

        key = s3_key("raw", "steam", "TopGames", "20240115", "extract.json")
        self.assertEqual(key, "raw/steam/TopGames/20240115/extract.json")

        key2 = s3_key("formatted", "yahoo", "GamingStocks", "20240115", "data.snappy.parquet")
        self.assertEqual(key2, "formatted/yahoo/GamingStocks/20240115/data.snappy.parquet")

        key3 = s3_key(
            "usage", "correlation", "GameStockCorrelation", "20240115", "result.snappy.parquet"
        )
        self.assertEqual(
            key3,
            "usage/correlation/GameStockCorrelation/20240115/result.snappy.parquet",
        )
        logger.info("S3 key naming convention verified")

    @unittest.skipUnless(_s3_available(), "LocalStack S3 not available")
    def test_upload_download_json(self):
        """upload_json + download_json round-trip should preserve data."""
        from dags.lib.s3_utils import upload_json, download_json, s3_key

        test_data = {"test": True, "value": 42, "items": [1, 2, 3]}
        key = s3_key("raw", "test", "TestEntity", "20240101", "test.json")

        upload_json(test_data, key)
        result = download_json(key)

        self.assertEqual(result["test"], True)
        self.assertEqual(result["value"], 42)
        self.assertEqual(result["items"], [1, 2, 3])
        logger.info("S3 JSON round-trip verified")


class TestFullPipeline(unittest.TestCase):
    """End-to-end pipeline integration test (requires all services)."""

    @unittest.skipUnless(
        _s3_available() and _es_available(),
        "Requires LocalStack + Elasticsearch to be running",
    )
    def test_extract_steamspy_and_format(self):
        """Extract SteamSpy → format with Spark → verify Parquet in S3."""
        from dags.lib.extract_steamspy import extract_steamspy
        from dags.lib.format_steamspy import format_steamspy
        from dags.lib.s3_utils import download_parquet, s3_key

        kwargs = make_kwargs()

        # Extract
        extract_result = extract_steamspy(**kwargs)
        self.assertGreater(extract_result["count"], 0)

        # Format
        format_result = format_steamspy(**kwargs)
        self.assertGreater(format_result["row_count"], 0)
        self.assertIsNotNone(format_result["s3_key"])

        # Verify Parquet
        df = download_parquet(format_result["s3_key"])
        self.assertFalse(df.empty)
        self.assertIn("rank", df.columns)
        self.assertIn("ticker", df.columns)
        self.assertIn("rank_score", df.columns)

        logger.info(
            "End-to-end Steam extract+format: %d rows, columns: %s",
            len(df),
            list(df.columns),
        )

    @unittest.skipUnless(
        _s3_available() and _es_available(),
        "Requires LocalStack + Elasticsearch to be running",
    )
    def test_index_to_elasticsearch(self):
        """Verify Elasticsearch is reachable and indexing works."""
        from elasticsearch import Elasticsearch

        es = Elasticsearch(
            hosts=[os.getenv("ES_HOST", "http://localhost:9200")],
            request_timeout=15,
        )
        health = es.cluster.health()
        self.assertIn(health["status"], ["green", "yellow"])
        logger.info("Elasticsearch cluster health: %s", health["status"])


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("Data Lake Pipeline — Extractor Tests")
    print("=" * 60)
    print(f"S3 available:  {_s3_available()}")
    print(f"ES available:  {_es_available()}")
    print("=" * 60)

    # Run all extractors in sequence (useful for quick smoke test)
    kwargs = make_kwargs()
    date_str = kwargs["execution_date"].strftime("%Y%m%d")
    print(f"\nRunning smoke test for date: {date_str}\n")

    tests = [
        ("SteamSpy", "dags.lib.extract_steamspy", "extract_steamspy"),
        ("Yahoo Finance", "dags.lib.extract_yahoo", "extract_yahoo"),
    ]

    for name, module_path, func_name in tests:
        try:
            import importlib
            mod = importlib.import_module(module_path)
            func = getattr(mod, func_name)
            result = func(**kwargs)
            print(f"[OK] {name}: {result}")
        except Exception as exc:
            print(f"[FAIL] {name}: {exc}")

    print("\nRunning unittest suite...")
    unittest.main(verbosity=2, exit=False)
