"""
realtime_stock_dag.py — Lightweight 5-minute stock price refresh

Fetches fast_info (latest price, market cap, etc.) for all gaming tickers
using yfinance and indexes directly to Elasticsearch `game-stock-live`.
No Spark required — pure pandas + ES bulk.

Schedule: every 5 minutes
"""

import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

# Ensure project root is on PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logger = logging.getLogger(__name__)

LIVE_INDEX = "game-stock-live"

GAMING_TICKERS = [
    "EA",
    "TTWO",
    "MSFT",
    "SONY",
    "NTDOY",
    "UBSFY",
    "RBLX",
    "OTGLY",     # CD Projekt
    "9697.T",    # Capcom
    "9684.T",    # Square Enix
    "7832.T",    # Bandai Namco
    "NTES",      # NetEase
    "TCEHY",     # Tencent
    "SE",        # Sea Limited
    "U",         # Unity Technologies
    "CRSR",      # Corsair Gaming
    "DKNG",      # DraftKings
]

LIVE_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s",
    },
    "mappings": {
        "properties": {
            "ticker": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "current_price": {"type": "float"},
            "previous_close": {"type": "float"},
            "change_pct": {"type": "float"},
            "market_cap": {"type": "long"},
            "fifty_two_week_high": {"type": "float"},
            "fifty_two_week_low": {"type": "float"},
            "currency": {"type": "keyword"},
            "exchange": {"type": "keyword"},
            "fetched_at": {"type": "date"},
        }
    },
}


def _get_es() -> Elasticsearch:
    host = os.getenv("ES_HOST", "http://localhost:9200")
    return Elasticsearch(hosts=[host], request_timeout=15)


def _clean(val):
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, (np.floating, np.integer)):
        v = val.item()
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    return val


def _ensure_live_index(es: Elasticsearch):
    """Create the live index if it does not exist (never delete — rolling data)."""
    if not es.indices.exists(index=LIVE_INDEX):
        es.indices.create(index=LIVE_INDEX, body=LIVE_INDEX_MAPPING)
        logger.info("Created live index: %s", LIVE_INDEX)


def refresh_stock_prices(**kwargs):
    """
    Fetch latest prices via yfinance fast_info and bulk-index to ES.
    Designed to run every 5 minutes.
    """
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat()

    logger.info("Refreshing live stock prices at %s", timestamp)

    es = _get_es()
    _ensure_live_index(es)

    actions = []
    for symbol in GAMING_TICKERS:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info

            current_price = _clean(getattr(info, "last_price", None))
            prev_close = _clean(getattr(info, "previous_close", None))
            market_cap = _clean(getattr(info, "market_cap", None))
            high_52w = _clean(getattr(info, "year_high", None))
            low_52w = _clean(getattr(info, "year_low", None))
            currency = getattr(info, "currency", "USD")
            exchange = getattr(info, "exchange", "")

            # Compute % change from previous close
            change_pct = None
            if current_price and prev_close and prev_close != 0:
                change_pct = round((current_price - prev_close) / prev_close * 100, 4)

            doc = {
                "ticker": symbol,
                "timestamp": timestamp,
                "current_price": current_price,
                "previous_close": prev_close,
                "change_pct": change_pct,
                "market_cap": int(market_cap) if market_cap else None,
                "fifty_two_week_high": high_52w,
                "fifty_two_week_low": low_52w,
                "currency": currency,
                "exchange": exchange,
                "fetched_at": timestamp,
            }

            # Deterministic ID: ticker + minute-precision timestamp
            doc_id = f"{symbol}_{now.strftime('%Y%m%d%H%M')}"

            actions.append(
                {
                    "_index": LIVE_INDEX,
                    "_id": doc_id,
                    "_source": doc,
                }
            )
            logger.debug("Fetched %s: price=%s change=%s%%", symbol, current_price, change_pct)

        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", symbol, exc)

    if actions:
        success, errors = es_helpers.bulk(
            es,
            actions,
            raise_on_error=False,
            chunk_size=100,
            request_timeout=30,
        )
        logger.info(
            "Live index updated: %d docs indexed, %d errors",
            success,
            len(errors) if errors else 0,
        )
    else:
        logger.warning("No stock data fetched — live index not updated")

    return {"timestamp": timestamp, "tickers": len(actions)}


# ── DAG definition ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-lake",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}

with DAG(
    dag_id="realtime_stock_refresh",
    description="5-minute live stock price refresh for gaming tickers → game-stock-live ES index",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["data-lake", "realtime", "finance"],
    doc_md="""
## Real-time Stock Price Refresh

Lightweight DAG running every 5 minutes.
Fetches latest prices for 8 gaming-sector tickers via yfinance `fast_info`
and indexes them to the `game-stock-live` Elasticsearch index.

No Spark — pure pandas + ES bulk for low latency.

### Access
- Live index: http://localhost:9200/game-stock-live/_search
- Kibana: http://localhost:5601 → game-stock-live data view
    """,
) as dag:

    PythonOperator(
        task_id="refresh_prices",
        python_callable=refresh_stock_prices,
    )
