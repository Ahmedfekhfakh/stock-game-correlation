"""
index_to_elastic.py — Bulk index correlation results into Elasticsearch 8.x

- Deletes and recreates the index with explicit field mapping
- Deterministic _id = ticker_date_game (idempotent)
- Cleans NaN/Inf before indexing
- Uses elasticsearch-py helpers.bulk()
"""

import logging
import math
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

from dags.lib.s3_utils import download_parquet, s3_key

load_dotenv()

logger = logging.getLogger(__name__)

INDEX_NAME = "game-stock-correlation"

# Explicit Elasticsearch field mapping
INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
    "mappings": {
        "properties": {
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "game_name": {"type": "keyword"},
            "ticker": {"type": "keyword"},
            "steam_rank": {"type": "integer"},
            "steam_rank_score": {"type": "float"},
            "peak_ccu": {"type": "long"},
            "avg_playtime_2weeks": {"type": "long"},
            "review_ratio": {"type": "float"},
            "twitch_viewers": {"type": "long"},
            "twitch_rank": {"type": "integer"},
            "twitch_rank_score": {"type": "float"},
            "stock_open": {"type": "float"},
            "stock_close": {"type": "float"},
            "stock_high": {"type": "float"},
            "stock_low": {"type": "float"},
            "stock_volume": {"type": "long"},
            "daily_change_pct": {"type": "float"},
            "daily_range_pct": {"type": "float"},
            "popularity_score": {"type": "float"},
            "corr_7d_popularity_price": {"type": "float"},
            "corr_7d_viewers_price": {"type": "float"},
            "signal": {"type": "keyword"},
            "ml_prediction": {"type": "keyword"},
            "ml_confidence": {"type": "float"},
            "ml_accuracy":    {"type": "float"},
            "ml_f1_weighted": {"type": "float"},
            "ml_f1_macro":    {"type": "float"},
            "ml_precision":   {"type": "float"},
            "ml_recall":      {"type": "float"},
            "indexed_at": {"type": "date"},
        }
    },
}


def _get_es_client() -> Elasticsearch:
    host = os.getenv("ES_HOST", "http://localhost:9200")
    return Elasticsearch(hosts=[host], request_timeout=30)


def _clean(value):
    """Replace NaN, Inf, -Inf with None for ES compatibility."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    if isinstance(value, np.floating):
        if np.isnan(value) or np.isinf(value):
            return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (np.int64, np.int32)):
        return int(value)
    if isinstance(value, (np.float64, np.float32)):
        f = float(value)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _make_doc(row: dict, indexed_at: str) -> dict:
    """Build an ES document from a pandas row dict."""
    doc = {k: _clean(v) for k, v in row.items()}
    doc["indexed_at"] = indexed_at

    # Ensure date is a string (ES date type expects ISO format)
    if "date" in doc and hasattr(doc["date"], "isoformat"):
        doc["date"] = doc["date"].isoformat()
    elif "date" in doc and doc["date"] is not None:
        doc["date"] = str(doc["date"])

    return doc


def _doc_id(row: dict) -> str:
    """Deterministic document ID: ticker_YYYYMMDD_gamename."""
    ticker = str(row.get("ticker", "UNKNOWN")).replace(" ", "_")
    date = str(row.get("date", "")).replace("-", "")
    game = str(row.get("game_name", "")).replace(" ", "_")[:30]
    return f"{ticker}_{date}_{game}"


def index_to_elastic(**kwargs) -> dict:
    """
    Download correlation Parquet from S3 and bulk-index into Elasticsearch.

    Returns:
        dict with 'date', 'indexed_count', 'index_name' metadata
    """
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    if hasattr(execution_date, "strftime"):
        date_str = execution_date.strftime("%Y%m%d")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    logger.info("Indexing correlation data for date %s into ES", date_str)

    # ── Download Parquet from S3 ──────────────────────────────────────────────
    parquet_key = s3_key(
        "usage",
        "correlation",
        "GameStockCorrelation",
        date_str,
        "result.snappy.parquet",
    )
    pdf = download_parquet(parquet_key)
    logger.info("Loaded %d rows from Parquet for indexing", len(pdf))

    if pdf.empty:
        logger.warning("Empty dataframe — nothing to index")
        return {"date": date_str, "indexed_count": 0, "index_name": INDEX_NAME}

    # ── Elasticsearch client + index setup ────────────────────────────────────
    es = _get_es_client()

    # Delete + recreate index (idempotent)
    try:
        exists = es.indices.exists(index=INDEX_NAME)
    except Exception:
        exists = False

    if exists:
        es.indices.delete(index=INDEX_NAME)
        logger.info("Deleted existing index: %s", INDEX_NAME)

    es.indices.create(
        index=INDEX_NAME,
        settings=INDEX_MAPPING["settings"],
        mappings=INDEX_MAPPING["mappings"],
    )
    logger.info("Created index %s with explicit mapping", INDEX_NAME)

    # ── Bulk indexing ─────────────────────────────────────────────────────────
    indexed_at = datetime.now(timezone.utc).isoformat()
    records = pdf.to_dict(orient="records")

    actions = [
        {
            "_index": INDEX_NAME,
            "_id": _doc_id(row),
            "_source": _make_doc(row, indexed_at),
        }
        for row in records
    ]

    success, errors = es_helpers.bulk(
        es,
        actions,
        raise_on_error=False,
        chunk_size=500,
        request_timeout=60,
    )

    if errors:
        logger.warning("Bulk index had %d errors: %s", len(errors), errors[:5])

    # Refresh index so data is immediately searchable
    es.indices.refresh(index=INDEX_NAME)

    count = es.count(index=INDEX_NAME)["count"]
    logger.info(
        "Elasticsearch indexing complete: %d docs in '%s'",
        count,
        INDEX_NAME,
    )

    return {"date": date_str, "indexed_count": count, "index_name": INDEX_NAME}
