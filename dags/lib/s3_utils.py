"""
s3_utils.py — S3 I/O helpers (boto3 + LocalStack)

Naming convention enforced by s3_key():
    layer/group/entity/YYYYMMDD/filename
    e.g. raw/steam/TopGames/20240115/extract.json
         formatted/yahoo/GamingStocks/20240115/data.snappy.parquet
         usage/correlation/GameStockCorrelation/20240115/result.snappy.parquet
"""

import io
import json
import logging
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Client factory ────────────────────────────────────────────────────────────

def get_s3_client():
    """Return a boto3 S3 client pointed at LocalStack (or real AWS via env)."""
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def get_bucket() -> str:
    return os.getenv("S3_BUCKET", "datalake")


# ── Key naming convention ──────────────────────────────────────────────────────

def s3_key(layer: str, group: str, entity: str, date_str: str, filename: str) -> str:
    """
    Build an S3 key following the project naming convention.

    Args:
        layer:    'raw' | 'formatted' | 'usage'
        group:    e.g. 'steam', 'twitch', 'yahoo', 'correlation'
        entity:   e.g. 'TopGames', 'GamingStocks', 'GameStockCorrelation'
        date_str: 'YYYYMMDD'
        filename: e.g. 'extract.json', 'data.snappy.parquet'

    Returns:
        'raw/steam/TopGames/20240115/extract.json'
    """
    return f"{layer}/{group}/{entity}/{date_str}/{filename}"


# ── JSON helpers ──────────────────────────────────────────────────────────────

def upload_json(data: dict | list, key: str, bucket: str | None = None) -> None:
    """Serialize *data* to JSON and upload to S3."""
    bucket = bucket or get_bucket()
    s3 = get_s3_client()
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    logger.info("Uploaded JSON to s3://%s/%s (%d bytes)", bucket, key, len(body))


def download_json(key: str, bucket: str | None = None) -> dict | list:
    """Download and deserialize a JSON object from S3."""
    bucket = bucket or get_bucket()
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    logger.info("Downloaded JSON from s3://%s/%s", bucket, key)
    return data


# ── Parquet helpers ───────────────────────────────────────────────────────────

def upload_parquet(df, key: str, bucket: str | None = None, compression: str = "snappy") -> None:
    """
    Upload a pandas DataFrame as Parquet to S3.

    Args:
        df:          pandas.DataFrame
        key:         S3 key (should end with .snappy.parquet)
        bucket:      S3 bucket name (defaults to env S3_BUCKET)
        compression: 'snappy' (default) | 'gzip' | 'none'
    """
    bucket = bucket or get_bucket()
    s3 = get_s3_client()

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression=compression)
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(
        "Uploaded Parquet to s3://%s/%s (%d rows, %d bytes)",
        bucket,
        key,
        len(df),
        buf.tell(),
    )


def download_parquet(key: str, bucket: str | None = None):
    """Download a Parquet file from S3 and return a pandas DataFrame."""
    import pandas as pd

    bucket = bucket or get_bucket()
    s3 = get_s3_client()

    response = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(response["Body"].read())
    df = pd.read_parquet(buf)
    logger.info("Downloaded Parquet from s3://%s/%s (%d rows)", bucket, key, len(df))
    return df


def list_keys(prefix: str, bucket: str | None = None) -> list[str]:
    """List all keys under a given S3 prefix."""
    bucket = bucket or get_bucket()
    s3 = get_s3_client()

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys
