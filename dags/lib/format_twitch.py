"""
format_twitch.py — Transform raw Twitch JSON → Snappy Parquet (Spark)

Reads: raw/twitch/TopGames/YYYYMMDD/extract.json
Writes: formatted/twitch/TopGames/YYYYMMDD/data.snappy.parquet
"""

import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dags.lib.s3_utils import download_json, get_bucket, s3_key, upload_parquet

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_SCHEMA = StructType(
    [
        StructField("rank", IntegerType(), True),
        StructField("game_id", StringType(), True),
        StructField("game_name", StringType(), True),
        StructField("box_art_url", StringType(), True),
        StructField("top_stream_viewers", LongType(), True),
        StructField("extracted_at", StringType(), True),
        StructField("date", StringType(), True),
    ]
)


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("FormatTwitch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def format_twitch(**kwargs) -> dict:
    """
    Read raw Twitch JSON from S3, apply transformations with Spark,
    write Snappy Parquet back to S3 (formatted layer).

    Returns:
        dict with 'date', 'row_count', 's3_key' metadata
    """
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    if hasattr(execution_date, "strftime"):
        date_str = execution_date.strftime("%Y%m%d")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    logger.info("Formatting Twitch data for date %s", date_str)

    # ── Download raw JSON from S3 ─────────────────────────────────────────────
    raw_key = s3_key("raw", "twitch", "TopGames", date_str, "extract.json")
    raw_data = download_json(raw_key)
    games = raw_data.get("games", [])
    logger.info("Loaded %d raw Twitch game records from S3", len(games))

    if not games:
        logger.warning("No Twitch games found in raw data — skipping format step")
        return {"date": date_str, "row_count": 0, "s3_key": None}

    # ── Spark transformation ──────────────────────────────────────────────────
    spark = _get_spark()

    df = spark.createDataFrame(games, schema=TWITCH_SCHEMA)

    df = (
        df
        # Normalise date to proper DateType in UTC
        .withColumn(
            "date",
            F.to_date(F.col("date"), "yyyyMMdd"),
        )
        # Normalise extracted_at to UTC timestamp
        .withColumn(
            "extracted_at",
            F.to_utc_timestamp(
                F.to_timestamp(F.col("extracted_at"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                "UTC",
            ),
        )
        # Coalesce nulls
        .withColumn(
            "top_stream_viewers",
            F.coalesce(F.col("top_stream_viewers"), F.lit(0)),
        )
        # Normalize viewer rank score (rank 1 = 100, rank 100 = 1)
        .withColumn(
            "viewer_rank_score",
            F.lit(101) - F.col("rank"),
        )
        # Log viewer count for normalization across games
        .withColumn(
            "log_viewers",
            F.when(F.col("top_stream_viewers") > 0, F.log(F.col("top_stream_viewers"))).otherwise(
                F.lit(0.0)
            ),
        )
        .orderBy("rank")
    )

    row_count = df.count()
    logger.info("Transformed %d rows", row_count)

    # ── Write Parquet and upload to S3 ────────────────────────────────────────
    pandas_df = df.toPandas()
    out_key = s3_key("formatted", "twitch", "TopGames", date_str, "data.snappy.parquet")
    upload_parquet(pandas_df, out_key)

    logger.info(
        "Twitch format complete: %d rows → s3://%s/%s",
        row_count,
        get_bucket(),
        out_key,
    )
    spark.stop()
    return {"date": date_str, "row_count": row_count, "s3_key": out_key}
