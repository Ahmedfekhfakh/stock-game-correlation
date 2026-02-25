"""
format_steamspy.py — Transform raw SteamSpy JSON → Snappy Parquet (Spark)

Reads: raw/steam/TopGames/YYYYMMDD/extract.json
Writes: formatted/steam/TopGames/YYYYMMDD/data.snappy.parquet
"""

import io
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dags.lib.s3_utils import download_json, get_bucket, s3_key, upload_parquet

load_dotenv()

logger = logging.getLogger(__name__)

STEAM_SCHEMA = StructType(
    [
        StructField("rank", IntegerType(), True),
        StructField("app_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("developer", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("positive", LongType(), True),
        StructField("negative", LongType(), True),
        StructField("owners", StringType(), True),
        StructField("average_forever", LongType(), True),
        StructField("average_2weeks", LongType(), True),
        StructField("peak_ccu", LongType(), True),
        StructField("price", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("extracted_at", StringType(), True),
        StructField("date", StringType(), True),
    ]
)


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("FormatSteamSpy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def format_steamspy(**kwargs) -> dict:
    """
    Read raw SteamSpy JSON from S3, apply transformations with Spark,
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

    logger.info("Formatting SteamSpy data for date %s", date_str)

    # ── Download raw JSON from S3 ─────────────────────────────────────────────
    raw_key = s3_key("raw", "steam", "TopGames", date_str, "extract.json")
    raw_data = download_json(raw_key)
    games = raw_data.get("games", [])
    logger.info("Loaded %d raw game records from S3", len(games))

    if not games:
        logger.warning("No games found in raw data — skipping format step")
        return {"date": date_str, "row_count": 0, "s3_key": None}

    # ── Spark transformation ──────────────────────────────────────────────────
    spark = _get_spark()

    df = spark.createDataFrame(games, schema=STEAM_SCHEMA)

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
        # Coalesce nulls with sensible defaults
        .withColumn("positive", F.coalesce(F.col("positive"), F.lit(0)))
        .withColumn("negative", F.coalesce(F.col("negative"), F.lit(0)))
        .withColumn("peak_ccu", F.coalesce(F.col("peak_ccu"), F.lit(0)))
        .withColumn("average_2weeks", F.coalesce(F.col("average_2weeks"), F.lit(0)))
        .withColumn("average_forever", F.coalesce(F.col("average_forever"), F.lit(0)))
        .withColumn("price", F.coalesce(F.col("price").cast(FloatType()), F.lit(0.0)))
        .withColumn("ticker", F.coalesce(F.col("ticker"), F.lit("UNKNOWN")))
        # Derived KPI: review ratio
        .withColumn(
            "review_ratio",
            F.when(
                (F.col("positive") + F.col("negative")) > 0,
                F.col("positive") / (F.col("positive") + F.col("negative")),
            ).otherwise(F.lit(None).cast(FloatType())),
        )
        # Normalize rank to score 0–100 (rank 1 = 100, rank 100 = 1)
        .withColumn(
            "rank_score",
            F.lit(101) - F.col("rank"),
        )
        .orderBy("rank")
    )

    row_count = df.count()
    logger.info("Transformed %d rows", row_count)

    # ── Write Parquet and upload to S3 ────────────────────────────────────────
    pandas_df = df.toPandas()
    out_key = s3_key("formatted", "steam", "TopGames", date_str, "data.snappy.parquet")
    upload_parquet(pandas_df, out_key)

    logger.info(
        "SteamSpy format complete: %d rows → s3://%s/%s",
        row_count,
        get_bucket(),
        out_key,
    )
    spark.stop()
    return {"date": date_str, "row_count": row_count, "s3_key": out_key}
