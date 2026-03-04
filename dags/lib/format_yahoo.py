"""
format_yahoo.py — Transform raw Yahoo Finance JSON → Snappy Parquet (Spark)

Reads:  raw/yahoo/GamingStocks/YYYYMMDD/extract.json
Writes: formatted/yahoo/GamingStocks/YYYYMMDD/data.snappy.parquet

Always writes the output Parquet even if no records exist (empty dataset).
"""

import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from lib.s3_utils import download_json, get_bucket, s3_key, upload_parquet

load_dotenv()
logger = logging.getLogger(__name__)

# Raw input schema
YAHOO_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), True),
        StructField("date", StringType(), True),   # will be cast to date later
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ]
)

# Output schema (raw + KPIs). We'll enforce these columns even if empty.
OUTPUT_COLS = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "daily_change_pct",
    "daily_range",
    "daily_range_pct",
]


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("FormatYahoo")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def format_yahoo(**kwargs) -> dict:
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    if hasattr(execution_date, "strftime"):
        date_str = execution_date.strftime("%Y%m%d")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    logger.info("Formatting Yahoo Finance data for date %s", date_str)

    raw_key = s3_key("raw", "yahoo", "GamingStocks", date_str, "extract.json")
    raw_data = download_json(raw_key)
    records = raw_data.get("records", [])
    logger.info("Loaded %d raw stock records from S3", len(records))

    spark = _get_spark()

    # ── Create DataFrame (even if empty) ─────────────────────────────────────
    if records:
        df = spark.createDataFrame(records, schema=YAHOO_SCHEMA)
    else:
        # Empty DF with the raw schema
        df = spark.createDataFrame([], schema=YAHOO_SCHEMA)

    # ── Transform (safe for empty DF) ────────────────────────────────────────
    df = (
        df
        # Parse date string -> date (Spark DateType) in UTC
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        # Coalesce nulls (keeps schema consistent)
        .withColumn("open", F.coalesce(F.col("open"), F.lit(0.0)))
        .withColumn("high", F.coalesce(F.col("high"), F.lit(0.0)))
        .withColumn("low", F.coalesce(F.col("low"), F.lit(0.0)))
        .withColumn("close", F.coalesce(F.col("close"), F.lit(0.0)))
        .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
        # KPI: daily % change
        .withColumn(
            "daily_change_pct",
            F.when(
                F.col("open") > 0,
                F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        # KPI: absolute range
        .withColumn("daily_range", F.round(F.col("high") - F.col("low"), 4))
        # KPI: range as % of open
        .withColumn(
            "daily_range_pct",
            F.when(
                F.col("open") > 0,
                F.round(F.col("daily_range") / F.col("open") * 100, 4),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .select(*OUTPUT_COLS)  # enforce column order + presence
        .orderBy("ticker", "date")
    )

    row_count = df.count()
    logger.info("Transformed %d rows", row_count)

    # ── Upload Parquet (even if empty) ───────────────────────────────────────
    pandas_df = df.toPandas()
    out_key = s3_key("formatted", "yahoo", "GamingStocks", date_str, "data.snappy.parquet")
    upload_parquet(pandas_df, out_key)

    logger.info(
        "Yahoo Finance format complete: %d rows → s3://%s/%s",
        row_count,
        get_bucket(),
        out_key,
    )

    spark.stop()
    return {"date": date_str, "row_count": row_count, "s3_key": out_key}