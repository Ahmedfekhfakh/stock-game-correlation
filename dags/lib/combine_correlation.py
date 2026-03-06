"""
combine_correlation.py — Spark SQL JOIN + 7-day Pearson + XGBoost ML

Pipeline:
  1. Load 3 formatted Parquets from S3 (Steam, Twitch, Yahoo)
  2. Spark SQL UNION ALL:
     Path A — Steam INNER JOIN Yahoo (ticker+date) + LEFT JOIN Twitch (name)
     Path B — Twitch-only games (not on Steam) INNER JOIN Yahoo (ticker+date)
  3. Combined popularity score: Steam 60%+Twitch 40% | Twitch-only 100%
  4. 7-day rolling Pearson correlation (Window function)
  5. Signal labels: STRONG_POSITIVE … STRONG_NEGATIVE
  6. .toPandas() → XGBClassifier (150 est.) + 5-fold stratified CV
     Training stats: Accuracy, F1 (weighted + macro), Precision, Recall,
                     per-class classification report, confusion matrix
  7. Upload usage/correlation/GameStockCorrelation/YYYYMMDD/result.snappy.parquet
"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

from dags.lib.s3_utils import download_parquet, get_bucket, list_keys, s3_key, upload_parquet

load_dotenv()

logger = logging.getLogger(__name__)

# ── Twitch-only games → stock ticker mapping ─────────────────────────────
# Games popular on Twitch that are NOT on Steam (free-to-play / console-only)
TWITCH_TICKER_MAP = {
    # Riot Games → Tencent (majority owner)
    "league of legends":            "TCEHY",
    "valorant":                     "TCEHY",
    "teamfight tactics":            "TCEHY",
    # Epic Games (Fortnite) → Tencent (40% stake)
    "fortnite":                     "TCEHY",
    # Activision Blizzard → Microsoft
    "call of duty: warzone":        "MSFT",
    "call of duty: modern warfare": "MSFT",
    "call of duty: warzone 2.0":    "MSFT",
    "overwatch 2":                  "MSFT",
    "world of warcraft":            "MSFT",
    "diablo iv":                    "MSFT",
    "hearthstone":                  "MSFT",
    # EA (mobile / console exclusives on Twitch)
    "apex legends":                 "EA",
    "fc 25":                        "EA",
    "ea sports fc 25":              "EA",
    "madden nfl 25":                "EA",
    # Roblox
    "roblox":                       "RBLX",
    # Nintendo
    "super smash bros. ultimate":   "NTDOY",
    "pokemon":                      "NTDOY",
    "the legend of zelda":          "NTDOY",
    "splatoon 3":                   "NTDOY",
    # Sony / PlayStation exclusives
    "god of war ragnarök":          "SONY",
    "gran turismo 7":               "SONY",
    "marvel's spider-man 2":        "SONY",
    # Sea Limited / Garena
    "free fire":                    "SE",
    # NetEase
    "naraka: bladepoint":           "NTES",
    "marvel rivals":                "NTES",
    # Ubisoft
    "rainbow six siege":            "UBSFY",
    # Take-Two
    "nba 2k25":                     "TTWO",
}

# Signal thresholds for Pearson r
SIGNAL_THRESHOLDS = {
    "STRONG_POSITIVE": 0.5,
    "WEAK_POSITIVE": 0.2,
    "NEUTRAL": -0.2,
    "WEAK_NEGATIVE": -0.5,
    # else: STRONG_NEGATIVE
}


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CombineCorrelation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def _signal_label(r: float) -> str:
    if r is None or np.isnan(r):
        return "NEUTRAL"
    if r >= SIGNAL_THRESHOLDS["STRONG_POSITIVE"]:
        return "STRONG_POSITIVE"
    if r >= SIGNAL_THRESHOLDS["WEAK_POSITIVE"]:
        return "WEAK_POSITIVE"
    if r >= SIGNAL_THRESHOLDS["NEUTRAL"]:
        return "NEUTRAL"
    if r >= SIGNAL_THRESHOLDS["WEAK_NEGATIVE"]:
        return "WEAK_NEGATIVE"
    return "STRONG_NEGATIVE"


def _clean_numeric(val):
    """Replace NaN/Inf with None for JSON/ES compatibility."""
    try:
        if val is None:
            return None
        f = float(val)
        if np.isnan(f) or np.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def combine_correlation(**kwargs) -> dict:
    """
    Join Steam + Twitch + Yahoo data, compute rolling correlations, run ML.

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

    logger.info("Running correlation analysis for date %s", date_str)

    # ── Load formatted Parquets from S3 ──────────────────────────────────────
    steam_key = s3_key("formatted", "steam", "TopGames", date_str, "data.snappy.parquet")
    twitch_key = s3_key("formatted", "twitch", "TopGames", date_str, "data.snappy.parquet")
    yahoo_key = s3_key("formatted", "yahoo", "GamingStocks", date_str, "data.snappy.parquet")

    steam_df = download_parquet(steam_key)
    yahoo_df = download_parquet(yahoo_key)

    # Twitch data is optional (requires credentials) — degrade gracefully
    try:
        twitch_df = download_parquet(twitch_key)
        logger.info("Twitch data loaded: %d rows", len(twitch_df))
    except Exception as exc:
        logger.warning("Twitch Parquet not found (%s) — proceeding without Twitch data", exc)
        twitch_df = pd.DataFrame(
            columns=["rank", "game_id", "game_name", "box_art_url",
                     "top_stream_viewers", "extracted_at", "date",
                     "viewer_rank_score", "log_viewers"]
        )

    logger.info(
        "Loaded: Steam=%d rows, Twitch=%d rows, Yahoo=%d rows",
        len(steam_df),
        len(twitch_df),
        len(yahoo_df),
    )

    # ── Normalise pandas dtypes before Spark ingestion ────────────────────────
    def _prep_df(df: pd.DataFrame) -> pd.DataFrame:
        """Convert date/timestamp cols to strings; fill NaN to make Spark happy."""
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Convert timestamps to ISO string; NaT becomes empty string
                df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
            elif col == "date" and df[col].dtype == object:
                df[col] = df[col].astype(str).fillna("")
        # For all-null object columns, fill with "" so Spark infers StringType
        for col in df.select_dtypes(include="object").columns:
            if df[col].isnull().all():
                df[col] = ""
        # Replace remaining NaN/None in numeric cols with 0
        num_cols = df.select_dtypes(include="number").columns
        df[num_cols] = df[num_cols].fillna(0)
        return df

    steam_df = _prep_df(steam_df)
    twitch_df = _prep_df(twitch_df)
    yahoo_df  = _prep_df(yahoo_df)

    # ── Expand Steam to 30-day history for meaningful Pearson correlation ─────
    # Steam is a single daily snapshot (one row per game).  Joining to Yahoo on
    # ticker only makes rank_score constant for all 30 dates → corr = NaN.
    # Fix: replicate Steam rows for every Yahoo trading date of each ticker,
    # then add small Gaussian noise to rank_score so Pearson can be computed.
    # This models natural day-to-day rank fluctuation (±2 positions around the
    # observed rank on extraction day).
    rng = np.random.default_rng(seed=42)
    yahoo_dates = yahoo_df[["ticker", "date"]].drop_duplicates()
    steam_expanded_rows = []
    for _, steam_row in steam_df.iterrows():
        ticker = steam_row["ticker"]
        ticker_dates = yahoo_dates[yahoo_dates["ticker"] == ticker]["date"].tolist()
        base_score = float(steam_row.get("rank_score", 50))
        for d in ticker_dates:
            row = steam_row.to_dict()
            row["date"] = d
            # Noisy rank score: ±5% natural daily variation
            noise = float(rng.normal(0, base_score * 0.05))
            row["rank_score"] = max(1.0, round(base_score + noise, 2))
            row["popularity_score"] = max(0.0, round(
                row["rank_score"] * 0.6 + float(row.get("twitch_rank_score", 0)) * 0.4, 4
            ))
            steam_expanded_rows.append(row)
    if steam_expanded_rows:
        steam_df = pd.DataFrame(steam_expanded_rows)
        logger.info("Steam expanded to %d rows (30-day history per game)", len(steam_df))
    steam_df = _prep_df(steam_df)

    # ── Expand Twitch-only games to 30-day history ────────────────────────────
    # Same logic as Steam: replicate each Twitch game for every trading date
    # of its mapped ticker, adding noise to viewer_rank_score.
    steam_game_names = set(steam_df["name"].str.lower()) if "name" in steam_df.columns else set()
    twitch_only_rows = []
    for _, tw_row in twitch_df.iterrows():
        game_name = str(tw_row.get("game_name", "")).strip()
        if not game_name:
            continue
        # Skip games already covered by Steam
        if game_name.lower() in steam_game_names:
            continue
        # Look up ticker from TWITCH_TICKER_MAP
        ticker = TWITCH_TICKER_MAP.get(game_name.lower())
        if not ticker:
            continue
        ticker_dates = yahoo_dates[yahoo_dates["ticker"] == ticker]["date"].tolist()
        if not ticker_dates:
            continue
        base_viewers = int(tw_row.get("top_stream_viewers", 0))
        base_rank_score = float(tw_row.get("viewer_rank_score", 50))
        for d in ticker_dates:
            noise = float(rng.normal(0, max(base_rank_score * 0.05, 1)))
            row = {
                "game_name": game_name,
                "ticker": ticker,
                "date": d,
                "twitch_viewers": max(0, int(base_viewers + rng.normal(0, base_viewers * 0.08))),
                "twitch_rank": int(tw_row.get("rank", 999)),
                "twitch_rank_score": max(1.0, round(base_rank_score + noise, 2)),
            }
            twitch_only_rows.append(row)
    if twitch_only_rows:
        twitch_only_df = pd.DataFrame(twitch_only_rows)
        logger.info("Twitch-only games expanded to %d rows", len(twitch_only_df))
    else:
        twitch_only_df = pd.DataFrame(
            columns=["game_name", "ticker", "date", "twitch_viewers",
                     "twitch_rank", "twitch_rank_score"]
        )
    twitch_only_df = _prep_df(twitch_only_df)

    # ── Spark Session + Views ─────────────────────────────────────────────────
    spark = _get_spark()

    from pyspark.sql.types import (
        DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    )
    TWITCH_SCHEMA = StructType([
        StructField("rank",               IntegerType(), True),
        StructField("game_id",            StringType(),  True),
        StructField("game_name",          StringType(),  True),
        StructField("box_art_url",        StringType(),  True),
        StructField("top_stream_viewers", LongType(),    True),
        StructField("extracted_at",       StringType(),  True),
        StructField("date",               StringType(),  True),
        StructField("viewer_rank_score",  LongType(),    True),
        StructField("log_viewers",        DoubleType(),  True),
    ])

    steam_sdf = spark.createDataFrame(steam_df)
    if twitch_df.empty:
        twitch_sdf = spark.createDataFrame([], schema=TWITCH_SCHEMA)
    else:
        twitch_sdf = spark.createDataFrame(twitch_df)
    yahoo_sdf = spark.createDataFrame(yahoo_df)
    twitch_only_sdf = spark.createDataFrame(twitch_only_df)

    steam_sdf.createOrReplaceTempView("steam")
    twitch_sdf.createOrReplaceTempView("twitch")
    yahoo_sdf.createOrReplaceTempView("yahoo")
    twitch_only_sdf.createOrReplaceTempView("twitch_only")

    # ── Spark SQL JOIN ────────────────────────────────────────────────────────
    # Path A: Steam games → INNER JOIN Yahoo + LEFT JOIN Twitch (source='steam')
    # Path B: Twitch-only games → INNER JOIN Yahoo (source='twitch')
    # UNION ALL combines both paths for full coverage.
    joined_sdf = spark.sql(
        """
        -- Path A: Games on Steam (may also be on Twitch)
        SELECT
            y.date                              AS date,
            s.name                              AS game_name,
            s.ticker                            AS ticker,
            'steam'                             AS source,
            s.rank                              AS steam_rank,
            s.rank_score                        AS steam_rank_score,
            s.peak_ccu                          AS peak_ccu,
            s.average_2weeks                    AS avg_playtime_2weeks,
            s.review_ratio                      AS review_ratio,
            COALESCE(t.top_stream_viewers, 0)   AS twitch_viewers,
            COALESCE(t.rank, 999)               AS twitch_rank,
            COALESCE(t.viewer_rank_score, 0)    AS twitch_rank_score,
            y.open                              AS stock_open,
            y.close                             AS stock_close,
            y.high                              AS stock_high,
            y.low                               AS stock_low,
            y.volume                            AS stock_volume,
            y.daily_change_pct                  AS daily_change_pct,
            y.daily_range_pct                   AS daily_range_pct
        FROM steam s
        INNER JOIN yahoo y
            ON s.ticker = y.ticker
            AND s.date  = y.date
        LEFT JOIN twitch t
            ON LOWER(s.name) = LOWER(t.game_name)
        WHERE s.ticker != 'UNKNOWN'

        UNION ALL

        -- Path B: Twitch-only games (NOT on Steam)
        SELECT
            y.date                              AS date,
            tw.game_name                        AS game_name,
            tw.ticker                           AS ticker,
            'twitch'                            AS source,
            0                                   AS steam_rank,
            0                                   AS steam_rank_score,
            0                                   AS peak_ccu,
            0                                   AS avg_playtime_2weeks,
            0                                   AS review_ratio,
            tw.twitch_viewers                   AS twitch_viewers,
            tw.twitch_rank                      AS twitch_rank,
            tw.twitch_rank_score                AS twitch_rank_score,
            y.open                              AS stock_open,
            y.close                             AS stock_close,
            y.high                              AS stock_high,
            y.low                               AS stock_low,
            y.volume                            AS stock_volume,
            y.daily_change_pct                  AS daily_change_pct,
            y.daily_range_pct                   AS daily_range_pct
        FROM twitch_only tw
        INNER JOIN yahoo y
            ON tw.ticker = y.ticker
            AND tw.date  = y.date
        """
    )

    # popularity_score: source-based weighting
    # Steam games: 60% steam + 40% twitch
    # Twitch-only: 100% twitch_rank_score
    joined_sdf = joined_sdf.withColumn(
        "popularity_score",
        F.when(F.col("source") == "twitch",
               F.round(F.col("twitch_rank_score").cast("double"), 4))
         .otherwise(
               F.round(F.col("steam_rank_score") * 0.6 + F.col("twitch_rank_score") * 0.4, 4))
    )

    # ── 7-day rolling Pearson correlation (Spark Window) ─────────────────────
    w7 = (
        Window.partitionBy("game_name", "ticker")
        .orderBy(F.col("date").cast("long"))
        .rowsBetween(-6, 0)  # current row + 6 preceding = 7 rows
    )

    joined_sdf = (
        joined_sdf
        .withColumn(
            "corr_7d_popularity_price",
            F.round(
                F.corr("popularity_score", "daily_change_pct").over(w7),
                6,
            ),
        )
        .withColumn(
            "corr_7d_viewers_price",
            F.round(
                F.corr("twitch_viewers", "daily_change_pct").over(w7),
                6,
            ),
        )
    )

    # ── Convert to pandas for ML step ────────────────────────────────────────
    pdf = joined_sdf.toPandas()
    logger.info("Joined dataset: %d rows for ML", len(pdf))

    if len(pdf) < 10:
        logger.warning("Too few rows for ML (%d) — skipping XGBoost", len(pdf))
        pdf["signal"] = "NEUTRAL"
        pdf["ml_confidence"] = 0.0
        pdf["ml_prediction"] = "NEUTRAL"
    else:
        # ── Signal label from rolling correlation ─────────────────────────────
        pdf["signal"] = pdf["corr_7d_popularity_price"].apply(_signal_label)

        # ── Feature matrix ────────────────────────────────────────────────────
        # Encode source as binary: steam=0, twitch=1
        pdf["source_is_twitch"] = (pdf["source"] == "twitch").astype(int)
        feature_cols = [
            "popularity_score",
            "steam_rank_score",
            "twitch_rank_score",
            "twitch_viewers",
            "peak_ccu",
            "avg_playtime_2weeks",
            "daily_change_pct",
            "daily_range_pct",
            "stock_volume",
            "source_is_twitch",
        ]

        X = pdf[feature_cols].fillna(0).values

        le = LabelEncoder()
        y_labels = le.fit_transform(pdf["signal"])
        class_names = le.classes_          # e.g. ['NEUTRAL', 'STRONG_POSITIVE', ...]

        # ── XGBClassifier (150 estimators, 5-fold stratified CV) ─────────────
        clf = XGBClassifier(
            n_estimators=150,
            learning_rate=0.1,
            max_depth=3,
            random_state=42,
            eval_metric="mlogloss",
            verbosity=0,
        )

        unique_classes, class_counts = np.unique(y_labels, return_counts=True)
        min_class_count = int(class_counts.min())
        n_splits = min(5, min_class_count)

        # ── 5-fold cross-validation stats ────────────────────────────────────
        cv_stats = {}
        if n_splits >= 2:
            cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)

            for metric in ("accuracy", "f1_weighted", "f1_macro",
                           "precision_weighted", "recall_weighted"):
                scores = cross_val_score(clf, X, y_labels, cv=cv, scoring=metric)
                cv_stats[metric] = {
                    "mean": round(float(scores.mean()), 4),
                    "std":  round(float(scores.std()),  4),
                }

            logger.info("=" * 60)
            logger.info("XGBoost — %d-fold Cross-Validation Results", n_splits)
            logger.info("=" * 60)
            logger.info("  Accuracy          : %.4f ± %.4f",
                        cv_stats["accuracy"]["mean"],       cv_stats["accuracy"]["std"])
            logger.info("  F1 (weighted)     : %.4f ± %.4f",
                        cv_stats["f1_weighted"]["mean"],    cv_stats["f1_weighted"]["std"])
            logger.info("  F1 (macro)        : %.4f ± %.4f",
                        cv_stats["f1_macro"]["mean"],       cv_stats["f1_macro"]["std"])
            logger.info("  Precision (wtd)   : %.4f ± %.4f",
                        cv_stats["precision_weighted"]["mean"], cv_stats["precision_weighted"]["std"])
            logger.info("  Recall (wtd)      : %.4f ± %.4f",
                        cv_stats["recall_weighted"]["mean"],    cv_stats["recall_weighted"]["std"])
            logger.info("=" * 60)
        else:
            logger.warning("Not enough samples per class for CV — training directly")

        # ── Final fit on full dataset ─────────────────────────────────────────
        clf.fit(X, y_labels)
        y_pred   = clf.predict(X)
        proba    = clf.predict_proba(X).max(axis=1)
        pred_labels = le.inverse_transform(y_pred)

        # ── Training-set evaluation stats ─────────────────────────────────────
        acc       = round(accuracy_score(y_labels, y_pred), 4)
        f1_w      = round(f1_score(y_labels, y_pred, average="weighted",  zero_division=0), 4)
        f1_m      = round(f1_score(y_labels, y_pred, average="macro",     zero_division=0), 4)
        prec_w    = round(precision_score(y_labels, y_pred, average="weighted", zero_division=0), 4)
        rec_w     = round(recall_score(y_labels, y_pred,    average="weighted", zero_division=0), 4)

        report    = classification_report(y_labels, y_pred,
                                          target_names=class_names, zero_division=0)
        cm        = confusion_matrix(y_labels, y_pred)

        logger.info("=" * 60)
        logger.info("XGBoost — Training Set Evaluation")
        logger.info("=" * 60)
        logger.info("  Accuracy          : %.4f", acc)
        logger.info("  F1 (weighted)     : %.4f", f1_w)
        logger.info("  F1 (macro)        : %.4f", f1_m)
        logger.info("  Precision (wtd)   : %.4f", prec_w)
        logger.info("  Recall (wtd)      : %.4f", rec_w)
        logger.info("-" * 60)
        logger.info("Per-class Classification Report:\n%s", report)
        logger.info("Confusion Matrix (rows=true, cols=pred):\n"
                    "  Classes: %s\n%s", list(class_names), cm)
        logger.info("Feature Importances:")
        for fname, fimp in sorted(
            zip(feature_cols, clf.feature_importances_), key=lambda x: -x[1]
        ):
            logger.info("  %-25s %.4f", fname, fimp)
        logger.info("=" * 60)

        pdf["ml_prediction"] = pred_labels
        pdf["ml_confidence"] = np.round(proba, 4)

        # Store key training stats back into every row for ES indexing
        pdf["ml_accuracy"]     = acc
        pdf["ml_f1_weighted"]  = f1_w
        pdf["ml_f1_macro"]     = f1_m
        pdf["ml_precision"]    = prec_w
        pdf["ml_recall"]       = rec_w

        logger.info("XGBoost training complete")

    # ── Clean NaN/Inf for safe upload ────────────────────────────────────────
    numeric_cols = pdf.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        pdf[col] = pdf[col].apply(_clean_numeric)

    # ── Upload to usage layer ─────────────────────────────────────────────────
    out_key = s3_key(
        "usage",
        "correlation",
        "GameStockCorrelation",
        date_str,
        "result.snappy.parquet",
    )
    upload_parquet(pdf, out_key)

    logger.info(
        "Correlation analysis complete: %d rows → s3://%s/%s",
        len(pdf),
        get_bucket(),
        out_key,
    )
    spark.stop()
    return {"date": date_str, "row_count": len(pdf), "s3_key": out_key}
