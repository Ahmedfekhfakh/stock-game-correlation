"""
combine_correlation.py — Spark SQL JOIN + 7-day Pearson + XGBoost ML

Goal:
  - Keep full final dataset (365 days if Yahoo has 365 days)
  - Train model only on last 30 days
"""

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
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

from dags.lib.s3_utils import download_parquet, get_bucket, s3_key, upload_parquet

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_TICKER_MAP = {
    "league of legends": "TCEHY",
    "valorant": "TCEHY",
    "teamfight tactics": "TCEHY",
    "fortnite": "TCEHY",
    "call of duty: warzone": "MSFT",
    "call of duty: modern warfare": "MSFT",
    "call of duty: warzone 2.0": "MSFT",
    "overwatch 2": "MSFT",
    "world of warcraft": "MSFT",
    "diablo iv": "MSFT",
    "hearthstone": "MSFT",
    "apex legends": "EA",
    "fc 25": "EA",
    "ea sports fc 25": "EA",
    "madden nfl 25": "EA",
    "roblox": "RBLX",
    "super smash bros. ultimate": "NTDOY",
    "pokemon": "NTDOY",
    "the legend of zelda": "NTDOY",
    "splatoon 3": "NTDOY",
    "god of war ragnarök": "SONY",
    "gran turismo 7": "SONY",
    "marvel's spider-man 2": "SONY",
    "free fire": "SE",
    "naraka: bladepoint": "NTES",
    "marvel rivals": "NTES",
    "rainbow six siege": "UBSFY",
    "nba 2k25": "TTWO",
}

SIGNAL_THRESHOLDS = {
    "STRONG_POSITIVE": 0.5,
    "WEAK_POSITIVE": 0.2,
    "NEUTRAL": -0.2,
    "WEAK_NEGATIVE": -0.5,
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
    try:
        if val is None:
            return None
        f = float(val)
        if np.isnan(f) or np.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _prep_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
        elif col == "date":
            df[col] = df[col].astype(str).fillna("")

    for col in df.select_dtypes(include="object").columns:
        if df[col].isnull().all():
            df[col] = ""

    num_cols = df.select_dtypes(include="number").columns
    if len(num_cols) > 0:
        df[num_cols] = df[num_cols].fillna(0)

    return df


def combine_correlation(**kwargs) -> dict:
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    if hasattr(execution_date, "strftime"):
        date_str = execution_date.strftime("%Y%m%d")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    logger.info("Running correlation analysis for date %s", date_str)

    steam_key = s3_key("formatted", "steam", "TopGames", date_str, "data.snappy.parquet")
    twitch_key = s3_key("formatted", "twitch", "TopGames", date_str, "data.snappy.parquet")
    yahoo_key = s3_key("formatted", "yahoo", "GamingStocks", date_str, "data.snappy.parquet")

    steam_df = download_parquet(steam_key)
    yahoo_df = download_parquet(yahoo_key)

    try:
        twitch_df = download_parquet(twitch_key)
        logger.info("Twitch data loaded: %d rows", len(twitch_df))
    except Exception as exc:
        logger.warning("Twitch Parquet not found (%s) — proceeding without Twitch data", exc)
        twitch_df = pd.DataFrame(
            columns=[
                "rank", "game_id", "game_name", "box_art_url",
                "top_stream_viewers", "extracted_at", "date",
                "viewer_rank_score", "log_viewers",
            ]
        )

    logger.info(
        "Loaded: Steam=%d rows, Twitch=%d rows, Yahoo=%d rows",
        len(steam_df),
        len(twitch_df),
        len(yahoo_df),
    )

    steam_df = _prep_df(steam_df)
    twitch_df = _prep_df(twitch_df)
    yahoo_df = _prep_df(yahoo_df)

    # Keep FULL Yahoo history
    yahoo_df["date"] = pd.to_datetime(yahoo_df["date"], errors="coerce")
    yahoo_df = yahoo_df.dropna(subset=["date"]).copy()

    logger.info(
        "Yahoo full history retained: %d rows from %s to %s",
        len(yahoo_df),
        yahoo_df["date"].min().strftime("%Y-%m-%d") if not yahoo_df.empty else "N/A",
        yahoo_df["date"].max().strftime("%Y-%m-%d") if not yahoo_df.empty else "N/A",
    )

    # Convert back to string for Spark/join logic
    yahoo_df["date"] = yahoo_df["date"].dt.strftime("%Y-%m-%d")

    # IMPORTANT: expand over FULL Yahoo history, not 30 days
    rng = np.random.default_rng(seed=42)
    yahoo_dates_full = yahoo_df[["ticker", "date"]].drop_duplicates()

    # ── Expand Steam over FULL Yahoo dates ───────────────────────────────────
    steam_expanded_rows = []
    for _, steam_row in steam_df.iterrows():
        ticker = steam_row["ticker"]
        ticker_dates = yahoo_dates_full[yahoo_dates_full["ticker"] == ticker]["date"].tolist()
        base_score = float(steam_row.get("rank_score", 50))

        for d in ticker_dates:
            row = steam_row.to_dict()
            row["date"] = d
            noise = float(rng.normal(0, base_score * 0.05))
            row["rank_score"] = max(1.0, round(base_score + noise, 2))
            steam_expanded_rows.append(row)

    if steam_expanded_rows:
        steam_df = pd.DataFrame(steam_expanded_rows)
        logger.info("Steam expanded to %d rows using FULL Yahoo history", len(steam_df))

    steam_df = _prep_df(steam_df)

    # ── Expand Twitch-only over FULL Yahoo dates ─────────────────────────────
    steam_game_names = set(steam_df["name"].str.lower()) if "name" in steam_df.columns else set()

    twitch_only_rows = []
    for _, tw_row in twitch_df.iterrows():
        game_name = str(tw_row.get("game_name", "")).strip()
        if not game_name:
            continue

        if game_name.lower() in steam_game_names:
            continue

        ticker = TWITCH_TICKER_MAP.get(game_name.lower())
        if not ticker:
            continue

        ticker_dates = yahoo_dates_full[yahoo_dates_full["ticker"] == ticker]["date"].tolist()
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
                "twitch_viewers": max(0, int(base_viewers + rng.normal(0, max(base_viewers * 0.08, 1)))),
                "twitch_rank": int(tw_row.get("rank", 999)),
                "twitch_rank_score": max(1.0, round(base_rank_score + noise, 2)),
            }
            twitch_only_rows.append(row)

    if twitch_only_rows:
        twitch_only_df = pd.DataFrame(twitch_only_rows)
        logger.info("Twitch-only games expanded to %d rows using FULL Yahoo history", len(twitch_only_df))
    else:
        twitch_only_df = pd.DataFrame(
            columns=[
                "game_name", "ticker", "date",
                "twitch_viewers", "twitch_rank", "twitch_rank_score",
            ]
        )

    twitch_only_df = _prep_df(twitch_only_df)

    spark = _get_spark()

    from pyspark.sql.types import (
        DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    )

    TWITCH_SCHEMA = StructType([
        StructField("rank", IntegerType(), True),
        StructField("game_id", StringType(), True),
        StructField("game_name", StringType(), True),
        StructField("box_art_url", StringType(), True),
        StructField("top_stream_viewers", LongType(), True),
        StructField("extracted_at", StringType(), True),
        StructField("date", StringType(), True),
        StructField("viewer_rank_score", LongType(), True),
        StructField("log_viewers", DoubleType(), True),
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

    joined_sdf = spark.sql(
        """
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
            AND s.date = y.date
        LEFT JOIN twitch t
            ON LOWER(s.name) = LOWER(t.game_name)
        WHERE s.ticker != 'UNKNOWN'

        UNION ALL

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
            AND tw.date = y.date
        """
    )

    joined_sdf = joined_sdf.withColumn(
        "popularity_score",
        F.when(
            F.col("source") == "twitch",
            F.round(F.col("twitch_rank_score").cast("double"), 4),
        ).otherwise(
            F.round(F.col("steam_rank_score") * 0.6 + F.col("twitch_rank_score") * 0.4, 4)
        )
    )

    w7 = (
        Window.partitionBy("game_name", "ticker")
        .orderBy(F.to_date("date"))
        .rowsBetween(-6, 0)
    )

    joined_sdf = (
        joined_sdf
        .withColumn(
            "corr_7d_popularity_price",
            F.round(F.corr("popularity_score", "daily_change_pct").over(w7), 6),
        )
        .withColumn(
            "corr_7d_viewers_price",
            F.round(F.corr("twitch_viewers", "daily_change_pct").over(w7), 6),
        )
    )

    # FULL final dataset
    pdf = joined_sdf.toPandas()
    logger.info("Final dataset total rows: %d", len(pdf))

    if pdf.empty:
        pdf["signal"] = "NEUTRAL"
        pdf["ml_prediction"] = "NEUTRAL"
        pdf["ml_confidence"] = 0.0
        pdf["ml_accuracy"] = None
        pdf["ml_f1_weighted"] = None
        pdf["ml_f1_macro"] = None
        pdf["ml_precision"] = None
        pdf["ml_recall"] = None
    else:
        pdf["signal"] = pdf["corr_7d_popularity_price"].apply(_signal_label)
        pdf["source_is_twitch"] = (pdf["source"] == "twitch").astype(int)

        # Separate training dataset ONLY here
        pdf["date"] = pd.to_datetime(pdf["date"], errors="coerce")
        pdf = pdf.dropna(subset=["date"]).copy()

        max_final_date = pdf["date"].max()
        train_cutoff = max_final_date - pd.Timedelta(days=29)
        train_pdf = pdf[pdf["date"] >= train_cutoff].copy()

        logger.info(
            "Full final dataset: %d rows | Training dataset last 30 days: %d rows | %s to %s",
            len(pdf),
            len(train_pdf),
            train_cutoff.strftime("%Y-%m-%d"),
            max_final_date.strftime("%Y-%m-%d"),
        )

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

        if len(train_pdf) < 10:
            logger.warning("Too few rows for ML in last 30 days (%d)", len(train_pdf))
            pdf["ml_prediction"] = "NEUTRAL"
            pdf["ml_confidence"] = 0.0
            pdf["ml_accuracy"] = None
            pdf["ml_f1_weighted"] = None
            pdf["ml_f1_macro"] = None
            pdf["ml_precision"] = None
            pdf["ml_recall"] = None
        else:
            X_train = train_pdf[feature_cols].fillna(0).values

            le = LabelEncoder()
            y_train = le.fit_transform(train_pdf["signal"])
            class_names = le.classes_

            clf = XGBClassifier(
                n_estimators=150,
                learning_rate=0.1,
                max_depth=3,
                random_state=42,
                eval_metric="mlogloss",
                verbosity=0,
            )

            unique_classes, class_counts = np.unique(y_train, return_counts=True)
            min_class_count = int(class_counts.min())
            n_splits = min(5, min_class_count)

            if n_splits >= 2:
                cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
                for metric in (
                    "accuracy",
                    "f1_weighted",
                    "f1_macro",
                    "precision_weighted",
                    "recall_weighted",
                ):
                    scores = cross_val_score(clf, X_train, y_train, cv=cv, scoring=metric)
                    logger.info(
                        "CV %s: %.4f ± %.4f",
                        metric,
                        float(scores.mean()),
                        float(scores.std()),
                    )

            clf.fit(X_train, y_train)

            y_train_pred = clf.predict(X_train)

            acc = round(accuracy_score(y_train, y_train_pred), 4)
            f1_w = round(f1_score(y_train, y_train_pred, average="weighted", zero_division=0), 4)
            f1_m = round(f1_score(y_train, y_train_pred, average="macro", zero_division=0), 4)
            prec_w = round(precision_score(y_train, y_train_pred, average="weighted", zero_division=0), 4)
            rec_w = round(recall_score(y_train, y_train_pred, average="weighted", zero_division=0), 4)

            report = classification_report(
                y_train, y_train_pred, target_names=class_names, zero_division=0
            )
            cm = confusion_matrix(y_train, y_train_pred)

            logger.info("Training report:\n%s", report)
            logger.info("Training confusion matrix:\n%s", cm)

            X_all = pdf[feature_cols].fillna(0).values
            y_all_pred = clf.predict(X_all)
            proba_all = clf.predict_proba(X_all).max(axis=1)
            pred_labels_all = le.inverse_transform(y_all_pred)

            pdf["ml_prediction"] = pred_labels_all
            pdf["ml_confidence"] = np.round(proba_all, 4)
            pdf["ml_accuracy"] = acc
            pdf["ml_f1_weighted"] = f1_w
            pdf["ml_f1_macro"] = f1_m
            pdf["ml_precision"] = prec_w
            pdf["ml_recall"] = rec_w

        pdf["date"] = pdf["date"].dt.strftime("%Y-%m-%d")

    numeric_cols = pdf.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        pdf[col] = pdf[col].apply(_clean_numeric)

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